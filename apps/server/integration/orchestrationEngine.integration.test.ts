// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import {
  ApprovalRequestId,
  CommandId,
  defaultInstanceIdForDriver,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  EnvironmentId,
  EventId,
  MessageId,
  OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  ModelSelection,
  ProviderInstanceId,
  type ServerProvider,
  type WorkflowIssueSummary,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import type { TestTurnResponse } from "./TestProviderAdapter.integration.ts";
import {
  gitRefExists,
  gitShowFileAtRef,
  makeOrchestrationIntegrationHarness,
  type OrchestrationIntegrationHarness,
} from "./OrchestrationEngineHarness.integration.ts";
import { checkpointRefForThreadTurn } from "../src/checkpointing/Utils.ts";
import {
  dispatchCreatedThreadTurnStart,
  reconcileCreatedThreadTurnStartFailure,
} from "../src/orchestration/dispatchCreatedThreadTurnStart.ts";
import * as ProjectionSnapshotQuery from "../src/orchestration/Services/ProjectionSnapshotQuery.ts";
import { makeSqlitePersistenceLive } from "../src/persistence/Layers/Sqlite.ts";
import * as ProviderRegistry from "../src/provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryMock } from "../src/provider/testUtils/providerRegistryMock.ts";
import * as ServerEnvironment from "../src/environment/ServerEnvironment.ts";
import * as GitHubCli from "../src/sourceControl/GitHubCli.ts";
import * as WorkflowService from "../src/workflow/WorkflowService.ts";
import * as WorkflowStartService from "../src/workflow/WorkflowStartService.ts";
import type {
  CheckpointDiffFinalizedReceipt,
  TurnProcessingQuiescedReceipt,
} from "../src/orchestration/Services/RuntimeReceiptBus.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";

const asMessageId = (value: string): MessageId => MessageId.make(value);
const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asApprovalRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);

const PROJECT_ID = asProjectId("project-1");
const THREAD_ID = ThreadId.make("thread-1");
const FIXTURE_TURN_ID = "fixture-turn";
const APPROVAL_REQUEST_ID = asApprovalRequestId("req-approval-1");
type IntegrationProvider = ProviderDriverKind;
const CODEX_PROVIDER = ProviderDriverKind.make("codex");
const CLAUDE_AGENT_PROVIDER = ProviderDriverKind.make("claudeAgent");

function nowIso() {
  return "2026-05-01T00:00:00.000Z";
}

class IntegrationWaitTimeoutError extends Schema.TaggedErrorClass<IntegrationWaitTimeoutError>()(
  "IntegrationWaitTimeoutError",
  {
    description: Schema.String,
  },
) {}

function waitForSync<A>(
  read: () => A,
  predicate: (value: A) => boolean,
  description: string,
  timeoutMs = 10_000,
): Effect.Effect<A, never> {
  return Effect.gen(function* () {
    const deadline = (yield* Clock.currentTimeMillis) + timeoutMs;

    while (true) {
      const value = read();
      if (predicate(value)) {
        return value;
      }
      if ((yield* Clock.currentTimeMillis) >= deadline) {
        return yield* Effect.die(new IntegrationWaitTimeoutError({ description }));
      }
      yield* Effect.sleep(10);
    }
  });
}

function runtimeBase(
  eventId: string,
  createdAt: string,
  provider: IntegrationProvider = CODEX_PROVIDER,
) {
  return {
    eventId: asEventId(eventId),
    provider,
    createdAt,
  };
}

function withHarness<A, E>(
  use: (harness: OrchestrationIntegrationHarness) => Effect.Effect<A, E>,
  provider: IntegrationProvider = CODEX_PROVIDER,
) {
  return Effect.acquireUseRelease(
    makeOrchestrationIntegrationHarness({ provider }),
    use,
    (harness) => harness.dispose,
  ).pipe(Effect.provide(NodeServices.layer));
}

function withRealCodexHarness<A, E>(
  use: (harness: OrchestrationIntegrationHarness) => Effect.Effect<A, E>,
) {
  return Effect.acquireUseRelease(
    makeOrchestrationIntegrationHarness({ provider: CODEX_PROVIDER, realCodex: true }),
    use,
    (harness) => harness.dispose,
  ).pipe(Effect.provide(NodeServices.layer));
}

const seedProjectAndThread = (harness: OrchestrationIntegrationHarness) =>
  Effect.gen(function* () {
    const createdAt = nowIso();
    const provider = harness.adapterHarness?.provider ?? CODEX_PROVIDER;
    const defaultModel = DEFAULT_MODEL_BY_PROVIDER[provider] ?? DEFAULT_MODEL;
    const instanceId = defaultInstanceIdForDriver(provider);

    yield* harness.engine.dispatch({
      type: "project.create",
      commandId: CommandId.make("cmd-project-create"),
      projectId: PROJECT_ID,
      title: "Integration Project",
      workspaceRoot: harness.workspaceDir,
      defaultModelSelection: {
        instanceId,
        model: defaultModel,
      },
      createdAt,
    });

    yield* harness.engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make("cmd-thread-create"),
      threadId: THREAD_ID,
      projectId: PROJECT_ID,
      title: "Integration Thread",
      modelSelection: {
        instanceId,
        model: defaultModel,
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      branch: null,
      worktreePath: harness.workspaceDir,
      createdAt,
    });
  });

const startTurn = (input: {
  readonly harness: OrchestrationIntegrationHarness;
  readonly commandId: string;
  readonly messageId: string;
  readonly text: string;
  readonly modelSelection?: ModelSelection;
  readonly createdAt?: string;
}) =>
  input.harness.engine.dispatch({
    type: "thread.turn.start",
    commandId: CommandId.make(input.commandId),
    threadId: THREAD_ID,
    message: {
      messageId: asMessageId(input.messageId),
      role: "user",
      text: input.text,
      attachments: [],
    },
    ...(input.modelSelection !== undefined
      ? {
          modelSelection: input.modelSelection,
        }
      : {}),
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    runtimeMode: "approval-required",
    createdAt: input.createdAt ?? nowIso(),
  });

it.live("starts workflow work once and reconciles real bootstrap failures", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      const modelSelection = {
        instanceId: defaultInstanceIdForDriver(CODEX_PROVIDER),
        model: DEFAULT_MODEL_BY_PROVIDER[CODEX_PROVIDER] ?? DEFAULT_MODEL,
        options: [{ id: "reasoningEffort", value: "high" }] as const,
      };
      yield* harness.engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("workflow-start-project"),
        projectId: PROJECT_ID,
        title: "Workflow project",
        workspaceRoot: harness.workspaceDir,
        defaultModelSelection: modelSelection,
        createdAt: nowIso(),
      });
      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("workflow-started", "2026-05-01T00:00:01.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "turn.completed",
            ...runtimeBase("workflow-completed", "2026-05-01T00:00:02.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
      });
      const repository = "Flow-Fly/t3code" as const;
      const workflowIssue = (
        number: number,
        kind: WorkflowIssueSummary["kind"],
        parentNumber: number | null,
        labels: ReadonlyArray<string>,
      ): WorkflowIssueSummary => ({
        id: `workflow-${number}`,
        repository,
        number,
        title: number === 15 ? "Choose the workflow launch contract" : `Workflow ${kind}`,
        url: `https://github.com/${repository}/issues/${number}`,
        kind,
        state: "open",
        stateReason: null,
        updatedAt: nowIso(),
        childCount: 1,
        parentNumber,
        labels: [...labels],
        readiness: { status: "ready", reasons: [] },
      });
      const root = workflowIssue(10, "container", null, ["workflow:container"]);
      const map = workflowIssue(12, "map", 10, ["wayfinder:map"]);
      const decisionFor = (number: number) =>
        workflowIssue(number, "decision", 12, ["wayfinder:research"]);
      const provider = {
        instanceId: modelSelection.instanceId,
        driver: CODEX_PROVIDER,
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: nowIso(),
        version: "1.0.0",
        models: [
          {
            slug: modelSelection.model,
            name: "Codex integration model",
            isCustom: false,
            capabilities: {
              optionDescriptors: [
                {
                  id: "reasoningEffort",
                  label: "Reasoning effort",
                  type: "select",
                  options: [{ id: "high", label: "High" }],
                },
              ],
            },
          },
        ],
        slashCommands: [],
        skills: ["wayfinder", "research", "to-spec"].map((name) => ({
          name,
          path: `/skills/${name}/SKILL.md`,
          enabled: true,
        })),
      } satisfies ServerProvider;
      const assignees = new Set<string>();
      const githubLayer = Layer.mock(GitHubCli.GitHubCli)({
        execute: ({ args }) =>
          Effect.sync(() => {
            let stdout = "";
            if (args[0] === "api") stdout = "Flow-Fly\n";
            else if (args.includes("--json")) stdout = [...assignees].join("\n");
            else {
              const addAt = args.indexOf("--add-assignee");
              if (addAt >= 0) assignees.add(args[addAt + 1]!);
              const removeAt = args.indexOf("--remove-assignee");
              if (removeAt >= 0) assignees.delete(args[removeAt + 1]!);
            }
            return {
              stdout,
              stderr: "",
              exitCode: ChildProcessSpawner.ExitCode(0),
              timedOut: false,
              stdoutTruncated: false,
              stderrTruncated: false,
              stdoutInvalidUtf8: false,
              stderrInvalidUtf8: false,
            };
          }),
      });
      const workflowLayer = Layer.mock(WorkflowService.WorkflowService)({
        children: ({ parentNumber }) =>
          Effect.succeed({
            parentNumber,
            children: [
              {
                ...decisionFor(15),
                state: "closed" as const,
                stateReason: "completed" as const,
                readiness: {
                  status: "resolved" as const,
                  reasons: [{ kind: "resolution" as const, message: "Resolved with evidence." }],
                },
              },
            ],
            frontier: {
              status: "complete" as const,
              message: "All visible work is resolved.",
              readyIssueIds: [],
            },
          }),
        issueDetail: ({ number }) =>
          Effect.succeed({
            ...(number === 10 ? root : number === 12 ? map : decisionFor(number)),
            body: "Decision context",
            blockedBy: [],
          }),
        locate: ({ number }) =>
          Effect.succeed({
            issue: decisionFor(number),
            ancestry: [root, map],
            ancestryComplete: true,
          }),
      });
      const startLayer = WorkflowStartService.layer.pipe(
        Layer.provide(workflowLayer),
        Layer.provide(
          Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, harness.snapshotQuery),
        ),
        Layer.provide(
          Layer.succeed(ProviderRegistry.ProviderRegistry, makeProviderRegistryMock([provider])),
        ),
        Layer.provide(githubLayer),
        Layer.provide(
          Layer.mock(ServerEnvironment.ServerEnvironment)({
            getEnvironmentId: Effect.succeed(EnvironmentId.make("workflow-environment")),
          }),
        ),
        Layer.provide(makeSqlitePersistenceLive(harness.dbPath)),
        Layer.provide(NodeServices.layer),
      );
      const makeDispatch = (
        label: string,
        mode: "success" | "reject-before-turn" | "lose-accepted-acknowledgement",
      ) => {
        let dispatchCount = 0;
        let threadId: ThreadId | null = null;
        const dispatch: Parameters<
          WorkflowStartService.WorkflowStartService["Service"]["start"]
        >[1] = (command) => {
          dispatchCount += 1;
          threadId = command.threadId;
          return dispatchCreatedThreadTurnStart({
            command: command as typeof command & {
              readonly bootstrap: {
                readonly createThread: NonNullable<
                  NonNullable<typeof command.bootstrap>["createThread"]
                >;
              };
            },
            createCommandId: Effect.succeed(CommandId.make(`${label}-create`)),
            dispatch: (bootstrapCommand) => {
              if (bootstrapCommand.type === "thread.turn.start" && mode === "reject-before-turn") {
                return Effect.fail(
                  new OrchestrationDispatchCommandError({
                    message: "The turn command was rejected before persistence.",
                  }),
                );
              }
              const dispatched = harness.engine.dispatch(bootstrapCommand).pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationDispatchCommandError({
                      message: "Workflow bootstrap dispatch failed.",
                      cause,
                    }),
                ),
              );
              return bootstrapCommand.type === "thread.turn.start" &&
                mode === "lose-accepted-acknowledgement"
                ? dispatched.pipe(
                    Effect.flatMap(() =>
                      Effect.fail(
                        new OrchestrationDispatchCommandError({
                          message: "The accepted turn acknowledgement was lost.",
                        }),
                      ),
                    ),
                  )
                : dispatched;
            },
            drainThreadDeletionThrough: () => Effect.void,
          }).pipe(
            Effect.catch((error) =>
              reconcileCreatedThreadTurnStartFailure({
                error,
                readTurnAcceptance: harness.commandReceiptRepository
                  .getByCommandId({ commandId: command.commandId })
                  .pipe(
                    Effect.map((receipt) =>
                      Option.isSome(receipt) && receipt.value.status === "accepted"
                        ? "accepted"
                        : "not-accepted",
                    ),
                    Effect.mapError(
                      (cause) =>
                        new OrchestrationDispatchCommandError({
                          message: "The first-turn receipt could not be read.",
                          cause,
                        }),
                    ),
                  ),
                cleanupCreatedThread: harness.engine
                  .dispatch({
                    type: "thread.delete",
                    commandId: CommandId.make(`${label}-delete`),
                    threadId: command.threadId,
                  })
                  .pipe(
                    Effect.as(true),
                    Effect.mapError(
                      (cause) =>
                        new OrchestrationDispatchCommandError({
                          message: "The empty bootstrap thread could not be deleted.",
                          cause,
                        }),
                    ),
                  ),
              }),
            ),
          );
        };
        return {
          dispatch,
          getDispatchCount: () => dispatchCount,
          getThreadId: () => threadId,
        };
      };
      const successfulDispatch = makeDispatch("workflow-success", "success");
      const results = yield* Effect.gen(function* () {
        const start = yield* WorkflowStartService.WorkflowStartService;
        return yield* Effect.all(
          [
            start.start(
              {
                projectId: PROJECT_ID,
                repository,
                rootNumber: 10,
                issueNumber: 15,
                modelSelection,
              },
              successfulDispatch.dispatch,
            ),
            start.start(
              {
                projectId: PROJECT_ID,
                repository,
                rootNumber: 10,
                issueNumber: 15,
                modelSelection,
              },
              successfulDispatch.dispatch,
            ),
          ],
          { concurrency: "unbounded" },
        );
      }).pipe(Effect.provide(startLayer));
      const threadId = results[0]!.threadId;
      yield* harness.waitForReceipt(
        (receipt): receipt is TurnProcessingQuiescedReceipt =>
          receipt.type === "turn.processing.quiesced" && receipt.threadId === threadId,
      );
      yield* harness.drainProviderRuntime;
      yield* harness.drainCheckpointReactor;

      const snapshot = yield* harness.snapshotQuery.getSnapshot();
      const thread = snapshot.threads.find((candidate) => candidate.id === threadId);
      assert.equal(thread?.title, "Choose the workflow launch contract");
      assert.match(thread?.messages[0]?.text ?? "", /Map: Flow-Fly\/t3code#12/u);
      assert.deepEqual(results.map((result) => result.disposition).sort(), ["existing", "started"]);
      assert.equal(new Set(results.map((result) => result.attemptId)).size, 1);
      assert.equal(successfulDispatch.getDispatchCount(), 1);
      assert.equal(harness.adapterHarness!.getStartCount(), 1);
      assert.deepEqual(harness.adapterHarness!.getTurnInputs()[0]?.skills, [
        { name: "wayfinder", path: "/skills/wayfinder/SKILL.md" },
        { name: "research", path: "/skills/research/SKILL.md" },
      ]);
      assert.deepEqual(harness.adapterHarness!.getTurnInputs()[0]?.modelSelection, modelSelection);

      const specificationTurnId = "workflow-specification-turn";
      yield* harness.adapterHarness!.queueTurnResponse(threadId, {
        events: [
          {
            type: "turn.started",
            ...runtimeBase("workflow-specification-started", "2026-05-01T00:00:02.100Z"),
            threadId,
            turnId: specificationTurnId,
          },
          {
            type: "turn.completed",
            ...runtimeBase("workflow-specification-completed", "2026-05-01T00:00:02.200Z"),
            threadId,
            turnId: specificationTurnId,
            status: "completed",
          },
        ],
      });
      let planningDispatchCount = 0;
      let planningCommandId: CommandId | null = null;
      const planningDispatch: Parameters<
        WorkflowStartService.WorkflowStartService["Service"]["start"]
      >[1] = (command) => {
        planningDispatchCount += 1;
        planningCommandId = command.commandId;
        return harness.engine.dispatch(command).pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationDispatchCommandError({
                message: "Workflow planning dispatch failed.",
                cause,
              }),
          ),
        );
      };
      const specificationResults = yield* Effect.gen(function* () {
        const start = yield* WorkflowStartService.WorkflowStartService;
        const input = {
          projectId: PROJECT_ID,
          repository,
          rootNumber: 10,
          issueNumber: 12,
          phase: "specification" as const,
          planningThreadId: threadId,
          modelSelection,
        };
        return yield* Effect.all(
          [start.start(input, planningDispatch), start.start(input, planningDispatch)],
          { concurrency: "unbounded" },
        );
      }).pipe(Effect.provide(startLayer));
      assert(planningCommandId !== null);
      const phaseCommandReceipt = yield* harness.commandReceiptRepository.getByCommandId({
        commandId: planningCommandId,
      });
      assert.equal(Option.isSome(phaseCommandReceipt), true);
      if (Option.isSome(phaseCommandReceipt)) {
        assert.equal(phaseCommandReceipt.value.status, "accepted");
      }
      yield* harness.waitForReceipt(
        (receipt): receipt is TurnProcessingQuiescedReceipt =>
          receipt.type === "turn.processing.quiesced" &&
          receipt.threadId === threadId &&
          receipt.checkpointTurnCount === 2,
      );
      yield* harness.drainProviderRuntime;
      yield* harness.drainCheckpointReactor;
      assert.deepEqual(specificationResults.map((result) => result.disposition).sort(), [
        "existing",
        "started",
      ]);
      assert.equal(planningDispatchCount, 1);
      assert.equal(new Set(specificationResults.map((result) => result.attemptId)).size, 1);
      assert.equal(harness.adapterHarness!.getStartCount(), 1);
      assert.equal(harness.adapterHarness!.getTurnInputs().length, 2);
      assert.deepEqual(harness.adapterHarness!.getTurnInputs()[1]?.skills, [
        { name: "to-spec", path: "/skills/to-spec/SKILL.md" },
      ]);
      assert.match(harness.adapterHarness!.getTurnInputs()[1]?.input ?? "", /Source map:/u);
      const afterSpecification = yield* harness.snapshotQuery.getSnapshot();
      assert.equal(
        afterSpecification.threads.find((candidate) => candidate.id === threadId)?.messages.length,
        2,
      );

      assignees.clear();
      const rejectedDispatch = makeDispatch("workflow-rejected", "reject-before-turn");
      const rejectedInput = {
        projectId: PROJECT_ID,
        repository,
        rootNumber: 10,
        issueNumber: 16,
        modelSelection,
      };
      const rejectedError = yield* Effect.gen(function* () {
        const start = yield* WorkflowStartService.WorkflowStartService;
        return yield* Effect.flip(start.start(rejectedInput, rejectedDispatch.dispatch));
      }).pipe(Effect.provide(startLayer));
      const rejectedThreadId = rejectedDispatch.getThreadId();
      assert.equal(rejectedError.failure, "dispatch-failed");
      assert.match(rejectedError.message, /claim added by this attempt was released/u);
      assert.equal(assignees.size, 0);
      assert.equal(harness.adapterHarness!.getStartCount(), 1);
      yield* harness.waitForDomainEvent(
        (event) => event.type === "thread.deleted" && event.aggregateId === rejectedThreadId,
      );

      assignees.clear();
      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("workflow-uncertain-started", "2026-05-01T00:00:03.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "turn.completed",
            ...runtimeBase("workflow-uncertain-completed", "2026-05-01T00:00:04.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
      });
      const uncertainDispatch = makeDispatch("workflow-uncertain", "lose-accepted-acknowledgement");
      const uncertainInput = { ...rejectedInput, issueNumber: 17 };
      const [uncertainError, repeatedUncertain] = yield* Effect.gen(function* () {
        const start = yield* WorkflowStartService.WorkflowStartService;
        const error = yield* Effect.flip(start.start(uncertainInput, uncertainDispatch.dispatch));
        const repeated = yield* start.start(uncertainInput, uncertainDispatch.dispatch);
        return [error, repeated] as const;
      }).pipe(Effect.provide(startLayer));
      const uncertainThreadId = uncertainDispatch.getThreadId();
      assert(uncertainThreadId !== null);
      yield* harness.waitForReceipt(
        (receipt): receipt is TurnProcessingQuiescedReceipt =>
          receipt.type === "turn.processing.quiesced" && receipt.threadId === uncertainThreadId,
      );
      assert.equal(uncertainError.failure, "dispatch-failed");
      assert.match(uncertainError.message, /uncertain/u);
      assert.equal(repeatedUncertain.status, "held");
      assert.equal(uncertainDispatch.getDispatchCount(), 1);
      assert.deepEqual(assignees, new Set(["Flow-Fly"]));
      assert.equal(harness.adapterHarness!.getStartCount(), 2);
      assert.equal(
        (yield* harness.snapshotQuery.getSnapshot()).threads.some(
          (candidate) => candidate.id === uncertainThreadId,
        ),
        true,
      );
    }),
  ),
);

it.live("runs a single turn end-to-end and persists checkpoint state in sqlite + git", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      yield* seedProjectAndThread(harness);

      const turnResponse: TestTurnResponse = {
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-single-1", "2026-02-24T10:00:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "message.delta",
            ...runtimeBase("evt-single-2", "2026-02-24T10:00:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            delta: "Single turn response.\n",
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-single-3", "2026-02-24T10:00:00.200Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
      };

      yield* harness.adapterHarness!.queueTurnResponseForNextSession(turnResponse);
      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-single",
        messageId: "msg-user-single",
        text: "Say hello",
      });
      // Finalization precedes quiescence. Concurrent waits prove that one matcher can retain an
      // earlier nonmatch for the other instead of consuming its evidence destructively.
      const [, finalizedReceipt] = yield* Effect.all(
        [
          harness.waitForReceipt(
            (receipt): receipt is TurnProcessingQuiescedReceipt =>
              receipt.type === "turn.processing.quiesced" &&
              receipt.threadId === THREAD_ID &&
              receipt.checkpointTurnCount === 1,
          ),
          harness.waitForReceipt(
            (receipt): receipt is CheckpointDiffFinalizedReceipt =>
              receipt.type === "checkpoint.diff.finalized" &&
              receipt.threadId === THREAD_ID &&
              receipt.checkpointTurnCount === 1,
          ),
        ],
        { concurrency: "unbounded" },
      );
      if (finalizedReceipt.type !== "checkpoint.diff.finalized") {
        throw new Error("Expected checkpoint.diff.finalized receipt.");
      }
      assert.equal(finalizedReceipt.status, "ready");

      const thread = yield* harness.waitForThread(
        THREAD_ID,
        (entry) =>
          entry.session?.status === "ready" &&
          entry.messages.some(
            (message) => message.role === "assistant" && message.streaming === false,
          ) &&
          entry.checkpoints.length === 1,
      );
      assert.equal(thread.checkpoints[0]?.status, "ready");
      assert.equal(thread.checkpoints[0]?.checkpointTurnCount, 1);

      const checkpointRows = yield* harness.checkpointRepository.listByThreadId({
        threadId: THREAD_ID,
      });
      assert.equal(checkpointRows.length, 1);
      assert.equal(checkpointRows[0]?.checkpointTurnCount, 1);
      assert.equal(checkpointRows[0]?.status, "ready");
      assert.deepEqual(checkpointRows[0]?.files, []);

      const ref0 = checkpointRefForThreadTurn(THREAD_ID, 0);
      const ref1 = checkpointRefForThreadTurn(THREAD_ID, 1);
      assert.equal(gitRefExists(harness.workspaceDir, ref0), true);
      assert.equal(gitRefExists(harness.workspaceDir, ref1), true);
      assert.equal(gitShowFileAtRef(harness.workspaceDir, ref0, "README.md"), "v1\n");
      assert.equal(gitShowFileAtRef(harness.workspaceDir, ref1, "README.md"), "v1\n");
    }),
  ),
);

it.live("lets a later receipt waiter trigger work required by an earlier waiter", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      yield* seedProjectAndThread(harness);
      const dependentThreadId = ThreadId.make("receipt-dependent-thread");
      yield* harness.engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-create-receipt-dependent"),
        threadId: dependentThreadId,
        projectId: PROJECT_ID,
        title: "Receipt-dependent thread",
        modelSelection: {
          instanceId: defaultInstanceIdForDriver(CODEX_PROVIDER),
          model: DEFAULT_MODEL_BY_PROVIDER[CODEX_PROVIDER] ?? DEFAULT_MODEL,
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: harness.workspaceDir,
        createdAt: nowIso(),
      });

      const dependentReceipt = yield* harness
        .waitForReceipt(
          (receipt): receipt is TurnProcessingQuiescedReceipt =>
            receipt.type === "turn.processing.quiesced" && receipt.threadId === dependentThreadId,
          5_000,
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("receipt-trigger-started", "2026-02-24T10:10:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "turn.completed",
            ...runtimeBase("receipt-trigger-completed", "2026-02-24T10:10:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
      });
      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-receipt-trigger",
        messageId: "msg-receipt-trigger",
        text: "Complete the trigger turn",
      });
      yield* harness.waitForReceipt(
        (receipt): receipt is TurnProcessingQuiescedReceipt =>
          receipt.type === "turn.processing.quiesced" && receipt.threadId === THREAD_ID,
        5_000,
      );

      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("receipt-dependent-started", "2026-02-24T10:11:00.000Z"),
            threadId: dependentThreadId,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "turn.completed",
            ...runtimeBase("receipt-dependent-completed", "2026-02-24T10:11:00.100Z"),
            threadId: dependentThreadId,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
      });
      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-receipt-dependent"),
        threadId: dependentThreadId,
        message: {
          messageId: asMessageId("msg-receipt-dependent"),
          role: "user",
          text: "Complete the dependent turn",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: nowIso(),
      });

      const receipt = yield* Fiber.join(dependentReceipt);
      assert.equal(receipt.threadId, dependentThreadId);
    }),
  ),
);

it.live.skipIf(!process.env.CODEX_BINARY_PATH)(
  "keeps the same Codex provider thread across runtime mode switches",
  () =>
    withRealCodexHarness((harness) =>
      Effect.gen(function* () {
        const createdAt = nowIso();

        yield* harness.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("cmd-project-create-real-codex"),
          projectId: PROJECT_ID,
          title: "Integration Project",
          workspaceRoot: harness.workspaceDir,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.3-codex",
          },
          createdAt,
        });

        yield* harness.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create-real-codex"),
          threadId: THREAD_ID,
          projectId: PROJECT_ID,
          title: "Integration Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.3-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          branch: null,
          worktreePath: harness.workspaceDir,
          createdAt,
        });

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-real-codex-1"),
          threadId: THREAD_ID,
          message: {
            messageId: asMessageId("msg-real-codex-1"),
            role: "user",
            text: "Reply with exactly ALPHA.",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "full-access",
          createdAt: nowIso(),
        });

        const firstThread = yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.session?.status === "ready" &&
            entry.session.providerName === "codex" &&
            entry.messages.some(
              (message) => message.role === "assistant" && message.streaming === false,
            ),
          180_000,
        );
        assert.equal(firstThread.session?.threadId, "thread-1");

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-real-codex-2"),
          threadId: THREAD_ID,
          message: {
            messageId: asMessageId("msg-real-codex-2"),
            role: "user",
            text: "Reply with exactly BETA.",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: nowIso(),
        });

        const secondThread = yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.session?.status === "ready" &&
            entry.session.providerName === "codex" &&
            entry.session.runtimeMode === "approval-required" &&
            entry.messages.some(
              (message) => message.role === "assistant" && message.text.includes("BETA"),
            ),
          180_000,
        );
        assert.equal(secondThread.session?.threadId, "thread-1");
      }),
    ),
);

it.live("runs multi-turn file edits and persists checkpoint diffs", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      yield* seedProjectAndThread(harness);

      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-multi-1", "2026-02-24T10:01:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "tool.started",
            ...runtimeBase("evt-multi-2", "2026-02-24T10:01:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            toolKind: "command",
            title: "Edit file",
            detail: "README.md",
          },
          {
            type: "tool.completed",
            ...runtimeBase("evt-multi-3", "2026-02-24T10:01:00.200Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            toolKind: "command",
            title: "Edit file",
            detail: "README.md",
          },
          {
            type: "message.delta",
            ...runtimeBase("evt-multi-4", "2026-02-24T10:01:00.300Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            delta: "Updated README to v2.\n",
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-multi-5", "2026-02-24T10:01:00.400Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
        mutateWorkspace: ({ cwd }) =>
          Effect.sync(() => {
            NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v2\n", "utf8");
          }),
      });

      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-multi-1",
        messageId: "msg-user-multi-1",
        text: "Make first edit",
      });
      yield* harness.waitForReceipt(
        (receipt): receipt is CheckpointDiffFinalizedReceipt =>
          receipt.type === "checkpoint.diff.finalized" &&
          receipt.threadId === THREAD_ID &&
          receipt.checkpointTurnCount === 1,
      );

      yield* harness.waitForThread(
        THREAD_ID,
        (entry) => entry.checkpoints.length === 1 && entry.session?.threadId === "thread-1",
      );

      yield* harness.adapterHarness!.queueTurnResponse(THREAD_ID, {
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-multi-6", "2026-02-24T10:02:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "message.delta",
            ...runtimeBase("evt-multi-7", "2026-02-24T10:02:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            delta: "Updated README to v3.\n",
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-multi-8", "2026-02-24T10:02:00.200Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
        mutateWorkspace: ({ cwd }) =>
          Effect.sync(() => {
            NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v3\n", "utf8");
          }),
      });

      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-multi-2",
        messageId: "msg-user-multi-2",
        text: "Make second edit",
      });
      const secondReceipt = yield* harness.waitForReceipt(
        (receipt): receipt is CheckpointDiffFinalizedReceipt =>
          receipt.type === "checkpoint.diff.finalized" &&
          receipt.threadId === THREAD_ID &&
          receipt.checkpointTurnCount === 2,
      );
      if (secondReceipt.type !== "checkpoint.diff.finalized") {
        throw new Error("Expected checkpoint.diff.finalized receipt.");
      }
      assert.equal(secondReceipt.status, "ready");
      yield* harness.waitForReceipt(
        (receipt): receipt is TurnProcessingQuiescedReceipt =>
          receipt.type === "turn.processing.quiesced" &&
          receipt.threadId === THREAD_ID &&
          receipt.checkpointTurnCount === 2,
      );

      const secondTurnThread = yield* harness.waitForThread(
        THREAD_ID,
        (entry) =>
          entry.latestTurn?.turnId === "turn-2" &&
          entry.checkpoints.length === 2 &&
          entry.checkpoints.some((checkpoint) => checkpoint.checkpointTurnCount === 2),
      );
      const secondCheckpoint = secondTurnThread.checkpoints.find(
        (checkpoint) => checkpoint.checkpointTurnCount === 2,
      );
      assert.equal(
        secondCheckpoint?.files.some((file) => file.path === "README.md"),
        true,
      );

      const checkpointRows = yield* harness.checkpointRepository.listByThreadId({
        threadId: THREAD_ID,
      });
      assert.deepEqual(
        checkpointRows.map((row) => row.checkpointTurnCount),
        [1, 2],
      );

      const incrementalDiff = yield* harness.checkpointStore.diffCheckpoints({
        cwd: harness.workspaceDir,
        fromCheckpointRef: checkpointRefForThreadTurn(THREAD_ID, 1),
        toCheckpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
        fallbackFromToHead: false,
        ignoreWhitespace: false,
      });
      assert.equal(incrementalDiff.includes("README.md"), true);

      const fullDiff = yield* harness.checkpointStore.diffCheckpoints({
        cwd: harness.workspaceDir,
        fromCheckpointRef: checkpointRefForThreadTurn(THREAD_ID, 0),
        toCheckpointRef: checkpointRefForThreadTurn(THREAD_ID, 2),
        fallbackFromToHead: false,
        ignoreWhitespace: false,
      });
      assert.equal(fullDiff.includes("README.md"), true);

      assert.equal(
        gitShowFileAtRef(
          harness.workspaceDir,
          checkpointRefForThreadTurn(THREAD_ID, 1),
          "README.md",
        ),
        "v2\n",
      );
      assert.equal(
        gitShowFileAtRef(
          harness.workspaceDir,
          checkpointRefForThreadTurn(THREAD_ID, 2),
          "README.md",
        ),
        "v3\n",
      );
    }),
  ),
);

it.live("tracks approval requests and resolves pending approvals on user response", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      yield* seedProjectAndThread(harness);

      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-approval-1", "2026-02-24T10:03:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "approval.requested",
            ...runtimeBase("evt-approval-2", "2026-02-24T10:03:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            requestId: APPROVAL_REQUEST_ID,
            requestKind: "command",
            detail: "Approve command execution",
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-approval-3", "2026-02-24T10:03:00.200Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
      });

      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-approval",
        messageId: "msg-user-approval",
        text: "Run command needing approval",
      });

      const thread = yield* harness.waitForThread(THREAD_ID, (entry) =>
        entry.activities.some((activity) => activity.kind === "approval.requested"),
      );
      assert.equal(
        thread.activities.some((activity) => activity.kind === "approval.requested"),
        true,
      );

      const pendingRow = yield* harness.waitForPendingApproval(
        "req-approval-1",
        (row) => row.status === "pending" && row.decision === null,
      );
      assert.equal(pendingRow.status, "pending");

      yield* harness.engine.dispatch({
        type: "thread.approval.respond",
        commandId: CommandId.make("cmd-approval-respond"),
        threadId: THREAD_ID,
        requestId: APPROVAL_REQUEST_ID,
        decision: "accept",
        createdAt: nowIso(),
      });

      const resolvedRow = yield* harness.waitForPendingApproval(
        "req-approval-1",
        (row) => row.status === "resolved" && row.decision === "accept",
      );
      assert.equal(resolvedRow.status, "resolved");
      assert.equal(resolvedRow.decision, "accept");

      const approvalResponses = yield* waitForSync(
        () => harness.adapterHarness!.getApprovalResponses(THREAD_ID),
        (responses) => responses.length === 1,
        "provider approval response",
      );
      assert.equal(approvalResponses.length, 1);
      assert.equal(approvalResponses[0]?.requestId, "req-approval-1");
      assert.equal(approvalResponses[0]?.decision, "accept");
    }),
  ),
);

it.live("records failed turn runtime state and checkpoint status as error", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      yield* seedProjectAndThread(harness);

      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-failure-1", "2026-02-24T10:04:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "content.delta",
            ...runtimeBase("evt-failure-2", "2026-02-24T10:04:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            payload: {
              streamKind: "assistant_text",
              delta: "Partial output before failure.\n",
            },
          },
          {
            type: "runtime.error",
            ...runtimeBase("evt-failure-3", "2026-02-24T10:04:00.200Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            payload: {
              message: "Sandbox command failed.",
            },
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-failure-4", "2026-02-24T10:04:00.300Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            payload: {
              state: "failed",
              errorMessage: "Sandbox command failed.",
            },
          },
        ],
      });

      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-failure",
        messageId: "msg-user-failure",
        text: "Run risky command",
      });

      const thread = yield* harness.waitForThread(
        THREAD_ID,
        (entry) =>
          entry.session?.status === "error" &&
          entry.session?.lastError === "Sandbox command failed." &&
          entry.activities.some((activity) => activity.kind === "runtime.error") &&
          entry.checkpoints.length === 1,
      );
      assert.equal(thread.session?.status, "error");
      assert.equal(thread.checkpoints[0]?.status, "error");

      const checkpointRow = yield* harness.checkpointRepository.getByThreadAndTurnCount({
        threadId: THREAD_ID,
        checkpointTurnCount: 1,
      });
      assert.equal(Option.isSome(checkpointRow), true);
      if (Option.isSome(checkpointRow)) {
        assert.equal(checkpointRow.value.status, "error");
      }
      assert.equal(
        gitRefExists(harness.workspaceDir, checkpointRefForThreadTurn(THREAD_ID, 1)),
        true,
      );
    }),
  ),
);

it.live("reverts to an earlier checkpoint and trims checkpoint projections + git refs", () =>
  withHarness((harness) =>
    Effect.gen(function* () {
      yield* seedProjectAndThread(harness);

      yield* harness.adapterHarness!.queueTurnResponseForNextSession({
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-revert-1", "2026-02-24T10:05:00.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "tool.started",
            ...runtimeBase("evt-revert-1-tool-started", "2026-02-24T10:05:00.025Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            toolKind: "command",
            title: "Edit file",
            detail: "README.md",
          },
          {
            type: "tool.completed",
            ...runtimeBase("evt-revert-1-tool-completed", "2026-02-24T10:05:00.035Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            toolKind: "command",
            title: "Edit file",
            detail: "README.md",
          },
          {
            type: "message.delta",
            ...runtimeBase("evt-revert-1a", "2026-02-24T10:05:00.050Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            delta: "Updated README to v2.\n",
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-revert-2", "2026-02-24T10:05:00.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
        mutateWorkspace: ({ cwd }) =>
          Effect.sync(() => {
            NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v2\n", "utf8");
          }),
      });
      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-revert-1",
        messageId: "msg-user-revert-1",
        text: "First edit",
        createdAt: "2026-02-24T10:04:59.900Z",
      });

      yield* harness.waitForThread(
        THREAD_ID,
        (entry) => entry.session?.threadId === "thread-1" && entry.checkpoints.length === 1,
      );

      yield* harness.adapterHarness!.queueTurnResponse(THREAD_ID, {
        events: [
          {
            type: "turn.started",
            ...runtimeBase("evt-revert-3", "2026-02-24T10:05:01.000Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
          },
          {
            type: "tool.started",
            ...runtimeBase("evt-revert-3-tool-started", "2026-02-24T10:05:01.025Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            toolKind: "command",
            title: "Edit file",
            detail: "README.md",
          },
          {
            type: "tool.completed",
            ...runtimeBase("evt-revert-3-tool-completed", "2026-02-24T10:05:01.035Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            toolKind: "command",
            title: "Edit file",
            detail: "README.md",
          },
          {
            type: "message.delta",
            ...runtimeBase("evt-revert-3a", "2026-02-24T10:05:01.050Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            delta: "Updated README to v3.\n",
          },
          {
            type: "turn.completed",
            ...runtimeBase("evt-revert-4", "2026-02-24T10:05:01.100Z"),
            threadId: THREAD_ID,
            turnId: FIXTURE_TURN_ID,
            status: "completed",
          },
        ],
        mutateWorkspace: ({ cwd }) =>
          Effect.sync(() => {
            NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v3\n", "utf8");
          }),
      });
      yield* startTurn({
        harness,
        commandId: "cmd-turn-start-revert-2",
        messageId: "msg-user-revert-2",
        text: "Second edit",
        createdAt: "2026-02-24T10:05:00.900Z",
      });

      yield* harness.waitForThread(
        THREAD_ID,
        (entry) =>
          entry.latestTurn?.turnId === "turn-2" &&
          entry.checkpoints.length === 2 &&
          entry.activities.some((activity) => activity.turnId === "turn-2"),
        8000,
      );

      yield* harness.engine.dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make("cmd-checkpoint-revert"),
        threadId: THREAD_ID,
        turnCount: 1,
        createdAt: nowIso(),
      });

      yield* harness.waitForDomainEvent((event) => event.type === "thread.reverted");
      const revertedThread = yield* harness.waitForThread(
        THREAD_ID,
        (entry) =>
          entry.checkpoints.length === 1 && entry.checkpoints[0]?.checkpointTurnCount === 1,
      );
      assert.equal(revertedThread.checkpoints[0]?.checkpointTurnCount, 1);
      assert.deepEqual(
        revertedThread.messages.map((message) => ({ role: message.role, text: message.text })),
        [
          { role: "user", text: "First edit" },
          { role: "assistant", text: "Updated README to v2.\n" },
        ],
      );
      assert.equal(
        revertedThread.activities.some((activity) => activity.turnId === "turn-2"),
        false,
      );
      assert.equal(
        revertedThread.activities.some(
          (activity) => activity.turnId === "turn-1" && activity.kind === "tool.started",
        ),
        true,
      );
      assert.equal(
        revertedThread.activities.some(
          (activity) => activity.turnId === "turn-1" && activity.kind === "tool.completed",
        ),
        true,
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(harness.workspaceDir, "README.md"), "utf8"),
        "v2\n",
      );
      assert.equal(
        gitRefExists(harness.workspaceDir, checkpointRefForThreadTurn(THREAD_ID, 2)),
        false,
      );
      assert.deepEqual(harness.adapterHarness!.getRollbackCalls(THREAD_ID), [1]);

      const checkpointRows = yield* harness.checkpointRepository.listByThreadId({
        threadId: THREAD_ID,
      });
      assert.equal(checkpointRows.length, 1);
    }),
  ),
);

it.live(
  "appends checkpoint.revert.failed activity when revert is requested without an active session",
  () =>
    withHarness((harness) =>
      Effect.gen(function* () {
        yield* seedProjectAndThread(harness);

        yield* harness.engine.dispatch({
          type: "thread.checkpoint.revert",
          commandId: CommandId.make("cmd-checkpoint-revert-no-session"),
          threadId: THREAD_ID,
          turnCount: 0,
          createdAt: nowIso(),
        });

        const thread = yield* harness.waitForThread(THREAD_ID, (entry) =>
          entry.activities.some(
            (activity) =>
              activity.kind === "checkpoint.revert.failed" &&
              typeof activity.payload === "object" &&
              activity.payload !== null,
          ),
        );
        const failureActivity = thread.activities.find(
          (activity) => activity.kind === "checkpoint.revert.failed",
        );
        assert.equal(failureActivity !== undefined, true);
        assert.equal(
          String(
            (failureActivity?.payload as { readonly detail?: string } | undefined)?.detail,
          ).includes("No active provider session"),
          true,
        );
      }),
    ),
);

it.live("starts a claudeAgent session on first turn when provider is requested", () =>
  withHarness(
    (harness) =>
      Effect.gen(function* () {
        yield* seedProjectAndThread(harness);

        yield* harness.adapterHarness!.queueTurnResponseForNextSession({
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-start-1",
                "2026-02-24T10:10:00.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "message.delta",
              ...runtimeBase(
                "evt-claude-start-2",
                "2026-02-24T10:10:00.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              delta: "Claude first turn.\n",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-start-3",
                "2026-02-24T10:10:00.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-initial",
          messageId: "msg-user-claude-initial",
          text: "Use Claude",
          modelSelection: {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: "claude-sonnet-4-6",
          },
        });

        const thread = yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.session?.providerName === "claudeAgent" &&
            entry.session.status === "ready" &&
            entry.messages.some(
              (message) => message.role === "assistant" && message.text === "Claude first turn.\n",
            ),
        );
        assert.equal(thread.session?.providerName, "claudeAgent");
      }),
    CLAUDE_AGENT_PROVIDER,
  ),
);

it.live("recovers claudeAgent sessions after provider stopAll using persisted resume state", () =>
  withHarness(
    (harness) =>
      Effect.gen(function* () {
        yield* seedProjectAndThread(harness);

        yield* harness.adapterHarness!.queueTurnResponseForNextSession({
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-recover-1",
                "2026-02-24T10:11:00.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "message.delta",
              ...runtimeBase(
                "evt-claude-recover-2",
                "2026-02-24T10:11:00.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              delta: "Turn before restart.\n",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-recover-3",
                "2026-02-24T10:11:00.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-recover-1",
          messageId: "msg-user-claude-recover-1",
          text: "Before restart",
          modelSelection: {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: "claude-sonnet-4-6",
          },
        });

        yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.latestTurn?.turnId === "turn-1" && entry.session?.threadId === "thread-1",
        );

        yield* harness.adapterHarness!.adapter.stopAll();
        yield* waitForSync(
          () => harness.adapterHarness!.listActiveSessionIds(),
          (sessionIds) => sessionIds.length === 0,
          "provider stopAll",
        );

        yield* harness.adapterHarness!.queueTurnResponseForNextSession({
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-recover-4",
                "2026-02-24T10:11:01.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "message.delta",
              ...runtimeBase(
                "evt-claude-recover-5",
                "2026-02-24T10:11:01.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              delta: "Turn after restart.\n",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-recover-6",
                "2026-02-24T10:11:01.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-recover-2",
          messageId: "msg-user-claude-recover-2",
          text: "After restart",
        });
        yield* waitForSync(
          () => harness.adapterHarness!.getStartCount(),
          (count) => count === 2,
          "claude provider recovery start",
        );

        const recoveredThread = yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.session?.providerName === "claudeAgent" &&
            entry.messages.some(
              (message) => message.role === "user" && message.text === "After restart",
            ) &&
            !entry.activities.some((activity) => activity.kind === "provider.turn.start.failed"),
        );
        assert.equal(recoveredThread.session?.providerName, "claudeAgent");
        assert.equal(recoveredThread.session?.threadId, "thread-1");
      }),
    CLAUDE_AGENT_PROVIDER,
  ),
);

it.live("forwards claudeAgent approval responses to the provider session", () =>
  withHarness(
    (harness) =>
      Effect.gen(function* () {
        yield* seedProjectAndThread(harness);

        yield* harness.adapterHarness!.queueTurnResponseForNextSession({
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-approval-1",
                "2026-02-24T10:12:00.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "approval.requested",
              ...runtimeBase(
                "evt-claude-approval-2",
                "2026-02-24T10:12:00.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              requestId: APPROVAL_REQUEST_ID,
              requestKind: "command",
              detail: "Approve Claude tool call",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-approval-3",
                "2026-02-24T10:12:00.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-approval",
          messageId: "msg-user-claude-approval",
          text: "Need approval",
          modelSelection: {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: "claude-sonnet-4-6",
          },
        });

        const thread = yield* harness.waitForThread(THREAD_ID, (entry) =>
          entry.activities.some((activity) => activity.kind === "approval.requested"),
        );
        assert.equal(thread.session?.threadId, "thread-1");

        yield* harness.engine.dispatch({
          type: "thread.approval.respond",
          commandId: CommandId.make("cmd-claude-approval-respond"),
          threadId: THREAD_ID,
          requestId: APPROVAL_REQUEST_ID,
          decision: "accept",
          createdAt: nowIso(),
        });

        yield* harness.waitForPendingApproval(
          "req-approval-1",
          (row) => row.status === "resolved" && row.decision === "accept",
        );

        const approvalResponses = yield* waitForSync(
          () => harness.adapterHarness!.getApprovalResponses(THREAD_ID),
          (responses) => responses.length === 1,
          "claude provider approval response",
        );
        assert.equal(approvalResponses[0]?.decision, "accept");
      }),
    CLAUDE_AGENT_PROVIDER,
  ),
);

it.live("forwards thread.turn.interrupt to claudeAgent provider sessions", () =>
  withHarness(
    (harness) =>
      Effect.gen(function* () {
        yield* seedProjectAndThread(harness);

        yield* harness.adapterHarness!.queueTurnResponseForNextSession({
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-interrupt-1",
                "2026-02-24T10:13:00.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "message.delta",
              ...runtimeBase(
                "evt-claude-interrupt-2",
                "2026-02-24T10:13:00.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              delta: "Long running output.\n",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-interrupt-3",
                "2026-02-24T10:13:00.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-interrupt",
          messageId: "msg-user-claude-interrupt",
          text: "Start long turn",
          modelSelection: {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: "claude-sonnet-4-6",
          },
        });

        const thread = yield* harness.waitForThread(
          THREAD_ID,
          (entry) => entry.session?.threadId === "thread-1",
        );
        assert.equal(thread.session?.threadId, "thread-1");

        yield* harness.engine.dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make("cmd-turn-interrupt-claude"),
          threadId: THREAD_ID,
          createdAt: nowIso(),
        });
        yield* harness.waitForDomainEvent(
          (event) => event.type === "thread.turn-interrupt-requested",
        );

        const interruptCalls = yield* waitForSync(
          () => harness.adapterHarness!.getInterruptCalls(THREAD_ID),
          (calls) => calls.length === 1,
          "claude provider interrupt call",
        );
        assert.equal(interruptCalls.length, 1);
      }),
    CLAUDE_AGENT_PROVIDER,
  ),
);

it.live("reverts claudeAgent turns and rolls back provider conversation state", () =>
  withHarness(
    (harness) =>
      Effect.gen(function* () {
        yield* seedProjectAndThread(harness);

        yield* harness.adapterHarness!.queueTurnResponseForNextSession({
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-revert-1",
                "2026-02-24T10:14:00.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "message.delta",
              ...runtimeBase(
                "evt-claude-revert-2",
                "2026-02-24T10:14:00.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              delta: "README -> v2\n",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-revert-3",
                "2026-02-24T10:14:00.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
          mutateWorkspace: ({ cwd }) =>
            Effect.sync(() => {
              NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v2\n", "utf8");
            }),
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-revert-1",
          messageId: "msg-user-claude-revert-1",
          text: "First Claude edit",
          modelSelection: {
            instanceId: ProviderInstanceId.make("claudeAgent"),
            model: "claude-sonnet-4-6",
          },
        });

        yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.latestTurn?.turnId === "turn-1" && entry.session?.threadId === "thread-1",
        );

        yield* harness.adapterHarness!.queueTurnResponse(THREAD_ID, {
          events: [
            {
              type: "turn.started",
              ...runtimeBase(
                "evt-claude-revert-4",
                "2026-02-24T10:14:01.000Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
            },
            {
              type: "message.delta",
              ...runtimeBase(
                "evt-claude-revert-5",
                "2026-02-24T10:14:01.050Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              delta: "README -> v3\n",
            },
            {
              type: "turn.completed",
              ...runtimeBase(
                "evt-claude-revert-6",
                "2026-02-24T10:14:01.100Z",
                CLAUDE_AGENT_PROVIDER,
              ),
              threadId: THREAD_ID,
              turnId: FIXTURE_TURN_ID,
              status: "completed",
            },
          ],
          mutateWorkspace: ({ cwd }) =>
            Effect.sync(() => {
              NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v3\n", "utf8");
            }),
        });

        yield* startTurn({
          harness,
          commandId: "cmd-turn-start-claude-revert-2",
          messageId: "msg-user-claude-revert-2",
          text: "Second Claude edit",
        });

        yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.latestTurn?.turnId === "turn-2" &&
            entry.checkpoints.length === 2 &&
            entry.session?.providerName === "claudeAgent",
        );

        yield* harness.engine.dispatch({
          type: "thread.checkpoint.revert",
          commandId: CommandId.make("cmd-checkpoint-revert-claude"),
          threadId: THREAD_ID,
          turnCount: 1,
          createdAt: nowIso(),
        });

        const revertedThread = yield* harness.waitForThread(
          THREAD_ID,
          (entry) =>
            entry.checkpoints.length === 1 && entry.checkpoints[0]?.checkpointTurnCount === 1,
        );
        assert.equal(revertedThread.checkpoints[0]?.checkpointTurnCount, 1);
        assert.equal(
          gitRefExists(harness.workspaceDir, checkpointRefForThreadTurn(THREAD_ID, 1)),
          true,
        );
        assert.equal(
          gitRefExists(harness.workspaceDir, checkpointRefForThreadTurn(THREAD_ID, 2)),
          false,
        );
        assert.deepEqual(harness.adapterHarness!.getRollbackCalls(THREAD_ID), [1]);
      }),
    CLAUDE_AGENT_PROVIDER,
  ),
);
