import {
  EnvironmentId,
  OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type ServerProvider,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
  type WorkflowChildrenResult,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import { ProviderDriverError } from "../provider/Errors.ts";
import { makeProviderRegistryMock } from "../provider/testUtils/providerRegistryMock.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import { interpretWorkflowEvidence } from "./WorkflowEvidence.ts";
import * as WorkflowService from "./WorkflowService.ts";
import * as WorkflowStartService from "./WorkflowStartService.ts";

const projectId = ProjectId.make("project-1");
const environmentId = EnvironmentId.make("environment-1");
const instanceId = ProviderInstanceId.make("codex-workflow");
const repository = "Flow-Fly/t3code" as const;
const workspaceRoot = "/tmp";
const modelSelection = {
  instanceId,
  model: "gpt-6-astra",
  options: [{ id: "reasoningEffort", value: "high" }],
} as const;

function summary(
  number: number,
  kind: WorkflowIssueSummary["kind"],
  parentNumber: number | null,
  labels: ReadonlyArray<string>,
): WorkflowIssueSummary {
  return {
    id: `issue-${number}`,
    repository,
    number,
    title: number === 15 ? "Choose the workflow launch contract" : `Workflow ${kind}`,
    url: `https://github.com/${repository}/issues/${number}`,
    kind,
    state: "open",
    stateReason: null,
    updatedAt: "2026-09-06T10:00:00.000Z",
    childCount: 1,
    parentNumber,
    labels: [...labels],
    readiness: { status: "ready", reasons: [] },
  };
}

const root = summary(10, "container", null, ["workflow:container"]);
const map = summary(12, "map", 10, ["wayfinder:map"]);
const decision = summary(15, "decision", 12, ["wayfinder:research"]);
const capabilityBody = [
  "## Summary",
  "",
  "Deliver the approved workflow planning actions.",
  "",
  "## Origin",
  "",
  "Standalone capability requested directly by the repository owner.",
].join("\n");
const capability = {
  ...summary(17, "capability", null, ["workflow:capability"]),
  title: "Plan workflow delivery",
};

function detail(
  issue: WorkflowIssueSummary,
  assignees: ReadonlyArray<string>,
  blocked: boolean,
): WorkflowIssueDetail {
  const blockedBy = blocked
    ? [
        {
          ...summary(14, "decision", 12, ["wayfinder:research"]),
          readiness: {
            status: "blocked" as const,
            reasons: [
              {
                kind: "open-blocker" as const,
                message: "A prerequisite remains open.",
                source: `https://github.com/${repository}/issues/14`,
              },
            ],
          },
        },
      ]
    : [];
  const body = "Decision context";
  const readiness = interpretWorkflowEvidence({
    issue: {
      id: issue.id,
      url: issue.url,
      number: issue.number,
      title: issue.title,
      kind: issue.kind,
      state: issue.state,
      stateReason: issue.stateReason,
      labels: issue.labels,
      assignees,
      body,
      comments: [],
      reopenedAt: [],
    },
    blockers: blockedBy,
  }).readiness;
  return { ...issue, body, blockedBy, readiness };
}

function output(stdout: string) {
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
}

function provider(skills = ["wayfinder", "research", "to-spec", "to-tickets"]): ServerProvider {
  return {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    status: "ready",
    enabled: true,
    installed: true,
    auth: { status: "authenticated" },
    checkedAt: "2026-09-06T10:00:00.000Z",
    version: "1.0.0",
    models: [
      {
        slug: "gpt-6-astra",
        name: "GPT-6 Astra",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "reasoningEffort",
              label: "Reasoning effort",
              type: "select",
              options: [
                { id: "medium", label: "Medium" },
                { id: "high", label: "High" },
              ],
            },
          ],
        },
      },
    ],
    slashCommands: [],
    skills: skills.map((name) => ({
      name,
      path: `/skills/${name}/SKILL.md`,
      enabled: true,
    })),
  };
}

function capabilityDetail(
  options: { readonly approvedContent?: string } = {},
): WorkflowIssueDetail {
  const approvalSource = {
    id: "approval-source",
    url: `https://github.com/${repository}/issues/17#issuecomment-1`,
    body: "I approve this specification and proceeding to the ticket proposal.",
    createdAt: "2026-09-06T09:00:00.000Z",
    author: "Flow-Fly",
    authorAssociation: "OWNER",
  } as const;
  const approval = {
    id: "approval-record",
    url: `https://github.com/${repository}/issues/17#issuecomment-2`,
    body: [
      "## Approval",
      "<!-- t3-workflow:v1 approval -->",
      "Kind: specification",
      "Approved by: Flow-Fly",
      `Source: ${approvalSource.url}`,
      "### Approved content",
      options.approvedContent ?? capabilityBody,
    ].join("\n"),
    createdAt: "2026-09-06T10:00:00.000Z",
    author: "Flow-Fly",
    authorAssociation: "OWNER",
  } as const;
  const interpreted = interpretWorkflowEvidence({
    issue: {
      id: capability.id,
      url: capability.url,
      number: capability.number,
      title: capability.title,
      kind: capability.kind,
      state: capability.state,
      stateReason: capability.stateReason,
      labels: capability.labels,
      assignees: [],
      body: capabilityBody,
      comments: [approvalSource, approval],
      reopenedAt: [],
    },
  });
  return {
    ...capability,
    body: capabilityBody,
    blockedBy: [],
    evidence: interpreted.evidence,
    readiness: interpreted.readiness,
  };
}

function harness(
  options: {
    readonly provider?: ServerProvider | undefined;
    readonly dispatchFailure?: OrchestrationDispatchCommandError | undefined;
    readonly probeFailure?: boolean | undefined;
    readonly selectedIssue?: WorkflowIssueSummary | undefined;
    readonly ancestry?: ReadonlyArray<WorkflowIssueSummary> | undefined;
    readonly initialAssignees?: ReadonlyArray<string> | undefined;
    readonly competingAssigneeAfterClaim?: string | undefined;
    readonly identityFailureCount?: number | undefined;
    readonly threadState?: "running" | "interrupted" | "completed" | "error" | undefined;
    readonly blocked?: boolean | undefined;
    readonly details?: ReadonlyMap<number, WorkflowIssueDetail> | undefined;
    readonly children?: ReadonlyMap<number, WorkflowChildrenResult> | undefined;
    readonly planningThreadState?: "running" | "interrupted" | "completed" | "error" | undefined;
  } = {},
) {
  const commands = new Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>();
  const githubCalls = new Array<ReadonlyArray<string>>();
  const assignees = new Set(options.initialAssignees ?? []);
  let sequence = 100;
  let probeCount = 0;
  let dispatchFailure = options.dispatchFailure;
  let identityFailuresRemaining = options.identityFailureCount ?? 0;
  let assigneeOnNextProbe: string | undefined;
  const acceptedCommands = new Set<string>();

  const selectedIssue = options.selectedIssue ?? decision;
  const ancestry = options.ancestry ?? [root, map];
  const workflowLayer = Layer.mock(WorkflowService.WorkflowService)({
    issueDetail: ({ number }) =>
      Effect.succeed(
        options.details?.get(number) ??
          detail(
            number === 10 ? root : number === 12 ? map : selectedIssue,
            number === selectedIssue.number ? [...assignees] : [],
            options.blocked === true && number === selectedIssue.number,
          ),
      ),
    children: ({ parentNumber }) =>
      Effect.succeed(
        options.children?.get(parentNumber) ?? {
          parentNumber,
          children: [],
          frontier: {
            status: "empty",
            message: "No immediate work is visible in this branch.",
            readyIssueIds: [],
          },
        },
      ),
    locate: () =>
      Effect.succeed({
        issue: selectedIssue,
        ancestry,
        ancestryComplete: true,
      }),
  });
  const githubLayer = Layer.mock(GitHubCli.GitHubCli)({
    execute: ({ args }) =>
      Effect.suspend(() => {
        githubCalls.push(args);
        if (args[0] === "api" && identityFailuresRemaining > 0) {
          identityFailuresRemaining -= 1;
          return Effect.fail(
            new GitHubCli.GitHubCliCommandError({
              command: "gh",
              cwd: workspaceRoot,
              cause: "controlled identity failure",
            }),
          );
        }
        return Effect.sync(() => {
          if (args[0] === "api") return output("Flow-Fly\n");
          if (args.includes("--json")) return output([...assignees].join("\n"));
          const addAt = args.indexOf("--add-assignee");
          if (addAt >= 0) {
            assignees.add(args[addAt + 1]!);
            if (options.competingAssigneeAfterClaim) {
              assignees.add(options.competingAssigneeAfterClaim);
            }
          }
          const removeAt = args.indexOf("--remove-assignee");
          if (removeAt >= 0) assignees.delete(args[removeAt + 1]!);
          return output("");
        });
      }),
  });
  const selectedProvider = Object.hasOwn(options, "provider") ? options.provider : provider();
  const registry = makeProviderRegistryMock(selectedProvider ? [selectedProvider] : []);
  const providerLayer = Layer.succeed(ProviderRegistry.ProviderRegistry, {
    ...registry,
    probeWorkspaceSnapshot: () =>
      Effect.suspend(() => {
        probeCount += 1;
        if (assigneeOnNextProbe) {
          assignees.add(assigneeOnNextProbe);
          assigneeOnNextProbe = undefined;
        }
        return options.probeFailure
          ? Effect.fail(
              new ProviderDriverError({
                driver: "codex",
                instanceId,
                detail: "controlled discovery failure",
              }),
            )
          : Effect.succeed(selectedProvider);
      }),
  });

  const serviceLayer = Layer.effect(
    WorkflowStartService.WorkflowStartService,
    WorkflowStartService.make,
  ).pipe(
    Layer.provide(workflowLayer),
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getProjectShellById: () =>
          Effect.succeed(Option.some({ id: projectId, title: "T3 Code", workspaceRoot } as never)),
        getThreadShellById: (threadId) =>
          Effect.succeed(
            options.threadState || options.planningThreadState
              ? Option.some({
                  id: threadId,
                  projectId,
                  latestTurn: {
                    turnId: "turn-1",
                    state: options.threadState ?? options.planningThreadState,
                    requestedAt: "2026-09-06T10:00:00.000Z",
                    startedAt: "2026-09-06T10:00:00.000Z",
                    completedAt: null,
                    assistantMessageId: null,
                  },
                  session: null,
                } as never)
              : Option.none(),
          ),
      }),
    ),
    Layer.provide(providerLayer),
    Layer.provide(githubLayer),
    Layer.provide(
      Layer.mock(OrchestrationCommandReceiptRepository)({
        upsert: () => Effect.void,
        getByCommandId: ({ commandId }) =>
          Effect.succeed(
            acceptedCommands.has(commandId)
              ? Option.some({
                  commandId,
                  aggregateKind: "thread" as const,
                  aggregateId: ThreadId.make("accepted-thread"),
                  acceptedAt: "2026-09-06T10:00:00.000Z",
                  resultSequence: 101,
                  status: "accepted" as const,
                  error: null,
                })
              : Option.none(),
          ),
      }),
    ),
    Layer.provide(
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(environmentId),
      }),
    ),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provide(NodeServices.layer),
  );

  const dispatch = (command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>) =>
    Effect.suspend(() => {
      commands.push(command);
      if (dispatchFailure) return Effect.fail(dispatchFailure);
      sequence += 1;
      return Effect.succeed({ sequence });
    });

  const input = { projectId, repository, rootNumber: 10, issueNumber: 15, modelSelection };

  return {
    assignees,
    acceptCommand: (commandId: string) => acceptedCommands.add(commandId),
    addAssignee: (login: string) => assignees.add(login),
    commands,
    dispatch,
    githubCalls,
    input,
    layer: serviceLayer,
    probeCount: () => probeCount,
    setAssigneeOnNextProbe: (login: string) => {
      assigneeOnNextProbe = login;
    },
    setDispatchFailure: (failure: OrchestrationDispatchCommandError | undefined) => {
      dispatchFailure = failure;
    },
  };
}

describe("WorkflowStartService", () => {
  it.effect(
    "continues capability specification once in the explicit planning thread and reopens it on repeat",
    () => {
      const planningThreadId = ThreadId.make("planning-thread");
      const resolvedDecision = {
        ...decision,
        state: "closed" as const,
        stateReason: "completed" as const,
        readiness: {
          status: "resolved" as const,
          reasons: [{ kind: "resolution" as const, message: "Resolved with current evidence." }],
        },
      };
      const test = harness({
        planningThreadState: "completed",
        children: new Map([
          [
            map.number,
            {
              parentNumber: map.number,
              children: [resolvedDecision],
              frontier: {
                status: "complete",
                message: "All visible work is resolved.",
                readyIssueIds: [],
              },
            },
          ],
        ]),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const input = {
          ...test.input,
          issueNumber: map.number,
          phase: "specification" as const,
          planningThreadId,
        };

        const started = yield* service.start(input, test.dispatch);
        const repeated = yield* service.start(input, test.dispatch);
        const recovery = yield* service.recovery({
          projectId,
          repository,
          issueNumber: map.number,
          phase: "specification",
        });
        const duplicate = yield* Effect.flip(
          service.recover(
            {
              ...input,
              attemptId: recovery.currentAttempt?.attemptId,
              action: "start-fresh",
              observation: recovery.observation,
            },
            test.dispatch,
          ),
        );

        expect(started).toMatchObject({
          disposition: "started",
          phase: "specification",
          threadId: planningThreadId,
        });
        expect(started.message).toContain("Capability specification");
        expect(repeated).toMatchObject({ disposition: "existing", threadId: planningThreadId });
        expect(recovery.actions).toEqual(["open"]);
        expect(recovery.currentAttempt?.threadId).toBe(planningThreadId);
        expect(duplicate.message).toContain("only reopen");
        expect(test.commands).toHaveLength(1);
        expect(test.commands[0]).not.toHaveProperty("bootstrap");
        expect(test.commands[0]?.skills).toEqual([
          { name: "to-spec", path: "/skills/to-spec/SKILL.md" },
        ]);
        expect(test.commands[0]?.message.text).toContain("Use the to-spec skill explicitly");
        expect(test.commands[0]?.message.text).toContain(map.url);
        expect(test.githubCalls).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "holds capability creation while complete descendant evidence or map fog is unresolved",
    () => {
      const test = harness({
        planningThreadState: "completed",
        children: new Map([
          [
            map.number,
            {
              parentNumber: map.number,
              children: [],
              frontier: {
                status: "empty-review",
                message:
                  "No immediate work can proceed while this map still records remaining unknowns.",
                readyIssueIds: [],
              },
            },
          ],
        ]),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const error = yield* Effect.flip(
          service.start(
            {
              ...test.input,
              issueNumber: map.number,
              phase: "specification",
              planningThreadId: ThreadId.make("planning-thread"),
            },
            test.dispatch,
          ),
        );

        expect(error).toMatchObject({ failure: "not-ready" });
        expect(error.detail).toContain("remaining unknowns");
        expect(test.commands).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "starts a standalone capability ticket proposal only from current verified specification approval and preserves publication authority",
    () => {
      const planningThreadId = ThreadId.make("planning-thread");
      const approved = capabilityDetail();
      const test = harness({
        selectedIssue: capability,
        ancestry: [],
        planningThreadState: "completed",
        details: new Map([
          [root.number, detail(root, [], false)],
          [capability.number, approved],
        ]),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const started = yield* service.start(
          {
            ...test.input,
            issueNumber: capability.number,
            phase: "ticket-breakdown",
            planningThreadId,
          },
          test.dispatch,
        );

        expect(started).toMatchObject({ phase: "ticket-breakdown", threadId: planningThreadId });
        expect(started.message).toContain("Ticket breakdown proposal");
        expect(test.commands).toHaveLength(1);
        expect(test.commands[0]?.skills).toEqual([
          { name: "to-tickets", path: "/skills/to-tickets/SKILL.md" },
        ]);
        expect(test.commands[0]?.message.text).toContain("draft the proposed breakdown");
        expect(test.commands[0]?.message.text).toContain(
          "Do not publish delivery issues until the owner separately approves",
        );
        expect(test.commands[0]?.message.text).toContain("native sub-issues");
        expect(test.commands[0]).not.toHaveProperty("bootstrap");
        expect(test.githubCalls).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("rejects ticket slicing when the preserved specification no longer matches", () => {
    const stale = capabilityDetail({ approvedContent: `${capabilityBody}\n\nOld scope.` });
    const test = harness({
      selectedIssue: capability,
      ancestry: [],
      planningThreadState: "completed",
      details: new Map([
        [root.number, detail(root, [], false)],
        [capability.number, stale],
      ]),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(
        service.start(
          {
            ...test.input,
            issueNumber: capability.number,
            phase: "ticket-breakdown",
            planningThreadId: ThreadId.make("planning-thread"),
          },
          test.dispatch,
        ),
      );

      expect(error).toMatchObject({ failure: "not-ready" });
      expect(error.detail).toContain("approved snapshot");
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("does not duplicate a planning turn whose command outcome is unknown", () => {
    const planningThreadId = ThreadId.make("planning-thread");
    const test = harness({
      planningThreadState: "completed",
      dispatchFailure: new OrchestrationDispatchCommandError({ message: "response lost" }),
      children: new Map([
        [
          map.number,
          {
            parentNumber: map.number,
            children: [
              {
                ...decision,
                state: "closed" as const,
                stateReason: "completed" as const,
                readiness: { status: "resolved" as const, reasons: [] },
              },
            ],
            frontier: {
              status: "complete",
              message: "All visible work is resolved.",
              readyIssueIds: [],
            },
          },
        ],
      ]),
    });
    const input = {
      ...test.input,
      issueNumber: map.number,
      phase: "specification" as const,
      planningThreadId,
    };
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(input, test.dispatch));
      const repeated = yield* service.start(input, test.dispatch);

      expect(error).toMatchObject({ failure: "dispatch-failed" });
      expect(repeated).toMatchObject({ disposition: "held", phase: "specification" });
      expect(repeated.message).toContain("uncertain");
      expect(test.commands).toHaveLength(1);
      expect(test.githubCalls).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("does not add a phase turn while the explicit planning thread is active", () => {
    const test = harness({
      planningThreadState: "running",
      children: new Map([
        [
          map.number,
          {
            parentNumber: map.number,
            children: [
              {
                ...decision,
                state: "closed" as const,
                stateReason: "completed" as const,
                readiness: { status: "resolved" as const, reasons: [] },
              },
            ],
            frontier: {
              status: "complete",
              message: "All visible work is resolved.",
              readyIssueIds: [],
            },
          },
        ],
      ]),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(
        service.start(
          {
            ...test.input,
            issueNumber: map.number,
            phase: "specification",
            planningThreadId: ThreadId.make("planning-thread"),
          },
          test.dispatch,
        ),
      );

      expect(error).toMatchObject({ failure: "not-ready" });
      expect(error.message).toContain("active turn");
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });
  it.effect(
    "persists one attempt before claiming and submits the existing bootstrap with explicit skills",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const started = yield* service.start(test.input, test.dispatch);
        const repeated = yield* service.start(test.input, test.dispatch);

        expect(started).toMatchObject({ disposition: "started", status: "submitted" });
        expect(repeated).toMatchObject({
          disposition: "existing",
          status: "submitted",
          attemptId: started.attemptId,
          threadId: started.threadId,
        });
        expect(test.commands).toHaveLength(1);
        expect(test.probeCount()).toBe(1);
        expect(test.commands[0]).toMatchObject({
          threadId: started.threadId,
          bootstrap: { createThread: { projectId, title: decision.title } },
          modelSelection,
          skills: [
            { name: "wayfinder", path: "/skills/wayfinder/SKILL.md" },
            { name: "research", path: "/skills/research/SKILL.md" },
          ],
        });
        expect(test.commands[0]?.message.text).toContain("Map: Flow-Fly/t3code#12");
        expect(test.commands[0]?.message.text).toContain("Decision: Flow-Fly/t3code#15");
        expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("serializes two clients onto the same durable attempt", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const results = yield* Effect.all(
        [service.start(test.input, test.dispatch), service.start(test.input, test.dispatch)],
        { concurrency: "unbounded" },
      );

      expect(results.map((result) => result.disposition).sort()).toEqual(["existing", "started"]);
      expect(new Set(results.map((result) => result.attemptId))).toHaveLength(1);
      expect(new Set(results.map((result) => result.threadId))).toHaveLength(1);
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("refuses a fresh start when live workspace discovery lacks a required skill", () => {
    const test = harness({ provider: provider(["wayfinder"]) });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "skill-unavailable" });
      expect(test.probeCount()).toBe(1);
      expect(test.githubCalls).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects an ambiguous process label before claiming", () => {
    const test = harness({
      selectedIssue: {
        ...decision,
        labels: ["wayfinder:research", "wayfinder:prototype"],
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "unsupported-issue" });
      expect(test.probeCount()).toBe(0);
      expect(test.githubCalls).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects work outside the viewed workflow branch", () => {
    const unrelatedRoot = summary(20, "container", null, ["workflow:container"]);
    const test = harness({ ancestry: [unrelatedRoot, map] });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "unsupported-issue" });
      expect(test.probeCount()).toBe(0);
      expect(test.githubCalls).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("starts from a nested map focused within the selected workflow branch", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const result = yield* service.start({ ...test.input, rootNumber: 12 }, test.dispatch);

      expect(result).toMatchObject({ disposition: "started", rootNumber: 12 });
      expect(test.commands[0]?.message.text).toContain("Map: Flow-Fly/t3code#12");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects ordinary Start for work already assigned to the authenticated account", () => {
    const test = harness({ initialAssignees: ["Flow-Fly"] });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const repeated = yield* Effect.flip(service.start(test.input, test.dispatch));
      const recovery = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "not-ready" });
      expect(repeated).toMatchObject({ _tag: "WorkflowStartError", failure: "not-ready" });
      expect(recovery.currentAttempt).toBeNull();
      expect(test.commands).toHaveLength(0);
      expect(test.githubCalls.some((args) => args.includes("--add-assignee"))).toBe(false);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("recovers a new attempt that failed before its initial turn was submitted", () => {
    const test = harness({ identityFailureCount: 1 });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const recovery = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "claim-failed" });
      expect(recovery.currentAttempt).toMatchObject({
        status: "claiming",
        evidence: "rejected",
      });
      expect(recovery.actions).toEqual(["start-fresh"]);
      expect(recovery.message).toContain("confirmed not accepted");

      const recovered = yield* service.recover(
        {
          ...test.input,
          attemptId: recovery.currentAttempt?.attemptId,
          action: "start-fresh",
          observation: recovery.observation,
        },
        test.dispatch,
      );
      const history = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(recovered.action).toBe("started-fresh");
      expect(test.commands).toHaveLength(1);
      expect(history.attempts).toHaveLength(2);
      expect(history.currentAttempt).toMatchObject({
        attemptId: recovered.attemptId,
        evidence: "accepted",
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("holds a legacy attempt with no receipt and no linked thread", () => {
    const test = harness();
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        INSERT INTO workflow_start_attempts (
          attempt_id, environment_id, project_id, repository, root_number,
          issue_number, phase, thread_id, command_id, message_id, status,
          claim_owned, created_at, updated_at, is_current
        ) VALUES (
          'legacy-attempt', ${environmentId}, ${projectId}, ${repository}, 10,
          15, 'decision', 'missing-thread', 'legacy-command', 'legacy-message', 'claiming',
          0, '2026-09-06T10:00:00.000Z', '2026-09-06T10:00:00.000Z', 1
        )
      `;
      const recovery = yield* (yield* WorkflowStartService.WorkflowStartService).recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(recovery.currentAttempt).toMatchObject({ evidence: "unknown" });
      expect(recovery.actions).toEqual([]);
      expect(recovery.message).toContain("no linked thread exists");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("holds a competing post-claim assignment and releases only its own claim", () => {
    const test = harness({ competingAssigneeAfterClaim: "another-owner" });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const repeated = yield* service.start(test.input, test.dispatch);

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "claim-failed" });
      expect(repeated).toMatchObject({ disposition: "held", status: "held" });
      expect(repeated.message).toContain("competing assignment");
      expect(test.assignees).toEqual(new Set(["another-owner"]));
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects a reasoning effort the selected model does not support", () => {
    const codex = provider();
    const test = harness({
      provider: {
        ...codex,
        models: codex.models.map((model) => ({
          ...model,
          capabilities: {
            optionDescriptors: [
              {
                id: "reasoningEffort",
                label: "Reasoning effort",
                type: "select" as const,
                options: [{ id: "medium", label: "Medium" }],
              },
            ],
          },
        })),
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "effort-required" });
      expect(test.githubCalls).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "refuses a start when fresh discovery fails instead of trusting cached provider data",
    () => {
      const test = harness({ probeFailure: true });
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const error = yield* Effect.flip(service.start(test.input, test.dispatch));

        expect(error).toMatchObject({
          _tag: "WorkflowStartError",
          failure: "provider-unavailable",
        });
        expect(test.probeCount()).toBe(1);
        expect(test.githubCalls).toHaveLength(0);
        expect(test.commands).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("holds an uncertain first submission and never dispatches it again", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({ message: "response lost" }),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const repeated = yield* service.start(test.input, test.dispatch);

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "dispatch-failed" });
      expect(repeated).toMatchObject({ disposition: "held", status: "held" });
      expect(repeated.message).toContain("uncertain");
      expect(test.commands).toHaveLength(1);
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("reconciles a durable accepted command after an uncertain response", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({ message: "response lost" }),
      threadState: "completed",
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      yield* Effect.flip(service.start(test.input, test.dispatch));
      test.acceptCommand(test.commands[0]!.commandId);

      const [first, reconnected] = yield* Effect.all(
        [
          service.recovery({
            projectId,
            repository,
            issueNumber: decision.number,
          }),
          service.recovery({
            projectId,
            repository,
            issueNumber: decision.number,
          }),
        ],
        { concurrency: "unbounded" },
      );

      expect(first.currentAttempt).toMatchObject({
        attemptId: reconnected.currentAttempt?.attemptId,
        status: "submitted",
        evidence: "accepted",
        threadId: test.commands[0]!.threadId,
      });
      expect(first.actions).toContain("open");
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("submits one continuation when two clients resume the same interrupted turn", () => {
    const test = harness({ threadState: "interrupted" });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const started = yield* service.start(test.input, test.dispatch);
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      const results = yield* Effect.all(
        [
          service.recover(
            {
              ...test.input,
              attemptId: started.attemptId,
              action: "resume",
              observation: state.observation,
            },
            test.dispatch,
          ),
          service.recover(
            {
              ...test.input,
              attemptId: started.attemptId,
              action: "resume",
              observation: state.observation,
            },
            test.dispatch,
          ),
        ],
        { concurrency: "unbounded" },
      );

      expect(results.map((result) => result.action)).toEqual(["resumed", "resumed"]);
      expect(test.commands).toHaveLength(2);
      expect(test.commands[1]).toMatchObject({
        threadId: started.threadId,
        modelSelection,
      });
      expect(test.commands[1]).not.toHaveProperty("bootstrap");
      expect(test.commands[1]?.message.text).toContain("Resume the interrupted work");
      expect(test.probeCount()).toBe(2);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("requires takeover before a new resume when the accepted assignment changes", () => {
    const test = harness({ threadState: "interrupted" });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const started = yield* service.start(test.input, test.dispatch);
      test.addAssignee("outside-owner");
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(state.actions).toContain("open");
      expect(state.actions).toContain("takeover");
      expect(state.actions).not.toContain("resume");
      expect(state.actions).not.toContain("start-fresh");
      const error = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            attemptId: started.attemptId,
            action: "resume",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );

      expect(error).toMatchObject({ failure: "claim-failed" });
      expect(error.message).toContain("explicit takeover");
      expect(test.assignees).toEqual(new Set(["Flow-Fly", "outside-owner"]));
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("opens an accepted resume retry after the assignment changes", () => {
    const test = harness({ threadState: "interrupted" });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const started = yield* service.start(test.input, test.dispatch);
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });
      yield* service.recover(
        {
          ...test.input,
          attemptId: started.attemptId,
          action: "resume",
          observation: state.observation,
        },
        test.dispatch,
      );
      test.addAssignee("outside-owner");
      const changed = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      const repeated = yield* service.recover(
        {
          ...test.input,
          attemptId: started.attemptId,
          action: "resume",
          observation: changed.observation,
        },
        test.dispatch,
      );

      expect(repeated.action).toBe("resumed");
      expect(repeated.message).toContain("existing accepted resume turn");
      expect(test.commands).toHaveLength(2);
      expect(test.probeCount()).toBe(2);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("holds an uncertain resume without releasing the accepted attempt claim", () => {
    const test = harness({ threadState: "interrupted" });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const started = yield* service.start(test.input, test.dispatch);
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });
      test.setDispatchFailure(
        new OrchestrationDispatchCommandError({ message: "resume response lost" }),
      );

      const error = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            attemptId: started.attemptId,
            action: "resume",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );
      const repeated = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            attemptId: started.attemptId,
            action: "resume",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );

      expect(error.message).toContain("uncertain");
      expect(repeated.message).toContain("uncertain");
      expect(test.commands).toHaveLength(2);
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      expect(test.githubCalls.filter((args) => args.includes("--remove-assignee"))).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "preserves an accepted attempt and its claim when a fresh bootstrap is rejected",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const started = yield* service.start(test.input, test.dispatch);
        const state = yield* service.recovery({
          projectId,
          repository,
          issueNumber: decision.number,
        });
        test.setDispatchFailure(
          new OrchestrationDispatchCommandError({
            message: "fresh turn rejected",
            bootstrapTurnDisposition: "not-accepted",
            bootstrapThreadDisposition: "deleted",
          }),
        );

        yield* Effect.flip(
          service.recover(
            {
              ...test.input,
              attemptId: started.attemptId,
              action: "start-fresh",
              observation: state.observation,
            },
            test.dispatch,
          ),
        );
        const history = yield* service.recovery({
          projectId,
          repository,
          issueNumber: decision.number,
        });

        expect(history.attempts).toHaveLength(2);
        expect(
          history.attempts.find((attempt) => attempt.attemptId === started.attemptId),
        ).toMatchObject({
          isCurrent: false,
          evidence: "accepted",
          threadId: started.threadId,
        });
        expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
        expect(test.githubCalls.filter((args) => args.includes("--remove-assignee"))).toHaveLength(
          0,
        );

        test.setDispatchFailure(undefined);
        const retried = yield* service.recover(
          {
            ...test.input,
            attemptId: history.currentAttempt?.attemptId,
            action: "start-fresh",
            observation: history.observation,
          },
          test.dispatch,
        );
        expect(retried.action).toBe("started-fresh");
        expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
        expect(test.githubCalls.filter((args) => args.includes("--add-assignee"))).toHaveLength(1);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("starts fresh from accepted work while retaining its expected claim", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const started = yield* service.start(test.input, test.dispatch);
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      const fresh = yield* service.recover(
        {
          ...test.input,
          attemptId: started.attemptId,
          action: "start-fresh",
          observation: state.observation,
        },
        test.dispatch,
      );

      expect(fresh.action).toBe("started-fresh");
      expect(fresh.threadId).not.toBe(started.threadId);
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      expect(test.commands).toHaveLength(2);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("preserves the current attempt when saving its replacement fails", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const sql = yield* SqlClient.SqlClient;
      const started = yield* service.start(test.input, test.dispatch);
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });
      yield* sql`
        CREATE TRIGGER reject_replacement_attempt
        BEFORE INSERT ON workflow_start_attempts
        BEGIN
          SELECT RAISE(FAIL, 'controlled replacement failure');
        END
      `;

      const error = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            attemptId: started.attemptId,
            action: "start-fresh",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );
      const history = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(error).toMatchObject({ failure: "persistence-failed" });
      expect(history.attempts).toHaveLength(1);
      expect(history.currentAttempt).toMatchObject({
        attemptId: started.attemptId,
        isCurrent: true,
        evidence: "accepted",
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects takeover when the observed assignment changes", () => {
    const test = harness({ initialAssignees: ["outside-owner"] });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      yield* Effect.flip(service.start(test.input, test.dispatch));
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });
      test.addAssignee("new-owner");

      const error = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            attemptId: state.currentAttempt?.attemptId,
            action: "takeover",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );

      expect(error.message).toContain("changed");
      expect(test.assignees).toEqual(new Set(["outside-owner", "new-owner"]));
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects takeover when an assignment changes during preflight", () => {
    const test = harness({ initialAssignees: ["outside-owner"] });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      yield* Effect.flip(service.start(test.input, test.dispatch));
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });
      test.setAssigneeOnNextProbe("new-owner");

      const error = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            attemptId: state.currentAttempt?.attemptId,
            action: "takeover",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );

      expect(error.message).toContain("changed");
      expect(test.assignees).toEqual(new Set(["outside-owner", "new-owner"]));
      expect(test.githubCalls.filter((args) => args.includes("--remove-assignee"))).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("takes over an unchanged outside assignment and preserves the held attempt", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      test.setAssigneeOnNextProbe("outside-owner");
      yield* Effect.flip(service.start(test.input, test.dispatch));
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      const result = yield* service.recover(
        {
          ...test.input,
          attemptId: state.currentAttempt?.attemptId,
          action: "takeover",
          observation: state.observation,
        },
        test.dispatch,
      );
      const history = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(result.action).toBe("taken-over");
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      expect(history.attempts).toHaveLength(2);
      expect(history.currentAttempt?.threadId).toBe(result.threadId);
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("takes over an external claim without creating a local attempt first", () => {
    const test = harness({ initialAssignees: ["outside-owner"] });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(state.currentAttempt).toBeNull();
      expect(state.actions).toEqual(["takeover"]);
      const result = yield* service.recover(
        {
          ...test.input,
          action: "takeover",
          observation: state.observation,
        },
        test.dispatch,
      );

      expect(result.action).toBe("taken-over");
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("keeps claimed work blocked when a prerequisite remains open", () => {
    const test = harness({ initialAssignees: ["outside-owner"], blocked: true });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const state = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      const error = yield* Effect.flip(
        service.recover(
          {
            ...test.input,
            action: "takeover",
            observation: state.observation,
          },
          test.dispatch,
        ),
      );

      expect(error).toMatchObject({ failure: "not-ready" });
      expect(error.detail).toContain("Prerequisite #14");
      expect(test.assignees).toEqual(new Set(["outside-owner"]));
      expect(test.commands).toHaveLength(0);
      expect(
        test.githubCalls.filter((args) => args.includes("issue") && args.includes("edit")),
      ).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("releases only its own claim when bootstrap deletion confirms no turn started", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({
        message: "provider rejected turn",
        bootstrapThreadDisposition: "deleted",
        bootstrapTurnDisposition: "not-accepted",
      }),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const repeated = yield* service.start(test.input, test.dispatch);
      const recovery = yield* service.recovery({
        projectId,
        repository,
        issueNumber: decision.number,
      });

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "dispatch-failed" });
      expect(error.message).toContain("claim added by this attempt was released");
      expect(repeated.message).toContain("claim added by this attempt was released");
      expect(recovery.currentAttempt?.evidence).toBe("rejected");
      expect(recovery.actions).toEqual(["start-fresh"]);
      expect(test.assignees).toEqual(new Set());
      expect(test.githubCalls.some((args) => args.includes("--remove-assignee"))).toBe(true);
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("retains its claim when deletion does not prove the first turn was unaccepted", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({
        message: "provider rejected turn",
        bootstrapThreadDisposition: "deleted",
      }),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error.message).toContain("uncertain");
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      expect(test.githubCalls.some((args) => args.includes("--remove-assignee"))).toBe(false);
    }).pipe(Effect.provide(test.layer));
  });
});
