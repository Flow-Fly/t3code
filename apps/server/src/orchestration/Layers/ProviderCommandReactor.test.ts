// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  ModelSelection,
  ProviderRuntimeEvent,
  ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSetupError,
  RuntimeItemId,
  RuntimeTaskId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import {
  ApprovalRequestId,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentId,
  EventId,
  MessageId,
  OrchestrationDispatchCommandError,
  WorkflowDirectorError,
  ProjectId,
  ThreadId,
  TurnId,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Crypto from "effect/Crypto";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { it as effectIt } from "@effect/vitest";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { deriveServerPaths, ServerConfig } from "../../config.ts";
import { TextGenerationError } from "@t3tools/contracts";
import {
  ProviderAdapterRequestError,
  ProviderWorkspaceMissingError,
  type ProviderServiceError,
} from "../../provider/Errors.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import * as OrchestrationCommandReceipts from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import { ProviderAuthService } from "../../provider/Services/ProviderAuthService.ts";
import { makeProviderRegistryLayer } from "../../provider/testUtils/providerRegistryMock.ts";
import { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import {
  providerErrorLabelFromInstanceHint,
  ProviderCommandReactorLive,
} from "./ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionLive } from "./ProviderRuntimeIngestion.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Clock from "effect/Clock";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ServerActivation } from "../../serverActivation.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import * as GitWorkflowService from "../../git/GitWorkflowService.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import { dispatchCreatedThreadTurnStart } from "../dispatchCreatedThreadTurnStart.ts";
import * as ProviderRegistry from "../../provider/Services/ProviderRegistry.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as GitHubCli from "../../sourceControl/GitHubCli.ts";
import * as WorkflowService from "../../workflow/WorkflowService.ts";
import * as WorkflowStartService from "../../workflow/WorkflowStartService.ts";
import * as WorkflowDirectorService from "../../workflow/WorkflowDirectorService.ts";
import * as WorkflowMonitor from "../../workflow/WorkflowMonitor.ts";
import {
  interpretWorkflowEvidence,
  workflowEvidenceBodyFingerprint,
} from "../../workflow/WorkflowEvidence.ts";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asApprovalRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);
const asMessageId = (value: string): MessageId => MessageId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);

const assistantQuoteText = "Retain the reconnect backoff.";
const assistantCitation = {
  version: 1 as const,
  environmentId: EnvironmentId.make("source-environment"),
  threadId: ThreadId.make("source-thread"),
  messageId: asMessageId("source-message"),
  text: assistantQuoteText,
  start: 0,
  end: assistantQuoteText.length,
  prefix: "",
  suffix: "",
};

const deriveServerPathsSync = (baseDir: string, devUrl: URL | undefined) =>
  Effect.runSync(deriveServerPaths(baseDir, devUrl).pipe(Effect.provide(NodeServices.layer)));

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<void> => {
    if (await predicate()) {
      return;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for expectation.");
    }
    await Effect.runPromise(Effect.yieldNow);
    return poll();
  };

  return poll();
}

describe("ProviderCommandReactor", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | OrchestrationEngineService
    | ProviderCommandReactor
    | ProviderRuntimeIngestionService
    | ProjectionSnapshotQuery
    | ProviderRegistry.ProviderRegistry
    | OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository
    | GitWorkflowService.GitWorkflowService
    | ServerConfig
    | Crypto.Crypto
    | Path.Path
    | SqlClient.SqlClient,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;
  const createdStateDirs = new Set<string>();
  const createdBaseDirs = new Set<string>();

  afterEach(async () => {
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
    for (const stateDir of createdStateDirs) {
      NodeFS.rmSync(stateDir, { recursive: true, force: true });
    }
    createdStateDirs.clear();
    for (const baseDir of createdBaseDirs) {
      NodeFS.rmSync(baseDir, { recursive: true, force: true });
    }
    createdBaseDirs.clear();
  });

  describe("provider error attribution", () => {
    it("uses the current provider instance slug when current instance lookup fails", () => {
      expect(
        providerErrorLabelFromInstanceHint({
          instanceId: "codex_personal",
          modelSelectionInstanceId: "codex",
          sessionProvider: "codex",
        }),
      ).toBe("codex_personal");
    });

    it("uses the desired provider instance slug when desired instance lookup fails", () => {
      expect(
        providerErrorLabelFromInstanceHint({
          instanceId: "claude_openrouter",
        }),
      ).toBe("claude_openrouter");
    });
  });

  async function createHarness(input?: {
    directorTicketCount?: number;
    readonly baseDir?: string;
    readonly threadModelSelection?: ModelSelection;
    readonly sessionModelSwitch?: "unsupported" | "in-session";
    readonly requiresNewThreadForModelChange?: boolean;
    readonly unreadableHistory?: boolean;
    readonly titleRegenerationCompletionDispatchFailures?: number;
    readonly titleRegenerationBeforeStart?: "one" | "two";
    readonly serverActivation?: Effect.Effect<void>;
    readonly beforeReadySessionDispatch?: () => Effect.Effect<void>;
    readonly beforeDirectorIssueDetail?: () => Effect.Effect<void>;
    readonly compactThreadEffect?: () => Effect.Effect<void, ProviderAdapterRequestError>;
    readonly interruptTurnEffect?: () => Effect.Effect<void, ProviderAdapterRequestError>;
    readonly stopSessionEffect?: () => Effect.Effect<void, ProviderAdapterRequestError>;
    readonly startSessionEffect?: (
      session: ProviderSession,
    ) => Effect.Effect<ProviderSession, ProviderServiceError>;
    readonly tryHandlePromptCommandEffect?: ProviderAuthService["Service"]["tryHandlePromptCommand"];
  }) {
    const now = "2026-01-01T00:00:00.000Z";
    const baseDir =
      input?.baseDir ?? NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-reactor-"));
    createdBaseDirs.add(baseDir);
    const { stateDir } = deriveServerPathsSync(baseDir, undefined);
    createdStateDirs.add(stateDir);
    const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
    const tryHandlePromptCommand = vi.fn<ProviderAuthService["Service"]["tryHandlePromptCommand"]>(
      input?.tryHandlePromptCommandEffect ?? (() => Effect.succeed(false)),
    );
    let nextSessionIndex = 1;
    const runtimeSessions: Array<ProviderSession> = [];
    const modelSelection = input?.threadModelSelection ?? {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    };
    const startSessionEffect = input?.startSessionEffect;
    const startSession = vi.fn((_: unknown, input: unknown) => {
      const sessionIndex = nextSessionIndex++;
      const resumeCursor =
        typeof input === "object" && input !== null && "resumeCursor" in input
          ? input.resumeCursor
          : undefined;
      const threadId =
        typeof input === "object" &&
        input !== null &&
        "threadId" in input &&
        typeof input.threadId === "string"
          ? ThreadId.make(input.threadId)
          : ThreadId.make(`thread-${sessionIndex}`);
      const inputModelSelection =
        typeof input === "object" && input !== null && "modelSelection" in input
          ? (input.modelSelection as ModelSelection | undefined)
          : undefined;
      const providerInstanceId =
        typeof input === "object" && input !== null && "providerInstanceId" in input
          ? (input.providerInstanceId as ProviderInstanceId | undefined)
          : inputModelSelection?.instanceId;
      const provider =
        typeof input === "object" &&
        input !== null &&
        "provider" in input &&
        typeof input.provider === "string"
          ? (input.provider as ProviderSession["provider"])
          : ProviderDriverKind.make(inputModelSelection?.instanceId ?? modelSelection.instanceId);
      const session: ProviderSession = {
        provider,
        ...(providerInstanceId ? { providerInstanceId } : {}),
        status: "ready" as const,
        runtimeMode:
          typeof input === "object" &&
          input !== null &&
          "runtimeMode" in input &&
          (input.runtimeMode === "approval-required" || input.runtimeMode === "full-access")
            ? input.runtimeMode
            : "full-access",
        ...(typeof input === "object" &&
        input !== null &&
        "cwd" in input &&
        typeof input.cwd === "string"
          ? { cwd: input.cwd }
          : {}),
        ...((inputModelSelection?.model ?? modelSelection.model)
          ? { model: inputModelSelection?.model ?? modelSelection.model }
          : {}),
        threadId,
        resumeCursor: resumeCursor ?? { opaque: `resume-${sessionIndex}` },
        createdAt: now,
        updatedAt: now,
      };
      return (startSessionEffect?.(session) ?? Effect.succeed(session)).pipe(
        Effect.tap((startedSession) =>
          Effect.sync(() => {
            runtimeSessions.push(startedSession);
          }),
        ),
      );
    });
    const sentTurnId = asTurnId("turn-1");
    let sentTurnCount = 0;
    const sendTurn = vi.fn((_: unknown) =>
      Effect.succeed({
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId(`turn-${++sentTurnCount}`),
      }),
    );
    const compactThread = vi.fn((_: ThreadId) => input?.compactThreadEffect?.() ?? Effect.void);
    const interruptTurn = vi.fn((_: unknown) => input?.interruptTurnEffect?.() ?? Effect.void);
    const respondToRequest = vi.fn<ProviderServiceShape["respondToRequest"]>(() => Effect.void);
    const respondToUserInput = vi.fn<ProviderServiceShape["respondToUserInput"]>(() => Effect.void);
    const stopSession = vi.fn((stopInput: unknown) =>
      (input?.stopSessionEffect?.() ?? Effect.void).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            const threadId =
              typeof stopInput === "object" && stopInput !== null && "threadId" in stopInput
                ? (stopInput as { threadId?: ThreadId }).threadId
                : undefined;
            if (!threadId) {
              return;
            }
            const index = runtimeSessions.findIndex((session) => session.threadId === threadId);
            if (index >= 0) {
              runtimeSessions.splice(index, 1);
            }
          }),
        ),
      ),
    );
    const renameBranch = vi.fn((input: unknown) =>
      Effect.succeed({
        branch:
          typeof input === "object" &&
          input !== null &&
          "newBranch" in input &&
          typeof input.newBranch === "string"
            ? input.newBranch
            : "renamed-branch",
      }),
    );
    const pruneWorktrees = vi.fn((_: { readonly cwd: string }) => Effect.void);
    let directorWorktreePath: string | null = null;
    const createWorktree = vi.fn(
      (
        input: Parameters<GitWorkflowService.GitWorkflowService["Service"]["createWorktree"]>[0],
      ) => {
        if (input.path) directorWorktreePath = input.path;
        return Effect.succeed({
          worktree: { path: input.path ?? "", refName: input.newRefName ?? input.refName },
        });
      },
    );
    const localStatus = vi.fn(({ cwd }: { readonly cwd: string }) =>
      Effect.succeed({
        isRepo: cwd === "/tmp/provider-project" || cwd === directorWorktreePath,
        hasPrimaryRemote: true,
        isDefaultRef: cwd === "/tmp/provider-project",
        refName:
          cwd === "/tmp/provider-project"
            ? "main"
            : cwd === directorWorktreePath
              ? "t3code/workflow-17"
              : null,
        hasWorkingTreeChanges: false,
        workingTree: { files: [], insertions: 0, deletions: 0 },
      }),
    );
    const refreshStatus = vi.fn((_: string) =>
      Effect.succeed({
        isRepo: true,
        hasPrimaryRemote: true,
        isDefaultRef: false,
        refName: "renamed-branch",
        hasWorkingTreeChanges: false,
        workingTree: {
          files: [],
          insertions: 0,
          deletions: 0,
        },
        hasUpstream: true,
        aheadCount: 0,
        behindCount: 0,
        pr: null,
      }),
    );
    const generateBranchName = vi.fn<TextGeneration["Service"]["generateBranchName"]>((_) =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateBranchName",
          detail: "disabled in test harness",
        }),
      ),
    );
    const generateThreadTitle = vi.fn<TextGeneration["Service"]["generateThreadTitle"]>((_) =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateThreadTitle",
          detail: "disabled in test harness",
        }),
      ),
    );
    const providerSnapshots = [
      {
        instanceId: modelSelection.instanceId,
        driver: ProviderDriverKind.make("codex"),
        status: "ready" as const,
        enabled: true,
        installed: true,
        auth: { status: "authenticated" as const },
        checkedAt: now,
        version: "test",
        models: [
          {
            slug: modelSelection.model,
            name: modelSelection.model,
            isCustom: false,
            capabilities: {
              optionDescriptors: [
                {
                  id: "reasoningEffort",
                  label: "Reasoning effort",
                  type: "select" as const,
                  options: [{ id: "high", label: "High" }],
                },
              ],
            },
          },
          ...(modelSelection.model === "gpt-5.6-sol"
            ? []
            : [
                {
                  slug: "gpt-5.6-sol",
                  name: "GPT-5.6 Sol",
                  isCustom: false,
                  capabilities: {
                    optionDescriptors: [
                      {
                        id: "reasoningEffort",
                        label: "Reasoning effort",
                        type: "select" as const,
                        options: [{ id: "high", label: "High" }],
                      },
                    ],
                  },
                },
              ]),
          ...(modelSelection.model === "gpt-6-astra"
            ? []
            : [
                {
                  slug: "gpt-6-astra",
                  name: "GPT-6 Astra",
                  isCustom: false,
                  capabilities: {
                    optionDescriptors: [
                      {
                        id: "reasoningEffort",
                        label: "Reasoning effort",
                        type: "select" as const,
                        options: [{ id: "high", label: "High" }],
                      },
                    ],
                  },
                },
              ]),
        ],
        slashCommands: [],
        skills: ["wayfinder", "research", "implement", "code-review"].map((name) => ({
          name,
          path: `/skills/${name}/SKILL.md`,
          enabled: true,
        })),
        ...(input?.requiresNewThreadForModelChange === true
          ? { requiresNewThreadForModelChange: true }
          : {}),
      },
    ];

    const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;
    const service: ProviderServiceShape = {
      startSession: startSession as ProviderServiceShape["startSession"],
      sendTurn: sendTurn as ProviderServiceShape["sendTurn"],
      compactThread,
      interruptTurn: interruptTurn as ProviderServiceShape["interruptTurn"],
      respondToRequest: respondToRequest as ProviderServiceShape["respondToRequest"],
      respondToUserInput: respondToUserInput as ProviderServiceShape["respondToUserInput"],
      stopSession: stopSession as ProviderServiceShape["stopSession"],
      listSessions: () => Effect.succeed(runtimeSessions),
      getCapabilities: (_provider) =>
        Effect.succeed({
          sessionModelSwitch: input?.sessionModelSwitch ?? "in-session",
        }),
      assertConversationRollbackSupported: () => unsupported(),
      getInstanceInfo: (instanceId) => {
        const raw = String(instanceId);
        const driverKind = ProviderDriverKind.make(
          raw.startsWith("claude")
            ? "claudeAgent"
            : raw.startsWith("codex")
              ? "codex"
              : raw.startsWith("antigravity")
                ? "antigravity"
                : raw,
        );
        return Effect.succeed({
          instanceId,
          driverKind,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind,
            continuationKey:
              driverKind === ProviderDriverKind.make("codex")
                ? "codex:home:/shared-codex"
                : `${driverKind}:instance:${instanceId}`,
          },
        });
      },
      rollbackConversation: () => unsupported(),
      uploadFeedback: () => unsupported(),
      get streamEvents() {
        return Stream.fromPubSub(runtimeEventPubSub);
      },
    };

    const orchestrationLayer = OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(OrchestrationEventStoreLive),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    const projectionSnapshotLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    let titleRegenerationCompletionDispatchAttempts = 0;
    const reactorOrchestrationLayer = Layer.effect(
      OrchestrationEngineService,
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        return {
          readEvents: engine.readEvents,
          readThreadEvents: engine.readThreadEvents,
          getThreadReplayStats: engine.getThreadReplayStats,
          dispatch: (command) => {
            if (command.type === "thread.title.regeneration.complete") {
              titleRegenerationCompletionDispatchAttempts += 1;
              if (
                titleRegenerationCompletionDispatchAttempts <=
                (input?.titleRegenerationCompletionDispatchFailures ?? 0)
              ) {
                return Effect.die(new Error("Injected title regeneration completion failure"));
              }
            }
            return (
              command.type === "thread.session.set" && command.session.status === "ready"
                ? (input?.beforeReadySessionDispatch?.() ?? Effect.void)
                : Effect.void
            ).pipe(Effect.andThen(engine.dispatch(command)));
          },
          get streamDomainEvents() {
            return engine.streamDomainEvents;
          },
          subscribeDomainEvents: engine.subscribeDomainEvents,
          latestSequence: engine.latestSequence,
        } satisfies OrchestrationEngineService["Service"];
      }),
    ).pipe(Layer.provide(orchestrationLayer));
    const layer = Layer.merge(ProviderCommandReactorLive, ProviderRuntimeIngestionLive).pipe(
      Layer.provideMerge(reactorOrchestrationLayer),
      Layer.provideMerge(projectionSnapshotLayer),
      Layer.provideMerge(Layer.succeed(ProviderService, service)),
      Layer.provide(Layer.mock(ProviderAuthService, { tryHandlePromptCommand })),
      Layer.provideMerge(makeProviderRegistryLayer(providerSnapshots as never)),
      Layer.provideMerge(
        Layer.mock(GitWorkflowService.GitWorkflowService)({
          renameBranch,
          pruneWorktrees,
          createWorktree,
          localStatus,
        } satisfies Partial<GitWorkflowService.GitWorkflowService["Service"]>),
      ),
      Layer.provideMerge(
        Layer.succeed(VcsStatusBroadcaster, {
          getStatus: () => Effect.die("getStatus should not be called in this test"),
          refreshLocalStatus: () =>
            Effect.die("refreshLocalStatus should not be called in this test"),
          refreshStatus,
          refreshPullRequestStatus: () =>
            Effect.die("refreshPullRequestStatus should not be called in this test"),
          streamStatus: () => Stream.die("streamStatus should not be called in this test"),
        }),
      ),
      Layer.provideMerge(
        Layer.mock(TextGeneration, {
          generateBranchName,
          generateThreadTitle,
        }),
      ),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(ThreadBackgroundLiveness.layer),
      Layer.provideMerge(ThreadPlanProgress.layer),
      Layer.provideMerge(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistry.layer))),
      Layer.provideMerge(VcsProcess.layer),
      Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
      Layer.provideMerge(SqlitePersistenceMemory),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), baseDir)),
      Layer.provideMerge(NodeServices.layer),
    );
    runtime = ManagedRuntime.make(layer);

    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
    const reactor = await runtime.runPromise(Effect.service(ProviderCommandReactor));
    const ingestion = await runtime.runPromise(Effect.service(ProviderRuntimeIngestionService));
    const runEffect = <A, E>(effect: Effect.Effect<A, E>) => runtime!.runPromise(effect);

    const workflowRepository = "Flow-Fly/t3code" as const;
    let directorImplementationHead = "a".repeat(40);
    const workflowSummary = (
      number: number,
      kind: WorkflowIssueSummary["kind"],
      parentNumber: number | null,
      labels: ReadonlyArray<string>,
    ): WorkflowIssueSummary => ({
      id: `workflow-issue-${number}`,
      repository: workflowRepository,
      number,
      title: `Workflow ${kind}`,
      url: `https://github.com/${workflowRepository}/issues/${number}`,
      kind,
      state: "open",
      stateReason: null,
      updatedAt: now,
      childCount: 1,
      parentNumber,
      labels: [...labels],
      readiness: { status: "ready", reasons: [] },
    });
    const workflowRoot = workflowSummary(10, "container", null, ["workflow:container"]);
    const workflowMap = workflowSummary(12, "map", 10, ["wayfinder:map"]);
    const workflowDecision = workflowSummary(15, "decision", 12, ["wayfinder:research"]);
    const directorCapabilitySummary = {
      ...workflowSummary(17, "capability", 10, ["workflow:capability"]),
      title: "Capability delivery",
    };
    const directorTicketSummaries = Array.from(
      { length: input?.directorTicketCount ?? 1 },
      (_, index) => ({
        ...workflowSummary(18 + index, "ticket", 17, ["workflow:ticket", "ready-for-agent"]),
        title: `T${String(index + 1).padStart(2, "0")} — Delivery ${index + 1}`,
      }),
    );
    const directorCapabilityBody =
      "## Summary\n\nDeliver the capability.\n\n## Source map\n\nNone (standalone)";
    const directorSlice = [
      "## What to build",
      "",
      "Build the first delivery.",
      "",
      "## Acceptance criteria",
      "",
      "- [ ] It works.",
      "",
      "## Blocked by",
      "",
      "None",
    ].join("\n");
    const directorBreakdown = directorTicketSummaries
      .map((ticket, index) =>
        [
          "<details>",
          `<summary>T${String(index + 1).padStart(2, "0")} — Delivery ${index + 1}</summary>`,
          directorSlice,
          "</details>",
        ].join("\n"),
      )
      .join("\n");
    const ownerSource = {
      id: "director-owner-source",
      url: `${directorCapabilitySummary.url}#issuecomment-owner-source`,
      body: "Owner approval source.",
      createdAt: now,
      author: "Flow-Fly",
      authorAssociation: "OWNER",
    };
    const specificationApproval = {
      ...ownerSource,
      id: "director-specification",
      url: `${directorCapabilitySummary.url}#issuecomment-specification`,
      body: [
        "<!-- t3-workflow:v1 approval -->",
        "Kind: specification",
        "Approved by: Flow-Fly (owner)",
        `Source: ${ownerSource.url}`,
        "## Approved content",
        directorCapabilityBody,
      ].join("\n"),
    };
    const breakdownApproval = {
      ...ownerSource,
      id: "director-breakdown",
      url: `${directorCapabilitySummary.url}#issuecomment-breakdown`,
      body: [
        "<!-- t3-workflow:v1 approval -->",
        "Kind: ticket-breakdown",
        "Approved by: Flow-Fly (owner)",
        `Source: ${ownerSource.url}`,
        "## Approved content",
        directorBreakdown,
      ].join("\n"),
    };
    const directorCapabilityEvidence = interpretWorkflowEvidence({
      issue: {
        id: directorCapabilitySummary.id,
        url: directorCapabilitySummary.url,
        number: directorCapabilitySummary.number,
        title: directorCapabilitySummary.title,
        kind: "capability",
        state: "open",
        stateReason: null,
        labels: directorCapabilitySummary.labels,
        assignees: [],
        body: directorCapabilityBody,
        comments: [ownerSource, specificationApproval, breakdownApproval],
        reopenedAt: [],
      },
    });
    const directorCapability: WorkflowIssueDetail = {
      ...directorCapabilitySummary,
      body: directorCapabilityBody,
      blockedBy: [],
      ...directorCapabilityEvidence,
    };
    const directorTickets: WorkflowIssueDetail[] = directorTicketSummaries.map((summary, index) => {
      const body = [
        `Approved slice: **T${String(index + 1).padStart(2, "0")}** ([ticket-breakdown approval](${breakdownApproval.url}))`,
        "",
        directorSlice,
      ].join("\n");
      const evidence = interpretWorkflowEvidence({
        issue: {
          id: summary.id,
          url: summary.url,
          number: summary.number,
          title: summary.title,
          kind: "ticket",
          state: "open",
          stateReason: null,
          labels: summary.labels,
          assignees: [],
          body,
          comments: [],
          reopenedAt: [],
        },
        approvalComments: [ownerSource, breakdownApproval],
      });
      return { ...summary, body, blockedBy: [], ...evidence };
    });
    const directorTicket = directorTickets[0]!;
    const completionComments: Array<{
      readonly id: string;
      readonly url: string;
      readonly body: string;
      readonly createdAt: string;
      readonly author: string;
      readonly authorAssociation: "OWNER";
    }> = [];
    const refreshDirectorCapability = (state: "open" | "closed") => {
      Object.assign(directorCapability, {
        state,
        stateReason: state === "closed" ? "completed" : null,
        ...interpretWorkflowEvidence({
          issue: {
            id: directorCapability.id,
            url: directorCapability.url,
            number: directorCapability.number,
            title: directorCapability.title,
            kind: directorCapability.kind,
            state,
            stateReason: state === "closed" ? "completed" : null,
            labels: directorCapability.labels,
            assignees: [],
            body: directorCapability.body,
            comments: [
              ownerSource,
              specificationApproval,
              breakdownApproval,
              ...completionComments,
            ],
            reopenedAt: [],
          },
        }),
      });
    };
    const resolveDirectorTickets = (count = directorTickets.length) => {
      for (const ticket of directorTickets.slice(0, count)) {
        const resolution = {
          id: `resolution-${ticket.number}`,
          url: `${ticket.url}#issuecomment-resolution`,
          body: [
            "<!-- t3-workflow:v1 resolution -->",
            "Outcome: resolved",
            "## Summary",
            "Delivered across the durable director batches.",
            "## Evidence",
            `https://github.com/${workflowRepository}/commit/${directorImplementationHead}`,
          ].join("\n"),
          createdAt: "2026-09-07T13:02:00.000Z",
          author: "Flow-Fly",
          authorAssociation: "OWNER" as const,
        };
        Object.assign(ticket, {
          state: "closed",
          stateReason: "completed",
          labels: ticket.labels.filter((label) => label !== "ready-for-agent"),
          ...interpretWorkflowEvidence({
            issue: {
              id: ticket.id,
              url: ticket.url,
              number: ticket.number,
              title: ticket.title,
              kind: ticket.kind,
              state: "closed",
              stateReason: "completed",
              labels: ticket.labels.filter((label) => label !== "ready-for-agent"),
              assignees: [],
              body: ticket.body,
              comments: [resolution],
              reopenedAt: [],
            },
            approvalComments: [ownerSource, breakdownApproval],
          }),
        });
      }
    };
    const holdDirectorTicket = (index: number) => {
      const ticket = directorTickets[index]!;
      Object.assign(ticket, {
        labels: ticket.labels.filter((label) => label !== "ready-for-agent"),
        readiness: {
          status: "blocked",
          reasons: [
            {
              kind: "open-blocker",
              message: "A required predecessor is still open.",
              source: ticket.url,
            },
          ],
        },
      });
    };
    const readyDirectorTicket = (index: number) => {
      const ticket = directorTickets[index]!;
      Object.assign(ticket, {
        labels: [...new Set([...ticket.labels, "ready-for-agent"])],
        readiness: { status: "ready", reasons: [] },
      });
    };
    const workflowAssignees = new Set<string>();
    const workflowDetail = (issue: WorkflowIssueSummary): WorkflowIssueDetail => {
      const body = "Workflow integration fixture";
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
          assignees: issue.id === workflowDecision.id ? [...workflowAssignees] : [],
          body,
          comments: [],
          reopenedAt: [],
        },
      }).readiness;
      return { ...issue, body, blockedBy: [], readiness };
    };
    const workflowStart = await runtime.runPromise(
      WorkflowStartService.make.pipe(
        Effect.provideService(
          WorkflowService.WorkflowService,
          WorkflowService.WorkflowService.of({
            issueDetail: ({
              number,
            }: Parameters<WorkflowService.WorkflowService["Service"]["issueDetail"]>[0]) =>
              Effect.succeed(
                workflowDetail(
                  number === workflowRoot.number
                    ? workflowRoot
                    : number === workflowMap.number
                      ? workflowMap
                      : workflowDecision,
                ),
              ),
            locate: () =>
              Effect.succeed({
                issue: workflowDecision,
                ancestry: [workflowRoot, workflowMap],
                ancestryComplete: true,
              }),
          } as unknown as WorkflowService.WorkflowService["Service"]),
        ),
        Effect.provideService(
          GitHubCli.GitHubCli,
          GitHubCli.GitHubCli.of({
            execute: ({ args }: Parameters<GitHubCli.GitHubCli["Service"]["execute"]>[0]) =>
              Effect.sync(() => {
                if (args[0] === "api") {
                  return {
                    stdout: "Flow-Fly\n",
                    stderr: "",
                    exitCode: 0,
                    timedOut: false,
                    stdoutTruncated: false,
                    stderrTruncated: false,
                    stdoutInvalidUtf8: false,
                    stderrInvalidUtf8: false,
                  } as never;
                }
                if (args.includes("--json")) {
                  return {
                    stdout: [...workflowAssignees].join("\n"),
                    stderr: "",
                    exitCode: 0,
                    timedOut: false,
                    stdoutTruncated: false,
                    stderrTruncated: false,
                    stdoutInvalidUtf8: false,
                    stderrInvalidUtf8: false,
                  } as never;
                }
                const addAt = args.indexOf("--add-assignee");
                if (addAt >= 0) workflowAssignees.add(args[addAt + 1]!);
                const removeAt = args.indexOf("--remove-assignee");
                if (removeAt >= 0) workflowAssignees.delete(args[removeAt + 1]!);
                return {
                  stdout: "",
                  stderr: "",
                  exitCode: 0,
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                } as never;
              }),
          } as unknown as GitHubCli.GitHubCli["Service"]),
        ),
        Effect.provideService(
          ServerEnvironment.ServerEnvironment,
          ServerEnvironment.ServerEnvironment.of({
            getEnvironmentId: Effect.succeed(EnvironmentId.make("workflow-environment")),
          } as ServerEnvironment.ServerEnvironment["Service"]),
        ),
        Effect.provide(NodeServices.layer),
      ),
    );
    let failNextDirectorGitStatus = false;
    const directorWorkflow = WorkflowService.WorkflowService.of({
      issueDetail: ({
        number,
      }: Parameters<WorkflowService.WorkflowService["Service"]["issueDetail"]>[0]) =>
        (input?.beforeDirectorIssueDetail?.() ?? Effect.void).pipe(
          Effect.as(
            number === directorCapability.number
              ? directorCapability
              : (directorTickets.find((ticket) => ticket.number === number) ?? directorTicket),
          ),
        ),
      children: ({
        parentNumber,
      }: Parameters<WorkflowService.WorkflowService["Service"]["children"]>[0]) =>
        Effect.succeed({
          parentNumber,
          children: parentNumber === directorCapability.number ? directorTickets : [],
          frontier: {
            status: parentNumber === directorCapability.number ? "available" : "empty",
            message:
              parentNumber === directorCapability.number
                ? `${directorTickets.length} items can proceed.`
                : "No work.",
            readyIssueIds:
              parentNumber === directorCapability.number
                ? directorTickets.map((ticket) => ticket.id)
                : [],
          },
        }),
      locate: () =>
        Effect.succeed({
          issue: directorTicket,
          ancestry: [directorCapability],
          ancestryComplete: true,
        }),
      validateProject: () => Effect.void,
      roots: () => Effect.succeed({ repository: workflowRepository, roots: [directorCapability] }),
    } as unknown as WorkflowService.WorkflowService["Service"]);
    let beforeNextDirectorGitStatus: (() => Effect.Effect<void>) | undefined;
    const workflowDirector = await runtime.runPromise(
      WorkflowDirectorService.make.pipe(
        Effect.provideService(WorkflowService.WorkflowService, directorWorkflow),
        Effect.provideService(
          GitHubCli.GitHubCli,
          GitHubCli.GitHubCli.of({
            execute: ({ args, stdin }: Parameters<GitHubCli.GitHubCli["Service"]["execute"]>[0]) =>
              Effect.sync(() => {
                if (args[0] === "issue" && args[1] === "comment" && stdin) {
                  completionComments.push({
                    id: "director-capability-completion",
                    url: `${directorCapability.url}#issuecomment-completion`,
                    body: stdin,
                    createdAt: "2026-09-07T13:03:00.000Z",
                    author: "Flow-Fly",
                    authorAssociation: "OWNER",
                  });
                  refreshDirectorCapability("open");
                }
                if (args[0] === "issue" && args[1] === "close") {
                  refreshDirectorCapability("closed");
                }
                const addAt = args.indexOf("--add-assignee");
                if (addAt >= 0) workflowAssignees.add(args[addAt + 1]!);
                return {
                  stdout:
                    args[0] === "repo"
                      ? `${workflowRepository}\n`
                      : args[0] === "api"
                        ? "Flow-Fly\n"
                        : args.includes("--json")
                          ? [...workflowAssignees].join("\n")
                          : "",
                  stderr: "",
                  exitCode: 0,
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                } as never;
              }),
            getRepositoryCloneUrls: () =>
              Effect.succeed({
                nameWithOwner: workflowRepository,
                url: `https://github.com/${workflowRepository}`,
                sshUrl: `git@github.com:${workflowRepository}.git`,
              }),
          } as unknown as GitHubCli.GitHubCli["Service"]),
        ),
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: ({ args }) =>
              Effect.succeed({
                stdout:
                  args[0] === "rev-parse"
                    ? `${directorImplementationHead}\n`
                    : args[0] === "status"
                      ? ""
                      : `fork\tgit@github.com:${workflowRepository}.git (fetch)\n`,
                stderr: "",
                code: ChildProcessSpawner.ExitCode(0),
                timedOut: false,
                stdoutTruncated: false,
                stderrTruncated: false,
                stdoutInvalidUtf8: false,
                stderrInvalidUtf8: false,
              } satisfies ProcessRunner.ProcessRunOutput).pipe(
                Effect.tap(() => {
                  if (args[0] !== "status" || !beforeNextDirectorGitStatus) return Effect.void;
                  const beforeGitStatus = beforeNextDirectorGitStatus;
                  beforeNextDirectorGitStatus = undefined;
                  return beforeGitStatus();
                }),
                Effect.map((result) => {
                  if (args[0] !== "status" || !failNextDirectorGitStatus) return result;
                  failNextDirectorGitStatus = false;
                  return { ...result, stdout: " M receipt-race.txt\n" };
                }),
              ),
          }),
        ),
        Effect.provideService(
          ServerEnvironment.ServerEnvironment,
          ServerEnvironment.ServerEnvironment.of({
            getEnvironmentId: Effect.succeed(EnvironmentId.make("workflow-environment")),
          } as ServerEnvironment.ServerEnvironment["Service"]),
        ),
      ),
    );
    const dispatchForWorkflow = (command: OrchestrationCommand) =>
      engine.dispatch(command).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationDispatchCommandError({
              message: "Workflow test dispatch failed.",
              cause,
            }),
        ),
      );
    const dispatchWorkflow = (
      command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
    ) => {
      const createThread = command.bootstrap?.createThread;
      if (!createThread) return dispatchForWorkflow(command);
      return dispatchCreatedThreadTurnStart({
        command: {
          ...command,
          bootstrap: { ...command.bootstrap, createThread },
        },
        createCommandId: Effect.succeed(
          WorkflowDirectorService.workflowSuccessorCreateCommandId(command.commandId),
        ),
        dispatch: dispatchForWorkflow,
        drainThreadDeletionThrough: () => Effect.void,
      });
    };
    const sqlClient = await runtime.runPromise(Effect.service(SqlClient.SqlClient));
    const makeWorkflowMonitor = () =>
      Effect.runPromise(
        Effect.scoped(
          WorkflowMonitor.make.pipe(
            Effect.provideService(WorkflowService.WorkflowService, directorWorkflow),
            Effect.provideService(
              WorkflowDirectorService.WorkflowDirectorService,
              workflowDirector,
            ),
            Effect.provideService(OrchestrationEngineService, engine),
            Effect.provideService(SqlClient.SqlClient, sqlClient),
          ),
        ),
      );
    const workflowMonitor = await makeWorkflowMonitor();

    await Effect.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-create"),
        projectId: asProjectId("project-1"),
        title: "Provider Project",
        workspaceRoot: "/tmp/provider-project",
        defaultModelSelection: modelSelection,
        createdAt: now,
      }),
    );
    await Effect.runPromise(
      engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cmd-thread-create"),
        threadId: ThreadId.make("thread-1"),
        projectId: asProjectId("project-1"),
        title: "Thread",
        modelSelection: modelSelection,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        branch: null,
        worktreePath: null,
        createdAt: now,
      }),
    );
    if (input?.unreadableHistory === true) {
      // Metadata commands must not decode this unrelated message body.
      await runtime.runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`
            INSERT INTO projection_thread_messages (
              message_id, thread_id, turn_id, role, text, attachments_json,
              is_streaming, created_at, updated_at
            ) VALUES (
              'old-unreadable-message', 'thread-1', NULL, 'assistant',
              'Old assistant output', 'invalid json', 0, ${now}, ${now}
            )
          `;
        }),
      );
    }
    if (input?.titleRegenerationBeforeStart === "two") {
      await Effect.runPromise(
        engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create-2"),
          threadId: ThreadId.make("thread-2"),
          projectId: asProjectId("project-1"),
          title: "Thread 2",
          modelSelection: modelSelection,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt: now,
        }),
      );
    }
    const titleRegenerationThreadIds =
      input?.titleRegenerationBeforeStart === "two"
        ? [ThreadId.make("thread-1"), ThreadId.make("thread-2")]
        : input?.titleRegenerationBeforeStart === "one"
          ? [ThreadId.make("thread-1")]
          : [];
    for (const [index, threadId] of titleRegenerationThreadIds.entries()) {
      await Effect.runPromise(
        engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make(
            `cmd-thread-title-regeneration-before-reactor-start-${index + 1}`,
          ),
          threadId,
          regenerateTitle: true,
        }),
      );
    }

    scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(
      Effect.all([
        reactor.start().pipe(Effect.provideService(ServerActivation, input?.serverActivation)),
        ingestion.start(),
      ]).pipe(Scope.provide(scope)),
    );
    const drain = async () => {
      await Effect.runPromise(reactor.drain);
      await runtime!.runPromise(ingestion.drain);
      await Effect.runPromise(reactor.drain);
    };
    const emitProviderEvents = (events: ReadonlyArray<ProviderRuntimeEvent>) =>
      runtime!.runPromise(
        Effect.forEach(events, (event) => PubSub.publish(runtimeEventPubSub, event), {
          discard: true,
        }).pipe(Effect.andThen(ingestion.drain)),
      );
    const readWorkflowResumeRows = (attemptId: string) =>
      runtime!.runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql<{ readonly commandId: string; readonly status: string }>`
            SELECT command_id AS "commandId", status FROM workflow_resume_attempts
            WHERE workflow_attempt_id = ${attemptId}
          `;
        }),
      );
    const readDirectorRows = (directorId: string) =>
      runtime!.runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql<{ readonly commandId: string; readonly status: string }>`
            SELECT command_id AS "commandId", status FROM workflow_directors
            WHERE director_id = ${directorId}
          `;
        }),
      );

    return {
      engine,
      snapshotQuery,
      readModel: () => Effect.runPromise(snapshotQuery.getSnapshot()),
      readPendingTurnStarts: () =>
        runtime!.runPromise(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return yield* sql<{ readonly threadId: string }>`
              SELECT thread_id AS "threadId"
              FROM projection_turns
              WHERE turn_id IS NULL AND state = 'pending'
            `;
          }),
        ),
      readCommandReceipt: (commandId: CommandId) =>
        runtime!.runPromise(
          Effect.gen(function* () {
            const receipts =
              yield* OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository;
            return yield* receipts.getByCommandId({ commandId });
          }),
        ),
      tryHandlePromptCommand,
      startSession,
      sendTurn,
      sentTurnId,
      compactThread,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      renameBranch,
      pruneWorktrees,
      createWorktree,
      refreshStatus,
      generateBranchName,
      generateThreadTitle,
      runtimeSessions,
      stateDir,
      drain,
      runEffect,
      sqlClient,
      emitProviderEvents,
      readWorkflowResumeRows,
      readDirectorRows,
      workflowStart,
      workflowDirector,
      beforeNextDirectorGitStatus: (effect: () => Effect.Effect<void>) => {
        beforeNextDirectorGitStatus = effect;
      },
      failNextDirectorGitStatus: () => {
        failNextDirectorGitStatus = true;
      },
      workflowMonitor,
      makeWorkflowMonitor,
      directorCapability,
      refreshDirectorCapability,
      directorTicket,
      directorTickets,
      directorImplementationHead,
      advanceDirectorImplementationHead: () => {
        directorImplementationHead = "b".repeat(40);
      },
      dispatchWorkflow,
      workflowAssignees,
      resolveDirectorTickets,
      holdDirectorTicket,
      readyDirectorTicket,
      get titleRegenerationCompletionDispatchAttempts() {
        return titleRegenerationCompletionDispatchAttempts;
      },
    };
  }

  effectIt.effect.each(["new", "ready", "stopped"] as const)(
    "handles sign-out for a %s thread before worktree repair, text helpers, or startup",
    (sessionStatus) =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("antigravity-personal");
        const handled = yield* Deferred.make<void>();
        const harness = yield* Effect.promise(() =>
          createHarness({
            ...(sessionStatus === "new"
              ? {}
              : {
                  threadModelSelection: { instanceId, model: "gemini-3.1-pro" },
                }),
            tryHandlePromptCommandEffect: () =>
              Deferred.succeed(handled, undefined).pipe(Effect.as(true)),
          }),
        );
        const threadId = ThreadId.make("thread-1");
        const createdAt = "2026-01-01T00:00:00.000Z";
        if (sessionStatus !== "new") {
          yield* harness.engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("cmd-sign-out-bound-session"),
            threadId,
            session: {
              threadId,
              providerInstanceId: instanceId,
              providerName: "antigravity",
              status: sessionStatus,
              runtimeMode: "approval-required",
              activeTurnId: null,
              lastError: null,
              updatedAt: createdAt,
            },
            createdAt,
          });
        }
        yield* harness.engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-sign-out-worktree"),
          threadId,
          title: "New thread",
          branch: "t3code/1234abcd",
          worktreePath: NodePath.join(harness.stateDir, "missing-worktree"),
        });

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-provider-sign-out"),
          threadId,
          message: {
            messageId: MessageId.make("message-provider-sign-out"),
            role: "user",
            text: "/logout",
            attachments: [],
          },
          modelSelection: {
            instanceId:
              sessionStatus === "new" ? instanceId : ProviderInstanceId.make("antigravity-other"),
            model: "gemini-3.1-pro",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        });
        yield* Deferred.await(handled);
        yield* Effect.promise(() => harness.drain());

        const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
          (entry) => entry.id === threadId,
        );
        expect(thread?.session).toMatchObject({
          status: "stopped",
          providerName: "antigravity",
          providerInstanceId: instanceId,
          activeTurnId: null,
          lastError: null,
        });
        expect(thread?.messages.map((message) => message.text)).toEqual(["/logout"]);
        expect(thread?.activities).toContainEqual(
          expect.objectContaining({ kind: "provider.auth.signed-out", tone: "info", turnId: null }),
        );
        expect(yield* Effect.promise(() => harness.readPendingTurnStarts())).toEqual([]);
        expect(harness.tryHandlePromptCommand).toHaveBeenCalledWith({
          instanceId,
          text: "/logout",
          hasAttachments: false,
        });
        expect(harness.pruneWorktrees).not.toHaveBeenCalled();
        expect(harness.createWorktree).not.toHaveBeenCalled();
        expect(harness.generateThreadTitle).not.toHaveBeenCalled();
        expect(harness.generateBranchName).not.toHaveBeenCalled();
        expect(harness.startSession).not.toHaveBeenCalled();
        expect(harness.sendTurn).not.toHaveBeenCalled();
      }),
  );

  effectIt.effect("clears a failed sign-out request without sending it as a prompt", () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("antigravity-personal");
      const handled = yield* Deferred.make<void>();
      const harness = yield* Effect.promise(() =>
        createHarness({
          threadModelSelection: { instanceId, model: "gemini-3.1-pro" },
          tryHandlePromptCommandEffect: () =>
            Deferred.succeed(handled, undefined).pipe(
              Effect.andThen(
                Effect.fail(
                  new ProviderSetupError({
                    instanceId,
                    operation: "logout",
                    detail: "The provider could not sign out. Try again.",
                  }),
                ),
              ),
            ),
        }),
      );
      const threadId = ThreadId.make("thread-1");

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-provider-sign-out-failed"),
        threadId,
        message: {
          messageId: MessageId.make("message-provider-sign-out-failed"),
          role: "user",
          text: "/logout",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      yield* Deferred.await(handled);
      yield* Effect.promise(() => harness.drain());

      const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === threadId,
      );
      expect(thread?.session).toMatchObject({
        status: "error",
        activeTurnId: null,
        lastError: expect.stringContaining("The provider could not sign out. Try again."),
      });
      expect(thread?.activities).toContainEqual(
        expect.objectContaining({ kind: "provider.turn.start.failed", tone: "error" }),
      );
      expect(
        thread?.activities.some((activity) => activity.kind === "provider.auth.signed-out"),
      ).toBe(false);
      expect(yield* Effect.promise(() => harness.readPendingTurnStarts())).toEqual([]);
      expect(harness.startSession).not.toHaveBeenCalled();
      expect(harness.sendTurn).not.toHaveBeenCalled();
    }),
  );

  effectIt.effect.each([
    { label: "a command mention", text: "What does /logout do?", attachments: [] },
    {
      label: "a command with an attachment",
      text: "/logout",
      attachments: [
        {
          type: "file" as const,
          id: "attached-notes",
          name: "notes.txt",
          mimeType: "text/plain",
          sizeBytes: 8,
        },
      ],
    },
    { label: "another provider's command", text: "/logout", attachments: [] },
  ])("sends $label when the provider auth handler leaves it unhandled", ({ text, attachments }) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const harness = yield* Effect.promise(() =>
        createHarness({
          startSessionEffect: (session) =>
            Deferred.succeed(started, undefined).pipe(Effect.as(session)),
        }),
      );

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-provider-command-unhandled"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: MessageId.make("message-provider-command-unhandled"),
          role: "user",
          text,
          attachments,
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      yield* Deferred.await(started);
      yield* Effect.promise(() => harness.drain());

      expect(harness.tryHandlePromptCommand).toHaveBeenCalledWith({
        instanceId: ProviderInstanceId.make("codex"),
        text,
        hasAttachments: attachments.length > 0,
      });
      expect(harness.sendTurn).toHaveBeenCalledWith(
        expect.objectContaining({
          input: text,
          ...(attachments.length > 0 ? { attachments } : {}),
        }),
      );
    }),
  );

  it("reacts to thread.turn.start by ensuring session and sending provider turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-1"),
          role: "user",
          text: "hello reactor",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[0]).toEqual(ThreadId.make("thread-1"));
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      cwd: "/tmp/provider-project",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "approval-required",
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.status).toBe("starting");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
  });

  effectIt.effect(
    "starts a capability director through the real orchestration reactor and records its receipt",
    () =>
      Effect.gen(function* () {
        const harness = yield* Effect.promise(() => createHarness());
        NodeFS.mkdirSync("/tmp/provider-project", { recursive: true });
        const started = yield* harness.workflowDirector.start(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            rootNumber: 10,
            capabilityNumber: 17,
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-6-astra",
              options: [{ id: "reasoningEffort", value: "high" }],
            },
          },
          harness.dispatchWorkflow,
        );
        yield* Effect.promise(() => harness.drain());
        const rows = yield* Effect.promise(() =>
          harness.readDirectorRows(started.director.directorId),
        );
        const receipt = yield* Effect.promise(() =>
          harness.readCommandReceipt(CommandId.make(rows[0]!.commandId)),
        );
        const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
          (candidate) => candidate.id === started.director.threadId,
        );

        expect(started).toMatchObject({ disposition: "started", director: { status: "active" } });
        expect(rows).toEqual([{ commandId: rows[0]!.commandId, status: "active" }]);
        expect(Option.getOrNull(receipt)).toMatchObject({
          commandId: rows[0]!.commandId,
          status: "accepted",
        });
        expect(thread).toMatchObject({
          id: started.director.threadId,
          worktreePath: started.director.worktreePath,
        });
        expect(harness.sendTurn).toHaveBeenCalledWith(
          expect.objectContaining({
            input: expect.stringContaining("Before every implementation delegation"),
            modelSelection: expect.objectContaining({ model: "gpt-6-astra" }),
          }),
        );
        expect(harness.sendTurn.mock.calls.at(-1)?.[0]).not.toHaveProperty("skills");
      }),
  );

  effectIt.effect(
    "rotates an eleven-ticket capability through the monitor and recovers the accepted first-turn receipt",
    () =>
      Effect.gen(function* () {
        const ownerPreflightEntered = yield* Deferred.make<void>();
        const releaseOwnerPreflight = yield* Deferred.make<void>();
        let blockNextDirectorIssueDetail = false;
        let beforeNextDirectorIssueDetail: (() => Effect.Effect<void>) | undefined;
        const harness = yield* Effect.promise(() =>
          createHarness({
            directorTicketCount: 11,
            beforeDirectorIssueDetail: () => {
              if (beforeNextDirectorIssueDetail) {
                const effect = beforeNextDirectorIssueDetail;
                beforeNextDirectorIssueDetail = undefined;
                return effect();
              }
              if (!blockNextDirectorIssueDetail) return Effect.void;
              blockNextDirectorIssueDetail = false;
              return Deferred.succeed(ownerPreflightEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseOwnerPreflight)),
              );
            },
          }),
        );
        NodeFS.mkdirSync("/tmp/provider-project", { recursive: true });
        const started = yield* harness.workflowDirector.start(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            rootNumber: 10,
            capabilityNumber: 17,
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-6-astra",
              options: [{ id: "reasoningEffort", value: "high" }],
            },
          },
          harness.dispatchWorkflow,
        );
        yield* Effect.promise(() => harness.drain());

        for (const [index, ticket] of harness.directorTickets.slice(0, 10).entries()) {
          const admission = yield* harness.workflowDirector.admit({
            projectId: ProjectId.make("project-1"),
            directorId: started.director.directorId,
            repository: "Flow-Fly/t3code",
            ticketNumber: ticket.number,
            purpose: index === 1 ? "retry" : index === 2 ? "review" : "implement",
            ownership: `delivery-${index + 1}`,
          });
          expect(admission).toMatchObject({ disposition: "admitted", admissionCount: index + 1 });
        }
        expect(
          yield* harness.workflowDirector.admit({
            projectId: ProjectId.make("project-1"),
            directorId: started.director.directorId,
            repository: "Flow-Fly/t3code",
            ticketNumber: harness.directorTickets[0]!.number,
            purpose: "retry",
            ownership: "delivery-1",
          }),
        ).toMatchObject({ disposition: "existing", admissionCount: 10 });
        expect(
          yield* harness.workflowDirector.admit({
            projectId: ProjectId.make("project-1"),
            directorId: started.director.directorId,
            repository: "Flow-Fly/t3code",
            ticketNumber: harness.directorTickets[10]!.number,
            purpose: "implement",
            ownership: "delivery-11",
          }),
        ).toMatchObject({ disposition: "limit-reached", admissionCount: 10 });

        const sourceWorker = yield* harness.workflowDirector.prepareWorker(
          EnvironmentId.make("workflow-environment"),
          started.director.threadId,
          ProviderInstanceId.make("codex"),
          {
            ticketNumber: harness.directorTickets[0]!.number,
            ownership: "source settlement worker",
            writePaths: ["apps/server/src/workflow"],
          },
        );
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "task.started",
              eventId: EventId.make("rotation-source-worker-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              createdAt: "2026-09-07T12:59:59.000Z",
              payload: {
                taskId: RuntimeTaskId.make("rotation-source-worker"),
                timelineBypass: true,
                nativeTurn: {
                  sessionId: "rotation-source-worker-session",
                  turnId: "rotation-source-worker-turn",
                  status: "running",
                },
              },
            },
          ]),
        );
        yield* harness.workflowDirector.associateWorker(
          EnvironmentId.make("workflow-environment"),
          started.director.threadId,
          ProviderInstanceId.make("codex"),
          {
            associationToken: sourceWorker.associationToken,
            providerThreadId: "rotation-source-worker",
          },
        );

        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-source-native-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-source-turn"),
              createdAt: "2026-09-07T13:00:00.000Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-source-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-source-native-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-source-turn"),
              createdAt: "2026-09-07T13:00:01.000Z",
              payload: { reason: "Interrupted for durable handoff." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-source-turn" },
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());

        const handoffInput = {
          lessons: ["Preserve exact native settlement evidence."],
          unresolvedContext: ["Ticket 28 is the next approved delivery slice."],
          suggestedSkills: ["implement", "code-review"],
          suggestedStaffing: ["Sol/high implementation", "Astra/medium review"],
        };
        const prepared = yield* harness.workflowDirector.prepareHandoff(
          EnvironmentId.make("workflow-environment"),
          started.director.threadId,
          ProviderInstanceId.make("codex"),
          handoffInput,
        );
        expect(
          yield* harness.workflowDirector.prepareHandoff(
            EnvironmentId.make("workflow-environment"),
            started.director.threadId,
            ProviderInstanceId.make("codex"),
            handoffInput,
          ),
        ).toMatchObject({ handoffId: prepared.handoffId, status: "waiting-settlement" });

        const heldForLiveSourceChild = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(heldForLiveSourceChild._tag).toBe("Failure");
        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "task.updated",
              eventId: EventId.make("rotation-source-worker-interrupted"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              createdAt: "2026-09-07T13:00:01.500Z",
              payload: {
                taskId: RuntimeTaskId.make("rotation-source-worker"),
                status: "interrupted",
                timelineBypass: true,
                nativeTurn: {
                  sessionId: "rotation-source-worker-session",
                  turnId: "rotation-source-worker-turn",
                  status: "interrupted",
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());

        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`
              INSERT INTO workflow_ticket_reviews (
                review_id, association_token, director_id, batch_id, admission_id,
                implementation_dispatch_id, repository, ticket_number, fixed_base,
                implementation_head, scope_body, requested_model, requested_effort,
                requested_skill_path, provider_thread_id, status, created_at, updated_at
              ) VALUES (
                'rotation-issued-review', 'rotation-issued-token',
                ${started.director.directorId}, ${started.director.batchId},
                ${sourceWorker.admission.admissionId}, ${sourceWorker.dispatchId},
                'Flow-Fly/t3code', ${harness.directorTickets[0]!.number}, ${"a".repeat(40)},
                ${harness.directorImplementationHead}, ${harness.directorTickets[0]!.body},
                'gpt-6-astra', 'medium', '/skills/code-review/SKILL.md', NULL,
                'spawn-issued', '2026-09-07T13:00:01.600Z', '2026-09-07T13:00:01.600Z'
              )
              `;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        const heldForIssuedReviewer = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(heldForIssuedReviewer._tag).toBe("Failure");
        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_ticket_reviews SET status = 'prepared'
                WHERE review_id = 'rotation-issued-review'`;
              yield* sql`INSERT INTO workflow_reassessments (
                reassessment_id, director_id, trigger_kind, trigger_issue_number, trigger_source,
                status, required_action, stop_request_status, tracker_status, created_at, updated_at
              ) VALUES (
                'rotation-active-reassessment', ${started.director.directorId}, 'scope-change', 17,
                'https://github.com/Flow-Fly/t3code/issues/17#reassessment', 'held',
                'Clear this reassessment before rotation.', 'not-issued', 'confirmed',
                '2026-09-07T13:00:01.650Z', '2026-09-07T13:00:01.650Z'
              )`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );

        const heldForCurrentReassessment = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(heldForCurrentReassessment._tag).toBe("Failure");
        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_reassessments SET status = 'cleared'
                WHERE reassessment_id = 'rotation-active-reassessment'`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );

        harness.resolveDirectorTickets(10);
        harness.holdDirectorTicket(10);
        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());
        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        for (const index of harness.directorTickets.keys()) harness.readyDirectorTicket(index);

        let rejectedCommand: Parameters<typeof harness.dispatchWorkflow>[0] | undefined;
        const unknownBeforeDispatch = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            (command) => {
              rejectedCommand = command;
              return Effect.fail(
                new OrchestrationDispatchCommandError({
                  message: "Simulated failure with no definitive engine receipt.",
                }),
              );
            },
          )
          .pipe(Effect.result);
        expect(unknownBeforeDispatch._tag).toBe("Failure");
        const pendingRejection = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        const rejectionIdentity = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly successorThreadId: string;
                readonly successorCommandId: string;
              }>`SELECT successor_thread_id AS "successorThreadId",
                successor_command_id AS "successorCommandId"
                FROM workflow_director_handoffs WHERE handoff_id = ${prepared.handoffId}`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        expect(pendingRejection).toMatchObject({
          status: "submitting",
          handoff: { status: "submitting" },
        });
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`INSERT INTO workflow_reassessments (
                reassessment_id, director_id, trigger_kind, trigger_issue_number, trigger_source,
                status, required_action, stop_request_status, tracker_status, created_at, updated_at
              ) VALUES (
                'rotation-pending-successor-reassessment', ${pendingRejection.directorId},
                'scope-change', 17,
                'https://github.com/Flow-Fly/t3code/issues/17#pending-successor-reassessment',
                'held', 'Clear before redispatch.', 'not-issued', 'confirmed',
                '2026-09-07T13:00:01.675Z', '2026-09-07T13:00:01.675Z'
              )`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        const pendingRedispatchHeldForReassessment = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(pendingRedispatchHeldForReassessment._tag).toBe("Failure");
        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_reassessments SET status = 'cleared'
                WHERE reassessment_id = 'rotation-pending-successor-reassessment'`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        let rejectedDuringFinalGit = false;
        harness.beforeNextDirectorGitStatus(() =>
          Effect.gen(function* () {
            rejectedDuringFinalGit = true;
            yield* harness.engine
              .dispatch({
                type: "thread.create",
                commandId: CommandId.make("rotation-conflicting-thread-create"),
                threadId: ThreadId.make(rejectionIdentity[0]!.successorThreadId),
                projectId: ProjectId.make("project-1"),
                title: "Conflicting successor thread",
                modelSelection: {
                  instanceId: ProviderInstanceId.make("codex"),
                  model: "gpt-6-astra",
                },
                runtimeMode: "approval-required",
                interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                branch: null,
                worktreePath: null,
                createdAt: "2026-09-07T13:00:01.700Z",
              })
              .pipe(Effect.orDie);
            const rejectedDispatch = yield* harness
              .dispatchWorkflow(rejectedCommand!)
              .pipe(Effect.result);
            expect(rejectedDispatch._tag).toBe("Failure");
            harness.failNextDirectorGitStatus();
          }),
        );
        const rejectedPreflight = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        yield* Effect.promise(() => harness.drain());
        expect(rejectedDuringFinalGit).toBe(true);
        expect(rejectedPreflight._tag).toBe("Failure");
        const rejectedCreate = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(rejectedCreate).toMatchObject({
          directorId: started.director.directorId,
          status: "waiting",
          handoff: { handoffId: prepared.handoffId, status: "held" },
        });
        const turnReceipt = yield* Effect.promise(() =>
          harness.readCommandReceipt(CommandId.make(rejectionIdentity[0]!.successorCommandId)),
        );
        const createReceipt = yield* Effect.promise(() =>
          harness.readCommandReceipt(
            WorkflowDirectorService.workflowSuccessorCreateCommandId(
              CommandId.make(rejectionIdentity[0]!.successorCommandId),
            ),
          ),
        );
        expect(Option.isNone(turnReceipt)).toBe(true);
        expect(Option.getOrNull(createReceipt)).toMatchObject({ status: "rejected" });
        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        expect(
          yield* harness.workflowDirector.prepareHandoff(
            EnvironmentId.make("workflow-environment"),
            started.director.threadId,
            ProviderInstanceId.make("codex"),
            handoffInput,
          ),
        ).toMatchObject({
          handoffId: prepared.handoffId,
          status: "waiting-settlement",
          successorDirectorId: null,
        });

        const stoppedBeforeDispatch = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            () =>
              Effect.fail(
                new OrchestrationDispatchCommandError({
                  message: "Simulated outage before engine dispatch.",
                  bootstrapTurnDisposition: "not-accepted",
                }),
              ),
          )
          .pipe(Effect.result);
        expect(stoppedBeforeDispatch._tag).toBe("Failure");
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-source-late-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-source-late-turn"),
              createdAt: "2026-09-07T13:00:02.000Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-source-late-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-source-late-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-source-late-turn"),
              createdAt: "2026-09-07T13:00:03.000Z",
              payload: { reason: "Late source activity invalidated the frozen handoff." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-source-late-turn" },
                },
              },
            },
          ]),
        );
        const heldForChangedSource = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(heldForChangedSource._tag).toBe("Success");
        if (heldForChangedSource._tag === "Success") {
          expect(heldForChangedSource.success).toMatchObject({
            directorId: started.director.directorId,
            status: "waiting",
          });
        }
        const recoveredHandoff = yield* harness.workflowDirector.prepareHandoff(
          EnvironmentId.make("workflow-environment"),
          started.director.threadId,
          ProviderInstanceId.make("codex"),
          handoffInput,
        );
        expect(recoveredHandoff).toMatchObject({
          handoffId: prepared.handoffId,
          status: "waiting-settlement",
          successorDirectorId: null,
        });

        let acceptedCommand: Parameters<typeof harness.dispatchWorkflow>[0] | undefined;
        const pendingAccepted = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            (command) => {
              acceptedCommand = command;
              return Effect.fail(
                new OrchestrationDispatchCommandError({
                  message: "Simulated unknown transport outcome before engine acceptance.",
                }),
              );
            },
          )
          .pipe(Effect.result);
        expect(pendingAccepted._tag).toBe("Failure");
        let acceptedDuringCapabilityPreflight = false;
        beforeNextDirectorIssueDetail = () =>
          Effect.gen(function* () {
            acceptedDuringCapabilityPreflight = true;
            yield* harness.dispatchWorkflow(acceptedCommand!).pipe(Effect.orDie);
            yield* Effect.promise(() =>
              harness.runEffect(
                Effect.gen(function* () {
                  const sql = yield* SqlClient.SqlClient;
                  yield* sql`INSERT INTO workflow_reassessments (
                          reassessment_id, director_id, trigger_kind, trigger_issue_number,
                          trigger_source, status, required_action, stop_request_status,
                          tracker_status, created_at, updated_at
                        ) SELECT
                          'rotation-accepted-receipt-reassessment', director_id, 'scope-change', 17,
                          'https://github.com/Flow-Fly/t3code/issues/17#accepted-receipt-reassessment',
                          'held', 'Preserve this hold while reconciling the accepted receipt.',
                          'not-issued', 'confirmed', '2026-09-07T13:00:03.500Z',
                          '2026-09-07T13:00:03.500Z'
                        FROM workflow_directors WHERE thread_id = ${acceptedCommand!.threadId}`;
                  yield* sql`UPDATE workflow_directors SET status = 'held',
                          detail = 'Durable reassessment hold.'
                          WHERE thread_id = ${acceptedCommand!.threadId}`;
                }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
              ),
            );
            harness.refreshDirectorCapability("closed");
          });
        const acceptedAfterFailedPreflight = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(acceptedDuringCapabilityPreflight).toBe(true);
        expect(acceptedAfterFailedPreflight._tag).toBe("Success");
        harness.refreshDirectorCapability("open");
        yield* Effect.promise(() => harness.drain());
        const lateAcceptedReceipt = yield* Effect.promise(() =>
          harness.readCommandReceipt(acceptedCommand!.commandId),
        );
        expect(Option.getOrNull(lateAcceptedReceipt)?.status).toBe("accepted");
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
        const acceptedDuringReassessment = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(acceptedDuringReassessment).toMatchObject({
          threadId: acceptedCommand!.threadId,
          status: "held",
          reassessment: { reassessmentId: "rotation-accepted-receipt-reassessment" },
          handoff: { handoffId: prepared.handoffId, status: "submitted" },
        });
        const heldReceiptRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly status: string;
                readonly initialTurnDisposition: string;
                readonly detail: string;
              }>`SELECT status, initial_turn_disposition AS "initialTurnDisposition", detail
                FROM workflow_directors WHERE director_id = ${acceptedDuringReassessment.directorId}`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        expect({ ...heldReceiptRows[0] }).toEqual({
          status: "held",
          initialTurnDisposition: "accepted",
          detail: "Durable reassessment hold.",
        });
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_reassessments SET status = 'cleared'
                WHERE reassessment_id = 'rotation-accepted-receipt-reassessment'`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        harness.advanceDirectorImplementationHead();

        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());

        const rotated = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(rotated).toMatchObject({
          status: "active",
          admissionCount: 0,
          requestedProfile: { model: "gpt-6-astra", effort: "high" },
          worktreePath: started.director.worktreePath,
          handoff: {
            handoffId: prepared.handoffId,
            status: "submitted",
            sourceDirectorId: started.director.directorId,
            implementationHead: harness.directorImplementationHead,
            admissionCount: 10,
          },
        });
        expect(rotated.directorId).not.toBe(started.director.directorId);
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
        expect(harness.sendTurn.mock.calls.at(-1)?.[0]).toMatchObject({
          input: expect.stringContaining("Reread current GitHub tracker authority"),
          modelSelection: expect.objectContaining({ model: "gpt-6-astra" }),
        });

        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_directors SET status = 'held'
                WHERE director_id = ${rotated.directorId}`;
              yield* sql`UPDATE workflow_director_handoffs SET status = 'held'
                WHERE handoff_id = ${prepared.handoffId}`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        const recoveredAcceptedHandoff = yield* harness.workflowDirector.reconcileHandoff(
          EnvironmentId.make("workflow-environment"),
          rotated.threadId,
          ProviderInstanceId.make("codex"),
          {
            handoffId: prepared.handoffId,
            summary: "Recovered the accepted successor without changing its identity.",
          },
        );
        expect(recoveredAcceptedHandoff).toMatchObject({
          status: "submitting",
          successorDirectorId: rotated.directorId,
          latestReconciliation: {
            acknowledgedByDirectorId: rotated.directorId,
            summary: "Recovered the accepted successor without changing its identity.",
          },
        });
        const finalizedAcceptedHandoff = yield* harness.workflowDirector.rotateReady(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            capabilityNumber: 17,
          },
          harness.dispatchWorkflow,
        );
        expect(finalizedAcceptedHandoff).toMatchObject({
          directorId: rotated.directorId,
          status: "active",
          handoff: { handoffId: prepared.handoffId, status: "submitted" },
        });
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);

        const originalHandoffRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{ readonly settlementsJson: string }>`
                SELECT settlements_json AS "settlementsJson"
                FROM workflow_director_handoffs WHERE handoff_id = ${prepared.handoffId}
              `;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-retired-source-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-retired-source-turn"),
              createdAt: "2026-09-07T13:00:04.000Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-retired-source-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-retired-source-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-retired-source-turn"),
              createdAt: "2026-09-07T13:00:05.000Z",
              payload: { reason: "Retired source activity needs explicit acknowledgement." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-retired-source-turn" },
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());
        const blockedByRetiredActivity = yield* harness.workflowDirector
          .admit({
            projectId: ProjectId.make("project-1"),
            directorId: rotated.directorId,
            repository: "Flow-Fly/t3code",
            ticketNumber: harness.directorTickets[10]!.number,
            purpose: "implement",
            ownership: "delivery-11",
          })
          .pipe(Effect.result);
        expect(blockedByRetiredActivity._tag).toBe("Failure");
        const reconciledHandoff = yield* harness.workflowDirector.reconcileHandoff(
          EnvironmentId.make("workflow-environment"),
          rotated.threadId,
          ProviderInstanceId.make("codex"),
          {
            handoffId: prepared.handoffId,
            summary: "Acknowledged the retired source's later terminal turn.",
          },
        );
        expect(reconciledHandoff).toMatchObject({
          handoffId: prepared.handoffId,
          latestReconciliation: {
            acknowledgedByDirectorId: rotated.directorId,
            summary: "Acknowledged the retired source's later terminal turn.",
          },
        });
        const reconciledRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly settlementsJson: string;
                readonly reconciliationCount: number;
              }>`SELECT h.settlements_json AS "settlementsJson",
                (SELECT count(*) FROM workflow_director_handoff_reconciliations r
                  WHERE r.handoff_id = h.handoff_id) AS "reconciliationCount"
                FROM workflow_director_handoffs h WHERE h.handoff_id = ${prepared.handoffId}`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        expect({ ...reconciledRows[0] }).toEqual({
          settlementsJson: originalHandoffRows[0]!.settlementsJson,
          reconciliationCount: 2,
        });

        const continued = yield* harness.workflowDirector.admit({
          projectId: ProjectId.make("project-1"),
          directorId: rotated.directorId,
          repository: "Flow-Fly/t3code",
          ticketNumber: harness.directorTickets[10]!.number,
          purpose: "implement",
          ownership: "delivery-11",
        });
        expect(continued).toMatchObject({ disposition: "admitted", admissionCount: 1 });
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-retired-source-newer-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-retired-source-newer-turn"),
              createdAt: "2026-09-07T13:00:06.000Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-retired-source-newer-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-retired-source-newer-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-retired-source-newer-turn"),
              createdAt: "2026-09-07T13:00:07.000Z",
              payload: { reason: "A newer retired source turn invalidates the acknowledgement." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-retired-source-newer-turn" },
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());
        const reviewBlockedByNewerActivity = yield* harness.workflowDirector
          .prepareTicketReview(
            EnvironmentId.make("workflow-environment"),
            rotated.threadId,
            ProviderInstanceId.make("codex"),
            {
              ticketNumber: harness.directorTickets[10]!.number,
              implementationProviderThreadId: "not-reached-before-predecessor-gate",
              fixedBase: "a".repeat(40),
              implementationHead: harness.directorImplementationHead,
              checks: [{ label: "focused", command: "vp test run workflow" }],
            },
          )
          .pipe(Effect.result);
        expect(reviewBlockedByNewerActivity._tag).toBe("Failure");
        if (reviewBlockedByNewerActivity._tag === "Failure") {
          expect(reviewBlockedByNewerActivity.failure).toMatchObject({ failure: "not-ready" });
          expect(reviewBlockedByNewerActivity.failure.message).toContain(
            "predecessor director activity",
          );
        }
        const secondReconciliation = yield* harness.workflowDirector.reconcileHandoff(
          EnvironmentId.make("workflow-environment"),
          rotated.threadId,
          ProviderInstanceId.make("codex"),
          {
            handoffId: prepared.handoffId,
            summary: "Acknowledged the newer terminal source turn.",
          },
        );
        expect(secondReconciliation.latestReconciliation).toMatchObject({
          acknowledgedByDirectorId: rotated.directorId,
          summary: "Acknowledged the newer terminal source turn.",
        });
        expect(secondReconciliation.latestReconciliation!.sequence).toBeGreaterThan(
          reconciledHandoff.latestReconciliation!.sequence,
        );
        const reviewAfterReconciliation = yield* harness.workflowDirector
          .prepareTicketReview(
            EnvironmentId.make("workflow-environment"),
            rotated.threadId,
            ProviderInstanceId.make("codex"),
            {
              ticketNumber: harness.directorTickets[10]!.number,
              implementationProviderThreadId: "not-reached-before-predecessor-gate",
              fixedBase: "a".repeat(40),
              implementationHead: harness.directorImplementationHead,
              checks: [{ label: "focused", command: "vp test run workflow" }],
            },
          )
          .pipe(Effect.result);
        expect(reviewAfterReconciliation._tag).toBe("Failure");
        if (reviewAfterReconciliation._tag === "Failure") {
          expect(reviewAfterReconciliation.failure).toMatchObject({ failure: "review-incomplete" });
        }
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_director_handoff_reconciliations
                SET created_at = '2026-09-07T13:00:08.000Z'
                WHERE handoff_id = ${prepared.handoffId}`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        const equalTimeStatus = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(equalTimeStatus.handoff?.latestReconciliation).toMatchObject({
          sequence: secondReconciliation.latestReconciliation!.sequence,
          summary: "Acknowledged the newer terminal source turn.",
          createdAt: "2026-09-07T13:00:08.000Z",
        });
        const durableRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly currentCount: number;
                readonly directorCount: number;
                readonly handoffCount: number;
                readonly reconciliationCount: number;
              }>`SELECT
                (SELECT count(*) FROM workflow_directors WHERE is_current = 1) AS "currentCount",
                (SELECT count(*) FROM workflow_directors WHERE capability_number = 17) AS "directorCount",
                (SELECT count(*) FROM workflow_director_handoffs WHERE status = 'submitted') AS "handoffCount",
                (SELECT count(*) FROM workflow_director_handoff_reconciliations) AS "reconciliationCount"`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        expect({ ...durableRows[0] }).toEqual({
          currentCount: 1,
          directorCount: 4,
          handoffCount: 1,
          reconciliationCount: 3,
        });

        for (const [index, ticket] of harness.directorTickets.slice(0, 9).entries()) {
          const successorAdmission = yield* harness.workflowDirector.admit({
            projectId: ProjectId.make("project-1"),
            directorId: rotated.directorId,
            repository: "Flow-Fly/t3code",
            ticketNumber: ticket.number,
            purpose: "retry",
            ownership: `successor-delivery-${index + 1}`,
          });
          expect(successorAdmission).toMatchObject({
            disposition: "admitted",
            admissionCount: index + 2,
          });
          if (index === 0) {
            expect(successorAdmission.admission).toMatchObject({
              claimLogin: "Flow-Fly",
              claimStatus: "confirmed",
            });
          }
        }
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-successor-native-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: rotated.threadId,
              turnId: TurnId.make("rotation-successor-turn"),
              createdAt: "2026-09-07T13:01:00.000Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-successor-session",
                  turn: { id: "rotation-successor-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-successor-native-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: rotated.threadId,
              turnId: TurnId.make("rotation-successor-turn"),
              createdAt: "2026-09-07T13:01:01.000Z",
              payload: { reason: "Second durable handoff." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-successor-session",
                  turn: { id: "rotation-successor-turn" },
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());
        yield* harness.workflowDirector.prepareHandoff(
          EnvironmentId.make("workflow-environment"),
          rotated.threadId,
          ProviderInstanceId.make("codex"),
          {
            ...handoffInput,
            lessons: ["The incoming handoff must not mask this outgoing handoff."],
          },
        );
        const pendingSecond = yield* harness.workflowDirector
          .rotateReady(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
            },
            () =>
              Effect.fail(
                new OrchestrationDispatchCommandError({
                  message: "Simulated unknown transport outcome before engine acceptance.",
                }),
              ),
          )
          .pipe(Effect.result);
        expect(pendingSecond._tag).toBe("Failure");
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
        let injectedDuringFinalGit = false;
        harness.beforeNextDirectorGitStatus(() =>
          Effect.gen(function* () {
            injectedDuringFinalGit = true;
            yield* Effect.promise(() =>
              harness.emitProviderEvents([
                {
                  type: "task.updated",
                  eventId: EventId.make("rotation-source-worker-restarted-during-final-git"),
                  provider: ProviderDriverKind.make("codex"),
                  providerInstanceId: ProviderInstanceId.make("codex"),
                  threadId: started.director.threadId,
                  createdAt: "2026-09-07T13:01:02.100Z",
                  payload: {
                    taskId: RuntimeTaskId.make("rotation-source-worker"),
                    status: "running",
                    timelineBypass: true,
                    nativeTurn: {
                      sessionId: "rotation-source-worker-restarted-session",
                      turnId: "rotation-source-worker-restarted-turn",
                      status: "running",
                    },
                  },
                },
              ]),
            );
          }),
        );
        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());
        expect(injectedDuringFinalGit).toBe(true);
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
        const heldPending = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(heldPending.status).toBe("submitting");
        expect(heldPending.handoffRecoveryTargets).toHaveLength(1);
        expect(heldPending.handoffRecoveryTargets?.[0]).toMatchObject({
          handoffId: prepared.handoffId,
          sourceBatchNumber: expect.any(Number),
          rootSettlement: { status: "interrupted" },
          childSettlements: { observedCount: 1, settledCount: 0 },
        });
        const pendingShell = yield* harness.snapshotQuery.getThreadShellById(heldPending.threadId);
        expect(Option.isNone(pendingShell)).toBe(true);
        const pendingDirectorRows = yield* Effect.promise(() =>
          harness.readDirectorRows(heldPending.directorId),
        );
        const pendingCommandId = pendingDirectorRows[0]?.commandId;
        const runningTarget = heldPending.handoffRecoveryTargets![0]!;
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "task.updated",
              eventId: EventId.make("rotation-source-worker-restarted-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              createdAt: "2026-09-07T13:01:03.100Z",
              payload: {
                taskId: RuntimeTaskId.make("rotation-source-worker"),
                status: "interrupted",
                timelineBypass: true,
                nativeTurn: {
                  sessionId: "rotation-source-worker-restarted-session",
                  turnId: "rotation-source-worker-restarted-turn",
                  status: "interrupted",
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());
        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
        const staleRecovery = yield* harness.workflowDirector
          .reconcileHandoffAsOwner(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
              expectedDirectorId: heldPending.directorId,
              expectedObservation: heldPending.observation,
              handoffId: runningTarget.handoffId,
              expectedTargetObservation: runningTarget.targetObservation,
              summary: "This running observation is stale after terminal evidence arrived.",
            },
            "owner-session-subject",
          )
          .pipe(Effect.result);
        expect(staleRecovery._tag).toBe("Failure");
        const terminalPending = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(terminalPending.handoffRecoveryTargets).toHaveLength(1);
        const terminalTarget = terminalPending.handoffRecoveryTargets!.find(
          (target) => target.handoffId === prepared.handoffId,
        )!;
        expect(terminalTarget.rootSettlement?.status).toBe("interrupted");
        expect(terminalTarget.childSettlements).toEqual({ observedCount: 1, settledCount: 1 });
        const wrongProjectRecovery = yield* harness.workflowDirector
          .reconcileHandoffAsOwner(
            {
              projectId: ProjectId.make("another-project"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
              expectedDirectorId: terminalPending.directorId,
              expectedObservation: terminalPending.observation,
              handoffId: terminalTarget.handoffId,
              expectedTargetObservation: terminalTarget.targetObservation,
              summary: "A different project cannot acknowledge this handoff.",
            },
            "owner-session-subject",
          )
          .pipe(Effect.result);
        expect(wrongProjectRecovery._tag).toBe("Failure");
        blockNextDirectorIssueDetail = true;
        const preflightRecovery = yield* harness.workflowDirector
          .reconcileHandoffAsOwner(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
              expectedDirectorId: terminalPending.directorId,
              expectedObservation: terminalPending.observation,
              handoffId: terminalTarget.handoffId,
              expectedTargetObservation: terminalTarget.targetObservation,
              summary: "Native evidence must remain the exact evidence reviewed by the owner.",
            },
            "owner-session-subject",
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(ownerPreflightEntered);
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-owner-preflight-source-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-owner-preflight-source-turn"),
              createdAt: "2026-09-07T13:01:03.100Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-owner-preflight-source-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-owner-preflight-source-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-owner-preflight-source-turn"),
              createdAt: "2026-09-07T13:01:03.200Z",
              payload: { reason: "Native evidence changed during owner preflight." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-owner-preflight-source-turn" },
                },
              },
            },
          ]),
        );
        yield* Deferred.succeed(releaseOwnerPreflight, undefined);
        const preflightRecoveryResult = yield* Fiber.join(preflightRecovery).pipe(Effect.result);
        expect(preflightRecoveryResult._tag).toBe("Failure");
        const refreshedPending = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        const refreshedTarget = refreshedPending.handoffRecoveryTargets!.find(
          (target) => target.handoffId === prepared.handoffId,
        )!;
        const recoveredPending = yield* harness.workflowDirector.reconcileHandoffAsOwner(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            capabilityNumber: 17,
            expectedDirectorId: refreshedPending.directorId,
            expectedObservation: refreshedPending.observation,
            handoffId: refreshedTarget.handoffId,
            expectedTargetObservation: refreshedTarget.targetObservation,
            summary: "The owner acknowledged the oldest predecessor's terminal evidence.",
          },
          "owner-session-subject",
        );
        expect(recoveredPending.handoffRecoveryTargets).toEqual([]);
        let projectedSourceDuringFinalGit = false;
        harness.beforeNextDirectorGitStatus(() =>
          Effect.gen(function* () {
            projectedSourceDuringFinalGit = true;
            yield* harness.engine
              .dispatch({
                type: "thread.turn.start",
                commandId: CommandId.make("rotation-source-projected-during-final-git"),
                threadId: rotated.threadId,
                message: {
                  messageId: MessageId.make("rotation-source-projected-message"),
                  role: "user",
                  text: "New source work during succession preflight",
                  attachments: [],
                },
                modelSelection: {
                  instanceId: ProviderInstanceId.make("codex"),
                  model: "gpt-6-astra",
                },
                runtimeMode: "approval-required",
                interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                createdAt: "2026-09-07T13:01:03.300Z",
              })
              .pipe(Effect.orDie);
          }),
        );
        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());
        expect(projectedSourceDuringFinalGit).toBe(true);
        expect(harness.sendTurn).toHaveBeenCalledTimes(3);
        const projectedSourcePending = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(projectedSourcePending.directorId).toBe(heldPending.directorId);
        expect(projectedSourcePending.handoffRecoveryTargets).toEqual([]);
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-projected-source-native-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: rotated.threadId,
              turnId: TurnId.make("turn-3"),
              createdAt: "2026-09-07T13:01:03.400Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-successor-session",
                  turn: { id: "turn-3" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-projected-source-native-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: rotated.threadId,
              turnId: TurnId.make("turn-3"),
              createdAt: "2026-09-07T13:01:03.500Z",
              payload: { reason: "The projected source turn settled before owner recovery." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-successor-session",
                  turn: { id: "turn-3" },
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());
        expect(harness.sendTurn).toHaveBeenCalledTimes(3);
        const terminalSourcePending = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(terminalSourcePending.handoffRecoveryTargets).toHaveLength(1);
        const immediateSourceTarget = terminalSourcePending.handoffRecoveryTargets![0]!;
        expect(immediateSourceTarget.handoffId).not.toBe(prepared.handoffId);
        const fullyRecoveredPending = yield* harness.workflowDirector.reconcileHandoffAsOwner(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            capabilityNumber: 17,
            expectedDirectorId: terminalSourcePending.directorId,
            expectedObservation: terminalSourcePending.observation,
            handoffId: immediateSourceTarget.handoffId,
            expectedTargetObservation: immediateSourceTarget.targetObservation,
            summary: "The owner acknowledged the immediate source's terminal evidence.",
          },
          "owner-session-subject",
        );
        expect(fullyRecoveredPending.handoffRecoveryTargets).toEqual([]);
        const ownerReconciliations = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly actorKind: string;
                readonly actorSubject: string;
              }>`SELECT actor_kind AS "actorKind", actor_subject AS "actorSubject"
                FROM workflow_director_handoff_reconciliations
                WHERE handoff_id = ${prepared.handoffId} ORDER BY sequence DESC LIMIT 1`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        expect({ ...ownerReconciliations[0] }).toEqual({
          actorKind: "owner-session",
          actorSubject: "owner-session-subject",
        });
        yield* Effect.promise(() => harness.makeWorkflowMonitor());
        yield* Effect.promise(() => harness.drain());
        const third = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(third).toMatchObject({ status: "active", admissionCount: 0 });
        expect(third.directorId).toBe(heldPending.directorId);
        const submittedDirectorRows = yield* Effect.promise(() =>
          harness.readDirectorRows(third.directorId),
        );
        expect(submittedDirectorRows[0]?.commandId).toBe(pendingCommandId);
        expect(harness.sendTurn).toHaveBeenCalledTimes(4);
        const repeatedRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly currentCount: number;
                readonly directorCount: number;
                readonly handoffCount: number;
              }>`SELECT
                (SELECT count(*) FROM workflow_directors WHERE is_current = 1) AS "currentCount",
                (SELECT count(*) FROM workflow_directors WHERE capability_number = 17) AS "directorCount",
                (SELECT count(*) FROM workflow_director_handoffs WHERE status = 'submitted') AS "handoffCount"`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        expect({ ...repeatedRows[0] }).toEqual({
          currentCount: 1,
          directorCount: 5,
          handoffCount: 2,
        });
        harness.workflowAssignees.clear();
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("rotation-oldest-source-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-oldest-source-turn"),
              createdAt: "2026-09-07T13:01:04.000Z",
              payload: {},
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-oldest-source-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("rotation-oldest-source-settled"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("rotation-oldest-source-turn"),
              createdAt: "2026-09-07T13:01:05.000Z",
              payload: { reason: "The oldest predecessor changed after another rotation." },
              raw: {
                source: "codex.app-server.notification",
                payload: {
                  threadId: "rotation-source-session",
                  turn: { id: "rotation-oldest-source-turn" },
                },
              },
            },
          ]),
        );
        yield* Effect.promise(() => harness.drain());
        const olderPredecessorBlocked = yield* harness.workflowDirector
          .prepareWorker(
            EnvironmentId.make("workflow-environment"),
            third.threadId,
            ProviderInstanceId.make("codex"),
            {
              ticketNumber: harness.directorTickets[10]!.number,
              ownership: "post-recovery worker",
              writePaths: ["apps/server/src/workflow"],
            },
          )
          .pipe(Effect.result);
        expect(olderPredecessorBlocked._tag).toBe("Failure");
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`INSERT INTO workflow_reassessments (
                reassessment_id, director_id, trigger_kind, trigger_issue_number, trigger_source,
                status, required_action, stop_request_status, tracker_status, created_at, updated_at
              ) VALUES (
                'rotation-historical-reassessment', ${started.director.directorId},
                'scope-change', 17,
                'https://github.com/Flow-Fly/t3code/issues/17#historical-reassessment', 'held',
                'Clear historical reassessment before acknowledgement.', 'not-issued', 'confirmed',
                '2026-09-07T13:01:05.500Z', '2026-09-07T13:01:05.500Z'
              )`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        const reconciliationBlockedByHistoricalReassessment = yield* harness.workflowDirector
          .reconcileHandoff(
            EnvironmentId.make("workflow-environment"),
            third.threadId,
            ProviderInstanceId.make("codex"),
            {
              handoffId: prepared.handoffId,
              summary: "This cannot acknowledge evidence while reassessment is active.",
            },
          )
          .pipe(Effect.result);
        expect(reconciliationBlockedByHistoricalReassessment._tag).toBe("Failure");
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`UPDATE workflow_reassessments SET status = 'cleared'
                WHERE reassessment_id = 'rotation-historical-reassessment'`;
            }).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlClient)),
          ),
        );
        const reconciledOlderPredecessor = yield* harness.workflowDirector.reconcileHandoff(
          EnvironmentId.make("workflow-environment"),
          third.threadId,
          ProviderInstanceId.make("codex"),
          {
            handoffId: prepared.handoffId,
            summary: "The current director acknowledged its oldest predecessor's terminal turn.",
          },
        );
        expect(reconciledOlderPredecessor.latestReconciliation).toMatchObject({
          acknowledgedByDirectorId: third.directorId,
          summary: "The current director acknowledged its oldest predecessor's terminal turn.",
        });
        const delegatedAfterRecovery = yield* harness.workflowDirector.prepareWorker(
          EnvironmentId.make("workflow-environment"),
          third.threadId,
          ProviderInstanceId.make("codex"),
          {
            ticketNumber: harness.directorTickets[10]!.number,
            ownership: "post-recovery worker",
            writePaths: ["apps/server/src/workflow"],
          },
        );
        expect(delegatedAfterRecovery).toMatchObject({
          disposition: "prepared",
          admission: { ticketNumber: harness.directorTickets[10]!.number },
        });
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "task.started",
              eventId: EventId.make("rotation-post-recovery-worker-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: third.threadId,
              createdAt: "2026-09-07T13:01:59.000Z",
              payload: {
                taskId: RuntimeTaskId.make("rotation-post-recovery-worker"),
                timelineBypass: true,
                nativeTurn: {
                  sessionId: "rotation-post-recovery-session",
                  turnId: "rotation-post-recovery-turn",
                  status: "running",
                },
              },
            },
          ]),
        );
        yield* harness.workflowDirector.associateWorker(
          EnvironmentId.make("workflow-environment"),
          third.threadId,
          ProviderInstanceId.make("codex"),
          {
            associationToken: delegatedAfterRecovery.associationToken,
            providerThreadId: "rotation-post-recovery-worker",
          },
        );
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "task.completed",
              eventId: EventId.make("rotation-post-recovery-worker-closed"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: third.threadId,
              createdAt: "2026-09-07T13:02:00.000Z",
              payload: {
                taskId: RuntimeTaskId.make("rotation-post-recovery-worker"),
                status: "completed",
                timelineBypass: true,
                nativeLifecycle: "closed",
                nativeTurn: {
                  sessionId: "rotation-post-recovery-session",
                  turnId: "rotation-post-recovery-turn",
                  status: "completed",
                },
              },
            },
          ]),
        );
        harness.resolveDirectorTickets();
        const completionInput = {
          resultingHead: "b".repeat(40),
          checks: [{ label: "combined", command: "vp test run workflow-rotation.test.ts" }],
          receipts: [],
        };
        const registeredCompletion = yield* harness.workflowDirector.completeCapability(
          EnvironmentId.make("workflow-environment"),
          third.threadId,
          ProviderInstanceId.make("codex"),
          completionInput,
        );
        expect(registeredCompletion).toMatchObject({
          disposition: "pending",
          completion: { status: "checks-pending" },
        });
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "item.started",
              eventId: EventId.make("rotation-combined-check-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: third.threadId,
              itemId: RuntimeItemId.make("rotation-combined-check"),
              createdAt: "2099-09-07T13:03:00.000Z",
              payload: {
                itemType: "command_execution",
                status: "inProgress",
                data: {
                  item: {
                    type: "commandExecution",
                    command: "vp test run workflow-rotation.test.ts",
                    cwd: third.worktreePath,
                  },
                },
              },
            },
            {
              type: "item.completed",
              eventId: EventId.make("rotation-combined-check-completed"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: third.threadId,
              itemId: RuntimeItemId.make("rotation-combined-check"),
              createdAt: "2099-09-07T13:03:01.000Z",
              payload: {
                itemType: "command_execution",
                status: "completed",
                data: {
                  item: {
                    type: "commandExecution",
                    command: "vp test run workflow-rotation.test.ts",
                    cwd: third.worktreePath,
                    status: "completed",
                    exitCode: 0,
                    aggregatedOutput: "combined rotation acceptance passed",
                  },
                },
              },
            },
          ]),
        );
        const completedCapability = yield* harness.workflowDirector.completeCapability(
          EnvironmentId.make("workflow-environment"),
          third.threadId,
          ProviderInstanceId.make("codex"),
          {
            ...completionInput,
            receipts: [{ label: "combined", toolCallId: "rotation-combined-check" }],
          },
        );
        expect(completedCapability).toMatchObject({
          disposition: "completed",
          completion: { status: "completed", authority: "current", resultingHead: "b".repeat(40) },
        });
      }),
  );

  effectIt.effect(
    "recovers reassessment through the real reactor without clearing projected native activity",
    () =>
      Effect.gen(function* () {
        const harness = yield* Effect.promise(() => createHarness());
        NodeFS.mkdirSync("/tmp/provider-project", { recursive: true });
        const started = yield* harness.workflowDirector.start(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            rootNumber: 10,
            capabilityNumber: 17,
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-6-astra",
              options: [{ id: "reasoningEffort", value: "high" }],
            },
          },
          harness.dispatchWorkflow,
        );
        yield* Effect.promise(() => harness.drain());
        const now = "2026-09-07T12:00:00.000Z";
        yield* harness.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("unrelated-capability-create"),
          threadId: ThreadId.make("unrelated-capability"),
          projectId: ProjectId.make("project-1"),
          title: "Unrelated capability",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-6-astra",
            options: [{ id: "reasoningEffort", value: "high" }],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: null,
          worktreePath: null,
          createdAt: now,
        });
        yield* harness.engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("unrelated-capability-session"),
          threadId: ThreadId.make("unrelated-capability"),
          session: {
            threadId: ThreadId.make("unrelated-capability"),
            status: "running",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: TurnId.make("unrelated-turn"),
            lastError: null,
            updatedAt: now,
          },
          createdAt: now,
        });
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`INSERT INTO workflow_director_admissions (
              admission_id,director_id,batch_id,repository,ticket_id,ticket_number,
              slot_ticket_number,purpose,ownership,claim_status,scope_body,scope_fingerprint,
              current_scope_body,current_scope_fingerprint,created_at,updated_at
            ) SELECT 'admission-reassess',director_id,batch_id,repository,'ticket-100',100,
              100,'implement','worker','confirmed',${harness.directorTicket.body},
              ${workflowEvidenceBodyFingerprint(harness.directorTicket.body)},
              ${harness.directorTicket.body},
              ${workflowEvidenceBodyFingerprint(harness.directorTicket.body)},${now},${now}
              FROM workflow_directors WHERE director_id = ${started.director.directorId}`;
              yield* sql`INSERT INTO workflow_worker_dispatches (
              dispatch_id,association_token,director_id,batch_id,admission_id,repository,
              ticket_number,ownership,write_paths_json,requested_model,requested_effort,
              requested_skill_path,provider_thread_id,status,created_at,updated_at
            ) SELECT 'dispatch-reassess','association-reassess',director_id,batch_id,
              'admission-reassess',repository,100,'worker','[]','gpt-5.6-sol','high',
              '/skills/implement/SKILL.md','native-worker','associated',${now},${now}
              FROM workflow_directors WHERE director_id = ${started.director.directorId}`;
              yield* sql`INSERT INTO workflow_ticket_reviews (
              review_id,association_token,director_id,batch_id,admission_id,
              implementation_dispatch_id,repository,ticket_number,fixed_base,
              implementation_head,scope_body,requested_model,requested_effort,
              requested_skill_path,provider_thread_id,status,created_at,updated_at
            ) SELECT 'review-reassess','review-association',director_id,batch_id,
              'admission-reassess','dispatch-reassess',repository,100,'base','head','scope',
              'gpt-6-astra','medium','/skills/code-review/SKILL.md','native-reviewer',
              'associated',${now},${now}
              FROM workflow_directors WHERE director_id = ${started.director.directorId}`;
              for (const child of ["native-worker", "native-reviewer"]) {
                yield* sql`INSERT INTO workflow_worker_observations (
                director_id,provider_thread_id,provider_status,last_event_kind,
                first_observed_at,updated_at
              ) VALUES (${started.director.directorId},${child},'running','task.started',${now},${now})`;
              }
            }),
          ),
        );
        const approvedCapability = structuredClone(harness.directorCapability);
        const changedApproval = harness.directorCapability.evidence!.records.map((record) =>
          record.kind === "approval" && record.approvalKind === "specification"
            ? { ...record, scope: "changed" as const }
            : record,
        );
        const invalidated = {
          ...harness.directorCapability,
          evidence: { ...harness.directorCapability.evidence!, records: changedApproval },
        };
        Object.assign(harness.directorCapability, invalidated);
        yield* harness.workflowDirector.reassess(
          { projectId: ProjectId.make("project-1"), issue: invalidated },
          (command) =>
            harness.engine.dispatch(command).pipe(
              Effect.mapError(
                (error) =>
                  new WorkflowDirectorError({
                    failure: "dispatch-failed",
                    message: "Interrupt dispatch failed.",
                    detail: String(error),
                  }),
              ),
            ),
        );
        yield* Effect.promise(() => harness.drain());
        expect(harness.interruptTurn).toHaveBeenCalledWith({ threadId: started.director.threadId });
        const reassessmentRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{ readonly commandId: string }>`
              SELECT stop_command_id AS "commandId" FROM workflow_reassessments
              WHERE director_id = ${started.director.directorId}
            `;
            }),
          ),
        );
        expect(
          Option.getOrNull(
            yield* Effect.promise(() =>
              harness.readCommandReceipt(CommandId.make(reassessmentRows[0]!.commandId)),
            ),
          ),
        ).toMatchObject({ status: "accepted" });
        const snapshot = yield* Effect.promise(() => harness.readModel());
        expect(
          snapshot.threads.find((thread) => thread.id === ThreadId.make("unrelated-capability"))
            ?.session?.status,
        ).toBe("running");
        const held = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(held.reassessment?.subjects).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ kind: "worker", providerThreadId: "native-worker" }),
            expect.objectContaining({ kind: "reviewer", providerThreadId: "native-reviewer" }),
          ]),
        );
        const premature = yield* harness.workflowDirector
          .resume(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
              directorId: started.director.directorId,
              observation: held.observation,
              modelSelection: {
                instanceId: ProviderInstanceId.make("codex"),
                model: "gpt-6-astra",
                options: [{ id: "reasoningEffort", value: "high" }],
              },
            },
            harness.dispatchWorkflow,
          )
          .pipe(Effect.result);
        expect(premature._tag).toBe("Failure");
        const retried = yield* harness.workflowDirector.retryReassessment(
          {
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            capabilityNumber: 17,
            directorId: started.director.directorId,
            observation: held.observation,
          },
          (command) =>
            harness.engine.dispatch(command).pipe(
              Effect.mapError(
                (error) =>
                  new WorkflowDirectorError({
                    failure: "dispatch-failed",
                    message: "Interrupt retry failed.",
                    detail: String(error),
                  }),
              ),
            ),
        );
        yield* Effect.promise(() => harness.drain());
        expect(retried.actions).toContain("stop");
        expect(harness.interruptTurn).toHaveBeenCalledTimes(2);

        const nativeEvents = (
          taskId: string,
          status: "running" | "interrupted",
          eventId: string,
        ): ProviderRuntimeEvent => ({
          type: "task.updated",
          eventId: EventId.make(eventId),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          threadId: started.director.threadId,
          createdAt: status === "running" ? "2026-09-07T12:01:00.000Z" : "2026-09-07T12:01:01.000Z",
          payload: {
            taskId,
            status,
            timelineBypass: true,
            nativeTurn: { sessionId: taskId, turnId: `${taskId}-turn`, status },
          },
        });
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("director-native-settlement-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("director-native-settlement-turn"),
              createdAt: "2026-09-07T12:01:00.000Z",
              payload: {},
              raw: {
                payload: {
                  threadId: "director-native-session",
                  turn: { id: "director-native-settlement-turn" },
                },
              },
            },
            nativeEvents("native-worker", "running", "worker-native-running"),
            nativeEvents("native-worker", "interrupted", "worker-native-interrupted"),
            nativeEvents("native-reviewer", "running", "reviewer-native-running"),
            nativeEvents("native-reviewer", "interrupted", "reviewer-native-interrupted"),
            {
              type: "turn.aborted",
              eventId: EventId.make("director-native-settlement-interrupted"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("director-native-settlement-turn"),
              createdAt: "2026-09-07T12:01:01.000Z",
              payload: { reason: "Interrupted for reassessment." },
              raw: {
                payload: {
                  threadId: "director-native-session",
                  turn: { id: "director-native-settlement-turn" },
                },
              },
            },
          ]),
        );
        Object.assign(harness.directorCapability, {
          ...approvedCapability,
          evidence: {
            ...approvedCapability.evidence!,
            records: [
              ...approvedCapability.evidence!.records,
              {
                id: "integrated-scope-change",
                url: `${approvedCapability.url}#issuecomment-integrated-scope-change`,
                createdAt: "2026-09-07T12:00:00.000Z",
                kind: "reassessment" as const,
                state: "superseded" as const,
                sourceAccess: "verified" as const,
                scope: "current" as const,
                summary: "Scope changed while delivery was active.",
                source: approvedCapability.url,
                outcome: "scope-change" as const,
              },
              {
                id: "integrated-reassessment-cleared",
                url: `${approvedCapability.url}#issuecomment-integrated-reassessment-cleared`,
                createdAt: "2099-09-07T12:02:00.000Z",
                kind: "reassessment" as const,
                state: "current" as const,
                sourceAccess: "verified" as const,
                scope: "current" as const,
                summary: "Reassessment cleared with renewed approval.",
                source: `${approvedCapability.url}#issuecomment-integrated-scope-change`,
                outcome: "cleared" as const,
              },
            ],
          },
        });
        const resumable = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(resumable.actions).toContain("resume");
        const resumeWithPersistenceFailure = yield* harness.workflowDirector
          .resume(
            {
              projectId: ProjectId.make("project-1"),
              repository: "Flow-Fly/t3code",
              capabilityNumber: 17,
              directorId: started.director.directorId,
              observation: resumable.observation,
              modelSelection: {
                instanceId: ProviderInstanceId.make("codex"),
                model: "gpt-6-astra",
                options: [{ id: "reasoningEffort", value: "high" }],
              },
            },
            (command) =>
              harness.engine.dispatch(command).pipe(
                Effect.tap(() =>
                  Effect.promise(async () => {
                    await harness.drain();
                    await harness.emitProviderEvents([
                      {
                        type: "turn.started",
                        eventId: EventId.make("accepted-resume-native-root"),
                        provider: ProviderDriverKind.make("codex"),
                        providerInstanceId: ProviderInstanceId.make("codex"),
                        threadId: started.director.threadId,
                        turnId: TurnId.make("turn-2"),
                        createdAt: "2026-09-07T12:02:01.000Z",
                        payload: {},
                        raw: {
                          payload: {
                            threadId: "director-native-session",
                            turn: { id: "turn-2" },
                          },
                        },
                      },
                      nativeEvents("conflicting-child", "running", "conflicting-child-running"),
                    ]);
                    await harness.runEffect(
                      Effect.gen(function* () {
                        const sql = yield* SqlClient.SqlClient;
                        yield* sql`
                          CREATE TRIGGER fail_director_resume_finalize
                          BEFORE UPDATE ON workflow_director_resumes
                          BEGIN
                            SELECT RAISE(FAIL, 'injected accepted Resume persistence failure');
                          END
                        `;
                      }),
                    );
                  }),
                ),
                Effect.mapError(
                  (error) =>
                    new OrchestrationDispatchCommandError({
                      message: "Workflow Resume dispatch failed.",
                      cause: error,
                    }),
                ),
              ),
          )
          .pipe(Effect.result);
        expect(resumeWithPersistenceFailure._tag).toBe("Failure");
        const resumeRows = yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* sql<{
                readonly commandId: string;
                readonly directorStatus: string;
                readonly reassessmentStatus: string;
                readonly status: string;
              }>`
                SELECT resume.command_id AS "commandId", resume.status,
                  director.status AS "directorStatus",
                  reassessment.status AS "reassessmentStatus"
                FROM workflow_director_resumes resume
                JOIN workflow_directors director
                  ON director.director_id = resume.director_id
                JOIN workflow_reassessments reassessment
                  ON reassessment.reassessment_id = resume.reassessment_id
                WHERE resume.director_id = ${started.director.directorId}
              `;
            }),
          ),
        );
        expect(resumeRows).toEqual([
          expect.objectContaining({
            status: "submitting",
            directorStatus: "held",
            reassessmentStatus: "clearing",
          }),
        ]);
        expect(
          Option.getOrNull(
            yield* Effect.promise(() =>
              harness.readCommandReceipt(CommandId.make(resumeRows[0]!.commandId)),
            ),
          ),
        ).toMatchObject({ status: "accepted" });
        const interruptedDuringFailure = yield* harness.workflowDirector
          .status({
            projectId: ProjectId.make("project-1"),
            repository: "Flow-Fly/t3code",
            capabilityNumber: 17,
          })
          .pipe(Effect.result);
        expect(interruptedDuringFailure._tag).toBe("Failure");
        yield* Effect.promise(() =>
          harness.runEffect(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`DROP TRIGGER fail_director_resume_finalize`;
            }),
          ),
        );
        const resumedWithConflict = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(resumedWithConflict).toMatchObject({
          status: "held",
          actions: expect.arrayContaining(["stop"]),
          reassessment: {
            status: "clearing",
            subjects: expect.arrayContaining([
              expect.objectContaining({ kind: "director", outcome: "resumed" }),
              expect.objectContaining({
                providerThreadId: "conflicting-child",
                outcome: "resumed",
              }),
            ]),
          },
        });
        const reconnectedWhileHeld = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(reconnectedWhileHeld.status).toBe("held");
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            nativeEvents("conflicting-child", "interrupted", "conflicting-child-interrupted"),
          ]),
        );
        yield* harness.engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("unrelated-root-projection-before-writer"),
          threadId: started.director.threadId,
          session: {
            threadId: started.director.threadId,
            status: "running",
            providerName: "codex",
            providerInstanceId: ProviderInstanceId.make("codex"),
            runtimeMode: "approval-required",
            activeTurnId: TurnId.make("unrelated-native-turn"),
            lastError: null,
            updatedAt: "2026-09-07T12:04:01.000Z",
          },
          createdAt: "2026-09-07T12:04:01.000Z",
        });
        const heldForProjectedRoot = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(heldForProjectedRoot.status).toBe("held");
        expect(heldForProjectedRoot.reassessment).not.toBeNull();

        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("unrelated-native-turn-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("unrelated-native-turn"),
              createdAt: "2026-09-07T12:04:02.000Z",
              payload: {},
              raw: {
                payload: {
                  threadId: "director-native-session",
                  turn: { id: "unrelated-native-turn" },
                },
              },
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("unrelated-native-turn-interrupted"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.director.threadId,
              turnId: TurnId.make("unrelated-native-turn"),
              createdAt: "2026-09-07T12:04:03.000Z",
              payload: { reason: "Interrupted after projected activity was reconciled." },
              raw: {
                payload: {
                  threadId: "director-native-session",
                  turn: { id: "unrelated-native-turn" },
                },
              },
            },
          ]),
        );
        const recovered = yield* harness.workflowDirector.status({
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code",
          capabilityNumber: 17,
        });
        expect(recovered.status).toBe("active");
        expect(recovered.reassessment).toBeNull();
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
      }),
  );

  effectIt.effect(
    "delivers one provider continuation for simultaneous workflow resume retries",
    () =>
      Effect.gen(function* () {
        const harness = yield* Effect.promise(() => createHarness());
        const createdAt = "2026-01-01T00:00:00.000Z";
        NodeFS.mkdirSync("/tmp/provider-project", { recursive: true });
        const workflowInput = {
          projectId: ProjectId.make("project-1"),
          repository: "Flow-Fly/t3code" as const,
          rootNumber: 10,
          issueNumber: 15,
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
            options: [{ id: "reasoningEffort", value: "high" }],
          },
        };
        const started = yield* harness.workflowStart.start(workflowInput, harness.dispatchWorkflow);
        yield* Effect.promise(() => harness.drain());
        yield* Effect.promise(() =>
          harness.emitProviderEvents([
            {
              type: "turn.started",
              eventId: EventId.make("workflow-initial-turn-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.threadId,
              turnId: harness.sentTurnId,
              createdAt,
              payload: {},
            },
            {
              type: "turn.aborted",
              eventId: EventId.make("workflow-initial-turn-aborted"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              threadId: started.threadId,
              turnId: harness.sentTurnId,
              createdAt,
              payload: { reason: "Interrupted for recovery." },
            },
          ]),
        );
        const interruptedThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
          (thread) => thread.id === started.threadId,
        );
        expect(interruptedThread).toMatchObject({
          latestTurn: { turnId: harness.sentTurnId, state: "interrupted" },
          session: { status: "interrupted", activeTurnId: null },
        });
        const recovery = yield* harness.workflowStart.recovery({
          projectId: workflowInput.projectId,
          repository: workflowInput.repository,
          issueNumber: workflowInput.issueNumber,
        });
        const resumeInput = {
          ...workflowInput,
          attemptId: started.attemptId,
          action: "resume" as const,
          observation: recovery.observation,
        };
        const resumed = yield* Effect.all(
          [
            harness.workflowStart.recover(resumeInput, harness.dispatchWorkflow),
            harness.workflowStart.recover(resumeInput, harness.dispatchWorkflow),
          ],
          { concurrency: "unbounded" },
        );
        yield* Effect.promise(() => harness.drain());
        const resumeRows = yield* Effect.promise(() =>
          harness.readWorkflowResumeRows(started.attemptId),
        );
        const receipt = yield* Effect.promise(() =>
          harness.readCommandReceipt(CommandId.make(resumeRows[0]!.commandId)),
        );
        const preserved = yield* harness.workflowStart.recovery({
          projectId: workflowInput.projectId,
          repository: workflowInput.repository,
          issueNumber: workflowInput.issueNumber,
        });

        expect(resumed.map((result) => result.threadId)).toEqual([
          started.threadId,
          started.threadId,
        ]);
        expect(resumeRows).toHaveLength(1);
        expect(resumeRows[0]?.status).toBe("submitted");
        expect(Option.getOrNull(receipt)).toMatchObject({
          commandId: resumeRows[0]?.commandId,
          status: "accepted",
        });
        const resumedThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
          (thread) => thread.id === started.threadId,
        );
        expect(resumedThread?.activities).toEqual([]);
        expect(harness.sendTurn).toHaveBeenCalledTimes(2);
        expect(preserved.currentAttempt).toMatchObject({
          attemptId: started.attemptId,
          threadId: started.threadId,
          evidence: "accepted",
        });
        expect(harness.workflowAssignees).toEqual(new Set(["Flow-Fly"]));
      }),
  );

  effectIt.effect("retains a turn dispatched immediately after start until activation", () =>
    Effect.gen(function* () {
      const activation = yield* Deferred.make<void>();
      const started = yield* Deferred.make<ProviderSession>();
      const harness = yield* Effect.promise(() =>
        createHarness({
          serverActivation: Deferred.await(activation),
          startSessionEffect: (session) =>
            Deferred.succeed(started, session).pipe(Effect.as(session)),
        }),
      );

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-activation"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: MessageId.make("message-before-activation"),
          role: "user",
          text: "Start after activation",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      expect(yield* Deferred.isDone(started)).toBe(false);

      yield* Deferred.succeed(activation, undefined);
      const session = yield* Deferred.await(started);
      yield* Effect.promise(() => harness.drain());
      expect(session.threadId).toBe(ThreadId.make("thread-1"));
      expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
        threadId: ThreadId.make("thread-1"),
        input: "Start after activation",
      });
    }),
  );

  effectIt.effect("rejects /compact without conversation context", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => createHarness());
      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-empty-compact"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-empty-compact"),
          role: "user",
          text: "/compact",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      yield* Effect.promise(() => harness.drain());
      expect(harness.compactThread).not.toHaveBeenCalled();
    }),
  );

  effectIt.effect("keeps turns blocked until compaction restores the session", () =>
    Effect.gen(function* () {
      const readyDispatchStarted = yield* Deferred.make<void>();
      const releaseReadyDispatch = yield* Deferred.make<void>();
      let blockReadyDispatch = false;
      const harness = yield* Effect.promise(() =>
        createHarness({
          beforeReadySessionDispatch: () =>
            blockReadyDispatch
              ? Deferred.succeed(readyDispatchStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseReadyDispatch)),
                )
              : Effect.void,
        }),
      );
      const threadId = ThreadId.make("thread-1");
      const now = "2026-01-01T00:00:00.000Z";
      const dispatchTurn = (id: string, text: string, createdAt: string) =>
        harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(`cmd-${id}`),
          threadId,
          message: {
            messageId: asMessageId(`user-message-${id}`),
            role: "user",
            text,
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        });

      yield* dispatchTurn("before-blocked-compact", "hello", now);
      yield* Effect.promise(() => waitFor(() => harness.sendTurn.mock.calls.length === 1));
      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-ready-before-blocked-compact"),
        threadId,
        session: {
          threadId,
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      });

      blockReadyDispatch = true;
      yield* dispatchTurn("blocked-compact", "/compact", "2026-01-01T00:00:01.000Z");
      yield* Deferred.await(readyDispatchStarted);

      yield* dispatchTurn("during-compact-recovery", "too soon", "2026-01-01T00:00:02.000Z");
      yield* Effect.promise(() =>
        waitFor(async () => {
          const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
          return (
            thread?.activities.some(
              (activity) => activity.kind === "provider.turn.start.failed",
            ) === true
          );
        }),
      );
      expect(harness.sendTurn).toHaveBeenCalledTimes(1);
      expect(yield* Effect.promise(() => harness.readPendingTurnStarts())).toEqual([
        { threadId: "thread-1" },
      ]);

      yield* Deferred.succeed(releaseReadyDispatch, undefined);
      yield* Effect.promise(() =>
        waitFor(async () => {
          const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
          return thread?.session?.status === "ready";
        }),
      );
    }),
  );

  effectIt.effect("does not overwrite concurrent session state after compaction failure", () =>
    Effect.gen(function* () {
      const releaseCompaction = yield* Deferred.make<void>();
      const releaseRunningCompaction = yield* Deferred.make<void>();
      const releaseFailedStop = yield* Deferred.make<void>();
      let compactionCount = 0;
      const harness = yield* Effect.promise(() =>
        createHarness({
          compactThreadEffect: () =>
            Deferred.await(
              compactionCount++ === 0 ? releaseCompaction : releaseRunningCompaction,
            ).pipe(Effect.andThen(Effect.die("Compaction stopped"))),
          stopSessionEffect: () =>
            Deferred.await(releaseFailedStop).pipe(
              Effect.andThen(
                Effect.fail(
                  new ProviderAdapterRequestError({
                    provider: "codex",
                    method: "session.stop",
                    detail: "provider stop failed",
                  }),
                ),
              ),
            ),
        }),
      );
      const threadId = ThreadId.make("thread-1");
      const now = "2026-01-01T00:00:00.000Z";
      const dispatchCompact = (suffix: string, createdAt: string) =>
        harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(`cmd-compact-${suffix}`),
          threadId,
          message: {
            messageId: asMessageId(`user-message-compact-${suffix}`),
            role: "user",
            text: "/compact",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt,
        });

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-message-before-compact"),
        threadId,
        message: {
          messageId: asMessageId("user-message-before-compact"),
          role: "user",
          text: "hello",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      });
      yield* Effect.promise(() => waitFor(() => harness.sendTurn.mock.calls.length === 1));
      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-ready-before-compact"),
        threadId,
        session: {
          threadId,
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      });
      yield* dispatchCompact("before-stop", now);
      yield* Effect.promise(() => waitFor(() => harness.compactThread.mock.calls.length === 1));
      const compactingThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === threadId,
      );
      expect(compactingThread?.session?.status).toBe("starting");
      yield* harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-stop-during-compact"),
        threadId,
        createdAt: "2026-01-01T00:00:01.000Z",
      });
      yield* Effect.promise(() => waitFor(() => harness.stopSession.mock.calls.length === 1));
      yield* Deferred.succeed(releaseCompaction, undefined);
      yield* Effect.promise(() =>
        waitFor(async () => {
          const compactingThread = (await harness.readModel()).threads.find(
            (entry) => entry.id === threadId,
          );
          return (
            compactingThread?.activities.some(
              (activity) => activity.kind === "provider.turn.start.failed",
            ) === true
          );
        }),
      );
      const stoppingThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === threadId,
      );
      expect(stoppingThread?.session?.status).toBe("starting");
      yield* Deferred.succeed(releaseFailedStop, undefined);
      yield* Effect.promise(() => harness.drain());

      const recoveredThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === threadId,
      );
      expect(recoveredThread?.session?.status).toBe("ready");
      expect(
        recoveredThread?.activities.find(
          (activity) => activity.kind === "provider.session.stop.failed",
        ),
      ).toMatchObject({
        summary: "Provider session stop failed",
        payload: { detail: "provider stop failed" },
      });

      yield* dispatchCompact("before-running", "2026-01-01T00:00:02.000Z");
      yield* Effect.promise(() => waitFor(() => harness.compactThread.mock.calls.length === 2));
      yield* harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-failed-stop-before-compaction-settles"),
        threadId,
        createdAt: "2026-01-01T00:00:02.500Z",
      });
      yield* Effect.promise(() =>
        waitFor(async () => {
          const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
          return (
            thread?.activities.filter(
              (activity) => activity.kind === "provider.session.stop.failed",
            ).length === 2
          );
        }),
      );
      const restartedThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === threadId,
      );
      expect(restartedThread?.session?.status).toBe("starting");
      const restartedSession = restartedThread?.session;
      if (!restartedSession) return yield* Effect.die("Compaction session missing");
      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-running-during-compact"),
        threadId,
        session: {
          ...restartedSession,
          status: "running",
          activeTurnId: asTurnId("compaction-turn"),
          updatedAt: "2026-01-01T00:00:03.000Z",
        },
        createdAt: "2026-01-01T00:00:03.000Z",
      });
      yield* Deferred.succeed(releaseRunningCompaction, undefined);
      yield* Effect.promise(() => harness.drain());
      const runningThread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === threadId,
      );
      expect(runningThread?.session?.status).toBe("running");
    }),
  );
  effectIt.effect("projects starting before a slow provider session finishes", () =>
    Effect.gen(function* () {
      const releaseStart = yield* Deferred.make<void>();
      const harness = yield* Effect.promise(() =>
        createHarness({
          startSessionEffect: (session) => Deferred.await(releaseStart).pipe(Effect.as(session)),
        }),
      );
      const now = "2026-01-01T00:00:00.000Z";

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-slow-provider"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-slow-provider"),
          role: "user",
          text: "start slowly",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      });

      yield* Effect.promise(() => waitFor(() => harness.startSession.mock.calls.length === 1));
      const duringStartup = yield* Effect.promise(() => harness.readModel());
      expect(
        duringStartup.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.session
          ?.status,
      ).toBe("starting");
      expect(harness.sendTurn).not.toHaveBeenCalled();

      yield* Deferred.succeed(releaseStart, undefined);
      yield* Effect.promise(() => waitFor(() => harness.sendTurn.mock.calls.length === 1));
    }),
  );

  effectIt.effect("shows the missing workspace message without a provider stack trace", () =>
    Effect.gen(function* () {
      const attempted = yield* Deferred.make<void>();
      const missingCwd = "/missing/project/worktree";
      const missingWorkspace = new ProviderWorkspaceMissingError({
        threadId: ThreadId.make("thread-1"),
        cwd: missingCwd,
      });
      const harness = yield* Effect.promise(() =>
        createHarness({
          startSessionEffect: () =>
            Deferred.succeed(attempted, undefined).pipe(
              Effect.andThen(Effect.fail(missingWorkspace)),
            ),
        }),
      );

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-missing-workspace"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-missing-workspace"),
          role: "user",
          text: "continue",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      yield* Deferred.await(attempted);
      yield* Effect.promise(() => harness.drain());

      const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === ThreadId.make("thread-1"),
      );
      expect(thread?.session).toMatchObject({
        status: "error",
        activeTurnId: null,
        lastError: missingWorkspace.message,
      });
      const failure = thread?.activities.find(
        (activity) => activity.kind === "provider.turn.start.failed",
      );
      expect(failure?.payload).toMatchObject({ detail: missingWorkspace.message });
      expect(harness.runtimeSessions).toEqual([]);
      expect(harness.sendTurn).not.toHaveBeenCalled();
      expect(yield* Effect.promise(() => harness.readPendingTurnStarts())).toEqual([]);
    }),
  );

  effectIt.effect("settles a failed provider startup and allows a clean retry", () =>
    Effect.gen(function* () {
      let failStartup = true;
      const harness = yield* Effect.promise(() =>
        createHarness({
          startSessionEffect: (session) =>
            failStartup
              ? Effect.fail(
                  new ProviderAdapterRequestError({
                    provider: "codex",
                    method: "thread.start",
                    detail: "deterministic startup failure",
                  }),
                )
              : Effect.succeed(session),
        }),
      );
      const now = "2026-01-01T00:00:00.000Z";

      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-failure"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-failure"),
          role: "user",
          text: "fail once",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      });

      yield* Effect.promise(() =>
        waitFor(async () => {
          const readModel = await harness.readModel();
          return (
            readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.session
              ?.status === "error"
          );
        }),
      );
      let readModel = yield* Effect.promise(() => harness.readModel());
      let thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      expect(thread?.session?.lastError).toContain("deterministic startup failure");
      expect(harness.sendTurn).not.toHaveBeenCalled();

      failStartup = false;
      yield* harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-retry"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-retry"),
          role: "user",
          text: "retry",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:01.000Z",
      });

      yield* Effect.promise(() => waitFor(() => harness.sendTurn.mock.calls.length === 1));
      readModel = yield* Effect.promise(() => harness.readModel());
      thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      expect(thread?.session?.status).toBe("starting");
      expect(thread?.session?.lastError).toBeNull();
    }),
  );

  it("retries thread title generation after a transient failure", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const seededTitle = "Please investigate reconnect failures after restar...";
    let attempts = 0;
    harness.generateThreadTitle.mockReturnValue(
      Effect.suspend(() => {
        attempts += 1;
        return attempts === 1
          ? Effect.fail(
              new TextGenerationError({
                operation: "generateThreadTitle",
                detail: "Claude CLI request timed out.",
              }),
            )
          : Effect.succeed({ title: "Generated title" });
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-seed"),
        threadId: ThreadId.make("thread-1"),
        title: seededTitle,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-title"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-title"),
          role: "user",
          text: "Please investigate reconnect failures after restarting the session.",
          attachments: [],
        },
        titleSeed: seededTitle,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.generateThreadTitle.mock.calls.length === 1);
    expect(harness.generateThreadTitle.mock.calls[0]?.[0]).toMatchObject({
      message: "Please investigate reconnect failures after restarting the session.",
    });

    await waitFor(async () => {
      const readModel = await harness.readModel();
      return (
        readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.title ===
        "Generated title"
      );
    });
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Generated title");
    expect(attempts).toBe(2);
  });

  it("regenerates a thread title from the current conversation", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({ title: "Resolve stale reconnect state" }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-existing"),
        threadId: ThreadId.make("thread-1"),
        title: "Investigate reconnect regressions",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-title-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-title-regeneration"),
          role: "user",
          text: "Please investigate reconnect regressions after restarting the session.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.message.assistant.delta",
        commandId: CommandId.make("cmd-assistant-before-title-regeneration"),
        threadId: ThreadId.make("thread-1"),
        messageId: asMessageId("assistant-message-before-title-regeneration"),
        delta: "The remaining issue is stale reconnect state.",
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.message.assistant.complete",
        commandId: CommandId.make("cmd-assistant-complete-before-title-regeneration"),
        threadId: ThreadId.make("thread-1"),
        messageId: asMessageId("assistant-message-before-title-regeneration"),
        createdAt: "2026-01-01T00:00:02.000Z",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-regenerate"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );

    await harness.drain();

    expect(harness.generateThreadTitle).toHaveBeenCalledTimes(1);
    expect(harness.generateThreadTitle.mock.calls[0]?.[0]).toMatchObject({
      cwd: "/tmp/provider-project",
      previousTitle: "Investigate reconnect regressions",
      message: [
        "USER:",
        "Please investigate reconnect regressions after restarting the session.",
        "",
        "ASSISTANT:",
        "The remaining issue is stale reconnect state.",
      ].join("\n"),
    });
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Resolve stale reconnect state");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("pins the first user message when regeneration context is truncated", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const quoteText = "界".repeat(1_000);
    const citation = serializeAssistantCitation({
      ...assistantCitation,
      text: quoteText,
      end: quoteText.length,
    });
    const firstUserMessage = `Review subagent monitoring risks. ${citation} ${"Opening context. ".repeat(200)}`;
    const recentUserMessage = `LATEST FINDING: ${"implementation detail ".repeat(320)}`;
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({ title: "Review subagent monitoring risks" }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-existing-long"),
        threadId: ThreadId.make("thread-1"),
        title: "Generic PR review",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-long-title-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-long-title-regeneration"),
          role: "user",
          text: firstUserMessage,
          attachments: [
            {
              type: "image",
              id: "opening-context-image",
              name: "image.png",
              mimeType: "image/png",
              sizeBytes: 5,
            },
          ],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-middle-turn-before-long-title-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("middle-message-before-long-title-regeneration"),
          role: "user",
          text: "Temporary handoff details.",
          attachments: [
            {
              type: "image",
              id: "middle-context-image",
              name: "image.png",
              mimeType: "image/png",
              sizeBytes: 5,
            },
          ],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-recent-turn-before-long-title-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("recent-message-before-long-title-regeneration"),
          role: "user",
          text: recentUserMessage,
          attachments: [
            {
              type: "image",
              id: "recent-context-image",
              name: "image.png",
              mimeType: "image/png",
              sizeBytes: 5,
            },
          ],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:02.000Z",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-regenerate-long"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );

    await harness.drain();

    expect(harness.generateThreadTitle).toHaveBeenCalledTimes(1);
    const input = harness.generateThreadTitle.mock.calls[0]?.[0];
    if (!input) {
      throw new Error("Expected a title generation input");
    }
    const message = input.message;
    expect(message.startsWith(`USER:\nReview subagent monitoring risks. ${quoteText} `)).toBe(true);
    expect(message).not.toContain("t3-citation://");
    expect(message).toContain("[First user message truncated]");
    expect(message).toContain("[Earlier content truncated]");
    expect(message).toContain("image.png");
    expect(message).toHaveLength(8_000);
    expect(input.attachments?.map((attachment) => attachment.id)).toEqual([
      "opening-context-image",
      "recent-context-image",
    ]);
    const readModel = await harness.readModel();
    expect(
      readModel.threads
        .find((entry) => entry.id === ThreadId.make("thread-1"))
        ?.messages.find(
          (entry) => entry.id === asMessageId("user-message-before-long-title-regeneration"),
        )?.text,
    ).toBe(firstUserMessage);
  });

  it("clears title regeneration state left pending across reactor startup", async () => {
    const harness = await createHarness({
      titleRegenerationBeforeStart: "one",
    });

    expect(harness.generateThreadTitle).not.toHaveBeenCalled();
    expect(harness.titleRegenerationCompletionDispatchAttempts).toBe(1);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Thread");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("continues clearing startup title regeneration state after one completion fails", async () => {
    const harness = await createHarness({
      titleRegenerationBeforeStart: "two",
      titleRegenerationCompletionDispatchFailures: 1,
    });

    expect(harness.generateThreadTitle).not.toHaveBeenCalled();
    expect(harness.titleRegenerationCompletionDispatchAttempts).toBe(2);
    const readModel = await harness.readModel();
    expect(
      readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))?.titleRegeneration,
    ).not.toBeNull();
    expect(
      readModel.threads.find((entry) => entry.id === ThreadId.make("thread-2"))?.titleRegeneration,
    ).toBeNull();
  });

  it("keeps the current title when regeneration returns the fallback", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.generateThreadTitle.mockReturnValue(Effect.succeed({ title: "New thread" }));

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-before-fallback-regeneration"),
        threadId: ThreadId.make("thread-1"),
        title: "Keep meaningful title",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-fallback-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-fallback-regeneration"),
          role: "user",
          text: "Investigate the reconnect state.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-fallback-regeneration"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );

    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Keep meaningful title");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("clears title regeneration state when generation fails", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-before-failed-regeneration"),
        threadId: ThreadId.make("thread-1"),
        title: "Keep title after failure",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-failed-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-failed-regeneration"),
          role: "user",
          text: "Investigate the reconnect state.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-failed-regeneration"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );

    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Keep title after failure");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("retries a failed completion and continues regenerating", async () => {
    const harness = await createHarness({
      titleRegenerationCompletionDispatchFailures: 1,
    });
    const now = "2026-01-01T00:00:00.000Z";
    harness.generateThreadTitle
      .mockReturnValueOnce(Effect.succeed({ title: "Title lost to completion failure" }))
      .mockReturnValueOnce(Effect.succeed({ title: "Recovered regeneration worker" }));

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-before-completion-failure"),
        threadId: ThreadId.make("thread-1"),
        title: "Existing title",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-completion-failure"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-completion-failure"),
          role: "user",
          text: "Investigate the reconnect state.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-regeneration-completion-failure"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );
    await harness.drain();

    let readModel = await harness.readModel();
    let thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Title lost to completion failure");
    expect(thread?.titleRegeneration).toBeNull();

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-regeneration-after-completion-failure"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );
    await harness.drain();

    expect(harness.generateThreadTitle).toHaveBeenCalledTimes(2);
    expect(harness.titleRegenerationCompletionDispatchAttempts).toBe(3);
    readModel = await harness.readModel();
    thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Recovered regeneration worker");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("pins the first user context and attachment before the retained tail", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const firstUserContext = "USER:\nOld visual issue\n[Attachments: old-issue.png]";
    const truncationMarker = "[Earlier content truncated]\n\n";
    const retainedContext = "x".repeat(
      8_000 - firstUserContext.length - "\n\n".length - truncationMarker.length,
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-before-truncated-regeneration"),
        threadId: ThreadId.make("thread-1"),
        title: "Existing title",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-truncated-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-truncated-regeneration"),
          role: "user",
          text: "Old visual issue",
          attachments: [
            {
              type: "image",
              id: "old-title-context-image",
              name: "old-issue.png",
              mimeType: "image/png",
              sizeBytes: 5,
            },
          ],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.message.assistant.delta",
        commandId: CommandId.make("cmd-assistant-truncated-regeneration-context"),
        threadId: ThreadId.make("thread-1"),
        messageId: asMessageId("assistant-truncated-regeneration-context"),
        delta: `content before retained tail${"x".repeat(8_100)}`,
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.message.assistant.complete",
        commandId: CommandId.make("cmd-assistant-truncated-regeneration-context-complete"),
        threadId: ThreadId.make("thread-1"),
        messageId: asMessageId("assistant-truncated-regeneration-context"),
        createdAt: "2026-01-01T00:00:02.000Z",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-regenerate-truncated-context"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );

    await harness.drain();

    expect(harness.generateThreadTitle.mock.calls[0]?.[0].message).toBe(
      `${firstUserContext}\n\n${truncationMarker}${retainedContext}`,
    );
    expect(harness.generateThreadTitle.mock.calls[0]?.[0].attachments).toEqual([
      expect.objectContaining({
        id: "old-title-context-image",
        name: "old-issue.png",
      }),
    ]);
  });

  it("does not overwrite a manual rename while title regeneration is running", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const generatedTitle = await harness.runEffect(
      Deferred.make<{ readonly title: string }, never>(),
    );
    harness.generateThreadTitle.mockReturnValue(Deferred.await(generatedTitle));

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-before-regeneration-race"),
        threadId: ThreadId.make("thread-1"),
        title: "Existing thread title",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-regeneration-race"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-regeneration-race"),
          role: "user",
          text: "Investigate the reconnect state.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-regeneration-race"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );
    await waitFor(() => harness.generateThreadTitle.mock.calls.length === 1);
    const pendingReadModel = await harness.readModel();
    expect(
      pendingReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"))
        ?.titleRegeneration?.requestId,
    ).toBe(CommandId.make("cmd-thread-title-regeneration-race"));

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-manual-rename-during-regeneration"),
        threadId: ThreadId.make("thread-1"),
        title: "Keep manual rename",
      }),
    );
    await harness.runEffect(
      Deferred.succeed(generatedTitle, { title: "Generated title should not win" }),
    );
    await harness.drain();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Keep manual rename");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("does not overwrite a manual rename while title regeneration is queued", async () => {
    let releaseStart = () => {};
    const startGate = new Promise<void>((resolve) => {
      releaseStart = resolve;
    });
    const harness = await createHarness({
      startSessionEffect: (session) => Effect.promise(() => startGate).pipe(Effect.as(session)),
    });
    const now = "2026-01-01T00:00:00.000Z";
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({ title: "Generated title should not win" }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-before-queued-regeneration"),
        threadId: ThreadId.make("thread-1"),
        title: "Existing thread title",
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-queued-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-queued-regeneration"),
          role: "user",
          text: "Investigate the reconnect state.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await waitFor(() => harness.startSession.mock.calls.length === 1);

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-queued-regeneration"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-manual-rename-before-regeneration-starts"),
        threadId: ThreadId.make("thread-1"),
        title: "Keep queued manual rename",
      }),
    );
    releaseStart();
    await harness.drain();

    expect(harness.generateThreadTitle).not.toHaveBeenCalled();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Keep queued manual rename");
  });

  it("skips superseded title regeneration before generation starts", async () => {
    let releaseStart = () => {};
    const startGate = new Promise<void>((resolve) => {
      releaseStart = resolve;
    });
    const harness = await createHarness({
      startSessionEffect: (session) => Effect.promise(() => startGate).pipe(Effect.as(session)),
    });
    const now = "2026-01-01T00:00:00.000Z";
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({ title: "Latest regenerated title" }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-before-superseded-regeneration"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-before-superseded-regeneration"),
          role: "user",
          text: "Investigate the reconnect state.",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );
    await waitFor(() => harness.startSession.mock.calls.length === 1);

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-superseded-regeneration"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-latest-regeneration"),
        threadId: ThreadId.make("thread-1"),
        regenerateTitle: true,
      }),
    );
    releaseStart();
    await harness.drain();

    expect(harness.generateThreadTitle).toHaveBeenCalledTimes(1);
    expect(harness.titleRegenerationCompletionDispatchAttempts).toBe(1);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Latest regenerated title");
    expect(thread?.titleRegeneration).toBeNull();
  });

  it("does not overwrite an existing custom thread title on the first turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const seededTitle = "Please investigate reconnect failures after restar...";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-custom"),
        threadId: ThreadId.make("thread-1"),
        title: "Keep this custom title",
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-title-preserve"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-title-preserve"),
          role: "user",
          text: "Please investigate reconnect failures after restarting the session.",
          attachments: [],
        },
        titleSeed: seededTitle,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.generateThreadTitle).not.toHaveBeenCalled();

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Keep this custom title");
  });

  it("matches the client-seeded title even when the outgoing prompt is reformatted", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const seededTitle = "Fix reconnect spinner on resume";
    const prompt = `[effort:high]\\n\\nFix reconnect spinner on resume ${serializeAssistantCitation(assistantCitation)}`;
    harness.generateThreadTitle.mockReturnValue(
      Effect.succeed({
        title: "Reconnect spinner resume bug",
      }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-title-formatted-seed"),
        threadId: ThreadId.make("thread-1"),
        title: seededTitle,
      }),
    );

    const titleUpdated = await harness.runEffect(
      harness.engine.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "thread.meta-updated" &&
            event.payload.title === "Reconnect spinner resume bug",
        ),
        Stream.take(1),
        Stream.toPull,
        Scope.provide(scope!),
      ),
    );
    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-title-formatted"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-title-formatted"),
          role: "user",
          text: prompt,
          attachments: [],
        },
        titleSeed: seededTitle,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await harness.runEffect(titleUpdated);
    await harness.drain();

    expect(harness.generateThreadTitle.mock.calls[0]?.[0].message).toBe(
      `[effort:high]\\n\\nFix reconnect spinner on resume ${assistantQuoteText}`,
    );
    expect(harness.generateThreadTitle.mock.calls[0]?.[0].message).not.toContain("t3-citation://");
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.title).toBe("Reconnect spinner resume bug");
    expect(
      thread?.messages.find((entry) => entry.id === asMessageId("user-message-title-formatted"))
        ?.text,
    ).toBe(prompt);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({ input: prompt });
  });

  it("generates a worktree branch name for the first turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const prompt = `Add a safer reconnect backoff. ${serializeAssistantCitation(assistantCitation)}`;
    const statusRefreshed = await harness.runEffect(Deferred.make<void>());
    const refreshStatus = harness.refreshStatus.getMockImplementation()!;
    harness.refreshStatus.mockImplementation((cwd) =>
      refreshStatus(cwd).pipe(Effect.tap(() => Deferred.succeed(statusRefreshed, undefined))),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-branch"),
        threadId: ThreadId.make("thread-1"),
        branch: "t3code/1234abcd",
        worktreePath: "/tmp/provider-project-worktree",
      }),
    );

    harness.generateBranchName.mockImplementation((input: unknown) =>
      Effect.succeed({
        branch:
          typeof input === "object" &&
          input !== null &&
          "modelSelection" in input &&
          typeof input.modelSelection === "object" &&
          input.modelSelection !== null &&
          "model" in input.modelSelection &&
          typeof input.modelSelection.model === "string"
            ? `feature/${input.modelSelection.model}`
            : "feature/generated",
      }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-branch-model"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-branch-model"),
          role: "user",
          text: prompt,
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await harness.runEffect(Deferred.await(statusRefreshed));
    await harness.drain();
    expect(harness.generateBranchName.mock.calls[0]?.[0].message).toBe(
      `Add a safer reconnect backoff. ${assistantQuoteText}`,
    );
    expect(harness.generateBranchName.mock.calls[0]?.[0].message).not.toContain("t3-citation://");
    expect(harness.refreshStatus.mock.calls[0]?.[0]).toBe("/tmp/provider-project-worktree");
    const readModel = await harness.readModel();
    expect(
      readModel.threads
        .find((entry) => entry.id === ThreadId.make("thread-1"))
        ?.messages.find((entry) => entry.id === asMessageId("user-message-branch-model"))?.text,
    ).toBe(prompt);
  });

  it("recreates a missing worktree from the thread branch before starting a turn", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    const worktreePath = NodePath.join(harness.stateDir, "missing-worktree");

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-missing-worktree"),
        threadId: ThreadId.make("thread-1"),
        branch: "feature/restore",
        worktreePath,
      }),
    );

    await harness.runEffect(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-missing-worktree"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-missing-worktree"),
          role: "user",
          text: "continue",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    expect(harness.pruneWorktrees).toHaveBeenCalledWith({ cwd: "/tmp/provider-project" });
    expect(harness.createWorktree).toHaveBeenCalledWith({
      cwd: "/tmp/provider-project",
      refName: "feature/restore",
      path: worktreePath,
    });
    expect(harness.createWorktree.mock.invocationCallOrder[0]).toBeLessThan(
      harness.startSession.mock.invocationCallOrder[0]!,
    );
  });

  it("forwards codex model options through session start and turn send", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-fast"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-fast"),
          role: "user",
          text: "hello fast mode",
          attachments: [],
        },
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ]),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]),
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]),
    });
  });

  it("forwards claude effort options through session start and turn send", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-effort"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-effort"),
          role: "user",
          text: "hello with effort",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-sonnet-4-6",
          [{ id: "effort", value: "max" }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-sonnet-4-6",
        [{ id: "effort", value: "max" }],
      ),
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-sonnet-4-6",
        [{ id: "effort", value: "max" }],
      ),
    });
  });

  it("forwards claude fast mode options through session start and turn send", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-fast-mode"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-fast-mode"),
          role: "user",
          text: "hello with fast mode",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-opus-4-6",
          [{ id: "fastMode", value: true }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-opus-4-6",
        [{ id: "fastMode", value: true }],
      ),
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-opus-4-6",
        [{ id: "fastMode", value: true }],
      ),
    });
  });

  it("forwards plan interaction mode to the provider turn request", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.interaction-mode.set",
        commandId: CommandId.make("cmd-interaction-mode-set-plan"),
        threadId: ThreadId.make("thread-1"),
        interactionMode: "plan",
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-plan"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-plan"),
          role: "user",
          text: "plan this change",
          attachments: [],
        },
        interactionMode: "plan",
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      interactionMode: "plan",
    });
  });

  it("preserves the active session model when in-session model switching is unsupported", async () => {
    const harness = await createHarness({ sessionModelSwitch: "unsupported" });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unsupported-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unsupported-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unsupported-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unsupported-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);

    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
    });
  });

  effectIt.effect(
    "rejects changing models after start when the provider requires a new thread",
    () =>
      Effect.gen(function* () {
        const harness = yield* Effect.promise(() =>
          createHarness({ requiresNewThreadForModelChange: true }),
        );
        const now = "2026-01-01T00:00:00.000Z";

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-restricted-1"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("user-message-restricted-1"),
            role: "user",
            text: "first",
            attachments: [],
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        });

        yield* Effect.promise(() => waitFor(() => harness.sendTurn.mock.calls.length === 1));

        yield* harness.engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-turn-start-restricted-2"),
          threadId: ThreadId.make("thread-1"),
          message: {
            messageId: asMessageId("user-message-restricted-2"),
            role: "user",
            text: "second",
            attachments: [],
          },
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.1-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          createdAt: now,
        });

        yield* Effect.promise(() =>
          waitFor(async () => {
            const readModel = await harness.readModel();
            const thread = readModel.threads.find(
              (entry) => entry.id === ThreadId.make("thread-1"),
            );
            return (
              thread?.activities.some(
                (activity) => activity.kind === "provider.turn.start.failed",
              ) ?? false
            );
          }),
        );

        expect(harness.sendTurn).toHaveBeenCalledTimes(1);
        const readModel = yield* Effect.promise(() => harness.readModel());
        const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
        expect(
          thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
        ).toMatchObject({
          payload: {
            detail: expect.stringContaining(
              "cannot switch models after the conversation has started",
            ),
          },
        });
      }),
  );

  it("starts a first turn on the requested provider instance even when it differs from the thread model", async () => {
    const harness = await createHarness({
      threadModelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-first"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-first"),
          role: "user",
          text: "hello claude",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    expect(harness.startSession).toHaveBeenCalledTimes(1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      provider: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.providerName).toBe("claudeAgent");
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("claudeAgent"));
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toBeUndefined();
  });

  it("reuses the same provider session when runtime mode is unchanged", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unchanged-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unchanged-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-unchanged-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-unchanged-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.startSession.mock.calls.length).toBe(1);
    expect(harness.stopSession.mock.calls.length).toBe(0);
  });

  it("restarts an existing Codex thread on a compatible requested instance", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-compatible-codex-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-compatible-codex-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-compatible-codex-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-compatible-codex-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex_work"),
          model: "gpt-5-codex",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);

    expect(harness.startSession).toHaveBeenCalledTimes(2);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex_work"),
      resumeCursor: { opaque: "resume-1" },
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("codex_work"));
  });

  it("restarts the provider session when the thread workspace changes", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-workspace-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-workspace-1"),
          role: "user",
          text: "first in project root",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);
    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      cwd: "/tmp/provider-project",
    });

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("cmd-thread-worktree-change"),
        threadId: ThreadId.make("thread-1"),
        worktreePath: "/tmp/provider-project-worktree",
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-workspace-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-workspace-2"),
          role: "user",
          text: "second in worktree",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.stopSession.mock.calls.length).toBe(0);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      cwd: "/tmp/provider-project-worktree",
      resumeCursor: { opaque: "resume-1" },
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
      runtimeMode: "approval-required",
    });
  });

  it("restarts claude sessions when claude effort changes", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-sonnet-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-effort-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-effort-1"),
          role: "user",
          text: "first claude turn",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-sonnet-4-6",
          [{ id: "effort", value: "medium" }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-claude-effort-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-claude-effort-2"),
          role: "user",
          text: "second claude turn",
          attachments: [],
        },
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-sonnet-4-6",
          [{ id: "effort", value: "max" }],
        ),
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await waitFor(() => harness.sendTurn.mock.calls.length === 2);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      resumeCursor: { opaque: "resume-1" },
      modelSelection: createModelSelection(
        ProviderInstanceId.make("claudeAgent"),
        "claude-sonnet-4-6",
        [{ id: "effort", value: "max" }],
      ),
    });
  });

  it("restarts the provider session when runtime mode is updated on the thread", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-initial-full-access"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-runtime-mode-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-runtime-mode-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-1"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.runtimeMode === "approval-required";
    });
    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-runtime-mode-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-runtime-mode-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.sendTurn.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.length).toBe(0);
    expect(harness.startSession.mock.calls[1]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      resumeCursor: { opaque: "resume-1" },
      runtimeMode: "approval-required",
    });
    expect(harness.sendTurn.mock.calls[1]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
  });

  it("does not inject derived model options when restarting claude on runtime mode changes", async () => {
    const harness = await createHarness({
      threadModelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
    });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-runtime-mode-claude"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "claudeAgent",
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-claude-no-options"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);

    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      modelSelection: {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        model: "claude-opus-4-6",
      },
      runtimeMode: "approval-required",
    });
  });

  it("does not stop the active session when restart fails before rebind", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-initial-full-access-2"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-restart-failure-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-restart-failure-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "full-access",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    harness.startSession.mockImplementationOnce(
      (_: unknown, __: unknown) => Effect.fail("simulated restart failure") as never,
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("cmd-runtime-mode-set-restart-failure"),
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return thread?.runtimeMode === "approval-required";
    });
    await waitFor(() => harness.startSession.mock.calls.length === 2);
    await harness.drain();

    expect(harness.stopSession.mock.calls.length).toBe(0);
    expect(harness.sendTurn.mock.calls.length).toBe(1);

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.runtimeMode).toBe("full-access");
  });

  it("rejects provider changes after a thread is already bound to a session provider", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-switch-1"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-switch-1"),
          role: "user",
          text: "first",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-provider-switch-2"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-provider-switch-2"),
          role: "user",
          text: "second",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false
      );
    });

    expect(harness.startSession.mock.calls.length).toBe(1);
    expect(harness.sendTurn.mock.calls.length).toBe(1);
    expect(harness.stopSession.mock.calls.length).toBe(0);

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.session?.threadId).toBe("thread-1");
    expect(thread?.session?.providerName).toBe("codex");
    expect(thread?.session?.runtimeMode).toBe("approval-required");
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("cannot switch to 'claudeAgent'"),
      },
    });
  });

  it("rejects cross-driver provider changes after the existing thread session has stopped", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stopped-provider-switch"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "stopped",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stopped-provider-switch"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stopped-provider-switch"),
          role: "user",
          text: "continue with claude",
          attachments: [],
        },
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false
      );
    });

    expect(harness.startSession.mock.calls.length).toBe(0);
    expect(harness.sendTurn.mock.calls.length).toBe(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("cannot switch to 'claudeAgent'"),
      },
    });
  });

  it("reacts to thread.turn.interrupt-requested by calling provider interrupt", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-1"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.interrupt",
        commandId: CommandId.make("cmd-turn-interrupt"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
        createdAt: now,
      }),
    );

    await waitFor(() => harness.interruptTurn.mock.calls.length === 1);
    expect(harness.interruptTurn.mock.calls[0]?.[0]).toEqual({
      threadId: "thread-1",
    });
  });

  effectIt.effect(
    "stops a running session and records the failure when provider interrupt fails",
    () =>
      Effect.gen(function* () {
        const harness = yield* Effect.promise(() =>
          createHarness({
            interruptTurnEffect: () =>
              Effect.fail(
                new ProviderAdapterRequestError({
                  provider: "codex",
                  method: "thread.interrupt",
                  detail: "provider session disappeared",
                }),
              ),
            stopSessionEffect: () =>
              Effect.fail(
                new ProviderAdapterRequestError({
                  provider: "codex",
                  method: "session.stop",
                  detail: "provider process already exited",
                }),
              ),
          }),
        );
        const now = "2026-01-01T00:00:00.000Z";

        yield* harness.engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("cmd-session-set-interrupt-failure"),
          threadId: ThreadId.make("thread-1"),
          session: {
            threadId: ThreadId.make("thread-1"),
            status: "running",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: asTurnId("turn-1"),
            lastError: null,
            updatedAt: now,
          },
          createdAt: now,
        });

        yield* harness.engine.dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make("cmd-turn-interrupt-provider-failure"),
          threadId: ThreadId.make("thread-1"),
          turnId: asTurnId("turn-1"),
          createdAt: now,
        });

        yield* Effect.promise(() =>
          waitFor(async () => {
            const thread = (await harness.readModel()).threads.find(
              (entry) => entry.id === ThreadId.make("thread-1"),
            );
            return thread?.session?.status === "stopped";
          }),
        );

        const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
          (entry) => entry.id === ThreadId.make("thread-1"),
        );
        expect(thread?.session).toMatchObject({
          status: "stopped",
          activeTurnId: null,
          lastError: "provider session disappeared",
        });
        expect(
          thread?.activities.find((activity) => activity.kind === "provider.turn.interrupt.failed"),
        ).toMatchObject({
          summary: "Provider turn interrupt failed",
          payload: { detail: "provider session disappeared" },
        });
        expect(harness.stopSession).toHaveBeenCalledWith({ threadId: ThreadId.make("thread-1") });
      }),
  );

  effectIt.effect("stops a starting session without a bound turn when interrupt fails", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() =>
        createHarness({
          interruptTurnEffect: () =>
            Effect.fail(
              new ProviderAdapterRequestError({
                provider: "codex",
                method: "thread.interrupt",
                detail: "provider session disappeared",
              }),
            ),
        }),
      );
      const now = "2026-01-01T00:00:00.000Z";

      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-interrupt-starting"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "starting",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      });

      yield* harness.engine.dispatch({
        type: "thread.turn.interrupt",
        commandId: CommandId.make("cmd-turn-interrupt-starting-provider-failure"),
        threadId: ThreadId.make("thread-1"),
        createdAt: now,
      });

      yield* Effect.promise(() => harness.drain());

      const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === ThreadId.make("thread-1"),
      );
      expect(thread?.session).toMatchObject({
        status: "stopped",
        activeTurnId: null,
        lastError: "provider session disappeared",
      });
      expect(harness.stopSession).toHaveBeenCalledWith({ threadId: ThreadId.make("thread-1") });
      expect(
        thread?.activities.find((activity) => activity.kind === "provider.turn.interrupt.failed"),
      ).toMatchObject({ payload: { detail: "provider session disappeared" } });
    }),
  );

  effectIt.effect("does not overwrite a session that became ready while an interrupt failed", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => createHarness());
      const now = "2026-01-01T00:00:00.000Z";
      const completedAt = "2026-01-01T00:00:01.000Z";

      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-interrupt-race"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-1"),
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      });

      harness.interruptTurn.mockImplementation(() =>
        harness.engine
          .dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("cmd-session-set-natural-completion"),
            threadId: ThreadId.make("thread-1"),
            session: {
              threadId: ThreadId.make("thread-1"),
              status: "ready",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: null,
              lastError: null,
              updatedAt: completedAt,
            },
            createdAt: completedAt,
          })
          .pipe(
            Effect.catchCause((cause) => Effect.die(cause)),
            Effect.andThen(
              Effect.fail(
                new ProviderAdapterRequestError({
                  provider: "codex",
                  method: "thread.interrupt",
                  detail: "provider session disappeared",
                }),
              ),
            ),
          ),
      );

      yield* harness.engine.dispatch({
        type: "thread.turn.interrupt",
        commandId: CommandId.make("cmd-turn-interrupt-race"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
        createdAt: now,
      });

      yield* Effect.promise(() => harness.drain());

      const thread = (yield* Effect.promise(() => harness.readModel())).threads.find(
        (entry) => entry.id === ThreadId.make("thread-1"),
      );
      expect(thread?.session).toMatchObject({
        status: "ready",
        activeTurnId: null,
        lastError: null,
        updatedAt: completedAt,
      });
      expect(harness.stopSession).not.toHaveBeenCalled();
      expect(
        thread?.activities.some((activity) => activity.kind === "provider.turn.interrupt.failed"),
      ).toBe(false);
    }),
  );

  it("starts a fresh session when only projected session state exists", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-stale"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-stale"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-stale"),
          role: "user",
          text: "resume codex",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(() => harness.startSession.mock.calls.length === 1);
    await waitFor(() => harness.sendTurn.mock.calls.length === 1);

    expect(harness.startSession.mock.calls[0]?.[1]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "approval-required",
    });
    expect(harness.sendTurn.mock.calls[0]?.[0]).toMatchObject({
      threadId: ThreadId.make("thread-1"),
    });
  });

  it("rejects active runtime sessions that are missing provider instance ids", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-missing-instance"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );
    harness.runtimeSessions.push({
      provider: ProviderDriverKind.make("codex"),
      status: "ready",
      runtimeMode: "approval-required",
      threadId: ThreadId.make("thread-1"),
      cwd: "/tmp/provider-project",
      resumeCursor: { opaque: "resume-without-instance" },
      createdAt: now,
      updatedAt: now,
    });

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-missing-instance"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: asMessageId("user-message-missing-instance"),
          role: "user",
          text: "resume codex",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      return (
        thread?.activities.some((activity) => activity.kind === "provider.turn.start.failed") ??
        false
      );
    });

    expect(harness.startSession.mock.calls.length).toBe(0);
    expect(harness.sendTurn.mock.calls.length).toBe(0);
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(
      thread?.activities.find((activity) => activity.kind === "provider.turn.start.failed"),
    ).toMatchObject({
      payload: {
        detail: expect.stringContaining("without a provider instance id"),
      },
    });
  });

  it("forwards approval responses without reading unrelated message bodies", async () => {
    const harness = await createHarness({ unreadableHistory: true });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-approval"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.approval.respond",
        commandId: CommandId.make("cmd-approval-respond"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("approval-request-1"),
        decision: "accept",
        createdAt: now,
      }),
    );

    await harness.drain();
    expect(harness.respondToRequest.mock.calls[0]?.[0]).toEqual({
      threadId: "thread-1",
      requestId: "approval-request-1",
      decision: "accept",
    });
  });

  it("forwards user input answers without reading unrelated message bodies", async () => {
    const harness = await createHarness({ unreadableHistory: true });
    const now = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-user-input"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.user-input.respond",
        commandId: CommandId.make("cmd-user-input-respond"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("user-input-request-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
        createdAt: now,
      }),
    );

    await harness.drain();
    expect(harness.respondToUserInput.mock.calls[0]?.[0]).toEqual({
      threadId: "thread-1",
      requestId: "user-input-request-1",
      answers: {
        sandbox_mode: "workspace-write",
      },
    });
  });

  it("normalizes stale Codex approval callbacks without faking approval resolution", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.respondToRequest.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: ProviderDriverKind.make("codex"),
          method: "item/requestApproval/decision",
          detail: "Unknown pending Codex approval request: approval-request-1",
        }),
      ),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-approval-error"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("cmd-approval-requested"),
        threadId: ThreadId.make("thread-1"),
        activity: {
          id: EventId.make("activity-approval-requested"),
          tone: "approval",
          kind: "approval.requested",
          summary: "Command approval requested",
          payload: {
            requestId: "approval-request-1",
            requestKind: "command",
          },
          turnId: null,
          createdAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.approval.respond",
        commandId: CommandId.make("cmd-approval-respond-stale"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("approval-request-1"),
        decision: "acceptForSession",
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      if (!thread) return false;
      return thread.activities.some(
        (activity) => activity.kind === "provider.approval.respond.failed",
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread).toBeDefined();

    const failureActivity = thread?.activities.find(
      (activity) => activity.kind === "provider.approval.respond.failed",
    );
    expect(failureActivity).toBeDefined();
    expect(failureActivity?.payload).toMatchObject({
      requestId: "approval-request-1",
      detail: expect.stringContaining("Stale pending approval request: approval-request-1"),
    });

    const resolvedActivity = thread?.activities.find(
      (activity) =>
        activity.kind === "approval.resolved" &&
        typeof activity.payload === "object" &&
        activity.payload !== null &&
        (activity.payload as Record<string, unknown>).requestId === "approval-request-1",
    );
    expect(resolvedActivity).toBeUndefined();
  });

  it("surfaces non-resumable provider user-input callbacks as stale failures", async () => {
    const harness = await createHarness();
    const now = "2026-01-01T00:00:00.000Z";
    harness.respondToUserInput.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: ProviderDriverKind.make("claudeAgent"),
          method: "item/tool/respondToUserInput",
          detail: "Unknown pending Codex user input request: user-input-request-1",
        }),
      ),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-user-input-error"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "claudeAgent",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("cmd-user-input-requested"),
        threadId: ThreadId.make("thread-1"),
        activity: {
          id: EventId.make("activity-user-input-requested"),
          tone: "info",
          kind: "user-input.requested",
          summary: "User input requested",
          payload: {
            requestId: "user-input-request-1",
            questions: [
              {
                id: "sandbox_mode",
                header: "Sandbox",
                question: "Which mode should be used?",
                options: [
                  {
                    label: "workspace-write",
                    description: "Allow workspace writes only",
                  },
                ],
              },
            ],
          },
          turnId: null,
          createdAt: now,
        },
        createdAt: now,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.user-input.respond",
        commandId: CommandId.make("cmd-user-input-respond-stale"),
        threadId: ThreadId.make("thread-1"),
        requestId: asApprovalRequestId("user-input-request-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
        createdAt: now,
      }),
    );

    await waitFor(async () => {
      const readModel = await harness.readModel();
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      if (!thread) return false;
      return thread.activities.some(
        (activity) => activity.kind === "provider.user-input.respond.failed",
      );
    });

    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread).toBeDefined();

    const failureActivity = thread?.activities.find(
      (activity) => activity.kind === "provider.user-input.respond.failed",
    );
    expect(failureActivity).toBeDefined();
    expect(failureActivity?.payload).toMatchObject({
      requestId: "user-input-request-1",
      detail: expect.stringContaining("Stale pending user-input request: user-input-request-1"),
    });

    const resolvedActivity = thread?.activities.find(
      (activity) =>
        activity.kind === "user-input.resolved" &&
        typeof activity.payload === "object" &&
        activity.payload !== null &&
        (activity.payload as Record<string, unknown>).requestId === "user-input-request-1",
    );
    expect(resolvedActivity).toBeUndefined();
  });

  effectIt.effect("stops a provider session without reading unrelated message bodies", () =>
    Effect.gen(function* () {
      const harness = yield* Effect.promise(() => createHarness({ unreadableHistory: true }));
      const now = "2026-01-01T00:00:00.000Z";

      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-stop"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex_work"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      });

      yield* harness.engine.dispatch({
        type: "thread.session.stop",
        commandId: CommandId.make("cmd-session-stop"),
        threadId: ThreadId.make("thread-1"),
        createdAt: now,
      });

      yield* Effect.promise(() => harness.drain());
      expect(harness.stopSession).toHaveBeenCalledWith({ threadId: ThreadId.make("thread-1") });
      const thread = yield* harness.snapshotQuery
        .getThreadShellById(ThreadId.make("thread-1"))
        .pipe(Effect.map(Option.getOrThrow));
      expect(thread.session).not.toBeNull();
      expect(thread.session?.status).toBe("stopped");
      expect(thread.session?.threadId).toBe("thread-1");
      expect(thread.session?.providerInstanceId).toBe(ProviderInstanceId.make("codex_work"));
      expect(thread.session?.activeTurnId).toBeNull();
    }),
  );

  effectIt.effect("stops a ready provider session after automatic settlement", () =>
    Effect.gen(function* () {
      const sessionStopped = yield* Deferred.make<void>();
      const harness = yield* Effect.promise(() =>
        createHarness({
          stopSessionEffect: () => Deferred.succeed(sessionStopped, undefined).pipe(Effect.asVoid),
        }),
      );
      const now = "2026-01-01T00:00:00.000Z";

      yield* harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-for-auto-settle"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex_work"),
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: now,
        },
        createdAt: now,
      });
      const beforeSettlement = yield* Effect.promise(() => harness.readModel());

      yield* harness.engine.dispatch({
        type: "thread.auto-settle",
        commandId: CommandId.make("cmd-auto-settle-with-session"),
        threadId: ThreadId.make("thread-1"),
        snapshotSequence: beforeSettlement.snapshotSequence,
        settledAt: now,
      });

      yield* Deferred.await(sessionStopped);
      yield* Effect.promise(() => harness.drain());
      const readModel = yield* Effect.promise(() => harness.readModel());
      const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
      expect(thread?.settledOverride).toBe("settled");
      expect(thread?.session?.status).toBe("stopped");
      expect(thread?.session?.providerInstanceId).toBe(ProviderInstanceId.make("codex_work"));
    }),
  );
});
