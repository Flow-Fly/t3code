import {
  EnvironmentId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeTaskId,
  type OrchestrationCommand,
  type ServerProvider,
  type ThreadId,
  TurnId,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import { ProviderRuntimeIngestionLive } from "../orchestration/Layers/ProviderRuntimeIngestion.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProviderRuntimeIngestionService } from "../orchestration/Services/ProviderRuntimeIngestion.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../orchestration/ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../orchestration/ThreadPlanProgress.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../provider/Services/ProviderService.ts";
import { makeProviderRegistryMock } from "../provider/testUtils/providerRegistryMock.ts";
import * as ProcessRunner from "../processRunner.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { workflowDirectorHandlers } from "../mcp/toolkits/workflow/handlers.ts";
import * as WorkflowDirectorService from "./WorkflowDirectorService.ts";
import { recordWorkflowWorkerObservation } from "./WorkflowWorkerPersistence.ts";
import { recordWorkflowCheckObservation } from "./WorkflowCheckObservation.ts";
import {
  interpretWorkflowEvidence,
  workflowEvidenceBodyFingerprint,
  type WorkflowBlockerEvidence,
  type WorkflowEvidenceComment,
} from "./WorkflowEvidence.ts";
import * as WorkflowService from "./WorkflowService.ts";

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

function provider(): ServerProvider {
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
      {
        slug: "gpt-5.6-sol",
        name: "GPT-5.6 Sol",
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
    skills: ["implement", "code-review"].map((name) => ({
      name,
      path: `/skills/${name}/SKILL.md`,
      enabled: true,
    })),
  };
}

function issue(
  number: number,
  title: string,
  kind: WorkflowIssueSummary["kind"],
  parentNumber: number | null,
): WorkflowIssueSummary {
  return {
    id: `issue-${number}`,
    repository,
    number,
    title,
    url: `https://github.com/${repository}/issues/${number}`,
    kind,
    state: "open",
    stateReason: null,
    updatedAt: "2026-09-06T10:00:00.000Z",
    childCount: kind === "capability" ? 1 : 0,
    parentNumber,
    labels: [kind === "capability" ? "workflow:capability" : "workflow:ticket"],
    readiness: {
      status: "ready",
      reasons:
        kind === "ticket"
          ? [{ kind: "approved-scope", message: "Ticket-breakdown approval covers current scope." }]
          : [],
    },
  };
}

const breakdown = [
  "## Proposed breakdown",
  "",
  "1. **T01 — First delivery slice**",
  "",
  "<details>",
  "<summary>T01 — First delivery slice</summary>",
  "",
  "## What to build",
  "",
  "Build the first slice.",
  "",
  "## Acceptance criteria",
  "",
  "- [ ] It works.",
  "",
  "## Blocked by",
  "",
  "None",
  "",
  "</details>",
].join("\n");
const capabilityBody = "## Summary\n\nDeliver Workflow.\n\n## Source map\n\nNone (standalone)\n";
const capabilitySummary = issue(17, "Workflow delivery", "capability", null);
const ticket = issue(18, "T01 — First delivery slice", "ticket", 17);
const ticketDetail: WorkflowIssueDetail = {
  ...ticket,
  body: [
    "Approved slice: **T01** ([ticket-breakdown approval](https://github.com/Flow-Fly/t3code/issues/17#issuecomment-breakdown))",
    "",
    "## What to build",
    "",
    "Build the first slice.",
    "",
    "## Acceptance criteria",
    "",
    "- [ ] It works.",
    "",
    "## Blocked by",
    "",
    "None",
  ].join("\n"),
  blockedBy: [],
  evidence: {
    records: [
      {
        id: "ticket-breakdown-approval",
        url: `${ticket.url}#issuecomment-breakdown`,
        createdAt: "2026-09-06T09:30:00.000Z",
        kind: "approval",
        state: "current",
        sourceAccess: "verified",
        scope: "current",
        summary: "Ticket breakdown approval",
        approvalKind: "ticket-breakdown",
        authority: "verified",
        approvedBy: "Flow-Fly",
        source: `${capabilitySummary.url}#issuecomment-source-breakdown`,
        approvedContent: breakdown,
      },
    ],
    manualConditions: [],
  },
};
const capability: WorkflowIssueDetail = {
  ...capabilitySummary,
  body: capabilityBody,
  blockedBy: [],
  evidence: {
    records: [
      {
        id: "spec-approval",
        url: `${capabilitySummary.url}#issuecomment-spec`,
        createdAt: "2026-09-06T09:00:00.000Z",
        kind: "approval",
        state: "current",
        sourceAccess: "verified",
        scope: "current",
        summary: "Specification approval",
        approvalKind: "specification",
        authority: "verified",
        approvedBy: "Flow-Fly",
        source: `${capabilitySummary.url}#issuecomment-source-spec`,
        approvedContent: capabilityBody,
      },
      {
        id: "breakdown-approval",
        url: `${capabilitySummary.url}#issuecomment-breakdown`,
        createdAt: "2026-09-06T09:30:00.000Z",
        kind: "approval",
        state: "current",
        sourceAccess: "verified",
        scope: "not-applicable",
        summary: "Ticket breakdown approval",
        approvalKind: "ticket-breakdown",
        authority: "verified",
        approvedBy: "Flow-Fly",
        source: `${capabilitySummary.url}#issuecomment-source-breakdown`,
        approvedContent: breakdown,
      },
    ],
    manualConditions: [],
  },
};

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

function acknowledgedCloseOutput(issue: WorkflowIssueSummary) {
  return {
    ...output(""),
    stderr: `✓ Closed issue ${repository}#${issue.number} (${issue.title})\n`,
  };
}

function processOutput(stdout: string) {
  return {
    stdout,
    stderr: "",
    code: ChildProcessSpawner.ExitCode(0),
    timedOut: false,
    stdoutTruncated: false,
    stderrTruncated: false,
    stdoutInvalidUtf8: false,
    stderrInvalidUtf8: false,
  };
}

function controlledProviderService() {
  return Effect.gen(function* () {
    const events = yield* Queue.unbounded<{
      readonly events: ReadonlyArray<ProviderRuntimeEvent>;
      readonly enqueued: Deferred.Deferred<void>;
    }>();
    const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;
    const service: ProviderServiceShape = {
      startSession: () => unsupported(),
      sendTurn: () => unsupported(),
      compactThread: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession: () => unsupported(),
      listSessions: () => Effect.succeed([] as ProviderSession[]),
      getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
      assertConversationRollbackSupported: () => unsupported(),
      getInstanceInfo: (providerInstanceId) =>
        Effect.succeed({
          instanceId: providerInstanceId,
          driverKind: ProviderDriverKind.make("codex"),
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind: ProviderDriverKind.make("codex"),
            continuationKey: `codex:instance:${providerInstanceId}`,
          },
        }),
      rollbackConversation: () => unsupported(),
      uploadFeedback: () => unsupported(),
      get streamEvents() {
        return Stream.fromQueue(events).pipe(
          Stream.flatMap((batch) =>
            Stream.concat(
              Stream.fromIterable(batch.events),
              Stream.fromEffect(Deferred.succeed(batch.enqueued, undefined)).pipe(Stream.drain),
            ),
          ),
        );
      },
    };
    return {
      service,
      emitAndWaitForEnqueue: (batch: ReadonlyArray<ProviderRuntimeEvent>) =>
        Effect.gen(function* () {
          const enqueued = yield* Deferred.make<void>();
          yield* Queue.offer(events, { events: batch, enqueued });
          yield* Deferred.await(enqueued);
        }),
    };
  });
}

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const processRunner = yield* ProcessRunner.ProcessRunner;
    const result = yield* processRunner.run({ command: "git", args, cwd });
    if (result.code !== 0) {
      return yield* Effect.die(new Error(result.stderr.trim() || `git ${args.join(" ")} failed`));
    }
    return result;
  });

function realWorktreeCreator(input: {
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly processRunner: ProcessRunner.ProcessRunner["Service"];
}): GitWorkflowService.GitWorkflowService["Service"]["createWorktree"] {
  return (worktree) =>
    Effect.gen(function* () {
      if (!worktree.path || !worktree.newRefName || !worktree.baseRefName) {
        return yield* Effect.die(
          new Error("The real worktree fixture requires explicit paths and refs."),
        );
      }
      yield* input.fileSystem
        .makeDirectory(input.path.dirname(worktree.path), { recursive: true })
        .pipe(Effect.orDie);
      const result = yield* input.processRunner
        .run({
          command: "git",
          args: ["worktree", "add", "-b", worktree.newRefName, worktree.path, worktree.baseRefName],
          cwd: worktree.cwd,
        })
        .pipe(Effect.orDie);
      if (result.code !== 0) {
        return yield* Effect.die(
          new Error(result.stderr.trim() || "The real worktree fixture could not be created."),
        );
      }
      return { worktree: { path: worktree.path, refName: worktree.newRefName } };
    });
}

const RealGitTestLayer = Layer.merge(
  NodeServices.layer,
  ProcessRunner.layer.pipe(Layer.provide(NodeServices.layer)),
);

function evidenceComment(input: {
  readonly id: string;
  readonly body: string;
  readonly createdAt?: string;
}): WorkflowEvidenceComment {
  return {
    id: input.id,
    url: `${capabilitySummary.url}#issuecomment-${input.id}`,
    body: input.body,
    createdAt: input.createdAt ?? "2026-09-06T09:00:00.000Z",
    author: "Flow-Fly",
    authorAssociation: "OWNER",
  };
}

function interpretedCapabilityFixture(
  count: number,
  closed = new Set<number>(),
  durableSource?: { readonly threadId: string; readonly messageId: string },
) {
  const units = Array.from({ length: count }, (_, index) => ({
    id: `T${String(index + 1).padStart(2, "0")}`,
    number: 100 + index,
    title: `Delivery slice ${index + 1}`,
    scope: [
      "## What to build",
      "",
      `Build delivery slice ${index + 1}.`,
      "",
      "## Acceptance criteria",
      "",
      "- [ ] The slice works.",
      "",
      "## Blocked by",
      "",
      "None",
    ].join("\n"),
  }));
  const approvedBreakdown = units
    .flatMap((unit) => [
      "<details>",
      `<summary>${unit.id} — ${unit.title}</summary>`,
      unit.scope,
      "</details>",
    ])
    .join("\n");
  const source = evidenceComment({ id: "owner-source", body: "Approved in the owner thread." });
  const specification = evidenceComment({
    id: "specification",
    body: [
      "<!-- t3-workflow:v1 approval -->",
      "Kind: specification",
      "Approved by: Flow-Fly (owner)",
      `Source: ${durableSource ? `T3 thread \`${durableSource.threadId}\`, message \`${durableSource.messageId}\`.` : source.url}`,
      "## Approved content",
      capabilityBody,
    ].join("\n"),
  });
  const breakdownRecord = evidenceComment({
    id: "breakdown",
    createdAt: "2026-09-06T09:30:00.000Z",
    body: [
      "<!-- t3-workflow:v1 approval -->",
      "Kind: ticket-breakdown",
      "Approved by: Flow-Fly (owner)",
      `Source: ${durableSource ? `T3 thread \`${durableSource.threadId}\`, message \`${durableSource.messageId}\`.` : source.url}`,
      "## Approved content",
      approvedBreakdown,
    ].join("\n"),
  });
  const capabilityEvidence = interpretWorkflowEvidence({
    issue: {
      id: capabilitySummary.id,
      url: capabilitySummary.url,
      number: capabilitySummary.number,
      title: capabilitySummary.title,
      kind: "capability",
      state: "open",
      stateReason: null,
      labels: capabilitySummary.labels,
      assignees: [],
      body: capabilityBody,
      comments: [source, specification, breakdownRecord],
      reopenedAt: [],
    },
  });
  const capabilityDetail: WorkflowIssueDetail = {
    ...capabilitySummary,
    body: capabilityBody,
    blockedBy: [],
    ...capabilityEvidence,
  };
  const ticketDetails = units.map((unit, index): WorkflowIssueDetail => {
    const isClosed = closed.has(index + 1);
    const resolution = evidenceComment({
      id: `resolution-${unit.number}`,
      createdAt: "2026-09-06T10:00:00.000Z",
      body: [
        "<!-- t3-workflow:v1 resolution -->",
        "Outcome: resolved",
        "## Summary",
        "Delivered.",
        "## Evidence",
        "https://github.com/Flow-Fly/t3code/commit/abc",
      ].join("\n"),
    });
    const body = [
      `Approved slice: **${unit.id}** ([ticket-breakdown approval](${breakdownRecord.url}))`,
      "",
      unit.scope,
    ].join("\n");
    const interpreted = interpretWorkflowEvidence({
      issue: {
        id: `issue-${unit.number}`,
        url: `https://github.com/${repository}/issues/${unit.number}`,
        number: unit.number,
        title: unit.title,
        kind: "ticket",
        state: isClosed ? "closed" : "open",
        stateReason: isClosed ? "completed" : null,
        labels: ["workflow:ticket", ...(isClosed ? [] : ["ready-for-agent"])],
        assignees: [],
        body,
        comments: isClosed ? [resolution] : [],
        reopenedAt: [],
      },
      approvalComments: [source, breakdownRecord],
    });
    return {
      id: `issue-${unit.number}`,
      repository,
      number: unit.number,
      title: unit.title,
      url: `https://github.com/${repository}/issues/${unit.number}`,
      kind: "ticket",
      state: isClosed ? "closed" : "open",
      stateReason: isClosed ? "completed" : null,
      updatedAt: "2026-09-06T10:00:00.000Z",
      childCount: 0,
      parentNumber: capabilitySummary.number,
      labels: ["workflow:ticket", ...(isClosed ? [] : ["ready-for-agent"])],
      body,
      blockedBy: [],
      ...interpreted,
    };
  });
  return {
    capability: capabilityDetail,
    ticketDetails,
    breakdown: approvedBreakdown,
    source,
    specification,
    breakdownRecord,
  };
}

function reinterpretCapability(
  fixture: ReturnType<typeof interpretedCapabilityFixture>,
  assignees: ReadonlyArray<string>,
) {
  return interpretWorkflowEvidence({
    issue: {
      id: fixture.capability.id,
      url: fixture.capability.url,
      number: fixture.capability.number,
      title: fixture.capability.title,
      kind: fixture.capability.kind,
      state: fixture.capability.state,
      stateReason: fixture.capability.stateReason,
      labels: fixture.capability.labels,
      assignees,
      body: fixture.capability.body,
      comments: [fixture.source, fixture.specification, fixture.breakdownRecord],
      reopenedAt: [],
    },
  });
}

function reinterpretTicket(
  ticket: WorkflowIssueDetail,
  approvalComments: ReadonlyArray<WorkflowEvidenceComment>,
  options: {
    readonly assignees?: ReadonlyArray<string>;
    readonly blockers?: ReadonlyArray<WorkflowBlockerEvidence>;
  } = {},
) {
  return interpretWorkflowEvidence({
    issue: {
      id: ticket.id,
      url: ticket.url,
      number: ticket.number,
      title: ticket.title,
      kind: ticket.kind,
      state: ticket.state,
      stateReason: ticket.stateReason,
      labels: ticket.labels,
      assignees: options.assignees ?? [],
      body: ticket.body,
      comments: [],
      reopenedAt: [],
    },
    approvalComments,
    ...(options.blockers ? { blockers: options.blockers } : {}),
  });
}

function updateTicketEvidence(
  ticket: WorkflowIssueDetail,
  approvalComments: ReadonlyArray<WorkflowEvidenceComment>,
  input: {
    readonly assignees: ReadonlyArray<string>;
    readonly comments?: ReadonlyArray<WorkflowEvidenceComment>;
    readonly state?: "open" | "closed";
  },
) {
  const state = input.state ?? "open";
  Object.assign(ticket, {
    state,
    stateReason: state === "closed" ? "completed" : null,
    ...interpretWorkflowEvidence({
      issue: {
        id: ticket.id,
        url: ticket.url,
        number: ticket.number,
        title: ticket.title,
        kind: ticket.kind,
        state,
        stateReason: state === "closed" ? "completed" : null,
        labels: ticket.labels,
        assignees: input.assignees,
        body: ticket.body,
        comments: input.comments ?? [],
        reopenedAt: [],
      },
      approvalComments,
    }),
  });
}

function rawIssueSummary(issue: WorkflowIssueSummary): WorkflowIssueSummary {
  return {
    id: issue.id,
    repository: issue.repository,
    number: issue.number,
    title: issue.title,
    url: issue.url,
    kind: issue.kind,
    state: issue.state,
    stateReason: issue.stateReason,
    updatedAt: issue.updatedAt,
    childCount: issue.childCount,
    parentNumber: issue.parentNumber,
    labels: issue.labels,
  };
}

function capabilityWithManualPrerequisite(
  fixture: ReturnType<typeof interpretedCapabilityFixture>,
  condition: string,
  cleared: boolean,
) {
  const body = `${fixture.capability.body.trim()}\n\n## Manual prerequisites\n\n${condition}\n`;
  const specification = evidenceComment({
    id: `specification-${cleared ? "cleared" : "blocked"}`,
    body: [
      "<!-- t3-workflow:v1 approval -->",
      "Kind: specification",
      "Approved by: Flow-Fly (owner)",
      `Source: ${fixture.source.url}`,
      "## Approved content",
      body,
    ].join("\n"),
  });
  const reassessment = evidenceComment({
    id: "manual-prerequisite-cleared",
    createdAt: "2026-09-06T10:00:00.000Z",
    body: [
      "## Reassessment",
      "<!-- t3-workflow:v1 reassessment -->",
      `Trigger: ${fixture.capability.url}`,
      "Outcome: cleared",
      "### Changes",
      `${condition} — verified`,
      "### Evidence",
      "https://github.com/Flow-Fly/t3code/issues/17#issuecomment-proof",
    ].join("\n"),
  });
  const interpreted = interpretWorkflowEvidence({
    issue: {
      id: fixture.capability.id,
      url: fixture.capability.url,
      number: fixture.capability.number,
      title: fixture.capability.title,
      kind: fixture.capability.kind,
      state: fixture.capability.state,
      stateReason: fixture.capability.stateReason,
      labels: fixture.capability.labels,
      assignees: [],
      body,
      comments: [
        fixture.source,
        specification,
        fixture.breakdownRecord,
        ...(cleared ? [reassessment] : []),
      ],
      reopenedAt: [],
    },
  });
  return { ...fixture.capability, body, ...interpreted };
}

interface HarnessOptions {
  readonly workspaceRoot?: string;
  readonly capability?: WorkflowIssueDetail;
  readonly worktreeCapability?: WorkflowIssueDetail;
  readonly ticketDetails?: ReadonlyArray<WorkflowIssueDetail>;
  readonly extraDetails?: ReadonlyArray<WorkflowIssueDetail>;
  readonly children?: WorkflowService.WorkflowService["Service"]["children"];
  readonly locate?: WorkflowService.WorkflowService["Service"]["locate"];
  readonly threadShell?: ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]["getThreadShellById"];
  readonly threadRuntimeContext?: ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]["getThreadRuntimeContext"];
  readonly threadDetail?: ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]["getThreadDetailById"];
  readonly githubExecute?: GitHubCli.GitHubCli["Service"]["execute"];
  readonly githubGetRepositoryCloneUrls?: GitHubCli.GitHubCli["Service"]["getRepositoryCloneUrls"];
  readonly processRunner?: ProcessRunner.ProcessRunner["Service"];
  readonly createWorktree?: GitWorkflowService.GitWorkflowService["Service"]["createWorktree"];
  readonly worktreeSkills?: ReadonlyArray<{
    readonly name: string;
    readonly path: string;
    readonly enabled: boolean;
  }>;
  readonly failWorktreeAttempts?: number;
}

function harness(options: HarnessOptions = {}) {
  const commands: Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>> = [];
  const worktreeCalls: Array<{ readonly cwd: string; readonly path: string | null }> = [];
  const statusCalls: string[] = [];
  let worktreeCreated = false;
  let remainingWorktreeFailures = options.failWorktreeAttempts ?? 0;
  let capabilityReads = 0;
  const selectedProvider = provider();
  const selectedWorkspaceRoot = options.workspaceRoot ?? workspaceRoot;
  const testCapability = options.capability ?? capability;
  const ticketDetails = options.ticketDetails ?? [ticketDetail];
  const registry = makeProviderRegistryMock([selectedProvider]);
  const projectionLayer = Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
    getProjectShellById: () =>
      Effect.succeed(
        Option.some({
          id: projectId,
          title: "T3 Code",
          workspaceRoot: selectedWorkspaceRoot,
        } as never),
      ),
    getThreadShellById: options.threadShell ?? (() => Effect.succeed(Option.none())),
    getThreadRuntimeContext: options.threadRuntimeContext ?? (() => Effect.succeed(Option.none())),
    getThreadDetailById: options.threadDetail ?? (() => Effect.succeed(Option.none())),
  });
  const layer = Layer.effect(
    WorkflowDirectorService.WorkflowDirectorService,
    WorkflowDirectorService.make,
  ).pipe(
    Layer.provideMerge(
      Layer.mock(WorkflowService.WorkflowService)({
        issueDetail: ({ number }) => {
          if (number === testCapability.number) {
            capabilityReads += 1;
            return Effect.succeed(
              capabilityReads > 1 && options.worktreeCapability
                ? options.worktreeCapability
                : testCapability,
            );
          }
          return Effect.succeed(
            [...ticketDetails, ...(options.extraDetails ?? [])].find(
              (candidate) => candidate.number === number,
            ) ?? ticketDetail,
          );
        },
        children:
          options.children ??
          (({ parentNumber }) =>
            Effect.succeed({
              parentNumber,
              children: parentNumber === testCapability.number ? ticketDetails : [],
              frontier:
                parentNumber === testCapability.number
                  ? {
                      status: "available",
                      message: `${ticketDetails.length} items can proceed.`,
                      readyIssueIds: ticketDetails
                        .filter((candidate) => candidate.readiness?.status === "ready")
                        .map((candidate) => candidate.id),
                    }
                  : { status: "empty", message: "No work.", readyIssueIds: [] },
            })),
        locate:
          options.locate ??
          (() =>
            Effect.succeed({
              issue: ticket,
              ancestry: [capabilitySummary],
              ancestryComplete: true,
            })),
      }),
    ),
    Layer.provideMerge(projectionLayer),
    Layer.provideMerge(
      Layer.succeed(ProviderRegistry.ProviderRegistry, {
        ...registry,
        probeWorkspaceSnapshot: ({ cwd }) =>
          Effect.succeed(
            cwd === selectedWorkspaceRoot || !options.worktreeSkills
              ? selectedProvider
              : { ...selectedProvider, skills: [...options.worktreeSkills] },
          ),
      }),
    ),
    Layer.provideMerge(
      Layer.mock(GitWorkflowService.GitWorkflowService)({
        localStatus: ({ cwd }) => {
          statusCalls.push(`${cwd}:${worktreeCreated}`);
          return Effect.succeed({
            isRepo: cwd === selectedWorkspaceRoot || worktreeCreated,
            hasPrimaryRemote: true,
            isDefaultRef: cwd === selectedWorkspaceRoot,
            refName:
              cwd === selectedWorkspaceRoot
                ? "main"
                : worktreeCreated
                  ? "t3code/workflow-17"
                  : null,
            hasWorkingTreeChanges: false,
            workingTree: { files: [], insertions: 0, deletions: 0 },
          });
        },
        createWorktree: (input) =>
          Effect.gen(function* () {
            worktreeCalls.push({ cwd: input.cwd, path: input.path });
            if (remainingWorktreeFailures > 0) {
              remainingWorktreeFailures -= 1;
              return yield* Effect.fail({ message: "simulated worktree failure" } as never);
            }
            const result = options.createWorktree
              ? yield* options.createWorktree(input)
              : { worktree: { path: input.path!, refName: input.newRefName! } };
            worktreeCreated = true;
            return result;
          }),
      }),
    ),
    Layer.provideMerge(
      Layer.mock(GitHubCli.GitHubCli)({
        execute:
          options.githubExecute ??
          (({ args }) => Effect.succeed(output(args[0] === "api" ? "Flow-Fly\n" : ""))),
        getRepositoryCloneUrls:
          options.githubGetRepositoryCloneUrls ??
          (() =>
            Effect.succeed({
              nameWithOwner: repository,
              url: `https://github.com/${repository}`,
              sshUrl: `git@github.com:${repository}.git`,
            })),
      }),
    ),
    Layer.provideMerge(
      Layer.succeed(
        ProcessRunner.ProcessRunner,
        options.processRunner ?? {
          run: () =>
            Effect.succeed(processOutput(`fork\tgit@github.com:${repository}.git (fetch)\n`)),
        },
      ),
    ),
    Layer.provideMerge(
      Layer.mock(OrchestrationCommandReceiptRepository)({
        upsert: () => Effect.void,
        getByCommandId: () => Effect.succeed(Option.none()),
      }),
    ),
    Layer.provideMerge(
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(environmentId),
      }),
    ),
    Layer.provideMerge(
      ServerConfig.layerTest(selectedWorkspaceRoot, { prefix: "workflow-director-test-" }),
    ),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  );
  const dispatch = (command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>) =>
    Effect.sync(() => {
      commands.push(command);
      return { sequence: 101 };
    });
  return { commands, dispatch, layer, projectionLayer, statusCalls, worktreeCalls };
}

describe("WorkflowDirectorService", () => {
  it.effect("pages active work across projects without leaking another environment", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const sql = yield* SqlClient.SqlClient;
      const timestamp = "2026-09-09T00:00:00.000Z";
      for (const id of ["project-1", "project-2"]) {
        yield* sql`
          INSERT INTO projection_projects
            (project_id, title, workspace_root, scripts_json, created_at, updated_at)
          VALUES (${id}, ${id}, ${`/tmp/${id}`}, '[]', ${timestamp}, ${timestamp})
        `;
      }
      for (let index = 0; index < 51; index++) {
        const directorId = `active-${String(index).padStart(2, "0")}`;
        const ownerThreadId = `thread-${directorId}`;
        const ownerProjectId = index % 2 === 0 ? "project-1" : "project-2";
        yield* sql`
          INSERT INTO projection_threads
            (thread_id, project_id, title, created_at, updated_at, archived_at, deleted_at)
          VALUES (${ownerThreadId}, ${ownerProjectId}, ${ownerThreadId}, ${timestamp}, ${timestamp}, NULL, NULL)
        `;
        yield* sql`
          INSERT INTO workflow_directors (
            director_id, batch_id, environment_id, project_id, repository, root_number,
            capability_number, thread_id, command_id, message_id, worktree_path,
            worktree_branch, status, requested_model, requested_instance_id,
            requested_effort, observed_match, initial_turn_disposition, is_current,
            created_at, updated_at
          ) VALUES (
            ${directorId}, ${`batch-${directorId}`}, ${environmentId}, ${ownerProjectId},
            ${repository}, ${index + 1}, ${index + 1}, ${ownerThreadId}, ${`command-${directorId}`},
            ${`message-${directorId}`}, ${`/tmp/${directorId}`}, ${`capability/${directorId}`},
            'active', 'gpt-6-astra', ${instanceId}, 'high', 'unknown', 'accepted', 1,
            ${timestamp}, ${timestamp}
          )
        `;
      }
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path,
          worktree_branch, status, requested_model, requested_instance_id,
          requested_effort, observed_match, initial_turn_disposition, is_current,
          created_at, updated_at
        ) VALUES (
          'other-environment', 'other-batch', 'environment-2', 'project-1', ${repository},
          100, 100, 'other-thread', 'other-command', 'other-message', '/tmp/other',
          'capability/other', 'active', 'gpt-6-astra', ${instanceId}, 'high', 'unknown',
          'accepted', 1, ${timestamp}, ${timestamp}
        )
      `;

      const first = yield* service.activeWork({});
      expect(first.entries).toHaveLength(50);
      expect(first.nextCursor).not.toBeNull();
      const second = yield* service.activeWork({ cursor: first.nextCursor! });
      expect(second.entries).toHaveLength(1);
      expect(second.nextCursor).toBeNull();
      expect(
        [...first.entries, ...second.entries].every(
          (entry) => entry.environmentId === environmentId,
        ),
      ).toBe(true);
      expect(
        new Set([...first.entries, ...second.entries].map((entry) => entry.projectId)),
      ).toEqual(new Set([ProjectId.make("project-1"), ProjectId.make("project-2")]));
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("keeps a completed capability visible when its root starts another native turn", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const sql = yield* SqlClient.SqlClient;
      const timestamp = "2026-09-09T00:00:00.000Z";
      yield* sql`
        INSERT INTO projection_projects
          (project_id, title, workspace_root, scripts_json, created_at, updated_at)
        VALUES (${projectId}, 'T3 Code', '/tmp/project-1', '[]', ${timestamp}, ${timestamp})
      `;
      yield* sql`
        INSERT INTO projection_threads
          (thread_id, project_id, title, created_at, updated_at, archived_at, deleted_at)
        VALUES ('resumed-root', ${projectId}, 'Resumed root', ${timestamp}, ${timestamp}, NULL, NULL)
      `;
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path,
          worktree_branch, status, requested_model, requested_instance_id,
          requested_effort, observed_match, initial_turn_disposition, is_current,
          created_at, updated_at
        ) VALUES (
          'completed-root', 'completed-batch', ${environmentId}, ${projectId}, ${repository},
          80, 80, 'resumed-root', 'completed-command', 'completed-message', '/tmp/completed',
          'capability/completed', 'active', 'gpt-6-astra', ${instanceId}, 'high', 'unknown',
          'accepted', 1, ${timestamp}, ${timestamp}
        )
      `;
      yield* sql`
        INSERT INTO workflow_capability_completions (
          completion_id, director_id, repository, capability_number, resulting_head,
          specification_fingerprint, breakdown_fingerprint, comment_body, status,
          close_confirmed, required_action, close_owned, created_at, updated_at
        ) VALUES (
          'completion-80', 'completed-root', ${repository}, 80, ${"a".repeat(40)},
          'spec', 'breakdown', 'Complete', 'completed', 1, 'none', 1, ${timestamp}, ${timestamp}
        )
      `;
      expect((yield* service.activeWork({})).entries).toEqual([]);

      yield* sql`
        INSERT INTO workflow_director_native_turns
          (director_id, native_session_id, native_turn_id, status, updated_at)
        VALUES ('completed-root', 'native-session', 'native-turn', 'running', ${timestamp})
      `;
      expect((yield* service.activeWork({})).entries).toContainEqual(
        expect.objectContaining({
          entryId: "director:completed-root",
          navigationThreadId: "resumed-root",
          activity: "running",
        }),
      );

      yield* sql`
        INSERT INTO projection_threads
          (thread_id, project_id, title, created_at, updated_at, archived_at, deleted_at)
        VALUES
          ('retired-root', ${projectId}, 'Retired root', ${timestamp}, ${timestamp}, NULL, NULL),
          ('successor-root', ${projectId}, 'Successor root', ${timestamp}, ${timestamp}, NULL, NULL)
      `;
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path,
          worktree_branch, status, requested_model, requested_instance_id,
          requested_effort, observed_match, initial_turn_disposition, is_current,
          created_at, updated_at
        ) VALUES
          ('retired-director', 'retired-batch', ${environmentId}, ${projectId}, ${repository},
            90, 90, 'retired-root', 'retired-command', 'retired-message', '/tmp/retired',
            'capability/retired', 'active', 'gpt-6-astra', ${instanceId}, 'high', 'unknown',
            'accepted', 0, ${timestamp}, ${timestamp}),
          ('successor-director', 'successor-batch', ${environmentId}, ${projectId}, ${repository},
            90, 90, 'successor-root', 'successor-command', 'successor-message', '/tmp/successor',
            'capability/successor', 'waiting', 'gpt-6-astra', ${instanceId}, 'high', 'unknown',
            'accepted', 1, ${timestamp}, ${timestamp})
      `;
      yield* sql`
        INSERT INTO workflow_capability_completions (
          completion_id, director_id, repository, capability_number, resulting_head,
          specification_fingerprint, breakdown_fingerprint, comment_body, status,
          close_confirmed, required_action, close_owned, created_at, updated_at
        ) VALUES (
          'completion-90', 'successor-director', ${repository}, 90, ${"b".repeat(40)},
          'spec-90', 'breakdown-90', 'Complete', 'completed', 1, 'none', 1,
          ${timestamp}, ${timestamp}
        )
      `;
      yield* sql`
        INSERT INTO workflow_director_native_turns
          (director_id, native_session_id, native_turn_id, status, updated_at)
        VALUES ('retired-director', 'retired-session', 'retired-turn', 'running', ${timestamp})
      `;
      expect((yield* service.activeWork({})).entries).toContainEqual(
        expect.objectContaining({
          entryId: "director:retired-director",
          title: "Earlier director for capability #90",
          navigationThreadId: "retired-root",
          activity: "running",
        }),
      );
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "admits before controlled collaboration dispatch and associates the ingested child",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const workerTicket = fixture.ticketDetails[0]!;
      let claimed = false;
      let directorThreadId: string | null = null;
      return Effect.gen(function* () {
        const controlledCollaboration = yield* controlledProviderService();
        const ingestedCommands: OrchestrationCommand[] = [];
        const test = harness({
          ...fixture,
          githubExecute: ({ args }) => {
            if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
            if (args[0] === "issue" && args[1] === "view") {
              return Effect.succeed(output(claimed ? "Flow-Fly\n" : ""));
            }
            if (args[0] === "issue" && args[1] === "edit") claimed = true;
            return Effect.succeed(output(""));
          },
          threadRuntimeContext: (threadId) =>
            Effect.succeed(
              threadId === directorThreadId
                ? Option.some({ id: threadId, title: "Capability director", session: null })
                : Option.none(),
            ),
        });
        const joinedLayer = ProviderRuntimeIngestionLive.pipe(
          Layer.provide(Layer.succeed(ProviderService, controlledCollaboration.service)),
          Layer.provide(
            Layer.mock(OrchestrationEngineService)({
              dispatch: (command) =>
                Effect.sync(() => {
                  ingestedCommands.push(command);
                  return { sequence: ingestedCommands.length };
                }),
              streamDomainEvents: Stream.empty,
            }),
          ),
          Layer.provide(test.projectionLayer),
          Layer.provide(ThreadBackgroundLiveness.layer),
          Layer.provide(ThreadPlanProgress.layer),
          Layer.provide(Layer.mock(CheckpointStore.CheckpointStore)({})),
          Layer.provide(ServerSettingsService.layerTest()),
          Layer.provideMerge(test.layer),
          Layer.provideMerge(NodeServices.layer),
        );

        return yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* WorkflowDirectorService.WorkflowDirectorService;
            const ingestion = yield* ProviderRuntimeIngestionService;
            yield* ingestion.start();
            const started = yield* service.start(
              { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
              test.dispatch,
            );
            directorThreadId = started.director.threadId;
            const invocation: McpInvocationContext.McpInvocationScope = {
              environmentId,
              threadId: started.director.threadId,
              providerInstanceId: instanceId,
              providerSessionId: "provider-session-director",
              capabilities: new Set(["preview"]),
              issuedAt: 1,
            };
            const prepared = yield* workflowDirectorHandlers
              .workflow_prepare_worker({
                ticketNumber: workerTicket.number,
                ownership: "workflow worker lifecycle",
                writePaths: ["apps/server/src/workflow"],
              })
              .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
            Object.assign(
              workerTicket,
              reinterpretTicket(workerTicket, [fixture.source, fixture.breakdownRecord], {
                assignees: ["Flow-Fly"],
              }),
            );

            const sql = yield* SqlClient.SqlClient;
            const durableDispatch = yield* sql<{
              readonly admissionId: string;
              readonly claimStatus: string;
              readonly dispatchId: string;
            }>`
            SELECT a.admission_id AS "admissionId", a.claim_status AS "claimStatus",
              d.dispatch_id AS "dispatchId"
            FROM workflow_director_admissions a
            JOIN workflow_worker_dispatches d ON d.admission_id = a.admission_id
            WHERE d.dispatch_id = ${prepared.dispatchId}
          `;
            expect(durableDispatch).toEqual([
              expect.objectContaining({
                claimStatus: "confirmed",
                dispatchId: prepared.dispatchId,
              }),
            ]);

            yield* controlledCollaboration.emitAndWaitForEnqueue([
              {
                type: "task.started",
                eventId: EventId.make("joined-worker-started"),
                provider: ProviderDriverKind.make("codex"),
                providerInstanceId: instanceId,
                threadId: started.director.threadId,
                createdAt: "2026-09-06T11:00:00.000Z",
                payload: {
                  taskId: RuntimeTaskId.make("joined-provider-child"),
                  description: "Implement ticket from prepared admission",
                  title: "joined-worker",
                  role: "worker",
                  model: "gpt-5.6-sol",
                  effort: "high",
                  parentAgentId: "provider-director",
                  agentPath: "/root/joined-worker",
                  timelineBypass: true,
                },
              },
            ]);
            yield* ingestion.drain;

            const associated = yield* workflowDirectorHandlers
              .workflow_associate_worker({
                associationToken: prepared.associationToken,
                providerThreadId: "joined-provider-child",
              })
              .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
            expect(associated).toMatchObject({
              dispatchId: prepared.dispatchId,
              association: "associated",
              providerThreadId: "joined-provider-child",
              providerStatus: "running",
              observedProfile: { match: "match" },
            });
            const status = yield* service.status({
              projectId,
              repository,
              ticketNumber: workerTicket.number,
            });
            expect(status.workers).toEqual([
              expect.objectContaining({
                dispatchId: prepared.dispatchId,
                providerThreadId: "joined-provider-child",
                association: "associated",
              }),
            ]);
            expect(ingestedCommands).toEqual(
              expect.arrayContaining([expect.objectContaining({ type: "thread.activity.append" })]),
            );
          }),
        ).pipe(Effect.provide(joinedLayer));
      });
    },
  );

  it.effect(
    "prepares admission before native child association and keeps it through reconnect",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const workerTicket = fixture.ticketDetails[0]!;
      let claimed = false;
      const test = harness({
        ...fixture,
        githubExecute: ({ args }) => {
          if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
          if (args[0] === "issue" && args[1] === "view") {
            return Effect.succeed(output(claimed ? "Flow-Fly\n" : ""));
          }
          if (args[0] === "issue" && args[1] === "edit") claimed = true;
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const prepared = yield* workflowDirectorHandlers
          .workflow_prepare_worker({
            ticketNumber: workerTicket.number,
            ownership: "workflow service and migration",
            writePaths: ["apps/server/src/workflow", "apps/server/src/persistence/Migrations"],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

        expect(prepared).toMatchObject({
          disposition: "prepared",
          admission: { ticketNumber: workerTicket.number, claimStatus: "confirmed" },
          requestedProfile: { model: "gpt-5.6-sol", effort: "high" },
        });
        Object.assign(
          workerTicket,
          reinterpretTicket(workerTicket, [fixture.source, fixture.breakdownRecord], {
            assignees: ["Flow-Fly"],
          }),
        );
        const repeated = yield* service.prepareWorker(
          environmentId,
          started.director.threadId,
          instanceId,
          {
            ticketNumber: workerTicket.number,
            ownership: "workflow service and migration",
            writePaths: ["apps/server/src/workflow"],
          },
        );
        expect(repeated).toMatchObject({
          disposition: "existing",
          dispatchId: prepared.dispatchId,
          associationToken: prepared.associationToken,
          worker: {
            association: "unconfirmed",
            providerStatus: "unconfirmed",
            writeReservation: "held",
          },
        });

        yield* recordWorkflowWorkerObservation({
          type: "task.started",
          eventId: EventId.make("worker-started"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-06T11:00:00.000Z",
          payload: {
            taskId: RuntimeTaskId.make("provider-child-1"),
            description: "worker-one",
            title: "worker-one",
            role: "worker",
            model: "gpt-5.6-sol",
            effort: "high",
            parentAgentId: "provider-director",
            agentPath: "/root/worker-one",
            timelineBypass: true,
          },
        });
        const associated = yield* workflowDirectorHandlers
          .workflow_associate_worker({
            associationToken: prepared.associationToken,
            providerThreadId: "provider-child-1",
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(associated).toMatchObject({
          ticketNumber: workerTicket.number,
          providerThreadId: "provider-child-1",
          association: "associated",
          providerStatus: "running",
          observedProfile: { match: "match" },
        });
        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("worker-profile-updated"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-06T11:00:30.000Z",
          payload: {
            taskId: RuntimeTaskId.make("provider-child-1"),
            model: "gpt-5.6-luna",
            effort: "low",
            timelineBypass: true,
          },
        });

        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("unknown-child-idle"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-06T11:01:00.000Z",
          payload: {
            taskId: RuntimeTaskId.make("provider-child-unknown"),
            status: "idle",
            model: "gpt-5.6-luna",
            effort: "low",
            parentAgentId: "provider-child-1",
            agentPath: "/root/worker-one/helper",
            timelineBypass: true,
          },
        });

        const reconnectedService = yield* WorkflowDirectorService.make;
        const reconnected = yield* reconnectedService.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(reconnected.workers).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              dispatchId: prepared.dispatchId,
              providerThreadId: "provider-child-1",
              association: "associated",
              providerStatus: "running",
              observedProfile: expect.objectContaining({ match: "mismatch" }),
              handoff: null,
            }),
            expect.objectContaining({
              dispatchId: null,
              providerThreadId: "provider-child-unknown",
              parentProviderThreadId: "provider-child-1",
              association: "unassociated",
              providerStatus: "idle",
            }),
          ]),
        );
        const fromTicket = yield* service.status({
          projectId,
          repository,
          ticketNumber: workerTicket.number,
        });
        expect(fromTicket.directorId).toBe(started.director.directorId);

        const handoff = yield* service.reportWorkerHandoff(
          environmentId,
          started.director.threadId,
          instanceId,
          {
            providerThreadId: "provider-child-1",
            outcome: "succeeded",
            summary: "Implemented and checked the worker slice.",
            commits: ["abc123"],
            checks: ["focused test passed"],
          },
        );
        expect(handoff.handoff).toEqual({
          outcome: "succeeded",
          summary: "Implemented and checked the worker slice.",
          commits: ["abc123"],
          checks: ["focused test passed"],
        });
        const repeatAfterHandoff = yield* service.prepareWorker(
          environmentId,
          started.director.threadId,
          instanceId,
          {
            ticketNumber: workerTicket.number,
            ownership: "workflow service and migration",
            writePaths: ["apps/server/src/workflow"],
          },
        );
        expect(repeatAfterHandoff).toMatchObject({
          disposition: "existing",
          dispatchId: prepared.dispatchId,
          associationToken: prepared.associationToken,
          worker: {
            association: "associated",
            providerStatus: "running",
            writeReservation: "held",
            handoff: { outcome: "succeeded" },
          },
        });
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("rejects overlapping write ownership before admitting another ticket", () => {
    const fixture = interpretedCapabilityFixture(2);
    const test = harness(fixture);
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const first = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "workflow server",
          writePaths: ["apps/server/src/workflow/"],
        },
      );
      yield* recordWorkflowWorkerObservation({
        type: "task.started",
        eventId: EventId.make("overlap-worker-started"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:00:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("overlap-worker"),
          description: "overlap worker",
          timelineBypass: true,
        },
      });
      yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
        associationToken: first.associationToken,
        providerThreadId: "overlap-worker",
      });
      yield* service.reportWorkerHandoff(environmentId, started.director.threadId, instanceId, {
        providerThreadId: "overlap-worker",
        outcome: "succeeded",
        summary: "Worker reported a result before provider settlement.",
        commits: ["abc123"],
        checks: ["focused test passed"],
      });
      const overlap = yield* service
        .prepareWorker(environmentId, started.director.threadId, instanceId, {
          ticketNumber: fixture.ticketDetails[1]!.number,
          ownership: "director service",
          writePaths: ["apps/server/src/workflow/WorkflowDirectorService.ts"],
        })
        .pipe(Effect.flip);
      expect(overlap).toMatchObject({
        _tag: "WorkflowDirectorError",
        failure: "not-ready",
        message: "Worker write ownership overlaps unsettled work.",
      });
      const status = yield* service.status({ projectId, repository, capabilityNumber: 17 });
      expect(status.admissionCount).toBe(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("derives reservation release from native closure and conservative ancestry", () => {
    const fixture = interpretedCapabilityFixture(3);
    const test = harness(fixture);
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const first = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "first worker",
          writePaths: ["apps/server/src/workflow"],
        },
      );
      const second = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[1]!.number,
          ownership: "known separate worker",
          writePaths: ["apps/web/src/components/workflow"],
        },
      );
      for (const [taskId, associationToken, parentAgentId] of [
        ["closure-worker", first.associationToken, "provider-director"],
        ["separate-worker", second.associationToken, "provider-director"],
      ] as const) {
        yield* recordWorkflowWorkerObservation({
          type: "task.started",
          eventId: EventId.make(`${taskId}-started`),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-06T11:00:00.000Z",
          payload: {
            taskId: RuntimeTaskId.make(taskId),
            parentAgentId,
            timelineBypass: true,
          },
        });
        yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
          associationToken,
          providerThreadId: taskId,
        });
      }

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("closure-worker-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:01:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("closure-worker"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      const reservation = () =>
        service
          .status({ projectId, repository, capabilityNumber: 17 })
          .pipe(
            Effect.map(
              (status) =>
                status.workers.find((worker) => worker.dispatchId === first.dispatchId)!
                  .writeReservation,
            ),
          );
      expect(yield* reservation()).toBe("released");

      const associatedDescendant = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[2]!.number,
          ownership: "associated descendant",
          writePaths: ["packages/contracts/src/workflow.ts"],
        },
      );
      yield* recordWorkflowWorkerObservation({
        type: "task.started",
        eventId: EventId.make("associated-descendant-started"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:01:30.000Z",
        payload: {
          taskId: RuntimeTaskId.make("associated-descendant"),
          parentAgentId: "closure-worker",
          timelineBypass: true,
        },
      });
      yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
        associationToken: associatedDescendant.associationToken,
        providerThreadId: "associated-descendant",
      });
      expect(yield* reservation()).toBe("held");
      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("associated-descendant-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:01:45.000Z",
        payload: {
          taskId: RuntimeTaskId.make("associated-descendant"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("released");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("nested-worker-idle"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:02:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("nested-worker"),
          parentAgentId: "closure-worker",
          status: "idle",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("held");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("nested-worker-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:03:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("nested-worker"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("released");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("unknown-worker-idle"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:04:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("unknown-worker"),
          parentAgentId: "unobserved-parent",
          status: "idle",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("held");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("unknown-worker-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:05:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("unknown-worker"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("released");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("closure-worker-late-metadata"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:06:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("closure-worker"),
          model: "gpt-5.6-sol",
          effort: "high",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("released");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("closure-worker-resumed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:07:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("closure-worker"),
          status: "running",
          timelineBypass: true,
        },
      });
      expect(yield* reservation()).toBe("held");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("starts a fresh same-ticket attempt only after native closure", () => {
    const fixture = interpretedCapabilityFixture(1);
    const test = harness(fixture);
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const first = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "workflow service",
          writePaths: ["apps/server/src/workflow"],
        },
      );
      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("retry-worker-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:00:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("retry-worker"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
        associationToken: first.associationToken,
        providerThreadId: "retry-worker",
      });

      const correction = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "workflow service correction",
          writePaths: ["apps/server/src/workflow"],
        },
      );
      expect(correction).toMatchObject({
        disposition: "prepared",
        admission: { admissionId: first.admission.admissionId },
        worker: { association: "unconfirmed", writeReservation: "held" },
      });
      expect(correction.dispatchId).not.toBe(first.dispatchId);

      const repeated = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "workflow service correction",
          writePaths: ["apps/server/src/workflow"],
        },
      );
      expect(repeated).toMatchObject({
        disposition: "existing",
        dispatchId: correction.dispatchId,
        associationToken: correction.associationToken,
        worker: { association: "unconfirmed", writeReservation: "held" },
      });

      const status = yield* service.status({ projectId, repository, capabilityNumber: 17 });
      expect(status.admissionCount).toBe(1);
      expect(status.workers.filter((worker) => worker.dispatchId)).toHaveLength(2);
      expect(
        status.workers.find((worker) => worker.dispatchId === first.dispatchId)?.writeReservation,
      ).toBe("released");

      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("correction-worker-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2099-09-06T11:01:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("correction-worker"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
        associationToken: correction.associationToken,
        providerThreadId: "correction-worker",
      });
      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("retry-worker-reactivated"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2099-09-06T11:02:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("retry-worker"),
          status: "running",
          timelineBypass: true,
        },
      });

      const reconciled = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "workflow service second correction",
          writePaths: ["apps/server/src/workflow"],
        },
      );
      expect(reconciled).toMatchObject({
        disposition: "existing",
        dispatchId: first.dispatchId,
        associationToken: first.associationToken,
        worker: { providerStatus: "running", writeReservation: "held" },
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("allows overlapping ownership after the prior native worker closes", () => {
    const fixture = interpretedCapabilityFixture(2);
    const test = harness(fixture);
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const first = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[0]!.number,
          ownership: "first worker",
          writePaths: ["apps/server/src/workflow"],
        },
      );
      yield* recordWorkflowWorkerObservation({
        type: "task.updated",
        eventId: EventId.make("overlap-release-closed"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: started.director.threadId,
        createdAt: "2026-09-06T11:00:00.000Z",
        payload: {
          taskId: RuntimeTaskId.make("overlap-release-worker"),
          status: "interrupted",
          nativeLifecycle: "closed",
          timelineBypass: true,
        },
      });
      yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
        associationToken: first.associationToken,
        providerThreadId: "overlap-release-worker",
      });

      const next = yield* service.prepareWorker(
        environmentId,
        started.director.threadId,
        instanceId,
        {
          ticketNumber: fixture.ticketDetails[1]!.number,
          ownership: "next worker",
          writePaths: ["apps/server/src/workflow/WorkflowDirectorService.ts"],
        },
      );
      expect(next).toMatchObject({
        disposition: "prepared",
        admission: { ticketNumber: fixture.ticketDetails[1]!.number },
        worker: { writeReservation: "held" },
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("accepts a selected fork when GitHub CLI defaults to the upstream repository", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "workflow-director-fork-repository-",
      });
      yield* runGit(cwd, ["init", "--initial-branch=main"]);
      yield* runGit(cwd, ["config", "user.name", "T3 Test"]);
      yield* runGit(cwd, ["config", "user.email", "t3@example.test"]);
      yield* runGit(cwd, ["commit", "--allow-empty", "-m", "initial"]);
      yield* runGit(cwd, ["remote", "add", "origin", "https://github.com/pingdotgg/t3code"]);
      yield* runGit(cwd, ["config", "remote.origin.gh-resolved", "base"]);
      yield* runGit(cwd, ["remote", "add", "fork", `git@github.com:${repository}.git`]);

      const fileSystemService = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const processRunner = yield* ProcessRunner.ProcessRunner;
      const implicitRepositoryCalls: ReadonlyArray<string>[] = [];
      const explicitRepositoryCalls: string[] = [];
      const test = harness({
        workspaceRoot: cwd,
        processRunner,
        createWorktree: realWorktreeCreator({
          fileSystem: fileSystemService,
          path,
          processRunner,
        }),
        githubExecute: ({ args }) => {
          if (args[0] === "repo") implicitRepositoryCalls.push(args);
          return Effect.succeed(output(args[0] === "repo" ? "pingdotgg/t3code\n" : "Flow-Fly\n"));
        },
        githubGetRepositoryCloneUrls: ({ repository: requestedRepository }) => {
          explicitRepositoryCalls.push(requestedRepository);
          return Effect.succeed({
            nameWithOwner: repository,
            url: `https://github.com/${repository}`,
            sshUrl: `git@github.com:${repository}.git`,
          });
        },
      });

      const evidence = yield* Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const result = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        return {
          result,
          worktreeExists: yield* fileSystem.exists(result.director.worktreePath),
          worktreeRemotes: (yield* runGit(result.director.worktreePath, ["remote", "-v"])).stdout,
        };
      }).pipe(Effect.provide(test.layer));

      expect(evidence.result).toMatchObject({
        disposition: "started",
        director: { status: "active" },
      });
      expect(evidence.worktreeExists).toBe(true);
      expect(evidence.worktreeRemotes).toContain(`git@github.com:${repository}.git`);
      expect(explicitRepositoryCalls).toEqual([repository, repository, repository]);
      expect(implicitRepositoryCalls).toEqual([]);
    }).pipe(Effect.provide(RealGitTestLayer)),
  );

  it.effect("rejects a checkout without a remote for the selected tracker", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "workflow-director-unrelated-repository-",
      });
      yield* runGit(cwd, ["init", "--initial-branch=main"]);
      yield* runGit(cwd, ["remote", "add", "origin", "https://github.com/example/unrelated.git"]);

      const processRunner = yield* ProcessRunner.ProcessRunner;
      const test = harness({ workspaceRoot: cwd, processRunner });
      const result = yield* Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        return yield* service
          .start(
            { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
            test.dispatch,
          )
          .pipe(Effect.result);
      }).pipe(Effect.provide(test.layer));

      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toMatchObject({
          failure: "workspace-unavailable",
          message: "The target workspace belongs to a different repository.",
        });
      }
      expect(test.worktreeCalls).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(RealGitTestLayer)),
  );

  it.effect("persists a fresh director and worktree before submitting its first turn", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const result = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );

      expect(result).toMatchObject({ disposition: "started", director: { status: "active" } });
      expect(test.worktreeCalls).toHaveLength(1);
      expect(test.commands).toHaveLength(1);
      expect(test.commands[0]?.bootstrap?.createThread).toMatchObject({
        projectId,
        worktreePath: result.director.worktreePath,
      });
      expect(test.commands[0]?.message.text).toContain("Sol/high");
      expect(test.commands[0]?.message.text).toContain(capabilityBody.trim());
      expect(test.commands[0]?.skills).toBeUndefined();
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("accepts the interpreter's proposal identifiers and resolved-slice evidence", () => {
    const fixture = interpretedCapabilityFixture(2, new Set([1]));
    const test = harness(fixture);
    return Effect.gen(function* () {
      expect(
        fixture.capability.evidence?.records.find(
          (record) => record.approvalKind === "ticket-breakdown",
        )?.scope,
      ).toBe("not-applicable");
      expect(fixture.ticketDetails[0]?.readiness).toMatchObject({
        status: "resolved",
        reasons: [{ kind: "resolution" }],
      });
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const result = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      expect(result.director.status).toBe("active");
      expect(test.commands[0]?.message.text).toContain("Delivery slice 1: resolved");
      expect(test.commands[0]?.message.text).toContain("Delivery slice 2: ready");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "verifies a reported approval from its persisted user message in another thread",
    () => {
      const fixture = interpretedCapabilityFixture(1, new Set(), {
        threadId: "approval-thread-b",
        messageId: "approval-message-b",
      });
      const test = harness({
        ...fixture,
        threadDetail: (requestedThreadId) =>
          Effect.succeed(
            requestedThreadId === "approval-thread-b"
              ? Option.some({
                  id: requestedThreadId,
                  messages: [{ id: "approval-message-b", role: "user", text: "I approve." }],
                } as never)
              : Option.none(),
          ),
      });
      return Effect.gen(function* () {
        expect(
          fixture.capability.evidence?.records.some(
            (record) =>
              record.approvalKind === "specification" && record.sourceAccess === "reported",
          ),
        ).toBe(true);
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const result = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        expect(result.director.status).toBe("active");
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "holds an incomplete native publication even when one interpreted slice is ready",
    () => {
      const fixture = interpretedCapabilityFixture(2);
      const test = harness({
        capability: fixture.capability,
        ticketDetails: [fixture.ticketDetails[0]!],
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const result = yield* service
          .start(
            { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
            test.dispatch,
          )
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure).toMatchObject({ failure: "breakdown-incomplete" });
        }
        expect(test.commands).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("keeps unavailable published evidence on the existing Start hold", () => {
    const fixture = interpretedCapabilityFixture(1);
    const deliveryTicket = fixture.ticketDetails[0]!;
    const unavailableBreakdown = evidenceComment({
      id: "start-unavailable-breakdown",
      createdAt: fixture.breakdownRecord.createdAt,
      body: fixture.breakdownRecord.body.replace(
        `Source: ${fixture.source.url}`,
        "Source: unavailable",
      ),
    });
    Object.assign(
      deliveryTicket,
      interpretWorkflowEvidence({
        issue: {
          id: deliveryTicket.id,
          url: deliveryTicket.url,
          number: deliveryTicket.number,
          title: deliveryTicket.title,
          kind: deliveryTicket.kind,
          state: "open",
          stateReason: null,
          labels: deliveryTicket.labels,
          assignees: [],
          body: deliveryTicket.body,
          comments: [],
          reopenedAt: [],
        },
        approvalComments: [unavailableBreakdown],
      }),
    );
    const test = harness({ ...fixture });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const result = yield* service
        .start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        )
        .pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toMatchObject({ failure: "breakdown-incomplete" });
      }
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("holds a fresh director while a capability prerequisite needs review", () => {
    const fixture = interpretedCapabilityFixture(1);
    const blockedCapability = capabilityWithManualPrerequisite(
      fixture,
      "Production access must be verified",
      false,
    );
    const test = harness({ ...fixture, capability: blockedCapability });
    return Effect.gen(function* () {
      expect(blockedCapability.readiness).toMatchObject({
        status: "needs-review",
        reasons: expect.arrayContaining([expect.objectContaining({ kind: "manual-condition" })]),
      });
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const result = yield* service
        .start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        )
        .pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toMatchObject({ failure: "not-ready" });
      }
      expect(test.worktreeCalls).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("retries proven unattempted worktree setup without replacing the director", () => {
    const fixture = interpretedCapabilityFixture(1);
    const test = harness({ ...fixture, failWorktreeAttempts: 1 });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const input = { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection };
      const held = yield* service.start(input, test.dispatch);
      expect(held).toMatchObject({ disposition: "held", director: { actions: ["retry"] } });
      const recovered = yield* service.start(input, test.dispatch);
      expect(recovered).toMatchObject({ disposition: "started", director: { status: "active" } });
      expect(recovered.director.directorId).toBe(held.director.directorId);
      expect(recovered.director.threadId).toBe(held.director.threadId);
      expect(test.worktreeCalls).toHaveLength(2);
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("holds worktree setup retry until capability reassessment clears", () => {
    const fixture = interpretedCapabilityFixture(1);
    const condition = "Production access must be verified";
    const clearedCapability = capabilityWithManualPrerequisite(fixture, condition, true);
    const worktreeCapability = capabilityWithManualPrerequisite(fixture, condition, false);
    const test = harness({
      ...fixture,
      capability: clearedCapability,
      worktreeCapability,
      failWorktreeAttempts: 1,
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const input = { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection };
      const held = yield* service.start(input, test.dispatch);
      const blockedRetry = yield* service.start(input, test.dispatch).pipe(Effect.result);
      expect(blockedRetry._tag).toBe("Failure");
      if (blockedRetry._tag === "Failure") {
        expect(blockedRetry.failure).toMatchObject({ failure: "not-ready" });
      }
      expect(test.worktreeCalls).toHaveLength(1);
      expect(test.commands).toHaveLength(0);

      Object.assign(worktreeCapability, clearedCapability);
      const recovered = yield* service.start(input, test.dispatch);
      expect(recovered).toMatchObject({
        disposition: "started",
        director: { directorId: held.director.directorId, status: "active" },
      });
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("uses the latest worktree-scoped evidence and skill paths for the first turn", () => {
    const fixture = interpretedCapabilityFixture(1);
    const worktreeCapability = { ...fixture.capability, title: "Fresh worktree capability" };
    const test = harness({
      ...fixture,
      worktreeCapability,
      worktreeSkills: [
        { name: "implement", path: "/worktree/skills/implement/SKILL.md", enabled: true },
        { name: "code-review", path: "/worktree/skills/code-review/SKILL.md", enabled: true },
      ],
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      expect(test.commands[0]?.message.text).toContain("Fresh worktree capability");
      expect(test.commands[0]?.message.text).toContain("/worktree/skills/implement/SKILL.md");
      expect(test.commands[0]?.message.text).toContain("/worktree/skills/code-review/SKILL.md");
      expect(test.commands[0]?.skills).toBeUndefined();
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "reuses one environment capability director across project and repository casing",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const test = harness(fixture);
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        const existing = yield* service.start(
          {
            projectId: ProjectId.make("another-project"),
            repository: "flow-fly/T3CODE",
            rootNumber: 10,
            capabilityNumber: 17,
            modelSelection,
          },
          test.dispatch,
        );
        expect(existing).toMatchObject({
          disposition: "existing",
          director: {
            directorId: started.director.directorId,
            projectId,
            threadId: started.director.threadId,
          },
        });
        expect(test.commands).toHaveLength(1);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("resumes an interrupted director once and reconciles an accepted retry", () => {
    const fixture = interpretedCapabilityFixture(1);
    let shell: Option.Option<unknown> = Option.none();
    const test = harness({
      ...fixture,
      threadShell: () => Effect.succeed(shell as never),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      shell = Option.some({
        id: started.director.threadId,
        latestTurn: { turnId: "interrupted-turn", state: "interrupted" },
        session: { status: "interrupted", activeTurnId: null },
      });
      const interrupted = yield* service.status({ projectId, repository, capabilityNumber: 17 });
      const input = {
        projectId,
        repository,
        capabilityNumber: 17,
        directorId: started.director.directorId,
        observation: interrupted.observation,
        modelSelection,
      };
      const resumed = yield* service.resume(input, test.dispatch);
      shell = Option.some({
        id: started.director.threadId,
        latestTurn: { turnId: "accepted-resume-turn", state: "active" },
        session: { status: "running", activeTurnId: "accepted-resume-turn" },
      });
      const retried = yield* service.resume(input, test.dispatch);
      expect(resumed.threadId).toBe(started.director.threadId);
      expect(retried.threadId).toBe(started.director.threadId);
      expect(test.commands).toHaveLength(2);
      expect(test.commands[1]?.bootstrap).toBeUndefined();
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "holds resume on current capability blockers but reconciles an accepted command",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const condition = "Production access must be verified";
      const clearedCapability = capabilityWithManualPrerequisite(fixture, condition, true);
      const worktreeCapability = { ...clearedCapability };
      let shell: Option.Option<unknown> = Option.none();
      const test = harness({
        ...fixture,
        capability: clearedCapability,
        worktreeCapability,
        threadShell: () => Effect.succeed(shell as never),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        shell = Option.some({
          id: started.director.threadId,
          latestTurn: { turnId: "interrupted-turn", state: "interrupted" },
          session: { status: "interrupted", activeTurnId: null },
        });
        const interrupted = yield* service.status({ projectId, repository, capabilityNumber: 17 });
        const input = {
          projectId,
          repository,
          capabilityNumber: 17,
          directorId: started.director.directorId,
          observation: interrupted.observation,
          modelSelection,
        };

        Object.assign(
          worktreeCapability,
          capabilityWithManualPrerequisite(fixture, condition, false),
        );
        const held = yield* service.resume(input, test.dispatch).pipe(Effect.result);
        expect(held._tag).toBe("Failure");
        if (held._tag === "Failure") {
          expect(held.failure).toMatchObject({ failure: "not-ready" });
        }
        expect(test.commands).toHaveLength(1);

        Object.assign(worktreeCapability, clearedCapability);
        const resumed = yield* service.resume(input, test.dispatch);
        expect(resumed.threadId).toBe(started.director.threadId);
        expect(test.commands).toHaveLength(2);

        Object.assign(
          worktreeCapability,
          capabilityWithManualPrerequisite(fixture, condition, false),
        );
        const reconciled = yield* service.resume(input, test.dispatch);
        expect(reconciled.threadId).toBe(started.director.threadId);
        expect(test.commands).toHaveLength(2);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "rejects unrelated work and treats a pre-existing same-login claim as a conflict",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const assignees = ["Flow-Fly"];
      const unrelated: WorkflowIssueDetail = {
        ...fixture.ticketDetails[0]!,
        id: "unrelated-ticket",
        number: 999,
        title: "Another capability's delivery",
      };
      const test = harness({
        ...fixture,
        extraDetails: [unrelated],
        githubExecute: ({ args }) => {
          if (args[0] === "repo") return Effect.succeed(output(`${repository}\n`));
          if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
          if (args[0] === "issue" && args[1] === "view") {
            return Effect.succeed(output(assignees.join("\n")));
          }
          if (args[0] === "issue" && args[1] === "edit")
            assignees.splice(0, assignees.length, "Flow-Fly");
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        const rejected = yield* service
          .admit({
            projectId,
            directorId: started.director.directorId,
            repository,
            ticketNumber: 999,
            purpose: "implement",
            ownership: "worker-unrelated",
          })
          .pipe(Effect.result);
        expect(rejected._tag).toBe("Failure");
        Object.assign(
          fixture.ticketDetails[0]!,
          reinterpretTicket(fixture.ticketDetails[0]!, [fixture.source, fixture.breakdownRecord], {
            assignees: ["Flow-Fly"],
          }),
        );
        const externalClaim = yield* service
          .admit({
            projectId,
            directorId: started.director.directorId,
            repository,
            ticketNumber: fixture.ticketDetails[0]!.number,
            purpose: "implement",
            ownership: "worker-one",
          })
          .pipe(Effect.result);
        expect(externalClaim._tag).toBe("Failure");
        Object.assign(
          fixture.ticketDetails[0]!,
          reinterpretTicket(fixture.ticketDetails[0]!, [fixture.source, fixture.breakdownRecord]),
        );
        const admitted = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: fixture.ticketDetails[0]!.number,
          purpose: "implement",
          ownership: "worker-one",
        });
        expect(admitted.admission).toMatchObject({
          claimLogin: "Flow-Fly",
          claimStatus: "conflict",
        });
        Object.assign(
          fixture.ticketDetails[0]!,
          reinterpretTicket(fixture.ticketDetails[0]!, [fixture.source, fixture.breakdownRecord], {
            assignees: ["Flow-Fly"],
          }),
        );
        const conflictedRetry = yield* service
          .admit({
            projectId,
            directorId: started.director.directorId,
            repository,
            ticketNumber: fixture.ticketDetails[0]!.number,
            purpose: "retry",
            ownership: "worker-one",
          })
          .pipe(Effect.result);
        expect(conflictedRetry._tag).toBe("Failure");
        expect(assignees).toEqual(["Flow-Fly"]);
        assignees.length = 0;
        Object.assign(
          fixture.ticketDetails[0]!,
          reinterpretTicket(fixture.ticketDetails[0]!, [fixture.source, fixture.breakdownRecord]),
        );
        const reconciled = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: fixture.ticketDetails[0]!.number,
          purpose: "retry",
          ownership: "worker-one",
        });
        expect(reconciled).toMatchObject({
          disposition: "existing",
          admission: { claimLogin: "Flow-Fly", claimStatus: "confirmed" },
          admissionCount: 1,
        });
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "holds an external same-login capability claim without consuming the existing ticket slot",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const assignees = new Map<number, string[]>();
      let claimEdits = 0;
      const test = harness({
        ...fixture,
        githubExecute: ({ args }) => {
          if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
          const number = Number(args[2]);
          if (args[0] === "issue" && args[1] === "view") {
            return Effect.succeed(output((assignees.get(number) ?? []).join("\n")));
          }
          if (args[0] === "issue" && args[1] === "edit") {
            claimEdits += 1;
            assignees.set(number, ["Flow-Fly"]);
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        const ticket = fixture.ticketDetails[0]!;
        const admitted = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: ticket.number,
          purpose: "implement",
          ownership: "worker-one",
        });
        expect(admitted.admission).toMatchObject({
          claimLogin: "Flow-Fly",
          claimStatus: "confirmed",
        });
        const editsAfterAdmission = claimEdits;

        assignees.set(fixture.capability.number, ["Flow-Fly"]);
        Object.assign(fixture.capability, reinterpretCapability(fixture, ["Flow-Fly"]));
        expect(fixture.capability.readiness).toMatchObject({ status: "claimed" });
        const held = yield* service
          .admit({
            projectId,
            directorId: started.director.directorId,
            repository,
            ticketNumber: ticket.number,
            purpose: "retry",
            ownership: "worker-one",
          })
          .pipe(Effect.result);
        expect(held._tag).toBe("Failure");
        if (held._tag === "Failure") {
          expect(held.failure).toMatchObject({ failure: "not-ready" });
        }
        expect(claimEdits).toBe(editsAfterAdmission);
        expect(test.commands).toHaveLength(1);
        expect(
          (yield* service.status({ projectId, repository, capabilityNumber: 17 })).admissionCount,
        ).toBe(1);

        assignees.delete(fixture.capability.number);
        Object.assign(fixture.capability, reinterpretCapability(fixture, []));
        expect(fixture.capability.readiness).toMatchObject({ status: "ready" });
        const resumed = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: ticket.number,
          purpose: "retry",
          ownership: "worker-one",
        });
        expect(resumed).toMatchObject({
          disposition: "existing",
          admission: {
            admissionId: admitted.admission?.admissionId,
            createdAt: admitted.admission?.createdAt,
            claimLogin: "Flow-Fly",
            claimStatus: "confirmed",
          },
          admissionCount: 1,
        });
        expect(claimEdits).toBe(editsAfterAdmission);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "preserves a blocked admission and permits review after its owned claim is ready again",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const assignees = new Map<number, string[]>();
      let claimEdits = 0;
      const test = harness({
        ...fixture,
        githubExecute: ({ args }) => {
          if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
          const number = Number(args[2]);
          if (args[0] === "issue" && args[1] === "view") {
            return Effect.succeed(output((assignees.get(number) ?? []).join("\n")));
          }
          if (args[0] === "issue" && args[1] === "edit") {
            claimEdits += 1;
            assignees.set(number, ["Flow-Fly"]);
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        const ticket = fixture.ticketDetails[0]!;
        const admitted = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: ticket.number,
          purpose: "implement",
          ownership: "worker-one",
        });
        expect(admitted.admission).toMatchObject({
          claimLogin: "Flow-Fly",
          claimStatus: "confirmed",
        });

        const blockerReadiness = interpretWorkflowEvidence({
          issue: {
            id: "blocking-dependency",
            url: `https://github.com/${repository}/issues/500`,
            number: 500,
            title: "Blocking dependency",
            kind: "task",
            state: "open",
            stateReason: null,
            labels: ["wayfinder:task"],
            assignees: [],
            body: "## Blocked by\n\nNone",
            comments: [],
            reopenedAt: [],
          },
        }).readiness;
        Object.assign(
          ticket,
          reinterpretTicket(ticket, [fixture.source, fixture.breakdownRecord], {
            blockers: [
              {
                id: "blocking-dependency",
                number: 500,
                title: "Blocking dependency",
                url: `https://github.com/${repository}/issues/500`,
                readiness: blockerReadiness,
              },
            ],
          }),
        );
        expect(ticket.readiness).toMatchObject({ status: "blocked" });
        const editsBeforeHold = claimEdits;
        const held = yield* service
          .admit({
            projectId,
            directorId: started.director.directorId,
            repository,
            ticketNumber: ticket.number,
            purpose: "retry",
            ownership: "worker-one",
          })
          .pipe(Effect.result);
        expect(held._tag).toBe("Failure");
        if (held._tag === "Failure") {
          expect(held.failure).toMatchObject({ failure: "not-ready" });
        }
        expect(claimEdits).toBe(editsBeforeHold);
        const heldStatus = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(heldStatus.admissionCount).toBe(1);

        Object.assign(
          ticket,
          reinterpretTicket(ticket, [fixture.source, fixture.breakdownRecord], {
            assignees: ["Flow-Fly"],
          }),
        );
        expect(ticket.readiness).toMatchObject({
          status: "claimed",
          reasons: expect.arrayContaining([expect.objectContaining({ kind: "claimed" })]),
        });
        const reviewed = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: ticket.number,
          purpose: "review",
          ownership: "reviewer-one",
        });
        expect(reviewed).toMatchObject({
          disposition: "existing",
          admission: {
            admissionId: admitted.admission?.admissionId,
            createdAt: admitted.admission?.createdAt,
            claimLogin: "Flow-Fly",
            claimStatus: "confirmed",
          },
          admissionCount: 1,
        });
        expect(claimEdits).toBe(editsBeforeHold);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("persists ten slice slots before claim results and rejects an eleventh", () => {
    const fixture = interpretedCapabilityFixture(11);
    const nestedTask: WorkflowIssueDetail = {
      ...fixture.ticketDetails[0]!,
      id: "nested-task",
      number: 999,
      title: "Nested task",
      kind: "task",
      parentNumber: fixture.ticketDetails[0]!.number,
      labels: ["wayfinder:task", "ready-for-agent"],
    };
    const test = harness({
      ...fixture,
      extraDetails: [nestedTask],
      locate: ({ number }) =>
        Effect.succeed({
          issue: number === nestedTask.number ? nestedTask : fixture.ticketDetails[0]!,
          ancestry: [
            rawIssueSummary(fixture.ticketDetails[0]!),
            rawIssueSummary(fixture.capability),
          ],
          ancestryComplete: true,
        }),
      githubExecute: ({ args }) =>
        Effect.succeed(output(args[0] === "repo" ? `${repository}\n` : "")),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      for (const [index, unit] of fixture.ticketDetails.slice(0, 10).entries()) {
        const result = yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: unit.number,
          purpose: index === 1 ? "retry" : index === 2 ? "review" : "implement",
          ownership: `worker-${index + 1}`,
        });
        expect(result).toMatchObject({
          disposition: "admitted",
          admission: { claimStatus: "uncertain" },
          admissionCount: index + 1,
        });
      }
      const retry = yield* service.admit({
        projectId,
        directorId: started.director.directorId,
        repository,
        ticketNumber: fixture.ticketDetails[0]!.number,
        purpose: "retry",
        ownership: "worker-one",
      });
      const nestedReuse = yield* service.admit({
        projectId,
        directorId: started.director.directorId,
        repository,
        ticketNumber: nestedTask.number,
        parentTicketNumber: fixture.ticketDetails[0]!.number,
        purpose: "implement",
        ownership: "worker-one/nested",
      });
      const parentTicket = fixture.ticketDetails[0]!;
      const blockerReadiness = interpretWorkflowEvidence({
        issue: {
          id: "parent-blocker",
          url: `https://github.com/${repository}/issues/500`,
          number: 500,
          title: "Parent blocker",
          kind: "task",
          state: "open",
          stateReason: null,
          labels: ["wayfinder:task"],
          assignees: [],
          body: "## Blocked by\n\nNone",
          comments: [],
          reopenedAt: [],
        },
      }).readiness;
      Object.assign(
        parentTicket,
        reinterpretTicket(parentTicket, [fixture.source, fixture.breakdownRecord], {
          blockers: [
            {
              id: "parent-blocker",
              number: 500,
              title: "Parent blocker",
              url: `https://github.com/${repository}/issues/500`,
              readiness: blockerReadiness,
            },
          ],
        }),
      );
      const blockedRetry = yield* service
        .admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: parentTicket.number,
          purpose: "retry",
          ownership: "worker-one",
        })
        .pipe(Effect.result);
      const nested = yield* service
        .admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: nestedTask.number,
          parentTicketNumber: parentTicket.number,
          purpose: "implement",
          ownership: "worker-one/nested",
        })
        .pipe(Effect.result);
      const eleventh = yield* service.admit({
        projectId,
        directorId: started.director.directorId,
        repository,
        ticketNumber: fixture.ticketDetails[10]!.number,
        purpose: "implement",
        ownership: "worker-eleven",
      });
      expect(retry).toMatchObject({ disposition: "existing", admissionCount: 10 });
      expect(nestedReuse).toMatchObject({ disposition: "admitted", admissionCount: 10 });
      expect(blockedRetry._tag).toBe("Failure");
      expect(nested._tag).toBe("Failure");
      expect(eleventh).toMatchObject({
        disposition: "limit-reached",
        admission: null,
        admissionCount: 10,
        directorStatus: "waiting",
      });
      expect(yield* service.status({ projectId, repository, capabilityNumber: 17 })).toMatchObject({
        admissionCount: 10,
      });
      const forgedParent = yield* service
        .admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: nestedTask.number,
          parentTicketNumber: fixture.ticketDetails[1]!.number,
          purpose: "retry",
          ownership: "worker-one/nested",
        })
        .pipe(Effect.result);
      expect(forgedParent._tag).toBe("Failure");
    }).pipe(Effect.provide(test.layer));
  });
  it.effect(
    "holds before interruption and preserves every live reassessment subject and trigger",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const liveCapability = structuredClone(fixture.capability);
      const githubCalls: ReadonlyArray<string>[] = [];
      let shell: Option.Option<unknown> = Option.none();
      let resumeThreadId: ThreadId | undefined;
      let triggerProjectedRoot = false;
      let triggerLateChild = false;
      const test = harness({
        capability: liveCapability,
        ticketDetails: fixture.ticketDetails,
        threadShell: () => Effect.succeed(shell as never),
        githubExecute: ({ args }) =>
          Effect.gen(function* () {
            githubCalls.push(args);
            if (triggerProjectedRoot && args.includes("--remove-label")) {
              triggerProjectedRoot = false;
              shell = Option.some({
                id: resumeThreadId!,
                latestTurn: { turnId: "projected-root", state: "active" },
                session: { status: "running", activeTurnId: "projected-root" },
              });
            } else if (triggerLateChild && args.includes("--remove-label")) {
              triggerLateChild = false;
              yield* recordWorkflowWorkerObservation({
                type: "task.updated",
                eventId: EventId.make("late-child-during-resume"),
                provider: ProviderDriverKind.make("codex"),
                providerInstanceId: instanceId,
                threadId: resumeThreadId!,
                createdAt: "2026-09-07T12:03:01.000Z",
                payload: {
                  taskId: RuntimeTaskId.make("late-resume-child"),
                  status: "running",
                  timelineBypass: true,
                  nativeTurn: {
                    sessionId: "late-resume-child",
                    turnId: "late-resume-turn",
                    status: "running",
                  },
                },
              });
              yield* recordWorkflowWorkerObservation({
                type: "turn.started",
                eventId: EventId.make("late-root-during-resume"),
                provider: ProviderDriverKind.make("codex"),
                providerInstanceId: instanceId,
                threadId: resumeThreadId!,
                turnId: TurnId.make("late-root-turn"),
                createdAt: "2026-09-07T12:03:01.000Z",
                payload: {},
                raw: {
                  payload: {
                    threadId: "native-director",
                    turn: { id: "late-root-turn" },
                  },
                },
              } as ProviderRuntimeEvent);
            }
            return output(args[0] === "api" ? "Flow-Fly\n" : "");
          }),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const sql = yield* SqlClient.SqlClient;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        resumeThreadId = started.director.threadId;
        yield* seedDirectorProviderIdentity(sql, started.director.directorId);
        yield* recordWorkflowWorkerObservation({
          type: "turn.started",
          eventId: EventId.make("director-native-running"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:00:00.000Z",
          payload: { turnId: "orchestration-turn" },
          raw: { payload: { threadId: "native-director", turn: { id: "native-root-turn" } } },
        } as ProviderRuntimeEvent);
        yield* recordWorkflowWorkerObservation({
          type: "task.started",
          eventId: EventId.make("child-running-before-hold"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:00:01.000Z",
          payload: {
            taskId: RuntimeTaskId.make("native-child"),
            description: "worker",
            timelineBypass: true,
            nativeTurn: {
              sessionId: "native-child",
              turnId: "native-child-turn",
              status: "running",
            },
          },
        });

        const changedRecords = liveCapability.evidence!.records.map((record) =>
          record.kind === "approval" && record.approvalKind === "specification"
            ? { ...record, scope: "changed" as const }
            : record,
        );
        const blocker = {
          ...issue(999, "Reopened prerequisite", "task", null),
          readiness: {
            status: "blocked" as const,
            reasons: [
              {
                kind: "open-blocker" as const,
                message: "The prerequisite reopened.",
                source: `https://github.com/${repository}/issues/999`,
              },
            ],
          },
        };
        const invalidated = {
          ...liveCapability,
          blockedBy: [blocker],
          evidence: { ...liveCapability.evidence!, records: changedRecords },
        };
        Object.assign(liveCapability, invalidated);
        const interruptCommands: OrchestrationCommand[] = [];
        yield* service.reassess({ projectId, issue: invalidated }, (command) =>
          Effect.gen(function* () {
            const rows = yield* sql<{
              readonly status: string;
              readonly trackerBody: string | null;
            }>`
            SELECT d.status, r.tracker_body AS "trackerBody"
            FROM workflow_directors d JOIN workflow_reassessments r
              ON r.director_id = d.director_id
            WHERE d.director_id = ${started.director.directorId}
          `;
            expect(rows[0]).toMatchObject({ status: "held" });
            expect(rows[0]?.trackerBody).toContain("Outcome: scope-change");
            interruptCommands.push(command);
            return { sequence: 501 };
          }),
        );

        const held = yield* service.status({ projectId, repository, capabilityNumber: 17 });
        expect(interruptCommands).toHaveLength(1);
        expect(held).toMatchObject({
          status: "held",
          reassessment: {
            triggerKind: "scope-change",
            stopRequestStatus: "submitted",
            trackerStatus: "uncertain",
            subjects: expect.arrayContaining([
              expect.objectContaining({ kind: "director", outcome: "unknown" }),
              expect.objectContaining({
                kind: "unknown-child",
                providerThreadId: "native-child",
                outcome: "unknown",
              }),
            ]),
          },
        });
        const triggers = yield* sql<{ readonly kind: string; readonly number: number }>`
        SELECT trigger_kind AS kind, trigger_issue_number AS number
        FROM workflow_reassessment_triggers
        WHERE reassessment_id = ${held.reassessment!.reassessmentId}
        ORDER BY trigger_kind, trigger_issue_number
      `;
        expect(triggers).toEqual([
          { kind: "prerequisite", number: 999 },
          { kind: "scope-change", number: 17 },
        ]);
        expect(held.reassessment?.triggers).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ kind: "prerequisite", issueNumber: 999 }),
            expect.objectContaining({ kind: "scope-change", issueNumber: 17 }),
          ]),
        );
        expect(githubCalls.some((args) => args[0] === "issue" && args[1] === "comment")).toBe(true);

        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("native-child-interrupt-failed"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:00:01.500Z",
          payload: {
            taskId: RuntimeTaskId.make("native-child"),
            timelineBypass: true,
            nativeInterruption: {
              attemptId: "first-stop-attempt",
              sessionId: "native-child",
              turnId: "native-child-turn",
              requestStatus: "failed",
              detail: "The provider rejected the child interrupt request.",
            },
          },
        });
        const afterRejectedChild = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(afterRejectedChild.reassessment?.subjects).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              providerThreadId: "native-child",
              nativeTurnId: "native-child-turn",
              requestStatus: "failed",
              outcome: "failed",
            }),
          ]),
        );

        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("late-child-running"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:00:02.000Z",
          payload: {
            taskId: RuntimeTaskId.make("late-child"),
            status: "running",
            timelineBypass: true,
          },
        });
        const withLateChild = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(withLateChild.actions).not.toContain("resume");
        expect(withLateChild.reassessment?.subjects).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ providerThreadId: "late-child", outcome: "resumed" }),
          ]),
        );

        const trackerRows = yield* sql<{ readonly body: string }>`
        SELECT tracker_body AS body FROM workflow_reassessments
        WHERE reassessment_id = ${held.reassessment!.reassessmentId}
      `;
        const trackerBody = trackerRows[0]!.body;
        const scopeChangeEvidence = {
          id: "persisted-scope-change",
          url: `${liveCapability.url}#issuecomment-scope-change`,
          createdAt: "2026-09-07T12:01:00.000Z",
          kind: "reassessment" as const,
          state: "superseded" as const,
          sourceAccess: "verified" as const,
          scope: "current" as const,
          summary: "Scope changed while work was active.",
          source: liveCapability.url,
          outcome: "scope-change" as const,
          evidence: `artifact: workflow-reassessment:${held.reassessment!.reassessmentId}`,
          bodyFingerprint: workflowEvidenceBodyFingerprint(trackerBody),
        };
        const clearedEvidence = {
          id: "current-cleared-reassessment",
          url: `${liveCapability.url}#issuecomment-cleared`,
          createdAt: "2099-09-07T12:02:00.000Z",
          kind: "reassessment" as const,
          state: "current" as const,
          sourceAccess: "verified" as const,
          scope: "current" as const,
          summary: "Reassessment cleared after renewed approval.",
          source: scopeChangeEvidence.url,
          outcome: "cleared" as const,
          evidence: `Supersedes: ${scopeChangeEvidence.url}`,
        };
        Object.assign(liveCapability, {
          ...fixture.capability,
          evidence: {
            ...fixture.capability.evidence!,
            records: [
              ...fixture.capability.evidence!.records,
              scopeChangeEvidence,
              clearedEvidence,
            ],
          },
        });
        const commentCount = githubCalls.filter(
          (args) => args[0] === "issue" && args[1] === "comment",
        ).length;
        const retried = yield* service.retryReassessment(
          {
            projectId,
            repository,
            capabilityNumber: 17,
            directorId: started.director.directorId,
            observation: withLateChild.observation,
          },
          (command) =>
            Effect.sync(() => {
              interruptCommands.push(command);
              return { sequence: 502 };
            }),
        );
        expect(retried.reassessment?.trackerStatus).toBe("confirmed");
        expect(interruptCommands).toHaveLength(2);
        expect(
          githubCalls.filter((args) => args[0] === "issue" && args[1] === "comment"),
        ).toHaveLength(commentCount);
        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("native-child-interrupt-acknowledged"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:02:30.000Z",
          payload: {
            taskId: RuntimeTaskId.make("native-child"),
            timelineBypass: true,
            nativeInterruption: {
              attemptId: "second-stop-attempt",
              sessionId: "native-child",
              turnId: "native-child-turn",
              requestStatus: "acknowledged",
            },
          },
        });
        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("native-child-interrupted"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:02:31.000Z",
          payload: {
            taskId: RuntimeTaskId.make("native-child"),
            status: "interrupted",
            timelineBypass: true,
            nativeTurn: {
              sessionId: "native-child",
              turnId: "native-child-turn",
              status: "interrupted",
            },
            nativeInterruption: {
              attemptId: "second-stop-attempt",
              sessionId: "native-child",
              turnId: "native-child-turn",
              requestStatus: "acknowledged",
              completionStatus: "interrupted",
            },
          },
        });
        const afterNativeChildStop = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(afterNativeChildStop.reassessment?.subjects).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ providerThreadId: "native-child", outcome: "stopped" }),
          ]),
        );

        yield* recordWorkflowWorkerObservation({
          type: "turn.completed",
          eventId: EventId.make("director-native-ended-after-hold"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:03:00.000Z",
          payload: { state: "completed" },
          raw: { payload: { threadId: "native-director", turn: { id: "native-root-turn" } } },
        } as ProviderRuntimeEvent);
        yield* sql`UPDATE projection_thread_sessions
          SET status = 'interrupted', active_turn_id = NULL
          WHERE thread_id = ${started.director.threadId}`;
        yield* sql`UPDATE workflow_interruption_subjects SET outcome = 'stopped',
        request_status = 'acknowledged', detail = 'Matching native child turns ended.'
        WHERE reassessment_id = ${held.reassessment!.reassessmentId} AND subject_kind != 'director'`;
        shell = Option.some({
          id: started.director.threadId,
          latestTurn: { turnId: "orchestration-turn", state: "completed" },
          session: { status: "interrupted", activeTurnId: null },
        });
        const resumable = yield* service.status({ projectId, repository, capabilityNumber: 17 });
        expect(resumable.actions).toContain("resume");

        triggerProjectedRoot = true;
        const projectedRootResume = yield* service
          .resume(
            {
              projectId,
              repository,
              capabilityNumber: 17,
              directorId: started.director.directorId,
              observation: resumable.observation,
              modelSelection,
            },
            test.dispatch,
          )
          .pipe(Effect.result);
        expect(triggerProjectedRoot).toBe(false);
        expect(projectedRootResume._tag).toBe("Failure");

        shell = Option.some({
          id: started.director.threadId,
          latestTurn: { turnId: "orchestration-turn", state: "completed" },
          session: { status: "interrupted", activeTurnId: null },
        });
        const resumableAfterProjectedRoot = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(resumableAfterProjectedRoot.actions).toContain("resume");
        triggerLateChild = true;
        const racedResume = yield* service
          .resume(
            {
              projectId,
              repository,
              capabilityNumber: 17,
              directorId: started.director.directorId,
              observation: resumableAfterProjectedRoot.observation,
              modelSelection,
            },
            test.dispatch,
          )
          .pipe(Effect.result);
        expect(racedResume._tag).toBe("Failure");
        const racedStatus = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(racedStatus.reassessment?.subjects).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ kind: "director", outcome: "resumed" }),
            expect.objectContaining({
              providerThreadId: "late-resume-child",
              outcome: "resumed",
            }),
          ]),
        );
        yield* recordWorkflowWorkerObservation({
          type: "task.updated",
          eventId: EventId.make("late-child-settled-before-retry"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          createdAt: "2026-09-07T12:03:02.000Z",
          payload: {
            taskId: RuntimeTaskId.make("late-resume-child"),
            status: "interrupted",
            timelineBypass: true,
            nativeTurn: {
              sessionId: "late-resume-child",
              turnId: "late-resume-turn",
              status: "interrupted",
            },
          },
        });
        yield* recordWorkflowWorkerObservation({
          type: "turn.completed",
          eventId: EventId.make("late-root-settled-before-retry"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: started.director.threadId,
          turnId: TurnId.make("late-root-turn"),
          createdAt: "2026-09-07T12:03:02.000Z",
          payload: { state: "completed" },
          raw: {
            payload: {
              threadId: "native-director",
              turn: { id: "late-root-turn" },
            },
          },
        } as ProviderRuntimeEvent);
        const resumableAgain = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        const finalResumeInput = {
          projectId,
          repository,
          capabilityNumber: 17,
          directorId: started.director.directorId,
          observation: resumableAgain.observation,
          modelSelection,
        };
        let acceptedMessageId: MessageId | undefined;
        const resumed = yield* service.resume(finalResumeInput, (command) =>
          Effect.gen(function* () {
            acceptedMessageId = command.message.messageId;
            yield* sql`INSERT INTO orchestration_command_receipts (
                command_id, aggregate_kind, aggregate_id, accepted_at,
                result_sequence, status, error
              ) VALUES (${command.commandId}, 'thread', ${command.threadId},
                '2026-09-07T12:03:03.000Z', 503, 'accepted', NULL)`;
            yield* sql`INSERT INTO projection_turns (
                thread_id, turn_id, pending_message_id, state, requested_at,
                checkpoint_files_json
              ) VALUES (${command.threadId}, 'unrelated-root', 'unrelated-message',
                'running', '2026-09-07T12:03:03.000Z', '[]')`;
            yield* sql`UPDATE projection_thread_sessions
                SET status = 'running', active_turn_id = 'unrelated-root'
                WHERE thread_id = ${command.threadId}`;
            return { sequence: 503 };
          }),
        );
        expect(resumed.status).toBe("held");
        expect(resumed.reassessment).not.toBeNull();

        yield* sql`DELETE FROM projection_turns
          WHERE thread_id = ${started.director.threadId}`;
        yield* sql`INSERT INTO projection_turns (
          thread_id, turn_id, pending_message_id, state, requested_at, checkpoint_files_json
        ) VALUES (${started.director.threadId}, NULL, ${acceptedMessageId!}, 'pending',
          '2026-09-07T12:03:03.000Z', '[]')`;
        yield* sql`UPDATE projection_thread_sessions
          SET status = 'starting', active_turn_id = NULL
          WHERE thread_id = ${started.director.threadId}`;
        shell = Option.some({
          id: started.director.threadId,
          latestTurn: { turnId: "orchestration-turn", state: "completed" },
          session: { status: "starting", activeTurnId: null },
        });

        const reconciled = yield* service.resume(finalResumeInput, test.dispatch);
        expect(reconciled.status).toBe("active");
        expect(reconciled.reassessment).toBeNull();
        expect(test.commands).toHaveLength(1);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "interrupts cancelled prerequisites but retains unavailable evidence as uncertainty",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const liveCapability = structuredClone(fixture.capability);
      const test = harness({ capability: liveCapability, ticketDetails: fixture.ticketDetails });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        let interrupts = 0;
        Object.assign(liveCapability, {
          ...fixture.capability,
          evidence: {
            ...fixture.capability.evidence!,
            records: fixture.capability.evidence!.records.map((record) =>
              record.kind === "approval" && record.approvalKind === "specification"
                ? { ...record, scope: "changed" as const, authority: "reported" as const }
                : record,
            ),
          },
        });
        yield* service.reassess({ projectId, issue: liveCapability }, () =>
          Effect.sync(() => {
            interrupts += 1;
            return { sequence: 1 };
          }),
        );
        expect(interrupts).toBe(0);

        const unavailableClosedBlocker = {
          ...issue(999, "Closed prerequisite", "task", null),
          state: "closed" as const,
          stateReason: "completed" as const,
          readiness: {
            status: "closed-unverified" as const,
            reasons: [
              {
                kind: "missing-resolution" as const,
                message: "Resolution evidence is unavailable.",
                source: `https://github.com/${repository}/issues/999`,
              },
            ],
          },
        };
        Object.assign(liveCapability, {
          ...fixture.capability,
          blockedBy: [unavailableClosedBlocker],
        });
        yield* service.reassess({ projectId, issue: liveCapability }, () =>
          Effect.sync(() => {
            interrupts += 1;
            return { sequence: 2 };
          }),
        );
        expect(interrupts).toBe(0);
        const status = yield* service.status({ projectId, repository, capabilityNumber: 17 });
        expect(status.directorId).toBe(started.director.directorId);
        expect(status.reassessment).toBeNull();

        Object.assign(liveCapability, {
          ...fixture.capability,
          blockedBy: [
            {
              ...unavailableClosedBlocker,
              stateReason: "not_planned" as const,
              readiness: {
                status: "cancelled" as const,
                reasons: [
                  {
                    kind: "cancelled" as const,
                    message: "This work was cancelled; it does not satisfy dependents.",
                    source: unavailableClosedBlocker.url,
                  },
                ],
              },
            },
          ],
        });
        yield* service.reassess({ projectId, issue: liveCapability }, () =>
          Effect.sync(() => {
            interrupts += 1;
            return { sequence: 3 };
          }),
        );
        expect(interrupts).toBe(1);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("holds legacy admissions that have no durable scope baseline", () => {
    const fixture = interpretedCapabilityFixture(1);
    let claimed = false;
    const test = harness({
      ...fixture,
      githubExecute: ({ args }) => {
        if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
        if (args[0] === "issue" && args[1] === "view") {
          return Effect.succeed(output(claimed ? "Flow-Fly\n" : ""));
        }
        if (args[0] === "issue" && args[1] === "edit") claimed = true;
        return Effect.succeed(output(""));
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const sql = yield* SqlClient.SqlClient;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const ticketNumber = fixture.ticketDetails[0]!.number;
      yield* service.admit({
        projectId,
        directorId: started.director.directorId,
        repository,
        ticketNumber,
        purpose: "implement",
        ownership: "legacy baseline test",
      });
      yield* sql`UPDATE workflow_director_admissions SET scope_body = NULL,
        scope_fingerprint = NULL WHERE director_id = ${started.director.directorId}`;

      const admission = yield* service
        .admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber,
          purpose: "retry",
          ownership: "legacy baseline test",
        })
        .pipe(Effect.result);
      const preparation = yield* service
        .prepareWorker(environmentId, started.director.threadId, instanceId, {
          ticketNumber,
          ownership: "legacy baseline test",
          writePaths: ["apps/server/src/workflow"],
        })
        .pipe(Effect.result);
      expect(admission).toMatchObject({
        _tag: "Failure",
        failure: { failure: "not-ready", message: expect.stringContaining("legacy admission") },
      });
      expect(preparation).toMatchObject({
        _tag: "Failure",
        failure: { failure: "not-ready", message: expect.stringContaining("legacy admission") },
      });
      yield* sql`UPDATE workflow_director_admissions SET scope_body = ${fixture.ticketDetails[0]!.body},
        scope_fingerprint = ${workflowEvidenceBodyFingerprint(fixture.ticketDetails[0]!.body)},
        current_scope_body = NULL, current_scope_fingerprint = NULL
        WHERE director_id = ${started.director.directorId}`;
      yield* sql`UPDATE workflow_directors SET specification_fingerprint = NULL
        WHERE director_id = ${started.director.directorId}`;
      const legacyDirector = yield* service
        .admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber,
          purpose: "retry",
          ownership: "legacy baseline test",
        })
        .pipe(Effect.result);
      expect(legacyDirector).toMatchObject({
        _tag: "Failure",
        failure: { failure: "not-ready", message: expect.stringContaining("legacy director") },
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "requires reassessment for changed admitted scope and refreshes authority on resume",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const liveCapability = structuredClone(fixture.capability);
      const liveTicket = structuredClone(fixture.ticketDetails[0]!);
      let shell: Option.Option<unknown> = Option.none();
      let claimed = false;
      const test = harness({
        capability: liveCapability,
        ticketDetails: [liveTicket],
        threadShell: () => Effect.succeed(shell as never),
        githubExecute: ({ args }) => {
          if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
          if (args[0] === "issue" && args[1] === "view") {
            return Effect.succeed(output(claimed ? "Flow-Fly\n" : ""));
          }
          if (args[0] === "issue" && args[1] === "edit") claimed = true;
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const sql = yield* SqlClient.SqlClient;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        yield* service.admit({
          projectId,
          directorId: started.director.directorId,
          repository,
          ticketNumber: liveTicket.number,
          purpose: "implement",
          ownership: "changed ticket scope",
        });
        Object.assign(liveTicket, {
          ...liveTicket,
          body: `${liveTicket.body}\n\nRenewed approved delivery detail.`,
          evidence: {
            ...liveTicket.evidence!,
            records: liveTicket.evidence!.records.map((record) =>
              record.kind === "approval"
                ? { ...record, scope: "current" as const, authority: "verified" as const }
                : record,
            ),
          },
        });

        const bypass = yield* service
          .admit({
            projectId,
            directorId: started.director.directorId,
            repository,
            ticketNumber: liveTicket.number,
            purpose: "retry",
            ownership: "changed ticket scope",
          })
          .pipe(Effect.result);
        expect(bypass).toMatchObject({
          _tag: "Failure",
          failure: { failure: "not-ready", message: expect.stringContaining("scope changed") },
        });

        let interruptCount = 0;
        yield* service.reassess({ projectId, issue: liveCapability }, () =>
          Effect.sync(() => {
            interruptCount += 1;
            return { sequence: 800 };
          }),
        );
        const held = yield* service.status({ projectId, repository, capabilityNumber: 17 });
        expect(held.reassessment?.triggers).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ kind: "scope-change", issueNumber: liveTicket.number }),
          ]),
        );
        const trackerRows = yield* sql<{ readonly body: string }>`
        SELECT tracker_body AS body FROM workflow_reassessments
        WHERE reassessment_id = ${held.reassessment!.reassessmentId}
      `;
        const scopeSource = held.reassessment!.triggers.find(
          (trigger) => trigger.issueNumber === liveTicket.number,
        )!.source;
        const scopeRecordUrl = `${liveCapability.url}#issuecomment-ticket-scope-change`;
        Object.assign(liveCapability, {
          ...liveCapability,
          evidence: {
            ...liveCapability.evidence!,
            records: [
              ...liveCapability.evidence!.records,
              {
                id: "ticket-scope-change",
                url: scopeRecordUrl,
                createdAt: "2026-09-07T13:00:00.000Z",
                kind: "reassessment" as const,
                state: "superseded" as const,
                sourceAccess: "verified" as const,
                scope: "current" as const,
                summary: "Ticket scope changed.",
                source: scopeSource,
                outcome: "scope-change" as const,
                evidence: `artifact: workflow-reassessment:${held.reassessment!.reassessmentId}`,
                bodyFingerprint: workflowEvidenceBodyFingerprint(trackerRows[0]!.body),
              },
              {
                id: "ticket-scope-cleared",
                url: `${liveCapability.url}#issuecomment-ticket-scope-cleared`,
                createdAt: "2099-09-07T13:01:00.000Z",
                kind: "reassessment" as const,
                state: "current" as const,
                sourceAccess: "verified" as const,
                scope: "current" as const,
                summary: "Ticket scope was reapproved.",
                source: scopeRecordUrl,
                outcome: "cleared" as const,
                evidence: `Supersedes: ${scopeRecordUrl}`,
              },
            ],
          },
        });
        Object.assign(liveTicket, {
          ...liveTicket,
          state: "closed" as const,
          stateReason: "completed" as const,
          labels: liveTicket.labels.filter((label) => label !== "ready-for-agent"),
          readiness: {
            status: "resolved" as const,
            reasons: [
              {
                kind: "resolution" as const,
                message: "Completed with current resolution evidence.",
                source: `${liveTicket.url}#issuecomment-current-resolution`,
              },
            ],
          },
        });
        yield* sql`UPDATE workflow_interruption_subjects SET outcome = 'stopped',
        request_status = 'acknowledged' WHERE reassessment_id = ${held.reassessment!.reassessmentId}`;
        shell = Option.some({
          id: started.director.threadId,
          latestTurn: { turnId: "ended-before-resume", state: "completed" },
          session: { status: "idle", activeTurnId: null },
        });
        const resumable = yield* service.status({ projectId, repository, capabilityNumber: 17 });
        const resumed = yield* service.resume(
          {
            projectId,
            repository,
            capabilityNumber: 17,
            directorId: started.director.directorId,
            observation: resumable.observation,
            modelSelection,
          },
          test.dispatch,
        );
        expect(resumed.reassessment).toBeNull();

        yield* service.reassess({ projectId, issue: liveCapability }, () =>
          Effect.sync(() => {
            interruptCount += 1;
            return { sequence: 801 };
          }),
        );
        expect(interruptCount).toBe(1);
        expect(
          (yield* service.status({ projectId, repository, capabilityNumber: 17 })).reassessment,
        ).toBeNull();
      }).pipe(Effect.provide(test.layer));
    },
  );
});

describe("workflowTicketResolutionBody", () => {
  it("keeps staffing limits outside the parsed completion evidence", () => {
    const body = WorkflowDirectorService.workflowTicketResolutionBody({
      resolutionId: "resolution-1",
      repository,
      ticketNumber: 18,
      review: {
        reviewId: "review-1",
        admissionId: "admission-1",
        ticketNumber: 18,
        implementationProviderThreadId: "implementation-1",
        fixedBase: "a".repeat(40),
        implementationHead: "b".repeat(40),
        status: "reported",
        association: "associated",
        providerThreadId: "review-coordinator",
        parentProviderThreadId: "provider-director",
        providerStatus: "idle",
        settlementEvidence: "native-closed",
        requestedProfile: {
          model: "gpt-6-astra",
          effort: "medium",
          skillPath: "/skills/code-review/SKILL.md",
        },
        observedProfile: { model: null, effort: null, match: "unknown" },
        checks: [
          {
            label: "focused tests",
            command: "vp test run focused.test.ts",
            toolCallId: "tool-1",
            exitCode: 0,
            output: "passed",
            startedHead: "b".repeat(40),
            finishedHead: "b".repeat(40),
            startedClean: true,
            finishedClean: true,
            status: "passed",
            verificationError: null,
          },
        ],
        axes: [
          {
            axis: "standards",
            providerThreadId: "standards-1",
            parentProviderThreadId: "review-coordinator",
            providerStatus: "idle",
            settlementEvidence: "native-closed",
            observedProfile: { model: null, effort: null, match: "unknown" },
          },
          {
            axis: "spec",
            providerThreadId: "spec-1",
            parentProviderThreadId: "review-coordinator",
            providerStatus: "idle",
            settlementEvidence: "native-closed",
            observedProfile: { model: null, effort: null, match: "unknown" },
          },
        ],
        findings: [],
        summary: "No findings.",
        updatedAt: "2026-09-07T10:00:00.000Z",
      },
    });
    const parsed = interpretWorkflowEvidence({
      issue: {
        id: "issue-18",
        url: ticket.url,
        number: ticket.number,
        title: ticket.title,
        kind: ticket.kind,
        state: "closed",
        stateReason: "completed",
        labels: ticket.labels,
        assignees: ["Flow-Fly"],
        body: ticketDetail.body,
        comments: [
          {
            id: "resolution-comment",
            url: `${ticket.url}#issuecomment-resolution`,
            body,
            createdAt: "2026-09-07T10:00:00.000Z",
            author: "Flow-Fly",
            authorAssociation: "OWNER",
          },
        ],
        reopenedAt: [],
      },
    });

    expect(body).toContain("observed unavailable/unavailable");
    expect(parsed.readiness.status).toBe("resolved");
    expect(parsed.evidence.records[0]).toMatchObject({
      sourceAccess: "reported",
      scope: "current",
      bodyFingerprint: workflowEvidenceBodyFingerprint(body),
    });
    expect(parsed.evidence.records[0]?.evidence).not.toContain("unavailable");
  });
});

const reviewBase = "a".repeat(40);
const reviewHead = "b".repeat(40);
const encodeReviewStrings = Schema.encodeUnknownSync(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);

function reviewProcessRunner(
  head = reviewHead,
  clean = true,
): ProcessRunner.ProcessRunner["Service"] {
  return {
    run: ({ args }) => {
      if (args[0] === "remote") {
        return Effect.succeed(processOutput(`fork\tgit@github.com:${repository}.git (fetch)\n`));
      }
      if (args[0] === "rev-parse") return Effect.succeed(processOutput(`${head}\n`));
      if (args[0] === "status")
        return Effect.succeed(processOutput(clean ? "" : " M changed.ts\n"));
      return Effect.succeed(processOutput(""));
    },
  };
}

function claimTicket(
  detail: WorkflowIssueDetail,
  approvalComments: ReadonlyArray<WorkflowEvidenceComment>,
) {
  Object.assign(detail, reinterpretTicket(detail, approvalComments, { assignees: ["Flow-Fly"] }));
}

function seedDirectorProviderIdentity(sql: SqlClient.SqlClient, directorId: string) {
  return sql`
    INSERT INTO projection_thread_sessions (
      thread_id, status, provider_name, provider_session_id, provider_thread_id,
      active_turn_id, last_error, updated_at, runtime_mode, provider_instance_id
    )
    SELECT thread_id, 'running', 'codex', 'provider-session-director',
      'provider-director', NULL, NULL, '2026-09-07T09:00:00.000Z',
      'full-access', requested_instance_id
    FROM workflow_directors
    WHERE director_id = ${directorId}
    ON CONFLICT(thread_id) DO UPDATE SET
      provider_thread_id = excluded.provider_thread_id,
      provider_instance_id = excluded.provider_instance_id,
      updated_at = excluded.updated_at
  `;
}

function seedReportedReview(
  sql: SqlClient.SqlClient,
  input: {
    readonly directorId: string;
    readonly batchId: string;
    readonly ticketNumber: number;
    readonly scopeBody: string;
    readonly suffix: string;
    readonly head?: string;
    readonly checkStatus?: "pending" | "passed" | "failed";
    readonly workerClosed?: boolean;
    readonly unresolvedFinding?: boolean;
    readonly createdAt?: string;
    readonly admissionId?: string;
  },
) {
  const head = input.head ?? reviewHead;
  const checkStatus = input.checkStatus ?? "passed";
  const workerClosed = input.workerClosed ?? true;
  const admissionId = input.admissionId ?? `admission-${input.suffix}`;
  const dispatchId = `dispatch-${input.suffix}`;
  const workerId = `worker-${input.suffix}`;
  const reviewId = `review-${input.suffix}`;
  const coordinatorId = `reviewer-${input.suffix}`;
  const standardsId = `standards-${input.suffix}`;
  const specId = `spec-${input.suffix}`;
  const createdAt = input.createdAt ?? "2026-09-07T09:03:00.000Z";
  return Effect.gen(function* () {
    yield* seedDirectorProviderIdentity(sql, input.directorId);
    if (!input.admissionId) {
      yield* sql`
        INSERT INTO workflow_director_admissions (
          admission_id, director_id, batch_id, repository, ticket_id, ticket_number, slot_ticket_number,
          purpose, ownership, claim_login, claim_status, scope_body, scope_fingerprint,
          current_scope_body, current_scope_fingerprint, created_at, updated_at
        ) VALUES (
          ${admissionId}, ${input.directorId}, ${input.batchId}, ${repository}, ${`ticket-${input.suffix}`}, ${input.ticketNumber},
          ${input.ticketNumber}, 'implement', ${`ticket-${input.suffix}`}, 'Flow-Fly', 'confirmed',
          ${input.scopeBody}, ${workflowEvidenceBodyFingerprint(input.scopeBody)},
          ${input.scopeBody}, ${workflowEvidenceBodyFingerprint(input.scopeBody)},
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
    }
    yield* sql`
      INSERT INTO workflow_worker_dispatches (
        dispatch_id, association_token, director_id, batch_id, admission_id, repository,
        ticket_number, ownership, write_paths_json, requested_model, requested_effort,
        requested_skill_path, provider_thread_id, status, handoff_summary,
        handoff_commits_json, handoff_checks_json, created_at, updated_at
      ) VALUES (
        ${dispatchId}, ${`worker-token-${input.suffix}`}, ${input.directorId}, ${input.batchId},
        ${admissionId}, ${repository}, ${input.ticketNumber}, ${`ticket-${input.suffix}`},
        ${encodeReviewStrings([`ticket-${input.suffix}`])}, 'gpt-5.6-sol', 'high',
        '/skills/implement/SKILL.md', ${workerId}, 'reported-succeeded', 'Implemented.',
        ${encodeReviewStrings([head])}, ${encodeReviewStrings(["focused"])},
        '2026-09-07T09:01:00.000Z', '2026-09-07T09:01:00.000Z'
      )
    `;
    yield* sql`
      INSERT INTO workflow_worker_observations (
        director_id, provider_thread_id, parent_provider_thread_id, observed_model,
        observed_effort, provider_status, native_lifecycle, last_event_kind,
        first_observed_at, updated_at
      ) VALUES (
        ${input.directorId}, ${workerId}, 'provider-director', 'gpt-5.6-sol', 'high',
        ${workerClosed ? "interrupted" : "idle"}, ${workerClosed ? "closed" : null},
        ${workerClosed ? "task.completed" : "task.updated"},
        '2026-09-07T09:01:00.000Z', '2026-09-07T09:02:00.000Z'
      )
    `;
    yield* sql`
      INSERT INTO workflow_ticket_reviews (
        review_id, association_token, director_id, batch_id, admission_id,
        implementation_dispatch_id, repository, ticket_number, fixed_base,
        implementation_head, scope_body, requested_model, requested_effort,
        requested_skill_path, provider_thread_id, status, report_summary, created_at, updated_at
      ) VALUES (
        ${reviewId}, ${`review-token-${input.suffix}`}, ${input.directorId}, ${input.batchId},
        ${admissionId}, ${dispatchId}, ${repository}, ${input.ticketNumber}, ${reviewBase}, ${head},
        ${input.scopeBody}, 'gpt-6-astra', 'medium', '/skills/code-review/SKILL.md',
        ${coordinatorId}, 'reported', 'Independent review completed.',
        ${createdAt}, ${createdAt}
      )
    `;
    yield* sql`
      INSERT INTO workflow_review_checks (
        review_id, label, command, tool_call_id, exit_code, output, started_head,
        finished_head, started_clean, finished_clean, verification_status,
        verification_error, native_started_at, native_completed_at, created_at, updated_at
      ) VALUES (
        ${reviewId}, 'focused', 'vp test run focused.test.ts', ${`tool-${input.suffix}`},
        ${checkStatus === "pending" ? null : checkStatus === "passed" ? 0 : 1}, ${checkStatus}, ${head}, ${checkStatus === "pending" ? null : head}, 1, ${checkStatus === "pending" ? null : 1},
        ${checkStatus}, ${checkStatus === "failed" ? "The native command exited with code 1." : null},
        ${checkStatus === "pending" ? null : "2026-09-07T09:03:10.000Z"}, ${checkStatus === "pending" ? null : "2026-09-07T09:03:20.000Z"},
        ${createdAt}, ${createdAt}
      )
    `;
    for (const [providerThreadId, parentProviderThreadId] of [
      [coordinatorId, "provider-director"],
      [standardsId, coordinatorId],
      [specId, coordinatorId],
    ] as const) {
      yield* sql`
        INSERT INTO workflow_worker_observations (
          director_id, provider_thread_id, parent_provider_thread_id, observed_model,
          observed_effort, provider_status, native_lifecycle, last_event_kind,
          first_observed_at, updated_at
        ) VALUES (
          ${input.directorId}, ${providerThreadId}, ${parentProviderThreadId},
          'gpt-6-astra', 'medium', 'interrupted', 'closed', 'task.completed',
          ${createdAt}, ${createdAt}
        )
      `;
    }
    yield* sql`
      INSERT INTO workflow_review_axes (review_id, axis, provider_thread_id)
      VALUES (${reviewId}, 'standards', ${standardsId}), (${reviewId}, 'spec', ${specId})
    `;
    if (input.unresolvedFinding) {
      yield* sql`
        INSERT INTO workflow_review_findings (
          review_id, finding_id, axis, severity, summary, updated_at
        ) VALUES (
          ${reviewId}, 'finding-1', 'standards', 'high', 'A confirmed issue.',
          ${createdAt}
        )
      `;
    }
    return { reviewId, admissionId };
  });
}

function observeReviewCheck(input: {
  readonly threadId: ThreadId;
  readonly toolCallId: string;
  readonly command?: string;
  readonly cwd?: string;
  readonly startedAt?: string;
  readonly completedAt?: string;
  readonly repeatedStartAt?: string;
  readonly status?: "completed" | "declined" | "failed" | "inProgress";
  readonly exitCode?: number;
}) {
  const base = {
    provider: ProviderDriverKind.make("codex"),
    providerInstanceId: instanceId,
    threadId: input.threadId,
    itemId: RuntimeItemId.make(input.toolCallId),
  } as const;
  const command = input.command ?? "vp test run focused.test.ts";
  const cwd = input.cwd ?? "/tmp/t3code-workflow-17";
  return Effect.gen(function* () {
    yield* recordWorkflowCheckObservation({
      ...base,
      type: "item.started",
      eventId: EventId.make(`${input.toolCallId}-started`),
      createdAt: input.startedAt ?? "2026-09-07T09:04:00.000Z",
      payload: {
        itemType: "command_execution",
        status: "inProgress",
        data: { item: { type: "commandExecution", command, cwd } },
      },
    });
    yield* recordWorkflowCheckObservation({
      ...base,
      type: "item.completed",
      eventId: EventId.make(`${input.toolCallId}-completed`),
      createdAt: input.completedAt ?? "2026-09-07T09:04:10.000Z",
      payload: {
        itemType: "command_execution",
        status: input.status ?? "completed",
        data: {
          item: {
            type: "commandExecution",
            command,
            cwd,
            status: input.status ?? "completed",
            ...(input.exitCode === undefined ? {} : { exitCode: input.exitCode }),
            aggregatedOutput: "focused output",
          },
        },
      },
    });
    if (!input.repeatedStartAt) return;
    yield* recordWorkflowCheckObservation({
      ...base,
      type: "item.started",
      eventId: EventId.make(`${input.toolCallId}-started-repeated`),
      createdAt: input.repeatedStartAt,
      payload: {
        itemType: "command_execution",
        status: "inProgress",
        data: { item: { type: "commandExecution", command, cwd } },
      },
    });
  });
}

describe("delivery ticket review resolution", () => {
  it.effect("retains receipts across staggered check registrations", () => {
    const fixture = interpretedCapabilityFixture(2);
    const test = harness({
      ...fixture,
      processRunner: reviewProcessRunner(),
      githubExecute: ({ args }) =>
        Effect.succeed(output(args[0] === "api" || args[1] === "view" ? "Flow-Fly\n" : "")),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      for (const detail of fixture.ticketDetails) {
        claimTicket(detail, [fixture.source, fixture.breakdownRecord]);
      }
      const sql = yield* SqlClient.SqlClient;
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const earlier = yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: fixture.ticketDetails[0]!.number,
        scopeBody: fixture.ticketDetails[0]!.body,
        suffix: "capacity-earlier",
        checkStatus: "pending",
        createdAt: "2026-09-07T09:03:00.000Z",
      });
      const later = yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: fixture.ticketDetails[1]!.number,
        scopeBody: fixture.ticketDetails[1]!.body,
        suffix: "capacity-later",
        checkStatus: "pending",
        createdAt: "2026-09-07T09:03:30.000Z",
      });

      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "native-before-registrations",
        cwd: started.director.worktreePath,
        startedAt: "2026-09-07T09:02:00.000Z",
        completedAt: "2026-09-07T09:02:10.000Z",
        exitCode: 0,
      });
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "native-capacity-earlier",
        cwd: started.director.worktreePath,
        startedAt: "2026-09-07T09:04:00.000Z",
        completedAt: "2026-09-07T09:04:05.000Z",
        exitCode: 0,
      });
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "native-capacity-later",
        cwd: started.director.worktreePath,
        startedAt: "2026-09-07T09:04:10.000Z",
        completedAt: "2026-09-07T09:04:15.000Z",
        exitCode: 0,
      });
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "native-between-registrations",
        cwd: started.director.worktreePath,
        startedAt: "2026-09-07T09:03:10.000Z",
        completedAt: "2026-09-07T09:03:20.000Z",
        exitCode: 0,
      });

      for (const toolCallId of ["native-before-registrations", "native-between-registrations"]) {
        const rejected = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: earlier.reviewId,
            receipts: [{ label: "focused", toolCallId }],
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(rejected._tag).toBe("Failure");
      }

      for (const [reviewId, toolCallId] of [
        [earlier.reviewId, "native-capacity-earlier"],
        [later.reviewId, "native-capacity-later"],
      ] as const) {
        const receipt = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId,
            receipts: [{ label: "focused", toolCallId }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(receipt.checks[0]).toMatchObject({ status: "passed", toolCallId });
      }
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "binds only post-registration native check receipts and retains explicit verification verdicts",
    () => {
      const fixture = interpretedCapabilityFixture(7);
      const test = harness({
        ...fixture,
        processRunner: reviewProcessRunner(),
        githubExecute: ({ args }) =>
          Effect.succeed(output(args[0] === "api" || args[1] === "view" ? "Flow-Fly\n" : "")),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        for (const detail of fixture.ticketDetails) {
          claimTicket(detail, [fixture.source, fixture.breakdownRecord]);
        }
        const sql = yield* SqlClient.SqlClient;
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const replayed = yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: fixture.ticketDetails[0]!.number,
          scopeBody: fixture.ticketDetails[0]!.body,
          suffix: "replayed-start",
          checkStatus: "pending",
        });
        yield* observeReviewCheck({
          threadId: started.director.threadId,
          toolCallId: "native-replayed-start",
          cwd: started.director.worktreePath,
          exitCode: 0,
          repeatedStartAt: "2026-09-07T09:04:20.000Z",
        });
        const replayedReceipt = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: replayed.reviewId,
            receipts: [{ label: "focused", toolCallId: "native-replayed-start" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(replayedReceipt.checks[0]).toMatchObject({
          status: "passed",
          toolCallId: "native-replayed-start",
        });

        const scenarios = [
          {
            suffix: "crossing",
            startedAt: "2026-09-07T09:02:59.000Z",
            completedAt: "2026-09-07T09:04:00.000Z",
          },
          { suffix: "command", command: "vp test run another.test.ts", exitCode: 0 },
          { suffix: "cwd", cwd: "/tmp/elsewhere", exitCode: 0 },
          { suffix: "exit", exitCode: 1 },
          { suffix: "unknown" },
          { suffix: "success", exitCode: 0 },
        ] as const;
        const outcomes = [];
        for (const [index, scenario] of scenarios.entries()) {
          const seeded = yield* seedReportedReview(sql, {
            directorId: started.director.directorId,
            batchId: started.director.batchId,
            ticketNumber: fixture.ticketDetails[index]!.number,
            scopeBody: fixture.ticketDetails[index]!.body,
            suffix: scenario.suffix,
            checkStatus: "pending",
            ...(index === 0 ? { admissionId: replayed.admissionId } : {}),
          });
          const toolCallId = `native-${scenario.suffix}`;
          yield* observeReviewCheck({
            threadId: started.director.threadId,
            toolCallId,
            cwd: started.director.worktreePath,
            ...scenario,
          });
          outcomes.push(
            yield* workflowDirectorHandlers
              .workflow_record_review_checks({
                reviewId: seeded.reviewId,
                receipts: [{ label: "focused", toolCallId }],
              })
              .pipe(
                Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
                Effect.result,
              ),
          );
        }

        expect(outcomes.slice(0, 3).map((outcome) => outcome._tag)).toEqual([
          "Failure",
          "Failure",
          "Failure",
        ]);
        expect(
          outcomes
            .slice(3)
            .map((outcome) =>
              outcome._tag === "Success" ? outcome.success.checks[0]?.status : null,
            ),
        ).toEqual(["failed", "failed", "passed"]);

        const failedExit = outcomes[3]!;
        if (failedExit._tag === "Failure") return yield* failedExit.failure;
        const prepared = yield* workflowDirectorHandlers
          .workflow_prepare_ticket_review({
            ticketNumber: failedExit.success.ticketNumber,
            implementationProviderThreadId: failedExit.success.implementationProviderThreadId,
            fixedBase: failedExit.success.fixedBase,
            implementationHead: failedExit.success.implementationHead,
            checks: [{ label: "focused", command: "vp test run focused.test.ts" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(prepared.disposition).toBe("held");
        expect(prepared.review.status).toBe("checks-failed");
        const resolve = yield* workflowDirectorHandlers
          .workflow_resolve_ticket({ reviewId: failedExit.success.reviewId })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(resolve._tag).toBe("Failure");

        const successful = outcomes[5]!;
        if (successful._tag === "Failure") return yield* successful.failure;
        const recovered = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: successful.success.reviewId,
            receipts: [{ label: "focused", toolCallId: "native-success" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(recovered.checks[0]).toMatchObject({
          status: "passed",
          toolCallId: "native-success",
          startedHead: reviewHead,
          finishedHead: reviewHead,
        });
        expect(recovered.status).toBe("reported");
        const receiptRetryResolution = yield* workflowDirectorHandlers
          .workflow_resolve_ticket({ reviewId: recovered.reviewId })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(receiptRetryResolution._tag).toBe("Success");

        const launch = yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: fixture.ticketDetails[6]!.number,
          scopeBody: fixture.ticketDetails[6]!.body,
          suffix: "launch",
        });
        yield* sql`DELETE FROM workflow_review_axes WHERE review_id = ${launch.reviewId}`;
        yield* sql`
          UPDATE workflow_ticket_reviews SET status = 'prepared', provider_thread_id = NULL
          WHERE review_id = ${launch.reviewId}
        `;
        const launchInput = {
          ticketNumber: fixture.ticketDetails[6]!.number,
          implementationProviderThreadId: "worker-launch",
          fixedBase: reviewBase,
          implementationHead: reviewHead,
          checks: [{ label: "focused", command: "vp test run focused.test.ts" }],
        } as const;
        const issued = yield* workflowDirectorHandlers
          .workflow_prepare_ticket_review(launchInput)
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(issued).toMatchObject({
          disposition: "prepared",
          review: { status: "spawn-issued" },
        });
        const issuedReceiptRetry = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: issued.review.reviewId,
            receipts: [{ label: "focused", toolCallId: "tool-launch" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(issuedReceiptRetry.status).toBe("spawn-issued");
        const retried = yield* workflowDirectorHandlers
          .workflow_prepare_ticket_review(launchInput)
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(retried).toMatchObject({
          disposition: "held",
          associationToken: issued.associationToken,
          review: { status: "spawn-issued", providerThreadId: null },
        });
        expect(retried.instructions).toContain("do not spawn a duplicate");
        for (const [providerThreadId, parentProviderThreadId] of [
          ["reviewer-null-parent", null],
          ["reviewer-missing-parent", "missing-parent"],
          ["reviewer-implementation-descendant", "worker-launch"],
          ["reviewer-wrong-root", "foreign-root"],
          ["foreign-root", null],
          ["reviewer-cycle", "cycle-parent"],
          ["cycle-parent", "reviewer-cycle"],
        ] as const) {
          yield* sql`
            INSERT INTO workflow_worker_observations (
              director_id, provider_thread_id, parent_provider_thread_id, observed_model,
              observed_effort, provider_status, last_event_kind, first_observed_at, updated_at
            ) VALUES (
              ${started.director.directorId}, ${providerThreadId}, ${parentProviderThreadId},
              'gpt-6-astra', 'medium', 'idle', 'task.started',
              '2026-09-07T09:03:00.000Z', '2026-09-07T09:03:00.000Z'
            )
          `;
        }
        const rejectedAncestries = [];
        for (const providerThreadId of [
          "reviewer-null-parent",
          "reviewer-missing-parent",
          "reviewer-implementation-descendant",
          "reviewer-wrong-root",
          "reviewer-cycle",
        ]) {
          rejectedAncestries.push(
            yield* workflowDirectorHandlers
              .workflow_associate_ticket_review({
                associationToken: issued.associationToken,
                providerThreadId,
              })
              .pipe(
                Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
                Effect.result,
              ),
          );
        }
        expect(rejectedAncestries.every((result) => result._tag === "Failure")).toBe(true);
        const associated = yield* workflowDirectorHandlers
          .workflow_associate_ticket_review({
            associationToken: issued.associationToken,
            providerThreadId: "reviewer-launch",
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(associated.status).toBe("associated");
        yield* sql`
          DELETE FROM workflow_worker_observations
          WHERE director_id = ${started.director.directorId}
            AND provider_thread_id IN (
              'reviewer-null-parent', 'reviewer-missing-parent',
              'reviewer-implementation-descendant', 'reviewer-wrong-root', 'foreign-root',
              'reviewer-cycle', 'cycle-parent'
            )
        `;
        const associatedReceiptRetry = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: associated.reviewId,
            receipts: [{ label: "focused", toolCallId: "tool-launch" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(associatedReceiptRetry.status).toBe("associated");
        const reported = yield* workflowDirectorHandlers
          .workflow_report_ticket_review({
            providerThreadId: "reviewer-launch",
            standardsReviewerThreadId: "standards-launch",
            specReviewerThreadId: "spec-launch",
            summary: "Fresh independent Standards and Spec review completed.",
            findings: [
              {
                id: "finding-launch",
                axis: "standards",
                severity: "low",
                summary: "A checked non-blocking observation.",
              },
            ],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(reported).toMatchObject({ status: "reported", findings: [{ disposition: null }] });
        const reportedReceiptRetry = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: reported.reviewId,
            receipts: [{ label: "focused", toolCallId: "tool-launch" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(reportedReceiptRetry.status).toBe("reported");
        const associatedAgain = yield* workflowDirectorHandlers
          .workflow_associate_ticket_review({
            associationToken: issued.associationToken,
            providerThreadId: "reviewer-launch",
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(associatedAgain.status).toBe("reported");
        const changedReport = yield* workflowDirectorHandlers
          .workflow_report_ticket_review({
            providerThreadId: "reviewer-launch",
            standardsReviewerThreadId: "standards-launch",
            specReviewerThreadId: "spec-launch",
            summary: "Fresh independent Standards and Spec review completed.",
            findings: [
              {
                id: "finding-launch",
                axis: "standards",
                severity: "high",
                summary: "A changed report must not replace durable evidence.",
              },
            ],
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(changedReport._tag).toBe("Failure");
        const disposed = yield* workflowDirectorHandlers
          .workflow_record_review_dispositions({
            reviewId: reported.reviewId,
            dispositions: [
              {
                findingId: "finding-launch",
                outcome: "dismissed",
                rationale: "Source validation confirms this is not a blocker.",
              },
            ],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(disposed.findings[0]?.disposition?.outcome).toBe("dismissed");
        yield* sql`
          UPDATE projection_thread_sessions SET provider_thread_id = 'changed-director-root'
          WHERE thread_id = ${started.director.threadId}
        `;
        const changedDirectorIdentity = yield* workflowDirectorHandlers
          .workflow_resolve_ticket({ reviewId: reported.reviewId })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(changedDirectorIdentity._tag).toBe("Failure");
        yield* sql`
          UPDATE projection_thread_sessions SET provider_thread_id = 'provider-director'
          WHERE thread_id = ${started.director.threadId}
        `;
        const handlerResolution = yield* workflowDirectorHandlers
          .workflow_resolve_ticket({ reviewId: reported.reviewId })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(handlerResolution._tag).toBe("Success");
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("starts corrected-head checks after a pre-spawn check failure", () => {
    const fixture = interpretedCapabilityFixture(1);
    let currentHead = reviewHead;
    let claimEstablished = false;
    const test = harness({
      ...fixture,
      processRunner: {
        run: ({ args }) => {
          if (args[0] === "remote") {
            return Effect.succeed(
              processOutput(`fork\tgit@github.com:${repository}.git (fetch)\n`),
            );
          }
          if (args[0] === "rev-parse") return Effect.succeed(processOutput(`${currentHead}\n`));
          if (args[0] === "status") return Effect.succeed(processOutput(""));
          return Effect.succeed(processOutput(""));
        },
      },
      githubExecute: ({ args }) => {
        if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
        if (args[0] === "issue" && args[1] === "view") {
          return Effect.succeed(output(claimEstablished ? "Flow-Fly\n" : ""));
        }
        if (args[0] === "issue" && args[1] === "edit") claimEstablished = true;
        return Effect.succeed(output(""));
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const ticket = fixture.ticketDetails[0]!;
      const sql = yield* SqlClient.SqlClient;
      yield* seedDirectorProviderIdentity(sql, started.director.directorId);
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const finishImplementation = (providerThreadId: string, head: string) =>
        Effect.gen(function* () {
          const prepared = yield* service.prepareWorker(
            environmentId,
            started.director.threadId,
            instanceId,
            {
              ticketNumber: ticket.number,
              ownership: "workflow review correction",
              writePaths: ["apps/server/src/workflow/"],
            },
          );
          yield* recordWorkflowWorkerObservation({
            type: "task.started",
            eventId: EventId.make(`${providerThreadId}-started`),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: instanceId,
            threadId: started.director.threadId,
            createdAt: "2026-09-07T09:01:00.000Z",
            payload: {
              taskId: RuntimeTaskId.make(providerThreadId),
              description: "implementation worker",
              parentAgentId: "provider-director",
              model: "gpt-5.6-sol",
              effort: "high",
              timelineBypass: true,
            },
          });
          yield* service.associateWorker(environmentId, started.director.threadId, instanceId, {
            associationToken: prepared.associationToken,
            providerThreadId,
          });
          yield* service.reportWorkerHandoff(environmentId, started.director.threadId, instanceId, {
            providerThreadId,
            outcome: "succeeded",
            summary: `Implemented ${head}.`,
            commits: [head],
            checks: ["implementation checks passed"],
          });
          yield* recordWorkflowWorkerObservation({
            type: "task.completed",
            eventId: EventId.make(`${providerThreadId}-closed`),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: instanceId,
            threadId: started.director.threadId,
            createdAt: "2026-09-07T09:02:00.000Z",
            payload: {
              taskId: RuntimeTaskId.make(providerThreadId),
              status: "stopped",
              nativeLifecycle: "closed",
              timelineBypass: true,
            },
          });
          return prepared;
        });

      yield* finishImplementation("worker-check-a", reviewHead);
      const first = yield* workflowDirectorHandlers
        .workflow_prepare_ticket_review({
          ticketNumber: ticket.number,
          implementationProviderThreadId: "worker-check-a",
          fixedBase: reviewBase,
          implementationHead: reviewHead,
          checks: [{ label: "focused", command: "vp test run focused.test.ts" }],
        })
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "failed-check-a",
        cwd: started.director.worktreePath,
        startedAt: "2099-09-07T09:03:00.000Z",
        completedAt: "2099-09-07T09:03:10.000Z",
        exitCode: 1,
      });
      const failed = yield* workflowDirectorHandlers
        .workflow_record_review_checks({
          reviewId: first.review.reviewId,
          receipts: [{ label: "focused", toolCallId: "failed-check-a" }],
        })
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      expect(failed).toMatchObject({ status: "checks-failed", providerThreadId: null });

      const correctedHead = "c".repeat(40);
      currentHead = correctedHead;
      const correction = yield* finishImplementation("worker-check-b", correctedHead);
      const second = yield* workflowDirectorHandlers
        .workflow_prepare_ticket_review({
          ticketNumber: ticket.number,
          implementationProviderThreadId: "worker-check-b",
          fixedBase: reviewBase,
          implementationHead: correctedHead,
          checks: [{ label: "focused", command: "vp test run focused.test.ts" }],
        })
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      expect(second).toMatchObject({
        disposition: "held",
        review: { status: "checks-pending", implementationHead: correctedHead },
      });
      expect(second.review.reviewId).not.toBe(first.review.reviewId);
      expect(correction.admission.admissionId).toBe(first.review.admissionId);

      const pendingCorrectionHead = "d".repeat(40);
      currentHead = pendingCorrectionHead;
      yield* finishImplementation("worker-check-c", pendingCorrectionHead);
      const thirdInput = {
        ticketNumber: ticket.number,
        implementationProviderThreadId: "worker-check-c",
        fixedBase: reviewBase,
        implementationHead: pendingCorrectionHead,
        checks: [{ label: "focused", command: "vp test run focused.test.ts" }],
      } as const;
      const third = yield* workflowDirectorHandlers
        .workflow_prepare_ticket_review(thirdInput)
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      expect(third).toMatchObject({
        disposition: "held",
        review: { status: "checks-pending", implementationHead: pendingCorrectionHead },
      });
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "passed-check-c",
        cwd: started.director.worktreePath,
        startedAt: "2099-09-07T09:04:00.000Z",
        completedAt: "2099-09-07T09:04:10.000Z",
        exitCode: 0,
      });
      yield* workflowDirectorHandlers
        .workflow_record_review_checks({
          reviewId: third.review.reviewId,
          receipts: [{ label: "focused", toolCallId: "passed-check-c" }],
        })
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      const issued = yield* workflowDirectorHandlers
        .workflow_prepare_ticket_review(thirdInput)
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      expect(issued.review.status).toBe("spawn-issued");

      const heldHead = "e".repeat(40);
      currentHead = heldHead;
      yield* finishImplementation("worker-check-d", heldHead);
      const held = yield* workflowDirectorHandlers
        .workflow_prepare_ticket_review({
          ticketNumber: ticket.number,
          implementationProviderThreadId: "worker-check-d",
          fixedBase: reviewBase,
          implementationHead: heldHead,
          checks: [{ label: "focused", command: "vp test run focused.test.ts" }],
        })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.result,
        );
      expect(held._tag).toBe("Failure");
      const persisted = yield* sql<{
        readonly status: string;
        readonly providerThreadId: string | null;
      }>`
        SELECT status, provider_thread_id AS "providerThreadId"
        FROM workflow_ticket_reviews
        WHERE review_id = ${first.review.reviewId}
      `;
      expect(persisted[0]).toEqual({ status: "checks-failed", providerThreadId: null });
      const admissions = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM workflow_director_admissions
        WHERE director_id = ${started.director.directorId}
      `;
      expect(admissions[0]?.count).toBe(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("holds failed checks, unresolved findings, idle workers and stale review heads", () => {
    const fixture = interpretedCapabilityFixture(4);
    const test = harness({
      ...fixture,
      processRunner: reviewProcessRunner(),
      githubExecute: ({ args }) =>
        Effect.succeed(output(args[0] === "api" || args[1] === "view" ? "Flow-Fly\n" : "")),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      for (const detail of fixture.ticketDetails) {
        claimTicket(detail, [fixture.source, fixture.breakdownRecord]);
      }
      const sql = yield* SqlClient.SqlClient;
      const cases = [
        { suffix: "failed", checkStatus: "failed" as const },
        { suffix: "finding", unresolvedFinding: true },
        { suffix: "idle", workerClosed: false },
        { suffix: "stale", head: "c".repeat(40) },
      ];
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const results = [];
      for (const [index, scenario] of cases.entries()) {
        const seeded = yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: fixture.ticketDetails[index]!.number,
          scopeBody: fixture.ticketDetails[index]!.body,
          ...scenario,
        });
        results.push(
          yield* workflowDirectorHandlers
            .workflow_resolve_ticket({ reviewId: seeded.reviewId })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.result,
            ),
        );
      }
      expect(results.map((result) => result._tag)).toEqual([
        "Failure",
        "Failure",
        "Failure",
        "Failure",
      ]);
      expect(
        results.map((result) =>
          result._tag === "Failure" && "failure" in result.failure ? result.failure.failure : null,
        ),
      ).toEqual(["review-incomplete", "review-incomplete", "review-incomplete", "stale-review"]);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "rejects receipts when the registered head changes or the worktree becomes dirty",
    () => {
      const fixture = interpretedCapabilityFixture(2);
      let currentHead = reviewHead;
      let clean = true;
      const test = harness({
        ...fixture,
        processRunner: {
          run: ({ args }) => {
            if (args[0] === "remote") {
              return Effect.succeed(
                processOutput(`fork\tgit@github.com:${repository}.git (fetch)\n`),
              );
            }
            if (args[0] === "rev-parse") return Effect.succeed(processOutput(`${currentHead}\n`));
            if (args[0] === "status")
              return Effect.succeed(processOutput(clean ? "" : " M file.ts\n"));
            return Effect.succeed(processOutput(""));
          },
        },
        githubExecute: ({ args }) =>
          Effect.succeed(output(args[0] === "api" || args[1] === "view" ? "Flow-Fly\n" : "")),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        for (const detail of fixture.ticketDetails) {
          claimTicket(detail, [fixture.source, fixture.breakdownRecord]);
        }
        const sql = yield* SqlClient.SqlClient;
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const reviews = [];
        for (const [index, suffix] of ["changed-head", "dirty"].entries()) {
          reviews.push(
            yield* seedReportedReview(sql, {
              directorId: started.director.directorId,
              batchId: started.director.batchId,
              ticketNumber: fixture.ticketDetails[index]!.number,
              scopeBody: fixture.ticketDetails[index]!.body,
              suffix,
              checkStatus: "pending",
            }),
          );
          yield* observeReviewCheck({
            threadId: started.director.threadId,
            toolCallId: `native-${suffix}`,
            cwd: started.director.worktreePath,
            exitCode: 0,
          });
        }

        currentHead = "c".repeat(40);
        const changedHead = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: reviews[0]!.reviewId,
            receipts: [{ label: "focused", toolCallId: "native-changed-head" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        currentHead = reviewHead;
        clean = false;
        const dirty = yield* workflowDirectorHandlers
          .workflow_record_review_checks({
            reviewId: reviews[1]!.reviewId,
            receipts: [{ label: "focused", toolCallId: "native-dirty" }],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

        expect(changedHead.checks[0]).toMatchObject({
          status: "failed",
          verificationError: "The implementation HEAD changed while checks ran.",
        });
        expect(dirty.checks[0]).toMatchObject({
          status: "failed",
          verificationError: "The capability worktree was dirty before or after the check.",
        });
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("requires later review evidence for fixes and a contextual owner user decision", () => {
    const fixture = interpretedCapabilityFixture(1);
    const test = harness({
      ...fixture,
      processRunner: reviewProcessRunner(),
      threadDetail: () =>
        Effect.succeed(
          Option.some({
            messages: [
              {
                id: "decision-prompt",
                role: "assistant",
                text: "Should this risk be accepted?",
                createdAt: "2026-09-07T09:04:00.000Z",
              },
              {
                id: "assistant-decision",
                role: "assistant",
                text: "Accept the risk.",
                createdAt: "2026-09-07T09:05:00.000Z",
              },
              {
                id: "contextual-prompt",
                role: "assistant",
                text: "Decision requested for review-disposition finding-1: accept this low risk?",
                createdAt: "2026-09-07T09:06:00.000Z",
              },
              {
                id: "owner-decision",
                role: "user",
                text: "I accept this low risk for the current delivery.",
                createdAt: "2026-09-07T09:07:00.000Z",
              },
            ],
          } as never),
        ),
      githubExecute: ({ args }) =>
        Effect.succeed(output(args[0] === "api" || args[1] === "view" ? "Flow-Fly\n" : "")),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      claimTicket(fixture.ticketDetails[0]!, [fixture.source, fixture.breakdownRecord]);
      const sql = yield* SqlClient.SqlClient;
      const seeded = yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: fixture.ticketDetails[0]!.number,
        scopeBody: fixture.ticketDetails[0]!.body,
        suffix: "disposition",
        unresolvedFinding: true,
      });
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const fixed = yield* workflowDirectorHandlers
        .workflow_record_review_dispositions({
          reviewId: seeded.reviewId,
          dispositions: [
            {
              findingId: "finding-1",
              outcome: "fixed",
              rationale: "Claimed fixed without a changed-head review.",
              resultingReviewId: seeded.reviewId,
            },
          ],
        })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.result,
        );
      const ownerAccepted = yield* workflowDirectorHandlers
        .workflow_record_review_dispositions({
          reviewId: seeded.reviewId,
          dispositions: [
            {
              findingId: "finding-1",
              outcome: "owner-accepted",
              rationale: "The director cites an assistant message.",
              evidenceSource: `T3 Code thread \`owner-thread\`, user message \`assistant-decision\`.`,
              evidenceQuote: "Accept the risk.",
            },
          ],
        })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.result,
        );
      expect(fixed._tag).toBe("Failure");
      expect(ownerAccepted._tag).toBe("Failure");
      const confirmed = yield* workflowDirectorHandlers
        .workflow_record_review_dispositions({
          reviewId: seeded.reviewId,
          dispositions: [
            {
              findingId: "finding-1",
              outcome: "owner-accepted",
              rationale: "The owner accepted the remaining low risk.",
              evidenceSource: `T3 Code thread \`owner-thread\`, user message \`owner-decision\`.`,
              evidenceQuote: "I accept this low risk for the current delivery.",
            },
          ],
        })
        .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
      expect(confirmed.findings[0]?.disposition).toMatchObject({
        outcome: "owner-accepted",
        evidenceQuote: "I accept this low risk for the current delivery.",
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "reconciles uncertain writes, revalidates authority and advances only the live frontier",
    () => {
      const fixture = interpretedCapabilityFixture(3);
      const ticket = fixture.ticketDetails[0]!;
      let claimLogin = "Flow-Fly";
      let commentWrites = 0;
      let closeWrites = 0;
      let pendingBody: string | null = null;
      const resolutionComments: WorkflowEvidenceComment[] = [];
      const approvals = [fixture.source, fixture.breakdownRecord];
      const test = harness({
        ...fixture,
        processRunner: reviewProcessRunner(),
        githubExecute: ({ args }) => {
          if (args[0] === "api") return Effect.succeed(output("Flow-Fly\n"));
          if (args[0] === "issue" && args[1] === "view") {
            return Effect.succeed(output(`${claimLogin}\n`));
          }
          if (args[0] === "issue" && args[1] === "comment") {
            commentWrites += 1;
            pendingBody = args[args.indexOf("--body") + 1] ?? null;
            return Effect.succeed(output("comment accepted without a readable response\n"));
          }
          if (args[0] === "issue" && args[1] === "close") {
            closeWrites += 1;
            updateTicketEvidence(ticket, approvals, {
              assignees: ["Flow-Fly"],
              comments: resolutionComments,
              state: "closed",
            });
            return Effect.succeed(output("closed\n"));
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        updateTicketEvidence(ticket, approvals, { assignees: ["Flow-Fly"] });
        updateTicketEvidence(fixture.ticketDetails[2]!, approvals, { assignees: ["Other"] });
        const sql = yield* SqlClient.SqlClient;
        const seeded = yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: ticket.number,
          scopeBody: ticket.body,
          suffix: "resolution",
        });
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const resolve = () =>
          workflowDirectorHandlers
            .workflow_resolve_ticket({ reviewId: seeded.reviewId })
            .pipe(
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
              Effect.result,
            );

        const first = yield* resolve();
        Object.assign(ticket, {
          evidence: ticket.evidence
            ? { ...ticket.evidence, historyComplete: false }
            : ticket.evidence,
        });
        const second = yield* resolve();
        expect(first._tag).toBe("Success");
        expect(second._tag).toBe("Success");
        expect(commentWrites).toBe(1);
        expect(closeWrites).toBe(0);
        expect(pendingBody).not.toBeNull();

        resolutionComments.push({
          id: "resolution-readback",
          url: `${ticket.url}#issuecomment-resolution-readback`,
          body: pendingBody!,
          createdAt: "2026-09-07T10:00:00.000Z",
          author: "Flow-Fly",
          authorAssociation: "OWNER",
        });
        claimLogin = "Other";
        updateTicketEvidence(ticket, approvals, {
          assignees: ["Other"],
          comments: resolutionComments,
        });
        const lostAuthority = yield* resolve();
        expect(lostAuthority._tag).toBe("Failure");
        expect(closeWrites).toBe(0);

        claimLogin = "Flow-Fly";
        updateTicketEvidence(ticket, approvals, {
          assignees: ["Flow-Fly"],
          comments: resolutionComments,
        });
        const final = yield* resolve();
        if (final._tag === "Failure") return yield* final.failure;
        expect(final.success.disposition).toBe("resolved");
        expect(final.success.resolution.readyIssueIds).toEqual([fixture.ticketDetails[1]!.id]);
        expect(commentWrites).toBe(1);
        expect(closeWrites).toBe(1);

        const rows = yield* sql<{ readonly commentBody: string; readonly status: string }>`
          SELECT comment_body AS "commentBody", status
          FROM workflow_ticket_resolution_intents WHERE review_id = ${seeded.reviewId}
        `;
        expect(rows[0]).toEqual({ commentBody: pendingBody, status: "resolved" });
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "carries prior findings into a corrected-head review until their fixes are evidenced",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const test = harness({
        ...fixture,
        processRunner: reviewProcessRunner("c".repeat(40)),
        githubExecute: ({ args }) =>
          Effect.succeed(output(args[0] === "api" || args[1] === "view" ? "Flow-Fly\n" : "")),
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        claimTicket(fixture.ticketDetails[0]!, [fixture.source, fixture.breakdownRecord]);
        const sql = yield* SqlClient.SqlClient;
        const first = yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: fixture.ticketDetails[0]!.number,
          scopeBody: fixture.ticketDetails[0]!.body,
          suffix: "review-a",
          head: reviewHead,
          unresolvedFinding: true,
          createdAt: "2026-09-07T09:03:00.000Z",
        });
        const second = yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: fixture.ticketDetails[0]!.number,
          scopeBody: fixture.ticketDetails[0]!.body,
          suffix: "review-b",
          admissionId: first.admissionId,
          head: "c".repeat(40),
          createdAt: "2026-09-07T09:06:00.000Z",
        });
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const before = yield* workflowDirectorHandlers
          .workflow_resolve_ticket({ reviewId: second.reviewId })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(before._tag).toBe("Failure");

        const disposition = yield* workflowDirectorHandlers
          .workflow_record_review_dispositions({
            reviewId: first.reviewId,
            dispositions: [
              {
                findingId: "finding-1",
                outcome: "fixed",
                rationale: "The corrected head received a fresh independent review.",
                resultingReviewId: second.reviewId,
              },
            ],
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));
        expect(disposition.findings[0]?.disposition).toMatchObject({
          outcome: "fixed",
          resultingReviewId: second.reviewId,
        });

        const after = yield* workflowDirectorHandlers
          .workflow_resolve_ticket({ reviewId: second.reviewId })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(after._tag).toBe("Success");
        if (after._tag === "Success") expect(after.success.disposition).toBe("pending");
        const resolution = yield* sql<{ readonly body: string }>`
        SELECT comment_body AS body FROM workflow_ticket_resolution_intents
        WHERE review_id = ${second.reviewId}
      `;
        expect(resolution[0]?.body).toContain(`workflow-review:${first.reviewId}/dispositions`);
      }).pipe(Effect.provide(test.layer));
    },
  );
});

describe("capability combined acceptance", () => {
  it("keeps completion limits outside authoritative evidence", () => {
    const body = WorkflowDirectorService.workflowCapabilityCompletionBody({
      completionId: "completion-1",
      repository,
      capabilityNumber: capability.number,
      resultingHead: reviewHead,
      requiredIssues: [ticketDetail],
      checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
    });
    const parsed = interpretWorkflowEvidence({
      issue: {
        id: capability.id,
        url: capability.url,
        number: capability.number,
        title: capability.title,
        kind: capability.kind,
        state: "closed",
        stateReason: "completed",
        labels: capability.labels,
        assignees: ["Flow-Fly"],
        body: capability.body,
        comments: [
          {
            id: "completion-comment",
            url: `${capability.url}#issuecomment-completion`,
            body,
            createdAt: "2026-09-07T10:00:00.000Z",
            author: "Flow-Fly",
            authorAssociation: "OWNER",
          },
        ],
        reopenedAt: [],
      },
    });
    expect(parsed.readiness.status).toBe("resolved");
    expect(parsed.evidence.records[0]).toMatchObject({
      sourceAccess: "reported",
      scope: "current",
      bodyFingerprint: workflowEvidenceBodyFingerprint(body),
    });
    expect(parsed.evidence.records[0]?.evidence).toContain(
      "workflow-capability-completion:completion-1",
    );
  });

  it.effect(
    "completes one batch only after resolved nested work and exact native acceptance receipts",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const completed = interpretedCapabilityFixture(1, new Set([1]));
      const deliveryTicket = fixture.ticketDetails[0]!;
      const completedTicket = completed.ticketDetails[0]!;
      const nestedTask: WorkflowIssueDetail = {
        ...completedTicket,
        id: "issue-200",
        number: 200,
        title: "Nested delivery task",
        url: `https://github.com/${repository}/issues/200`,
        kind: "task",
        parentNumber: completedTicket.number,
        childCount: 0,
        labels: [],
      };
      let commentWrites = 0;
      let closeWrites = 0;
      let completionBody: string | null = null;
      let directorThreadId: string | null = null;
      const completionComments: WorkflowEvidenceComment[] = [];
      const refreshCapability = (state: "open" | "closed") => {
        Object.assign(fixture.capability, {
          state,
          stateReason: state === "closed" ? "completed" : null,
          ...interpretWorkflowEvidence({
            issue: {
              id: fixture.capability.id,
              url: fixture.capability.url,
              number: fixture.capability.number,
              title: fixture.capability.title,
              kind: fixture.capability.kind,
              state,
              stateReason: state === "closed" ? "completed" : null,
              labels: fixture.capability.labels,
              assignees: [],
              body: fixture.capability.body,
              comments: [
                fixture.source,
                fixture.specification,
                fixture.breakdownRecord,
                ...completionComments,
              ],
              reopenedAt: [],
            },
          }),
        });
      };
      const test = harness({
        ...fixture,
        extraDetails: [nestedTask],
        children: ({ parentNumber }) =>
          Effect.succeed({
            parentNumber,
            children:
              parentNumber === fixture.capability.number
                ? [deliveryTicket]
                : parentNumber === deliveryTicket.number
                  ? [nestedTask]
                  : [],
            frontier: { status: "empty", message: "No work.", readyIssueIds: [] },
          }),
        processRunner: reviewProcessRunner(),
        threadRuntimeContext: (threadId) =>
          Effect.succeed(
            threadId === directorThreadId
              ? Option.some({ id: threadId, title: "Capability director", session: null })
              : Option.none(),
          ),
        githubExecute: ({ args, stdin }) => {
          if (args[0] === "issue" && args[1] === "comment") {
            commentWrites += 1;
            completionBody = stdin ?? null;
            completionComments.push({
              id: "capability-completion",
              url: `${fixture.capability.url}#issuecomment-completion`,
              body: completionBody!,
              createdAt: "2026-09-07T11:00:00.000Z",
              author: "Flow-Fly",
              authorAssociation: "OWNER",
            });
            refreshCapability("open");
            return Effect.succeed(output("commented\n"));
          }
          if (args[0] === "issue" && args[1] === "close") {
            closeWrites += 1;
            refreshCapability("closed");
            return Effect.succeed(acknowledgedCloseOutput(fixture.capability));
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const controlled = yield* controlledProviderService();
        const joinedLayer = ProviderRuntimeIngestionLive.pipe(
          Layer.provide(Layer.succeed(ProviderService, controlled.service)),
          Layer.provide(
            Layer.mock(OrchestrationEngineService)({
              dispatch: () => Effect.succeed({ sequence: 1 }),
              streamDomainEvents: Stream.empty,
            }),
          ),
          Layer.provide(test.projectionLayer),
          Layer.provide(ThreadBackgroundLiveness.layer),
          Layer.provide(ThreadPlanProgress.layer),
          Layer.provide(Layer.mock(CheckpointStore.CheckpointStore)({})),
          Layer.provide(ServerSettingsService.layerTest()),
          Layer.provideMerge(test.layer),
          Layer.provideMerge(NodeServices.layer),
        );
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const service = yield* WorkflowDirectorService.WorkflowDirectorService;
            const ingestion = yield* ProviderRuntimeIngestionService;
            yield* ingestion.start();
            const started = yield* service.start(
              { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
              test.dispatch,
            );
            directorThreadId = started.director.threadId;
            Object.assign(deliveryTicket, completedTicket, { childCount: 1 });
            const sql = yield* SqlClient.SqlClient;
            yield* seedReportedReview(sql, {
              directorId: started.director.directorId,
              batchId: started.director.batchId,
              ticketNumber: deliveryTicket.number,
              scopeBody: deliveryTicket.body,
              suffix: "capability-completion",
            });
            const invocation: McpInvocationContext.McpInvocationScope = {
              environmentId,
              threadId: started.director.threadId,
              providerInstanceId: instanceId,
              providerSessionId: "provider-session-director",
              capabilities: new Set(["preview"]),
              issuedAt: 1,
            };
            const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
              workflowDirectorHandlers
                .workflow_complete_capability({
                  resultingHead: reviewHead,
                  checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
                  receipts,
                })
                .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

            const registered = yield* complete([]);
            expect(registered.disposition).toBe("pending");
            expect(registered.completion.status).toBe("checks-pending");
            expect(commentWrites).toBe(0);
            expect(closeWrites).toBe(0);

            const nativeCommand = {
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: instanceId,
              threadId: started.director.threadId,
              itemId: RuntimeItemId.make("combined-tool-call"),
            } as const;
            yield* controlled.emitAndWaitForEnqueue([
              {
                ...nativeCommand,
                type: "item.started",
                eventId: EventId.make("combined-check-started"),
                createdAt: "2099-09-07T10:00:00.000Z",
                payload: {
                  itemType: "command_execution",
                  status: "inProgress",
                  data: {
                    item: {
                      type: "commandExecution",
                      command: "vp test run combined.test.ts",
                      cwd: started.director.worktreePath,
                    },
                  },
                },
              },
              {
                ...nativeCommand,
                type: "item.completed",
                eventId: EventId.make("combined-check-completed"),
                createdAt: "2099-09-07T10:00:01.000Z",
                payload: {
                  itemType: "command_execution",
                  status: "completed",
                  data: {
                    item: {
                      type: "commandExecution",
                      command: "vp test run combined.test.ts",
                      cwd: started.director.worktreePath,
                      status: "completed",
                      exitCode: 0,
                      aggregatedOutput: "combined acceptance passed",
                    },
                  },
                },
              },
            ]);
            yield* ingestion.drain;
            const completedResult = yield* complete([
              { label: "combined", toolCallId: "combined-tool-call" },
            ]);
            expect(completedResult.disposition).toBe("completed");
            expect(completedResult.completion).toMatchObject({
              status: "completed",
              authority: "current",
              resultingHead: reviewHead,
            });
            expect(commentWrites).toBe(1);
            expect(closeWrites).toBe(1);
            expect(completionBody).toContain("workflow-capability-completion:");
            expect(completionBody).toContain(reviewHead);

            const status = yield* service.status({
              projectId,
              repository,
              capabilityNumber: 17,
            });
            expect(status.status).toBe("completed");
            expect(status.completion).toMatchObject({ status: "completed", authority: "current" });
            const repeated = yield* complete([]);
            expect(repeated.disposition).toBe("completed");
            expect(commentWrites).toBe(1);
            expect(closeWrites).toBe(1);
          }),
        ).pipe(Effect.provide(joinedLayer));
      });
    },
  );

  it.effect("keeps a failed combined check open and registers a fresh immutable retry", () => {
    const fixture = interpretedCapabilityFixture(1);
    const completed = interpretedCapabilityFixture(1, new Set([1]));
    const deliveryTicket = fixture.ticketDetails[0]!;
    let trackerWrites = 0;
    const test = harness({
      ...fixture,
      processRunner: reviewProcessRunner(),
      githubExecute: ({ args }) => {
        if (args[0] === "issue") trackerWrites += 1;
        return Effect.succeed(output(""));
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      Object.assign(deliveryTicket, completed.ticketDetails[0]);
      const sql = yield* SqlClient.SqlClient;
      yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: deliveryTicket.number,
        scopeBody: deliveryTicket.body,
        suffix: "capability-failed",
      });
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
        workflowDirectorHandlers
          .workflow_complete_capability({
            resultingHead: reviewHead,
            checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
            receipts,
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

      const registered = yield* complete([]);
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "failed-combined",
        command: "vp test run combined.test.ts",
        cwd: started.director.worktreePath,
        startedAt: "2099-09-07T11:00:00.000Z",
        completedAt: "2099-09-07T11:00:01.000Z",
        exitCode: 1,
      });
      const failed = yield* complete([{ label: "combined", toolCallId: "failed-combined" }]);
      expect(failed.disposition).toBe("held");
      expect(failed.completion).toMatchObject({
        status: "checks-failed",
        authority: "historical",
      });
      expect(failed.completion.checks[0]).toMatchObject({ status: "failed", exitCode: 1 });
      expect(fixture.capability.state).toBe("open");
      expect(trackerWrites).toBe(0);

      const retried = yield* complete([]);
      expect(retried.disposition).toBe("pending");
      expect(retried.completion.status).toBe("checks-pending");
      expect(retried.completion.completionId).not.toBe(registered.completion.completionId);
      const history = yield* sql<{ readonly status: string }>`
        SELECT status FROM workflow_capability_completions ORDER BY created_at, rowid
      `;
      expect(history.map((row) => row.status)).toEqual(["invalidated", "checks-pending"]);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("invalidates registered acceptance when required nested work reopens", () => {
    const fixture = interpretedCapabilityFixture(1);
    const completed = interpretedCapabilityFixture(1, new Set([1]));
    const reopened = interpretedCapabilityFixture(1);
    const deliveryTicket = fixture.ticketDetails[0]!;
    const completedTicket = completed.ticketDetails[0]!;
    const nestedTask: WorkflowIssueDetail = {
      ...completedTicket,
      id: "issue-201",
      number: 201,
      title: "Nested task",
      url: `https://github.com/${repository}/issues/201`,
      kind: "task",
      parentNumber: deliveryTicket.number,
      childCount: 0,
      labels: [],
    };
    let trackerWrites = 0;
    const test = harness({
      ...fixture,
      extraDetails: [nestedTask],
      children: ({ parentNumber }) =>
        Effect.succeed({
          parentNumber,
          children:
            parentNumber === fixture.capability.number
              ? [deliveryTicket]
              : parentNumber === deliveryTicket.number
                ? [nestedTask]
                : [],
          frontier: { status: "empty", message: "No work.", readyIssueIds: [] },
        }),
      processRunner: reviewProcessRunner(),
      githubExecute: ({ args }) => {
        if (args[0] === "issue") trackerWrites += 1;
        return Effect.succeed(output(""));
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      Object.assign(deliveryTicket, completedTicket, { childCount: 1 });
      const sql = yield* SqlClient.SqlClient;
      yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: deliveryTicket.number,
        scopeBody: deliveryTicket.body,
        suffix: "capability-reopened",
      });
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
        workflowDirectorHandlers
          .workflow_complete_capability({
            resultingHead: reviewHead,
            checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
            receipts,
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

      yield* complete([]);
      Object.assign(nestedTask, {
        ...reopened.ticketDetails[0],
        id: "issue-201",
        number: 201,
        title: "Nested task",
        url: `https://github.com/${repository}/issues/201`,
        kind: "task",
        parentNumber: deliveryTicket.number,
        childCount: 0,
        labels: [],
      });
      const result = yield* complete([]);
      expect(result.disposition).toBe("held");
      expect(result.completion).toMatchObject({
        status: "invalidated",
        authority: "historical",
      });
      expect(result.completion.requiredAction).toContain("current resolution evidence");
      expect(fixture.capability.state).toBe("open");
      expect(trackerWrites).toBe(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "does not acquire reopen ownership from an external close during comment recovery",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const completed = interpretedCapabilityFixture(1, new Set([1]));
      const deliveryTicket = fixture.ticketDetails[0]!;
      const completedTicket = completed.ticketDetails[0]!;
      const nestedTask: WorkflowIssueDetail = {
        ...completedTicket,
        id: "issue-202",
        number: 202,
        title: "Nested delivery task",
        url: `https://github.com/${repository}/issues/202`,
        kind: "task",
        parentNumber: completedTicket.number,
        childCount: 0,
        labels: [],
      };
      let commentWrites = 0;
      let closeWrites = 0;
      let reopenWrites = 0;
      const completionComments: WorkflowEvidenceComment[] = [];
      const refreshCapability = (state: "open" | "closed") => {
        Object.assign(fixture.capability, {
          state,
          stateReason: state === "closed" ? "completed" : null,
          ...interpretWorkflowEvidence({
            issue: {
              id: fixture.capability.id,
              url: fixture.capability.url,
              number: fixture.capability.number,
              title: fixture.capability.title,
              kind: fixture.capability.kind,
              state,
              stateReason: state === "closed" ? "completed" : null,
              labels: fixture.capability.labels,
              assignees: [],
              body: fixture.capability.body,
              comments: [
                fixture.source,
                fixture.specification,
                fixture.breakdownRecord,
                ...completionComments,
              ],
              reopenedAt: [],
            },
          }),
        });
      };
      const test = harness({
        ...fixture,
        extraDetails: [nestedTask],
        children: ({ parentNumber }) =>
          Effect.succeed({
            parentNumber,
            children:
              parentNumber === fixture.capability.number
                ? [deliveryTicket]
                : parentNumber === deliveryTicket.number
                  ? [nestedTask]
                  : [],
            frontier: { status: "empty", message: "No work.", readyIssueIds: [] },
          }),
        processRunner: reviewProcessRunner(),
        githubExecute: ({ args, stdin }) => {
          if (args[0] === "issue" && args[1] === "comment") {
            commentWrites += 1;
            completionComments.push({
              id: "capability-completion",
              url: `${fixture.capability.url}#issuecomment-completion`,
              body: stdin!,
              createdAt: "2026-09-07T11:00:00.000Z",
              author: "Flow-Fly",
              authorAssociation: "OWNER",
            });
            refreshCapability("closed");
            return Effect.succeed(output("commented\n"));
          }
          if (args[0] === "issue" && args[1] === "close") {
            closeWrites += 1;
            refreshCapability("closed");
            return Effect.succeed(acknowledgedCloseOutput(fixture.capability));
          }
          if (args[0] === "issue" && args[1] === "reopen") {
            reopenWrites += 1;
            refreshCapability("open");
            return Effect.succeed(output("reopened\n"));
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        Object.assign(deliveryTicket, completedTicket, { childCount: 1 });
        const sql = yield* SqlClient.SqlClient;
        yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: deliveryTicket.number,
          scopeBody: deliveryTicket.body,
          suffix: "capability-external-close",
        });
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
          workflowDirectorHandlers
            .workflow_complete_capability({
              resultingHead: reviewHead,
              checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
              receipts,
            })
            .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

        const registered = yield* complete([]);
        yield* observeReviewCheck({
          threadId: started.director.threadId,
          toolCallId: "external-close-combined",
          command: "vp test run combined.test.ts",
          cwd: started.director.worktreePath,
          startedAt: new Date(Date.parse(registered.completion.createdAt) + 1_000).toISOString(),
          completedAt: new Date(Date.parse(registered.completion.createdAt) + 2_000).toISOString(),
          exitCode: 0,
        });
        yield* sql`CREATE TRIGGER fail_completion_comment_acknowledgment
          BEFORE UPDATE ON workflow_capability_completions
          WHEN NEW.status = 'close-pending'
          BEGIN SELECT RAISE(FAIL, 'injected comment acknowledgment failure'); END`;
        const interrupted = yield* complete([
          { label: "combined", toolCallId: "external-close-combined" },
        ]).pipe(Effect.result);
        expect(interrupted._tag).toBe("Failure");
        expect(commentWrites).toBe(1);
        expect(closeWrites).toBe(0);
        const saved = yield* sql<{
          readonly status: string;
          readonly closeConfirmed: number;
          readonly closeOwned: number;
        }>`
          SELECT status, close_confirmed AS "closeConfirmed", close_owned AS "closeOwned"
          FROM workflow_capability_completions
        `;
        expect(saved).toEqual([{ status: "comment-uncertain", closeConfirmed: 0, closeOwned: 0 }]);
        yield* sql`DROP TRIGGER fail_completion_comment_acknowledgment`;

        Object.assign(nestedTask, {
          state: "open",
          stateReason: null,
          readiness: {
            status: "blocked",
            reasons: [{ kind: "reopened", message: "Required task reopened after acceptance." }],
          },
        });
        const recovered = yield* complete([]);
        expect(recovered).toMatchObject({
          disposition: "held",
          completion: { status: "invalidated", authority: "historical" },
        });
        expect(closeWrites).toBe(0);
        expect(reopenWrites).toBe(0);
        expect(fixture.capability.state).toBe("closed");
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "keeps unavailable nested resolution unknown until required work is visibly reopened",
    () => {
      const fixture = interpretedCapabilityFixture(1);
      const completed = interpretedCapabilityFixture(1, new Set([1]));
      const deliveryTicket = fixture.ticketDetails[0]!;
      const completedTicket = completed.ticketDetails[0]!;
      const nestedTask: WorkflowIssueDetail = {
        ...completedTicket,
        id: "issue-203",
        number: 203,
        title: "Nested delivery task",
        url: `https://github.com/${repository}/issues/203`,
        kind: "task",
        parentNumber: completedTicket.number,
        childCount: 0,
        labels: [],
      };
      let commentWrites = 0;
      let closeWrites = 0;
      let reopenWrites = 0;
      const completionComments: WorkflowEvidenceComment[] = [];
      const refreshCapability = (state: "open" | "closed") => {
        Object.assign(fixture.capability, {
          state,
          stateReason: state === "closed" ? "completed" : null,
          ...interpretWorkflowEvidence({
            issue: {
              id: fixture.capability.id,
              url: fixture.capability.url,
              number: fixture.capability.number,
              title: fixture.capability.title,
              kind: fixture.capability.kind,
              state,
              stateReason: state === "closed" ? "completed" : null,
              labels: fixture.capability.labels,
              assignees: [],
              body: fixture.capability.body,
              comments: [
                fixture.source,
                fixture.specification,
                fixture.breakdownRecord,
                ...completionComments,
              ],
              reopenedAt: [],
            },
          }),
        });
      };
      const test = harness({
        ...fixture,
        extraDetails: [nestedTask],
        children: ({ parentNumber }) =>
          Effect.succeed({
            parentNumber,
            children:
              parentNumber === fixture.capability.number
                ? [deliveryTicket]
                : parentNumber === deliveryTicket.number
                  ? [nestedTask]
                  : [],
            frontier: { status: "empty", message: "No work.", readyIssueIds: [] },
          }),
        processRunner: reviewProcessRunner(),
        githubExecute: ({ args, stdin }) => {
          if (args[0] === "issue" && args[1] === "comment") {
            commentWrites += 1;
            completionComments.push({
              id: "capability-completion",
              url: `${fixture.capability.url}#issuecomment-completion`,
              body: stdin!,
              createdAt: "2026-09-07T11:00:00.000Z",
              author: "Flow-Fly",
              authorAssociation: "OWNER",
            });
            refreshCapability("open");
            return Effect.succeed(output("commented\n"));
          }
          if (args[0] === "issue" && args[1] === "close") {
            closeWrites += 1;
            refreshCapability("closed");
            return Effect.succeed(acknowledgedCloseOutput(fixture.capability));
          }
          if (args[0] === "issue" && args[1] === "reopen") {
            reopenWrites += 1;
            refreshCapability("open");
            return Effect.succeed(output("reopened\n"));
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        Object.assign(deliveryTicket, completedTicket, { childCount: 1 });
        const sql = yield* SqlClient.SqlClient;
        yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: deliveryTicket.number,
          scopeBody: deliveryTicket.body,
          suffix: "capability-resolution-unavailable",
        });
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
          workflowDirectorHandlers
            .workflow_complete_capability({
              resultingHead: reviewHead,
              checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
              receipts,
            })
            .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

        const registered = yield* complete([]);
        yield* observeReviewCheck({
          threadId: started.director.threadId,
          toolCallId: "resolution-unavailable-combined",
          command: "vp test run combined.test.ts",
          cwd: started.director.worktreePath,
          startedAt: new Date(Date.parse(registered.completion.createdAt) + 1_000).toISOString(),
          completedAt: new Date(Date.parse(registered.completion.createdAt) + 2_000).toISOString(),
          exitCode: 0,
        });
        const completedResult = yield* complete([
          { label: "combined", toolCallId: "resolution-unavailable-combined" },
        ]);
        expect(completedResult.disposition).toBe("completed");

        const unavailableResolution = evidenceComment({
          id: "unavailable-resolution",
          createdAt: "2026-09-07T12:00:00.000Z",
          body: [
            "<!-- t3-workflow:v1 resolution -->",
            "Outcome: resolved",
            "## Summary",
            "Delivered.",
            "## Evidence",
            "https://github.com/Flow-Fly/t3code/commit/abc is temporarily unavailable.",
          ].join("\n"),
        });
        Object.assign(nestedTask, {
          ...interpretWorkflowEvidence({
            issue: {
              id: nestedTask.id,
              url: nestedTask.url,
              number: nestedTask.number,
              title: nestedTask.title,
              kind: nestedTask.kind,
              state: "closed",
              stateReason: "completed",
              labels: nestedTask.labels,
              assignees: [],
              body: nestedTask.body,
              comments: [unavailableResolution],
              reopenedAt: [],
            },
          }),
        });
        expect(nestedTask.readiness?.status).toBe("closed-unverified");
        expect(nestedTask.evidence?.records[0]).toMatchObject({
          scope: "current",
          sourceAccess: "unavailable",
        });

        const uncertain = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(uncertain.completion?.authority).toBe("unknown");
        expect(uncertain.status).not.toBe("completed");
        expect(reopenWrites).toBe(0);
        expect(fixture.capability.state).toBe("closed");

        Object.assign(
          nestedTask,
          interpretWorkflowEvidence({
            issue: {
              id: nestedTask.id,
              url: nestedTask.url,
              number: nestedTask.number,
              title: nestedTask.title,
              kind: nestedTask.kind,
              state: "closed",
              stateReason: "completed",
              labels: nestedTask.labels,
              assignees: [],
              body: nestedTask.body,
              comments: [],
              reopenedAt: [],
            },
            historyComplete: false,
          }),
        );
        expect(nestedTask.readiness?.status).toBe("closed-unverified");
        const truncated = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(truncated.completion?.authority).toBe("unknown");
        expect(reopenWrites).toBe(0);

        Object.assign(nestedTask, { readiness: undefined, evidence: undefined });
        const missing = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(missing.completion?.authority).toBe("unknown");
        expect(reopenWrites).toBe(0);

        Object.assign(nestedTask, {
          state: "open",
          stateReason: null,
          readiness: {
            status: "blocked",
            reasons: [{ kind: "reopened", message: "Required task reopened after acceptance." }],
          },
        });
        const invalidated = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(invalidated.completion).toMatchObject({
          status: "invalidated",
          authority: "historical",
        });
        expect(reopenWrites).toBe(1);
        expect(fixture.capability.state).toBe("open");
        expect(commentWrites).toBe(1);
        expect(closeWrites).toBe(1);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "prefers confirmed reopened work when another required resolution is unavailable",
    () => {
      const fixture = interpretedCapabilityFixture(2);
      const test = harness({ ...fixture, processRunner: reviewProcessRunner() });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const sql = yield* SqlClient.SqlClient;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        const unavailableTicket = fixture.ticketDetails[0]!;
        const reopenedTicket = fixture.ticketDetails[1]!;
        const unavailableResolution = evidenceComment({
          id: "mixed-unavailable-resolution",
          createdAt: "2026-09-07T10:00:00.000Z",
          body: [
            "<!-- t3-workflow:v1 resolution -->",
            "Outcome: resolved",
            "## Summary",
            "Delivered.",
            "## Evidence",
            "https://github.com/Flow-Fly/t3code/commit/abc is unavailable.",
          ].join("\n"),
        });
        const staleResolution = evidenceComment({
          id: "mixed-stale-resolution",
          createdAt: "2026-09-07T10:00:00.000Z",
          body: [
            "<!-- t3-workflow:v1 resolution -->",
            "Outcome: resolved",
            "## Summary",
            "Delivered before reopening.",
            "## Evidence",
            "https://github.com/Flow-Fly/t3code/commit/def",
          ].join("\n"),
        });
        Object.assign(
          unavailableTicket,
          {
            state: "closed",
            stateReason: "completed",
          },
          interpretWorkflowEvidence({
            issue: {
              id: unavailableTicket.id,
              url: unavailableTicket.url,
              number: unavailableTicket.number,
              title: unavailableTicket.title,
              kind: unavailableTicket.kind,
              state: "closed",
              stateReason: "completed",
              labels: unavailableTicket.labels,
              assignees: [],
              body: unavailableTicket.body,
              comments: [unavailableResolution],
              reopenedAt: [],
            },
            approvalComments: [fixture.source, fixture.breakdownRecord],
          }),
        );
        Object.assign(
          reopenedTicket,
          {
            state: "closed",
            stateReason: "completed",
          },
          interpretWorkflowEvidence({
            issue: {
              id: reopenedTicket.id,
              url: reopenedTicket.url,
              number: reopenedTicket.number,
              title: reopenedTicket.title,
              kind: reopenedTicket.kind,
              state: "closed",
              stateReason: "completed",
              labels: reopenedTicket.labels,
              assignees: [],
              body: reopenedTicket.body,
              comments: [staleResolution],
              reopenedAt: ["2026-09-07T11:00:00.000Z"],
            },
            approvalComments: [fixture.source, fixture.breakdownRecord],
          }),
        );
        expect(unavailableTicket.readiness).toMatchObject({ status: "closed-unverified" });
        expect(unavailableTicket.evidence?.records).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              kind: "resolution",
              scope: "current",
              sourceAccess: "unavailable",
            }),
          ]),
        );
        expect(reopenedTicket.readiness).toMatchObject({ status: "closed-unverified" });
        expect(reopenedTicket.evidence?.records).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ kind: "resolution", scope: "changed" }),
          ]),
        );

        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const result = yield* workflowDirectorHandlers
          .workflow_complete_capability({
            resultingHead: reviewHead,
            checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
            receipts: [],
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.result,
          );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure).toMatchObject({ failure: "completion-pending" });
          expect(result.failure.detail).toContain(`#${reopenedTicket.number}`);
          expect(result.failure.detail).not.toContain(`#${unavailableTicket.number}`);
        }
        const attempts = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM workflow_capability_completions
        `;
        expect(attempts).toEqual([{ count: 0 }]);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "invalidates an owned completion when published evidence is unavailable and another slice reopens",
    () => {
      const fixture = interpretedCapabilityFixture(2);
      const completed = interpretedCapabilityFixture(2, new Set([1, 2]));
      const unavailableTicket = fixture.ticketDetails[0]!;
      const reopenedTicket = fixture.ticketDetails[1]!;
      let commentWrites = 0;
      let closeWrites = 0;
      let reopenWrites = 0;
      const completionComments: WorkflowEvidenceComment[] = [];
      const refreshCapability = (state: "open" | "closed") => {
        Object.assign(fixture.capability, {
          state,
          stateReason: state === "closed" ? "completed" : null,
          ...interpretWorkflowEvidence({
            issue: {
              id: fixture.capability.id,
              url: fixture.capability.url,
              number: fixture.capability.number,
              title: fixture.capability.title,
              kind: fixture.capability.kind,
              state,
              stateReason: state === "closed" ? "completed" : null,
              labels: fixture.capability.labels,
              assignees: [],
              body: fixture.capability.body,
              comments: [
                fixture.source,
                fixture.specification,
                fixture.breakdownRecord,
                ...completionComments,
              ],
              reopenedAt: [],
            },
          }),
        });
      };
      const test = harness({
        ...fixture,
        processRunner: reviewProcessRunner(),
        githubExecute: ({ args, stdin }) => {
          if (args[0] === "issue" && args[1] === "comment") {
            commentWrites += 1;
            completionComments.push({
              id: "capability-completion",
              url: `${fixture.capability.url}#issuecomment-completion`,
              body: stdin!,
              createdAt: "2026-09-07T11:00:00.000Z",
              author: "Flow-Fly",
              authorAssociation: "OWNER",
            });
            refreshCapability("open");
            return Effect.succeed(output("commented\n"));
          }
          if (args[0] === "issue" && args[1] === "close") {
            closeWrites += 1;
            refreshCapability("closed");
            return Effect.succeed(acknowledgedCloseOutput(fixture.capability));
          }
          if (args[0] === "issue" && args[1] === "reopen") {
            reopenWrites += 1;
            refreshCapability("open");
            return Effect.succeed(output("reopened\n"));
          }
          return Effect.succeed(output(""));
        },
      });
      return Effect.gen(function* () {
        const service = yield* WorkflowDirectorService.WorkflowDirectorService;
        const started = yield* service.start(
          { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
          test.dispatch,
        );
        Object.assign(unavailableTicket, completed.ticketDetails[0]);
        Object.assign(reopenedTicket, completed.ticketDetails[1]);
        const sql = yield* SqlClient.SqlClient;
        yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: unavailableTicket.number,
          scopeBody: unavailableTicket.body,
          suffix: "mixed-published-unavailable",
        });
        yield* seedReportedReview(sql, {
          directorId: started.director.directorId,
          batchId: started.director.batchId,
          ticketNumber: reopenedTicket.number,
          scopeBody: reopenedTicket.body,
          suffix: "mixed-published-reopened",
        });
        const invocation: McpInvocationContext.McpInvocationScope = {
          environmentId,
          threadId: started.director.threadId,
          providerInstanceId: instanceId,
          providerSessionId: "provider-session-director",
          capabilities: new Set(["preview"]),
          issuedAt: 1,
        };
        const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
          workflowDirectorHandlers
            .workflow_complete_capability({
              resultingHead: reviewHead,
              checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
              receipts,
            })
            .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

        const registered = yield* complete([]);
        yield* observeReviewCheck({
          threadId: started.director.threadId,
          toolCallId: "mixed-published-combined",
          command: "vp test run combined.test.ts",
          cwd: started.director.worktreePath,
          startedAt: new Date(Date.parse(registered.completion.createdAt) + 1_000).toISOString(),
          completedAt: new Date(Date.parse(registered.completion.createdAt) + 2_000).toISOString(),
          exitCode: 0,
        });
        const completedResult = yield* complete([
          { label: "combined", toolCallId: "mixed-published-combined" },
        ]);
        expect(completedResult).toMatchObject({
          disposition: "completed",
          completion: { status: "completed", authority: "current" },
        });
        const owned = yield* sql<{ readonly status: string; readonly closeOwned: number }>`
          SELECT status, close_owned AS "closeOwned" FROM workflow_capability_completions
        `;
        expect(owned).toEqual([{ status: "completed", closeOwned: 1 }]);

        Object.assign(unavailableTicket, {
          evidence: {
            ...unavailableTicket.evidence!,
            records: unavailableTicket.evidence!.records.map((record) =>
              record.kind === "approval" && record.approvalKind === "ticket-breakdown"
                ? { ...record, sourceAccess: "unavailable" as const }
                : record,
            ),
          },
        });
        Object.assign(reopenedTicket, {
          state: "open",
          stateReason: null,
          readiness: {
            status: "blocked",
            reasons: [{ kind: "reopened", message: "Required slice reopened after acceptance." }],
          },
        });

        const status = yield* service.status({
          projectId,
          repository,
          capabilityNumber: 17,
        });
        expect(status.completion).toMatchObject({
          status: "invalidated",
          authority: "historical",
        });
        expect(status.completion?.lastError).toContain(`#${reopenedTicket.number}`);
        expect(fixture.capability.state).toBe("open");
        expect(commentWrites).toBe(1);
        expect(closeWrites).toBe(1);
        expect(reopenWrites).toBe(1);
        const invalidated = yield* sql<{ readonly status: string; readonly closeOwned: number }>`
          SELECT status, close_owned AS "closeOwned" FROM workflow_capability_completions
        `;
        expect(invalidated).toEqual([{ status: "invalidated", closeOwned: 1 }]);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("holds completion when published breakdown evidence is unavailable", () => {
    const fixture = interpretedCapabilityFixture(1);
    const deliveryTicket = fixture.ticketDetails[0]!;
    const test = harness({ ...fixture, processRunner: reviewProcessRunner() });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const sql = yield* SqlClient.SqlClient;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      const unavailableBreakdown = evidenceComment({
        id: "unavailable-breakdown-approval",
        createdAt: fixture.breakdownRecord.createdAt,
        body: fixture.breakdownRecord.body.replace(
          `Source: ${fixture.source.url}`,
          "Source: unavailable",
        ),
      });
      const resolution = evidenceComment({
        id: "breakdown-outage-resolution",
        createdAt: "2026-09-07T10:00:00.000Z",
        body: [
          "<!-- t3-workflow:v1 resolution -->",
          "Outcome: resolved",
          "## Summary",
          "Delivered.",
          "## Evidence",
          "https://github.com/Flow-Fly/t3code/commit/abc",
        ].join("\n"),
      });
      Object.assign(
        deliveryTicket,
        { state: "closed", stateReason: "completed" },
        interpretWorkflowEvidence({
          issue: {
            id: deliveryTicket.id,
            url: deliveryTicket.url,
            number: deliveryTicket.number,
            title: deliveryTicket.title,
            kind: deliveryTicket.kind,
            state: "closed",
            stateReason: "completed",
            labels: deliveryTicket.labels,
            assignees: [],
            body: deliveryTicket.body,
            comments: [resolution],
            reopenedAt: [],
          },
          approvalComments: [unavailableBreakdown],
        }),
      );
      expect(deliveryTicket.readiness).toMatchObject({ status: "resolved" });
      expect(deliveryTicket.evidence?.records).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            approvalKind: "ticket-breakdown",
            scope: "current",
            sourceAccess: "unavailable",
          }),
        ]),
      );

      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const result = yield* workflowDirectorHandlers
        .workflow_complete_capability({
          resultingHead: reviewHead,
          checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
          receipts: [],
        })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.result,
        );
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toMatchObject({ failure: "completion-unavailable" });
      }
      const attempts = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM workflow_capability_completions
      `;
      expect(attempts).toEqual([{ count: 0 }]);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("keeps a GitHub CLI already-closed result unowned", () => {
    const fixture = interpretedCapabilityFixture(1);
    Object.assign(fixture.capability, {
      title: `Closed issue ${repository}#${fixture.capability.number} (lookalike)`,
    });
    const completed = interpretedCapabilityFixture(1, new Set([1]));
    const deliveryTicket = fixture.ticketDetails[0]!;
    const completedTicket = completed.ticketDetails[0]!;
    const nestedTask: WorkflowIssueDetail = {
      ...completedTicket,
      id: "issue-204",
      number: 204,
      title: "Nested delivery task",
      url: `https://github.com/${repository}/issues/204`,
      kind: "task",
      parentNumber: completedTicket.number,
      childCount: 0,
      labels: [],
    };
    let commentWrites = 0;
    let closeWrites = 0;
    let reopenWrites = 0;
    const completionComments: WorkflowEvidenceComment[] = [];
    const refreshCapability = (state: "open" | "closed") => {
      Object.assign(fixture.capability, {
        state,
        stateReason: state === "closed" ? "completed" : null,
        ...interpretWorkflowEvidence({
          issue: {
            id: fixture.capability.id,
            url: fixture.capability.url,
            number: fixture.capability.number,
            title: fixture.capability.title,
            kind: fixture.capability.kind,
            state,
            stateReason: state === "closed" ? "completed" : null,
            labels: fixture.capability.labels,
            assignees: [],
            body: fixture.capability.body,
            comments: [
              fixture.source,
              fixture.specification,
              fixture.breakdownRecord,
              ...completionComments,
            ],
            reopenedAt: [],
          },
        }),
      });
    };
    const test = harness({
      ...fixture,
      extraDetails: [nestedTask],
      children: ({ parentNumber }) =>
        Effect.succeed({
          parentNumber,
          children:
            parentNumber === fixture.capability.number
              ? [deliveryTicket]
              : parentNumber === deliveryTicket.number
                ? [nestedTask]
                : [],
          frontier: { status: "empty", message: "No work.", readyIssueIds: [] },
        }),
      processRunner: reviewProcessRunner(),
      githubExecute: ({ args, stdin }) => {
        if (args[0] === "issue" && args[1] === "comment") {
          commentWrites += 1;
          completionComments.push({
            id: "capability-completion",
            url: `${fixture.capability.url}#issuecomment-completion`,
            body: stdin!,
            createdAt: "2026-09-07T11:00:00.000Z",
            author: "Flow-Fly",
            authorAssociation: "OWNER",
          });
          refreshCapability("open");
          return Effect.succeed(output("commented\n"));
        }
        if (args[0] === "issue" && args[1] === "close") {
          closeWrites += 1;
          refreshCapability("closed");
          return Effect.succeed({
            ...output(""),
            stderr: `! Issue ${repository}#${fixture.capability.number} (${fixture.capability.title}) is already closed\n`,
          });
        }
        if (args[0] === "issue" && args[1] === "reopen") {
          reopenWrites += 1;
          refreshCapability("open");
          return Effect.succeed(output("reopened\n"));
        }
        return Effect.succeed(output(""));
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      Object.assign(deliveryTicket, completedTicket, { childCount: 1 });
      const sql = yield* SqlClient.SqlClient;
      yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: deliveryTicket.number,
        scopeBody: deliveryTicket.body,
        suffix: "capability-close-noop",
      });
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const complete = (receipts: ReadonlyArray<{ label: string; toolCallId: string }>) =>
        workflowDirectorHandlers
          .workflow_complete_capability({
            resultingHead: reviewHead,
            checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
            receipts,
          })
          .pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, invocation));

      const registered = yield* complete([]);
      yield* observeReviewCheck({
        threadId: started.director.threadId,
        toolCallId: "close-noop-combined",
        command: "vp test run combined.test.ts",
        cwd: started.director.worktreePath,
        startedAt: new Date(Date.parse(registered.completion.createdAt) + 1_000).toISOString(),
        completedAt: new Date(Date.parse(registered.completion.createdAt) + 2_000).toISOString(),
        exitCode: 0,
      });
      const completedResult = yield* complete([
        { label: "combined", toolCallId: "close-noop-combined" },
      ]);
      expect(completedResult).toMatchObject({
        disposition: "completed",
        completion: { status: "completed", authority: "current" },
      });
      const ownership = yield* sql<{ readonly closeOwned: number }>`
        SELECT close_owned AS "closeOwned" FROM workflow_capability_completions
      `;
      expect(ownership).toEqual([{ closeOwned: 0 }]);

      Object.assign(nestedTask, {
        state: "open",
        stateReason: null,
        readiness: {
          status: "blocked",
          reasons: [{ kind: "reopened", message: "Required task reopened after acceptance." }],
        },
      });
      const status = yield* service.status({
        projectId,
        repository,
        capabilityNumber: 17,
      });
      expect(status.completion).toMatchObject({
        status: "invalidated",
        authority: "historical",
      });
      expect(fixture.capability.state).toBe("closed");
      expect(commentWrites).toBe(1);
      expect(closeWrites).toBe(1);
      expect(reopenWrites).toBe(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("does not treat an empty frontier or confirmed interruption as completion", () => {
    const fixture = interpretedCapabilityFixture(1);
    const completed = interpretedCapabilityFixture(1, new Set([1]));
    const deliveryTicket = fixture.ticketDetails[0]!;
    const test = harness({
      ...fixture,
      children: ({ parentNumber }) =>
        Effect.succeed({
          parentNumber,
          children: parentNumber === fixture.capability.number ? [deliveryTicket] : [],
          frontier: { status: "empty", message: "No work.", readyIssueIds: [] },
        }),
      processRunner: reviewProcessRunner(),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowDirectorService.WorkflowDirectorService;
      const started = yield* service.start(
        { projectId, repository, rootNumber: 10, capabilityNumber: 17, modelSelection },
        test.dispatch,
      );
      Object.assign(deliveryTicket, completed.ticketDetails[0]);
      const sql = yield* SqlClient.SqlClient;
      yield* seedReportedReview(sql, {
        directorId: started.director.directorId,
        batchId: started.director.batchId,
        ticketNumber: deliveryTicket.number,
        scopeBody: deliveryTicket.body,
        suffix: "capability-interrupted",
        workerClosed: false,
      });
      const invocation: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: started.director.threadId,
        providerInstanceId: instanceId,
        providerSessionId: "provider-session-director",
        capabilities: new Set(["preview"]),
        issuedAt: 1,
      };
      const result = yield* workflowDirectorHandlers
        .workflow_complete_capability({
          resultingHead: reviewHead,
          checks: [{ label: "combined", command: "vp test run combined.test.ts" }],
          receipts: [],
        })
        .pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.result,
        );
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure.failure).toBe("completion-pending");
        expect(result.failure.message).toContain("exactly closed");
      }
      const attempts = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM workflow_capability_completions
      `;
      expect(attempts[0]?.count).toBe(0);
      expect(fixture.capability.state).toBe("open");
    }).pipe(Effect.provide(test.layer));
  });
});
