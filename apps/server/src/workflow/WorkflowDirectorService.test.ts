import {
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type OrchestrationCommand,
  type ServerProvider,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import { makeProviderRegistryMock } from "../provider/testUtils/providerRegistryMock.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowDirectorService from "./WorkflowDirectorService.ts";
import {
  interpretWorkflowEvidence,
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
  readonly locate?: WorkflowService.WorkflowService["Service"]["locate"];
  readonly threadShell?: ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]["getThreadShellById"];
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
  const layer = Layer.effect(
    WorkflowDirectorService.WorkflowDirectorService,
    WorkflowDirectorService.make,
  ).pipe(
    Layer.provide(
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
        children: ({ parentNumber }) =>
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
          }),
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
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getProjectShellById: () =>
          Effect.succeed(
            Option.some({
              id: projectId,
              title: "T3 Code",
              workspaceRoot: selectedWorkspaceRoot,
            } as never),
          ),
        getThreadShellById: options.threadShell ?? (() => Effect.succeed(Option.none())),
        getThreadDetailById: options.threadDetail ?? (() => Effect.succeed(Option.none())),
      }),
    ),
    Layer.provide(
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
    Layer.provide(
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
    Layer.provide(
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
    Layer.provide(
      Layer.succeed(
        ProcessRunner.ProcessRunner,
        options.processRunner ?? {
          run: () =>
            Effect.succeed(processOutput(`fork\tgit@github.com:${repository}.git (fetch)\n`)),
        },
      ),
    ),
    Layer.provide(
      Layer.mock(OrchestrationCommandReceiptRepository)({
        upsert: () => Effect.void,
        getByCommandId: () => Effect.succeed(Option.none()),
      }),
    ),
    Layer.provide(
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(environmentId),
      }),
    ),
    Layer.provide(
      ServerConfig.layerTest(selectedWorkspaceRoot, { prefix: "workflow-director-test-" }),
    ),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provide(NodeServices.layer),
  );
  const dispatch = (command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>) =>
    Effect.sync(() => {
      commands.push(command);
      return { sequence: 101 };
    });
  return { commands, dispatch, layer, statusCalls, worktreeCalls };
}

describe("WorkflowDirectorService", () => {
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
      expect(blockedRetry._tag).toBe("Failure");
      expect(nested._tag).toBe("Failure");
      expect(eleventh).toMatchObject({
        disposition: "limit-reached",
        admission: null,
        admissionCount: 10,
        directorStatus: "waiting",
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
});
