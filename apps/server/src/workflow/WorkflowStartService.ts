import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentId,
  MessageId,
  type OrchestrationCommand,
  type OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  WorkflowQueryError,
  type WorkflowRecoverInput,
  type WorkflowRecoverResult,
  type WorkflowRecoveryAttempt,
  type WorkflowRecoveryInput,
  type WorkflowRecoveryResult,
  WorkflowPhase,
  WorkflowStartError,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
  type WorkflowStartInput,
  type WorkflowStartResult,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowService from "./WorkflowService.ts";

type Dispatch = (
  command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;

const AttemptRow = Schema.Struct({
  attemptId: Schema.String,
  environmentId: Schema.String,
  projectId: Schema.String,
  repository: Schema.String,
  rootNumber: Schema.Number,
  issueNumber: Schema.Number,
  phase: WorkflowPhase,
  commandId: Schema.String,
  threadId: Schema.String,
  status: Schema.String,
  claimLogin: Schema.NullOr(Schema.String),
  claimOwned: Schema.Number,
  sequence: Schema.NullOr(Schema.Number),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  detail: Schema.NullOr(Schema.String),
  initialTurnDisposition: Schema.NullOr(Schema.String),
  isCurrent: Schema.Number,
});
type AttemptRow = typeof AttemptRow.Type;
const decodeAttemptRow = Schema.decodeUnknownEffect(AttemptRow);

function workflowPhase(input: { readonly phase?: WorkflowPhase | undefined }): WorkflowPhase {
  return input.phase ?? "decision";
}

function startError(failure: WorkflowStartError["failure"], message: string, detail?: string) {
  return new WorkflowStartError({ failure, message, ...(detail ? { detail } : {}) });
}

function markdownSection(body: string, name: string): string | undefined {
  return new RegExp(`(?:^|\\n)#{2,6}\\s+${name}\\s*\\n([\\s\\S]*?)(?=\\n#{1,6}\\s|$)`, "iu")
    .exec(body)?.[1]
    ?.trim();
}

function explicitlyClearedFog(body: string): boolean {
  const remaining = markdownSection(body, "(?:Remaining fog|Remaining unknowns)");
  return Boolean(remaining && /^(?:none|no remaining (?:fog|unknowns?))\b/iu.test(remaining));
}

function sourceMapReferences(body: string) {
  const source = markdownSection(body, "Source map");
  if (!source) {
    const standaloneOrigin =
      markdownSection(body, "Origin") ?? markdownSection(body, "Specification");
    return standaloneOrigin
      ? { kind: "standalone" as const, references: [] }
      : { kind: "missing" as const, references: [] };
  }
  if (/^none(?:\s*\(standalone\))?\.?\s*$/iu.test(source)) {
    return { kind: "standalone" as const, references: [] };
  }
  const references = [
    ...new Map(
      [...source.matchAll(/https:\/\/github\.com\/([^/\s]+\/[^/\s)]+)\/issues\/(\d+)/giu)].map(
        (match) => {
          const repository = match[1]!;
          const number = Number(match[2]);
          return [`${repository.toLowerCase()}#${number}`, { repository, number }] as const;
        },
      ),
    ).values(),
  ];
  return {
    kind: references.length === 1 ? ("mapped" as const) : ("ambiguous" as const),
    references,
  };
}

function samePreservedContent(approvedContent: string, currentContent: string): boolean {
  return approvedContent.trim() === currentContent.trim();
}

function durableT3SourceReference(source: string | undefined) {
  if (!source) return null;
  const threadIds = [...source.matchAll(/\bT3(?: Code)? thread\s+`([^`]+)`/giu)];
  const messageIds = [...source.matchAll(/\b(?:user\s+)?message\s+`([^`]+)`/giu)];
  if (threadIds.length !== 1 || messageIds.length !== 1) return null;
  return { threadId: threadIds[0]![1]!, messageId: messageIds[0]![1]! };
}

function requiredSkillNames(labels: ReadonlyArray<string>): ReadonlyArray<string> {
  if (labels.includes("wayfinder:grilling"))
    return ["wayfinder", "grill-with-docs", "grilling", "domain-modeling"];
  if (labels.includes("wayfinder:research")) return ["wayfinder", "research"];
  if (labels.includes("wayfinder:prototype")) return ["wayfinder", "prototype"];
  if (labels.includes("wayfinder:task")) return ["wayfinder"];
  return ["wayfinder"];
}

const PROCESS_LABELS = [
  "wayfinder:grilling",
  "wayfinder:research",
  "wayfinder:prototype",
  "wayfinder:task",
] as const;

const WORKFLOW_KIND_LABELS = [
  "wayfinder:map",
  ...PROCESS_LABELS,
  "workflow:map",
  "workflow:decision",
  "workflow:capability",
  "workflow:container",
  "workflow:ticket",
  "workflow:task",
] as const;

function decisionProcess(labels: ReadonlyArray<string>): string {
  if (labels.includes("wayfinder:grilling"))
    return "Run grill-with-docs: grill the owner interactively with the grilling skill and use domain-modeling to document the durable decision.";
  if (labels.includes("wayfinder:research"))
    return "Research the question from authoritative sources and present evidence for the owner to decide.";
  if (labels.includes("wayfinder:prototype"))
    return "Build a throwaway prototype that answers the decision question and review it with the owner.";
  return "Complete the prerequisite task and bring the result back to the map before advancing dependent work.";
}

export function workflowDecisionInstructions(input: {
  readonly repository: string;
  readonly mapNumber: number;
  readonly mapTitle: string;
  readonly issueNumber: number;
  readonly issueTitle: string;
  readonly issueUrl: string;
  readonly labels: ReadonlyArray<string>;
}): string {
  return [
    "Use Wayfinder to work this selected workflow decision.",
    "",
    `Map: ${input.repository}#${input.mapNumber} — ${input.mapTitle}`,
    `Decision: ${input.repository}#${input.issueNumber} — ${input.issueTitle}`,
    `Source: ${input.issueUrl}`,
    "",
    decisionProcess(input.labels),
    "Use every attached skill explicitly. Do not substitute another process or skill.",
    "Keep required human participation in the loop. Do not infer approval from labels or silence.",
    "Before reporting this decision resolved, record the outcome and current evidence in a Resolution record, then update the map's decision index and remaining unknowns.",
    "Recheck live prerequisites before changing tracker state or advancing dependent work.",
  ].join("\n");
}

function currentSpecificationApprovals(issue: WorkflowIssueDetail) {
  return (
    issue.evidence?.records.filter(
      (record) =>
        record.kind === "approval" &&
        record.approvalKind === "specification" &&
        record.state === "current" &&
        record.scope === "current" &&
        record.authority === "verified" &&
        record.sourceAccess !== "unavailable" &&
        typeof record.approvedBy === "string" &&
        record.approvedBy.length > 0 &&
        typeof record.approvedContent === "string" &&
        samePreservedContent(record.approvedContent, issue.body),
    ) ?? []
  );
}

export function workflowSpecificationInstructions(input: {
  readonly map: WorkflowIssueDetail;
  readonly resolvedDecisions: ReadonlyArray<WorkflowIssueSummary>;
  readonly excludedHistory: ReadonlyArray<WorkflowIssueSummary>;
}): string {
  return [
    "Use the to-spec skill explicitly to turn this resolved Wayfinder map into a capability specification.",
    "",
    `Source map: ${input.map.url}`,
    `Map title: ${input.map.title}`,
    "",
    "Accepted map context:",
    input.map.body,
    "",
    "Resolved in-scope decisions:",
    ...input.resolvedDecisions.map(
      (decision) =>
        `- ${decision.repository}#${decision.number} — ${decision.title} (${decision.url})`,
    ),
    "",
    "Excluded history (preserve as context; do not treat as resolved decisions):",
    ...(input.excludedHistory.length > 0
      ? input.excludedHistory.map(
          (decision) =>
            `- ${decision.repository}#${decision.number} — ${decision.title} (${decision.url})`,
        )
      : ["- None."]),
    "",
    "Publish the new capability as the specification issue. Start its body with a concise Summary, include this Source map, and apply workflow:capability. Keep decision issues under the map; the map may inform other capabilities later.",
    "After publishing the specification, stop and ask the owner for explicit specification approval. Preserve any approval as a versioned Approval record with the owner, its explicit source, and the exact approved content. Do not infer approval from labels, issue prose, silence, or an earlier map decision.",
    "Do not slice or publish delivery tickets in this phase.",
  ].join("\n");
}

export function workflowTicketBreakdownInstructions(input: {
  readonly capability: WorkflowIssueDetail;
  readonly approvalUrl: string;
  readonly approvedBy: string;
}): string {
  return [
    "Use the to-tickets skill explicitly for the current approved capability specification.",
    "",
    `Capability: ${input.capability.repository}#${input.capability.number} — ${input.capability.title}`,
    `Source: ${input.capability.url}`,
    `Current specification approval: ${input.approvalUrl} by ${input.approvedBy}`,
    "",
    "Current approved specification:",
    input.capability.body,
    "",
    "First draft the proposed breakdown and present it to the owner with scopes, acceptance criteria, and blocking edges.",
    "Do not publish delivery issues until the owner separately approves that complete proposed breakdown in a later message. Specification approval authorizes this proposal step only.",
    "When that separate approval arrives, preserve the exact approved breakdown in a ticket-breakdown Approval record with the owner and explicit source. Then publish delivery tickets as native sub-issues of this capability, add native blocker edges, and apply workflow:ticket. Apply ready-for-agent only from current live readiness.",
    "Publishing tickets does not start implementation or a director. Implementation remains behind the capability Start action.",
  ].join("\n");
}

function resultFromRow(
  row: Pick<
    AttemptRow,
    | "attemptId"
    | "environmentId"
    | "projectId"
    | "repository"
    | "rootNumber"
    | "issueNumber"
    | "phase"
    | "threadId"
    | "status"
    | "createdAt"
    | "detail"
  >,
  disposition: WorkflowStartResult["disposition"],
): WorkflowStartResult {
  const held = row.status !== "submitted";
  const phase = row.phase;
  const submittedMessage =
    phase === "decision"
      ? "Decision work started."
      : phase === "specification"
        ? "Capability specification started in the planning thread."
        : "Ticket breakdown proposal started in the planning thread.";
  const existingMessage =
    phase === "decision"
      ? "Opening the existing decision attempt."
      : "Opening the existing planning phase.";
  return {
    disposition: held ? "held" : disposition,
    attemptId: row.attemptId,
    environmentId: row.environmentId,
    projectId: row.projectId,
    repository: row.repository,
    rootNumber: row.rootNumber,
    issueNumber: row.issueNumber,
    phase,
    threadId: row.threadId,
    status: held ? "held" : "submitted",
    createdAt: row.createdAt,
    message: held
      ? (row.detail ?? "The first submission is uncertain. Reconcile this attempt before retrying.")
      : disposition === "started"
        ? submittedMessage
        : existingMessage,
  } as WorkflowStartResult;
}

export class WorkflowStartService extends Context.Service<
  WorkflowStartService,
  {
    readonly start: (
      input: WorkflowStartInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowStartResult, WorkflowQueryError | WorkflowStartError>;
    readonly recovery: (
      input: WorkflowRecoveryInput,
    ) => Effect.Effect<WorkflowRecoveryResult, WorkflowQueryError | WorkflowStartError>;
    readonly recover: (
      input: WorkflowRecoverInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowRecoverResult, WorkflowQueryError | WorkflowStartError>;
  }
>()("t3/workflow/WorkflowStartService") {}

export const make = Effect.gen(function* () {
  const workflow = yield* WorkflowService.WorkflowService;
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const github = yield* GitHubCli.GitHubCli;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const sql = yield* SqlClient.SqlClient;
  const commandReceipts = yield* OrchestrationCommandReceiptRepository;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const lock = yield* Semaphore.make(1);

  const loadAttempts = Effect.fn("WorkflowStartService.loadAttempts")(function* (
    input: Pick<WorkflowStartInput, "projectId" | "repository" | "issueNumber" | "phase">,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT attempt_id AS "attemptId", environment_id AS "environmentId",
        project_id AS "projectId", repository, root_number AS "rootNumber",
        issue_number AS "issueNumber", phase, command_id AS "commandId", thread_id AS "threadId", status,
        claim_login AS "claimLogin", claim_owned AS "claimOwned", sequence,
        created_at AS "createdAt", updated_at AS "updatedAt", detail,
        initial_turn_disposition AS "initialTurnDisposition", is_current AS "isCurrent"
      FROM workflow_start_attempts
      WHERE project_id = ${input.projectId} AND repository = ${input.repository}
        AND issue_number = ${input.issueNumber} AND phase = ${workflowPhase(input)}
      ORDER BY is_current DESC, created_at DESC
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The existing workflow attempt could not be read.",
          String(error),
        ),
      ),
    );
    return yield* Effect.forEach(rows, (row) => decodeAttemptRow(row)).pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The existing workflow attempt is invalid.",
          String(error),
        ),
      ),
    );
  });

  const readEvidence = Effect.fn("WorkflowStartService.readEvidence")(function* (row: AttemptRow) {
    const receipt = yield* commandReceipts
      .getByCommandId({ commandId: CommandId.make(row.commandId) })
      .pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "Workflow command evidence could not be read.",
            String(error),
          ),
        ),
      );
    if (Option.isNone(receipt)) {
      if (row.status === "submitted" && row.sequence !== null) {
        return { evidence: "accepted" as const, row };
      }
      if (
        row.initialTurnDisposition === "not-attempted" ||
        row.initialTurnDisposition === "not-accepted"
      ) {
        return { evidence: "rejected" as const, row };
      }
      return { evidence: "unknown" as const, row };
    }
    if (receipt.value.status === "accepted") {
      let updatedAt = row.updatedAt;
      if (row.status !== "submitted" || row.detail !== null) {
        const reconciledAt = DateTime.formatIso(yield* DateTime.now);
        updatedAt = reconciledAt;
        yield* sql`
          UPDATE workflow_start_attempts SET status = 'submitted', sequence = ${receipt.value.resultSequence},
            detail = NULL, initial_turn_disposition = 'accepted', updated_at = ${reconciledAt}
          WHERE attempt_id = ${row.attemptId}
        `.pipe(
          Effect.mapError((error) =>
            startError(
              "persistence-failed",
              "Accepted workflow evidence could not be saved.",
              String(error),
            ),
          ),
        );
      }
      return {
        evidence: "accepted" as const,
        row: { ...row, status: "submitted", detail: null, updatedAt },
      };
    }
    return { evidence: "rejected" as const, row };
  });

  const loadAttempt = Effect.fn("WorkflowStartService.loadAttempt")(function* (
    input: WorkflowStartInput,
  ) {
    return (yield* loadAttempts(input)).find((row) => row.isCurrent === 1);
  });

  const selectedProject = Effect.fn("WorkflowStartService.selectedProject")(function* (
    projectId: ProjectId,
  ) {
    const project = yield* projection
      .getProjectShellById(projectId)
      .pipe(
        Effect.mapError((error) =>
          startError(
            "workspace-unavailable",
            "The target project could not be read.",
            error.message,
          ),
        ),
      );
    return yield* Option.match(project, {
      onNone: () =>
        Effect.fail(
          startError("workspace-unavailable", "The target project is no longer available."),
        ),
      onSome: Effect.succeed,
    });
  });

  const executeClaim = Effect.fn("WorkflowStartService.executeClaim")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
  ) {
    return yield* github
      .execute({ cwd, args, maxOutputBytes: 100_000 })
      .pipe(
        Effect.mapError((error) =>
          startError("claim-failed", "GitHub could not claim this decision.", error.message),
        ),
      );
  });

  const readAssignees = Effect.fn("WorkflowStartService.readAssignees")(function* (input: {
    readonly cwd: string;
    readonly repository: string;
    readonly issueNumber: number;
  }) {
    const result = yield* executeClaim(input.cwd, [
      "issue",
      "view",
      String(input.issueNumber),
      "--repo",
      input.repository,
      "--json",
      "assignees",
      "--jq",
      ".assignees[].login",
    ]);
    return result.stdout
      .split("\n")
      .map((value) => value.trim())
      .filter(Boolean)
      .toSorted();
  });

  const recoveryAttempt = (
    row: AttemptRow,
    evidence: WorkflowRecoveryAttempt["evidence"],
  ): WorkflowRecoveryAttempt => ({
    attemptId: row.attemptId,
    environmentId: EnvironmentId.make(row.environmentId),
    projectId: ProjectId.make(row.projectId),
    repository: row.repository as WorkflowRecoveryAttempt["repository"],
    rootNumber: row.rootNumber,
    issueNumber: row.issueNumber,
    phase: row.phase,
    threadId: ThreadId.make(row.threadId),
    status:
      row.status === "claiming" ||
      row.status === "submitting" ||
      row.status === "submitted" ||
      row.status === "held"
        ? row.status
        : "held",
    evidence,
    claimLogin: row.claimLogin,
    isCurrent: row.isCurrent === 1,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    detail: row.detail,
  });

  const recoveryUnlocked = Effect.fn("WorkflowStartService.recovery")(function* (
    input: WorkflowRecoveryInput,
  ) {
    const project = yield* selectedProject(input.projectId);
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        startError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const rows = yield* loadAttempts(input);
    const reconciled = yield* Effect.forEach(rows, readEvidence);
    const attempts = reconciled.map(({ row, evidence }) => recoveryAttempt(row, evidence));
    const currentAttempt = attempts.find((attempt) => attempt.isCurrent) ?? null;
    if (workflowPhase(input) !== "decision") {
      const shell = currentAttempt
        ? yield* projection
            .getThreadShellById(currentAttempt.threadId)
            .pipe(
              Effect.mapError((error) =>
                startError(
                  "persistence-failed",
                  "The linked planning thread state could not be read.",
                  String(error),
                ),
              ),
            )
        : Option.none();
      const observation = [
        workflowPhase(input),
        currentAttempt?.attemptId ?? "none",
        currentAttempt?.status ?? "none",
        currentAttempt?.evidence ?? "none",
      ].join("|");
      const canOpen = currentAttempt !== null && Option.isSome(shell);
      return {
        environmentId,
        projectId: input.projectId,
        repository: input.repository,
        issueNumber: input.issueNumber,
        attempts,
        currentAttempt,
        assignees: [],
        observation,
        actions: canOpen ? ["open" as const] : [],
        message: canOpen
          ? currentAttempt.evidence === "accepted"
            ? "This planning phase is linked to its preserved thread."
            : "The planning submission is uncertain. Open the linked thread before taking another action."
          : "No planning phase is linked to this issue in this environment.",
      } satisfies WorkflowRecoveryResult;
    }
    const resumeRows = currentAttempt
      ? yield* sql<{
          readonly resumeId: string;
          readonly sourceTurnId: string;
          readonly status: string;
          readonly commandId: string;
        }>`
          SELECT resume_id AS "resumeId", source_turn_id AS "sourceTurnId",
            status, command_id AS "commandId"
          FROM workflow_resume_attempts
          WHERE workflow_attempt_id = ${currentAttempt.attemptId}
          ORDER BY created_at DESC
        `.pipe(
          Effect.mapError((error) =>
            startError("persistence-failed", "Resume history could not be read.", String(error)),
          ),
        )
      : [];
    const assignees = yield* readAssignees({
      cwd: project.workspaceRoot,
      repository: input.repository,
      issueNumber: input.issueNumber,
    });
    const observation = [
      currentAttempt?.attemptId ?? "none",
      currentAttempt?.status ?? "none",
      currentAttempt?.evidence ?? "none",
      assignees.map(encodeURIComponent).join(","),
      resumeRows
        .map(
          (resume) =>
            `${resume.resumeId}:${resume.sourceTurnId}:${resume.status}:${resume.commandId}`,
        )
        .join(","),
    ].join("|");
    const actions = new Array<WorkflowRecoveryResult["actions"][number]>();
    let message = "No execution attempt is linked to this issue in this environment.";
    const recoverableClaimLogins = new Set(
      reconciled.flatMap(({ row, evidence }) =>
        row.claimLogin !== null &&
        (evidence === "accepted" || (evidence === "rejected" && row.claimOwned === 1))
          ? [row.claimLogin]
          : [],
      ),
    );
    const acceptedClaimMatches =
      currentAttempt?.evidence === "accepted" &&
      currentAttempt.claimLogin !== null &&
      assignees.length === 1 &&
      assignees[0] === currentAttempt.claimLogin;
    const freshClaimMatches =
      assignees.length === 0 ||
      (assignees.length === 1 && recoverableClaimLogins.has(assignees[0]!));
    if (currentAttempt?.evidence === "accepted") {
      if (freshClaimMatches) actions.push("start-fresh");
      const shell = yield* projection
        .getThreadShellById(currentAttempt.threadId)
        .pipe(
          Effect.mapError((error) =>
            startError(
              "persistence-failed",
              "The linked thread state could not be read.",
              String(error),
            ),
          ),
        );
      if (Option.isSome(shell)) {
        actions.unshift("open");
        if (
          acceptedClaimMatches &&
          (shell.value.latestTurn?.state === "interrupted" ||
            shell.value.latestTurn?.state === "error") &&
          shell.value.session?.activeTurnId == null &&
          shell.value.session?.status !== "running" &&
          shell.value.session?.status !== "starting"
        ) {
          actions.push("resume");
        }
        message = "This environment has accepted work linked to the preserved thread.";
      } else {
        message =
          "The initial turn was accepted, but its linked thread is not available in this environment. Start fresh only after reviewing the preserved attempt history.";
      }
    } else if (currentAttempt?.evidence === "rejected") {
      if (freshClaimMatches) actions.push("start-fresh");
      message =
        "The initial turn was confirmed not accepted, so a fresh attempt is safe after current checks.";
    } else if (currentAttempt) {
      const shell = yield* projection
        .getThreadShellById(currentAttempt.threadId)
        .pipe(
          Effect.mapError((error) =>
            startError(
              "persistence-failed",
              "The linked thread state could not be read.",
              String(error),
            ),
          ),
        );
      if (Option.isSome(shell)) {
        actions.push("open");
        message =
          "Initial submission evidence is unavailable. Open the linked thread to inspect it; a new first turn is held.";
      } else {
        message =
          "Initial submission evidence is unavailable and no linked thread exists. Retry only after the original command outcome can be verified.";
      }
    }
    const hasUnexpectedAssignee =
      currentAttempt?.evidence === "accepted"
        ? assignees.length > 0 && !acceptedClaimMatches
        : assignees.some((assignee) => !recoverableClaimLogins.has(assignee));
    if (hasUnexpectedAssignee) {
      actions.push("takeover");
      message =
        "GitHub shows an existing assignment. Confirm the handoff outside T3 Code or explicitly take over after checking the other environment.";
    }
    return {
      environmentId,
      projectId: input.projectId,
      repository: input.repository,
      issueNumber: input.issueNumber,
      attempts,
      currentAttempt,
      assignees,
      observation,
      actions,
      message,
    } satisfies WorkflowRecoveryResult;
  });

  const prepareContinuation = Effect.fn("WorkflowStartService.prepareContinuation")(function* (
    input: WorkflowRecoverInput & {
      readonly modelSelection: NonNullable<WorkflowRecoverInput["modelSelection"]>;
      readonly expectedClaimLogin: string | null;
    },
  ) {
    const project = yield* selectedProject(input.projectId);
    const issue = yield* workflow.issueDetail({
      projectId: input.projectId,
      repository: input.repository,
      number: input.issueNumber,
    });
    if (issue.readiness?.status !== "ready" && issue.readiness?.status !== "claimed") {
      return yield* startError(
        "not-ready",
        "This work is no longer ready to continue.",
        issue.readiness?.reasons.map((reason) => reason.message).join(" ") ??
          "Refresh Workflow to load current readiness evidence.",
      );
    }
    const assignees = yield* readAssignees({
      cwd: project.workspaceRoot,
      repository: input.repository,
      issueNumber: input.issueNumber,
    });
    if (
      input.expectedClaimLogin === null ||
      assignees.length !== 1 ||
      assignees[0] !== input.expectedClaimLogin
    ) {
      return yield* startError(
        "claim-failed",
        "The GitHub assignment no longer matches this accepted attempt. Refresh and use explicit takeover after confirming the other environment.",
      );
    }
    const scopedProvider = yield* providerRegistry
      .probeWorkspaceSnapshot({
        instanceId: input.modelSelection.instanceId,
        cwd: project.workspaceRoot,
      })
      .pipe(
        Effect.mapError((error) =>
          startError(
            "provider-unavailable",
            "Codex workspace discovery failed. Check the provider and retry.",
            String(error),
          ),
        ),
      );
    if (
      !scopedProvider ||
      scopedProvider.driver !== ProviderDriverKind.make("codex") ||
      !scopedProvider.enabled ||
      !scopedProvider.installed ||
      scopedProvider.auth.status !== "authenticated" ||
      scopedProvider.status === "error" ||
      scopedProvider.status === "disabled"
    ) {
      return yield* startError(
        "provider-unavailable",
        "Choose an enabled, authenticated Codex provider in this environment.",
      );
    }
    const model = scopedProvider.models.find(
      (candidate) => candidate.slug === input.modelSelection.model,
    );
    if (!model) {
      return yield* startError(
        "model-unavailable",
        `Model '${input.modelSelection.model}' is not available from this Codex provider.`,
      );
    }
    const effort = getModelSelectionStringOptionValue(input.modelSelection, "reasoningEffort");
    const effortDescriptor = model.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort",
    );
    if (
      !effort ||
      effortDescriptor?.type !== "select" ||
      !effortDescriptor.options.some((option) => option.id === effort)
    ) {
      return yield* startError(
        "effort-required",
        "Choose a supported Codex reasoning effort before resuming work.",
      );
    }
    const skillNames = requiredSkillNames(issue.labels);
    const skills = skillNames.map((name) => {
      const matches = scopedProvider.skills.filter((skill) => skill.name === name && skill.enabled);
      return matches.length === 1 ? matches[0] : undefined;
    });
    const missingSkill = skillNames.find((_, index) => !skills[index]);
    if (missingSkill) {
      return yield* startError(
        "skill-unavailable",
        `Enable the '${missingSkill}' skill at one unambiguous path in the target workspace.`,
      );
    }
    return { issue, skills: skills.map((skill) => skill!) };
  });

  const resumeUnlocked = Effect.fn("WorkflowStartService.resume")(function* (
    input: WorkflowRecoverInput & {
      readonly attemptId: string;
      readonly modelSelection: NonNullable<WorkflowRecoverInput["modelSelection"]>;
    },
    dispatch: Dispatch,
  ) {
    const current = yield* loadAttempt(input);
    if (!current || current.attemptId !== input.attemptId) {
      return yield* startError(
        "dispatch-failed",
        "The linked workflow attempt changed. Refresh before resuming.",
      );
    }
    const evidence = yield* readEvidence(current);
    if (evidence.evidence !== "accepted") {
      return yield* startError(
        "dispatch-failed",
        "Resume requires durable evidence that the initial turn was accepted.",
      );
    }
    const shellOption = yield* projection
      .getThreadShellById(ThreadId.make(current.threadId))
      .pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "The linked thread state could not be read.",
            String(error),
          ),
        ),
      );
    if (Option.isNone(shellOption)) {
      return yield* startError(
        "dispatch-failed",
        "The linked thread is unavailable. Open its history before choosing another action.",
      );
    }
    const shell = shellOption.value;
    const latestTurn = shell.latestTurn;
    if (
      !latestTurn ||
      (latestTurn.state !== "interrupted" && latestTurn.state !== "error") ||
      shell.session?.activeTurnId != null ||
      shell.session?.status === "running" ||
      shell.session?.status === "starting"
    ) {
      return yield* startError(
        "dispatch-failed",
        "Resume is only available for confirmed interrupted work with no active turn.",
      );
    }
    const previous = yield* sql<{
      readonly resumeId: string;
      readonly commandId: string;
      readonly status: string;
      readonly sequence: number | null;
    }>`
      SELECT resume_id AS "resumeId", command_id AS "commandId", status, sequence
      FROM workflow_resume_attempts
      WHERE workflow_attempt_id = ${current.attemptId} AND source_turn_id = ${latestTurn.turnId}
      LIMIT 1
    `.pipe(
      Effect.mapError((error) =>
        startError("persistence-failed", "Resume history could not be read.", String(error)),
      ),
    );
    if (previous[0]) {
      const receipt = yield* commandReceipts
        .getByCommandId({ commandId: CommandId.make(previous[0].commandId) })
        .pipe(
          Effect.mapError((error) =>
            startError(
              "persistence-failed",
              "Resume command evidence could not be read.",
              String(error),
            ),
          ),
        );
      if (
        (Option.isSome(receipt) && receipt.value.status === "accepted") ||
        (previous[0].status === "submitted" && previous[0].sequence !== null)
      ) {
        return {
          action: "resumed",
          attemptId: current.attemptId,
          environmentId: EnvironmentId.make(current.environmentId),
          projectId: input.projectId,
          repository: input.repository,
          rootNumber: current.rootNumber,
          issueNumber: current.issueNumber,
          threadId: ThreadId.make(current.threadId),
          message: "Opening the existing accepted resume turn.",
        } satisfies WorkflowRecoverResult;
      }
      return yield* startError(
        "dispatch-failed",
        "The resume submission is uncertain and will not be sent again automatically.",
      );
    }
    const prepared = yield* prepareContinuation({
      ...input,
      expectedClaimLogin: current.claimLogin,
    });
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const resumeId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    yield* sql`
      INSERT INTO workflow_resume_attempts (
        resume_id, workflow_attempt_id, source_turn_id, command_id, message_id,
        status, created_at, updated_at
      ) VALUES (
        ${resumeId}, ${current.attemptId}, ${latestTurn.turnId}, ${commandId}, ${messageId},
        'submitting', ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        startError("persistence-failed", "The resume intent could not be saved.", String(error)),
      ),
    );
    const command = {
      type: "thread.turn.start" as const,
      commandId,
      threadId: ThreadId.make(current.threadId),
      message: {
        messageId,
        role: "user" as const,
        text: `Resume the interrupted work for ${input.repository}#${input.issueNumber}. Recheck the current issue scope and continue from the preserved thread history.`,
        attachments: [],
      },
      modelSelection: input.modelSelection,
      skills: prepared.skills.map((skill) => ({ name: skill.name, path: skill.path })),
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
    const dispatched = yield* dispatch(command).pipe(
      Effect.catch((error) =>
        commandReceipts.getByCommandId({ commandId }).pipe(
          Effect.mapError((cause) =>
            startError(
              "persistence-failed",
              "Resume command evidence could not be read.",
              String(cause),
            ),
          ),
          Effect.flatMap((receipt) => {
            const accepted = Option.isSome(receipt) && receipt.value.status === "accepted";
            return sql`
              UPDATE workflow_resume_attempts SET status = ${accepted ? "submitted" : "held"},
                sequence = ${accepted ? receipt.value.resultSequence : null},
                detail = ${accepted ? null : "The resume submission is uncertain and will not be sent again automatically."},
                updated_at = ${createdAt}
              WHERE resume_id = ${resumeId}
            `.pipe(
              Effect.mapError((cause) =>
                startError(
                  "persistence-failed",
                  "The resume outcome could not be saved.",
                  String(cause),
                ),
              ),
              Effect.andThen(
                accepted
                  ? Effect.succeed({ sequence: receipt.value.resultSequence })
                  : Effect.fail(
                      startError(
                        "dispatch-failed",
                        "The resume submission is uncertain and will not be sent again automatically.",
                        String(error),
                      ),
                    ),
              ),
            );
          }),
        ),
      ),
    );
    yield* sql`
      UPDATE workflow_resume_attempts SET status = 'submitted', sequence = ${dispatched.sequence},
        detail = NULL, updated_at = ${createdAt}
      WHERE resume_id = ${resumeId}
    `.pipe(
      Effect.mapError((error) =>
        startError("persistence-failed", "The accepted resume could not be saved.", String(error)),
      ),
    );
    return {
      action: "resumed",
      attemptId: current.attemptId,
      environmentId: EnvironmentId.make(current.environmentId),
      projectId: input.projectId,
      repository: input.repository,
      rootNumber: current.rootNumber,
      issueNumber: current.issueNumber,
      threadId: ThreadId.make(current.threadId),
      message: "Interrupted work resumed in the preserved thread.",
    } satisfies WorkflowRecoverResult;
  });

  const preparePlanningPhase = Effect.fn("WorkflowStartService.preparePlanningPhase")(function* (
    input: WorkflowStartInput,
    skillName: "to-spec" | "to-tickets",
  ) {
    const project = yield* selectedProject(input.projectId);
    const workspace = yield* fileSystem
      .stat(project.workspaceRoot)
      .pipe(
        Effect.mapError((error) =>
          startError(
            "workspace-unavailable",
            "The target workspace is not accessible in this environment.",
            String(error),
          ),
        ),
      );
    if (workspace.type !== "Directory") {
      return yield* startError(
        "workspace-unavailable",
        "The target workspace is not a directory in this environment.",
      );
    }
    if (!input.planningThreadId) {
      return yield* startError(
        "workspace-unavailable",
        "Open this action from the planning thread that should retain the specification history.",
      );
    }
    const shellOption = yield* projection
      .getThreadShellById(input.planningThreadId)
      .pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "The target planning thread could not be read.",
            String(error),
          ),
        ),
      );
    if (Option.isNone(shellOption) || shellOption.value.projectId !== input.projectId) {
      return yield* startError(
        "workspace-unavailable",
        "The selected planning thread is unavailable in this project and environment.",
      );
    }
    const shell = shellOption.value;
    if (
      shell.latestTurn?.state === "running" ||
      shell.session?.activeTurnId != null ||
      shell.session?.status === "running" ||
      shell.session?.status === "starting"
    ) {
      return yield* startError(
        "not-ready",
        "The planning thread already has an active turn.",
        "Wait for the current turn to settle before starting another planning phase.",
      );
    }
    const scopedProvider = yield* providerRegistry
      .probeWorkspaceSnapshot({
        instanceId: input.modelSelection.instanceId,
        cwd: project.workspaceRoot,
      })
      .pipe(
        Effect.mapError((error) =>
          startError(
            "provider-unavailable",
            "Codex workspace discovery failed. Check the provider and retry.",
            String(error),
          ),
        ),
      );
    if (
      !scopedProvider ||
      scopedProvider.driver !== ProviderDriverKind.make("codex") ||
      !scopedProvider.enabled ||
      !scopedProvider.installed ||
      scopedProvider.auth.status !== "authenticated" ||
      scopedProvider.status === "error" ||
      scopedProvider.status === "disabled"
    ) {
      return yield* startError(
        "provider-unavailable",
        "Choose an enabled, authenticated Codex provider in this environment.",
      );
    }
    const model = scopedProvider.models.find(
      (candidate) => candidate.slug === input.modelSelection.model,
    );
    if (!model) {
      return yield* startError(
        "model-unavailable",
        `Model '${input.modelSelection.model}' is not available from this Codex provider.`,
      );
    }
    const effort = getModelSelectionStringOptionValue(input.modelSelection, "reasoningEffort");
    const effortDescriptor = model.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort",
    );
    if (
      !effort ||
      effortDescriptor?.type !== "select" ||
      !effortDescriptor.options.some((option) => option.id === effort)
    ) {
      return yield* startError(
        "effort-required",
        "Choose a supported Codex reasoning effort before starting this planning phase.",
      );
    }
    const matches = scopedProvider.skills.filter(
      (skill) => skill.name === skillName && skill.enabled,
    );
    if (matches.length !== 1) {
      return yield* startError(
        "skill-unavailable",
        `Enable the '${skillName}' skill at one unambiguous path in the target workspace.`,
      );
    }
    return { project, shell, skill: matches[0]! };
  });

  const capabilityPlanningThread = Effect.fn("WorkflowStartService.capabilityPlanningThread")(
    function* (
      input: WorkflowStartInput,
      capability: WorkflowIssueDetail,
      environmentId: EnvironmentId,
    ) {
      const source = sourceMapReferences(capability.body);
      if (source.kind === "missing" || source.kind === "ambiguous") {
        return yield* startError(
          "not-ready",
          "The capability source is missing or ambiguous.",
          "Record one Source map issue or an explicit standalone origin before slicing tickets.",
        );
      }
      if (source.kind === "standalone") return input.planningThreadId;

      const reference = source.references[0]!;
      const associations = yield* sql<{
        readonly projectId: string;
        readonly threadId: string;
      }>`
        SELECT project_id AS "projectId", thread_id AS "threadId"
        FROM workflow_start_attempts
        WHERE environment_id = ${environmentId}
          AND repository COLLATE NOCASE = ${reference.repository}
          AND issue_number = ${reference.number}
          AND phase = 'specification' AND is_current = 1
      `.pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "The source map planning association could not be read.",
            String(error),
          ),
        ),
      );
      const local = associations.find((association) => association.projectId === input.projectId);
      if (local) return ThreadId.make(local.threadId);
      if (associations.length > 0) {
        return yield* startError(
          "workspace-unavailable",
          "The source map planning thread belongs to another project.",
          "Open the capability from the project that owns its specification planning history.",
        );
      }
      return input.planningThreadId;
    },
  );

  const currentSpecificationApproval = Effect.fn(
    "WorkflowStartService.currentSpecificationApproval",
  )(function* (issue: WorkflowIssueDetail) {
    for (const record of currentSpecificationApprovals(issue).toReversed()) {
      if (record.sourceAccess === "verified") return record;
      const source = durableT3SourceReference(record.source);
      if (!source) continue;
      const sourceThreadId = ThreadId.make(source.threadId);
      const thread = yield* projection
        .getThreadDetailById(sourceThreadId, { activityKinds: [] })
        .pipe(
          Effect.mapError((error) =>
            startError(
              "persistence-failed",
              "The specification approval source could not be read.",
              String(error),
            ),
          ),
        );
      if (
        Option.isSome(thread) &&
        thread.value.messages.some(
          (message) => message.id === source.messageId && message.role === "user",
        )
      ) {
        return record;
      }
    }
    return undefined;
  });

  const resolvedMapContext = Effect.fn("WorkflowStartService.resolvedMapContext")(function* (
    input: Pick<WorkflowStartInput, "projectId" | "repository">,
    map: WorkflowIssueDetail,
  ) {
    const visited = new Set<string>();
    const resolvedDecisions: WorkflowIssueSummary[] = [];
    const excludedHistory: WorkflowIssueSummary[] = [];
    const pending: Array<{
      readonly parentNumber: number;
      readonly map: WorkflowIssueDetail | null;
    }> = [{ parentNumber: map.number, map }];
    while (pending.length > 0) {
      const next = pending.shift()!;
      const fog = next.map
        ? markdownSection(next.map.body, "(?:Remaining fog|Remaining unknowns)")
        : undefined;
      if (next.map && !explicitlyClearedFog(next.map.body)) {
        return yield* startError(
          "not-ready",
          "This map is not ready to become a capability.",
          fog
            ? `Remaining unknowns for #${next.map.number}: ${fog}`
            : `Map #${next.map.number} does not explicitly clear its remaining unknowns.`,
        );
      }
      const result = yield* workflow.children({
        projectId: input.projectId,
        repository: input.repository,
        parentNumber: next.parentNumber,
      });
      for (const child of result.children) {
        if (visited.has(child.id)) continue;
        visited.add(child.id);
        if (child.readiness?.status === "out-of-scope") {
          excludedHistory.push(child);
          continue;
        }
        if (child.kind === "map") {
          if (child.readiness?.status !== "ready" && child.readiness?.status !== "resolved") {
            return yield* startError(
              "not-ready",
              "This map is not ready to become a capability.",
              `Nested map #${child.number} ${child.title} is ${child.readiness?.status ?? "unverified"}.`,
            );
          }
          const nestedMap = yield* workflow.issueDetail({
            projectId: input.projectId,
            repository: child.repository,
            number: child.number,
          });
          pending.push({ parentNumber: child.number, map: nestedMap });
          continue;
        }
        if (child.readiness?.status !== "resolved") {
          return yield* startError(
            "not-ready",
            "This map is not ready to become a capability.",
            `Descendant #${child.number} ${child.title} is ${child.readiness?.status ?? "unverified"}.`,
          );
        }
        resolvedDecisions.push(child);
        if (child.childCount > 0) {
          pending.push({ parentNumber: child.number, map: null });
        }
      }
    }
    return { resolvedDecisions, excludedHistory };
  });

  const startPlanningUnlocked = Effect.fn("WorkflowStartService.startPlanning")(function* (
    input: WorkflowStartInput,
    dispatch: Dispatch,
  ) {
    const phase = workflowPhase(input);
    const existing = yield* loadAttempt(input);
    if (existing) return resultFromRow(existing, "existing");
    const issue = yield* workflow.issueDetail({
      projectId: input.projectId,
      repository: input.repository,
      number: input.issueNumber,
    });
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        startError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    let instructions: string;
    let skillName: "to-spec" | "to-tickets";
    let planningThreadId = input.planningThreadId;
    if (phase === "specification") {
      if (issue.kind !== "map") {
        return yield* startError(
          "unsupported-issue",
          "Create capability is available for a Wayfinder map.",
        );
      }
      const context = yield* resolvedMapContext(input, issue);
      instructions = workflowSpecificationInstructions({ map: issue, ...context });
      skillName = "to-spec";
    } else {
      if (issue.kind !== "capability") {
        return yield* startError(
          "unsupported-issue",
          "Slice tickets is available for a capability specification.",
        );
      }
      planningThreadId = yield* capabilityPlanningThread(input, issue, environmentId);
      const approval = yield* currentSpecificationApproval(issue);
      if (issue.readiness?.status !== "ready" || !approval) {
        return yield* startError(
          "not-ready",
          "This capability does not have current verified specification approval.",
          issue.readiness?.status !== "ready"
            ? issue.readiness?.reasons.map((reason) => reason.message).join(" ")
            : "Record the owner's explicit available approval source and exact current specification content.",
        );
      }
      instructions = workflowTicketBreakdownInstructions({
        capability: issue,
        approvalUrl: approval.url,
        approvedBy: approval.approvedBy!,
      });
      skillName = "to-tickets";
    }
    const planningInput = planningThreadId ? { ...input, planningThreadId } : input;
    const prepared = yield* preparePlanningPhase(planningInput, skillName);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const attemptId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const threadId = planningInput.planningThreadId!;
    yield* sql`
      INSERT INTO workflow_start_attempts (
        attempt_id, environment_id, project_id, repository, root_number,
        issue_number, phase, thread_id, command_id, message_id, status,
        initial_turn_disposition, created_at, updated_at
      ) VALUES (
        ${attemptId}, ${environmentId}, ${input.projectId}, ${input.repository}, ${input.rootNumber},
        ${input.issueNumber}, ${phase}, ${threadId}, ${commandId}, ${messageId}, 'submitting',
        'unknown', ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The planning phase intent could not be saved.",
          String(error),
        ),
      ),
    );
    const command = {
      type: "thread.turn.start" as const,
      commandId,
      threadId,
      message: { messageId, role: "user" as const, text: instructions, attachments: [] },
      modelSelection: input.modelSelection,
      skills: [{ name: prepared.skill.name, path: prepared.skill.path }],
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
    const dispatched = yield* dispatch(command).pipe(
      Effect.catch((error) =>
        commandReceipts.getByCommandId({ commandId }).pipe(
          Effect.mapError((cause) =>
            startError(
              "persistence-failed",
              "Planning command evidence could not be read.",
              String(cause),
            ),
          ),
          Effect.flatMap((receipt) => {
            const accepted = Option.isSome(receipt) && receipt.value.status === "accepted";
            const detail = accepted
              ? null
              : "The planning submission is uncertain and will not be sent again automatically.";
            return sql`
              UPDATE workflow_start_attempts SET status = ${accepted ? "submitted" : "held"},
                sequence = ${accepted ? receipt.value.resultSequence : null}, detail = ${detail},
                initial_turn_disposition = ${accepted ? "accepted" : "unknown"}, updated_at = ${createdAt}
              WHERE attempt_id = ${attemptId}
            `.pipe(
              Effect.mapError((cause) =>
                startError(
                  "persistence-failed",
                  "The planning phase outcome could not be saved.",
                  String(cause),
                ),
              ),
              Effect.andThen(
                accepted
                  ? Effect.succeed({ sequence: receipt.value.resultSequence })
                  : Effect.fail(startError("dispatch-failed", detail!, String(error))),
              ),
            );
          }),
        ),
      ),
    );
    yield* sql`
      UPDATE workflow_start_attempts SET status = 'submitted', sequence = ${dispatched.sequence},
        detail = NULL, initial_turn_disposition = 'accepted', updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The accepted planning phase could not be saved.",
          String(error),
        ),
      ),
    );
    return resultFromRow(
      {
        attemptId,
        environmentId,
        projectId: input.projectId,
        repository: input.repository,
        rootNumber: input.rootNumber,
        issueNumber: input.issueNumber,
        phase,
        threadId,
        status: "submitted",
        createdAt,
        detail: null,
      },
      "started",
    );
  });

  const startUnlocked = Effect.fn("WorkflowStartService.start")(function* (
    input: WorkflowStartInput,
    dispatch: Dispatch,
    options?: {
      readonly mode: "fresh" | "takeover";
      readonly expectedObservation: string;
    },
  ) {
    const existing = yield* loadAttempt(input);
    if (existing && !options) return resultFromRow(existing, "existing");
    let existingEvidence:
      | { readonly evidence: "accepted" | "rejected" | "unknown"; readonly row: AttemptRow }
      | undefined;
    let acceptedClaimLogin: string | null = null;
    let confirmedTakeoverAssignees: ReadonlyArray<string> | undefined;
    let allowsClaimedReadiness = false;
    if (options) {
      const observed = yield* recoveryUnlocked(input);
      if (observed.observation !== options.expectedObservation) {
        return yield* startError(
          "claim-failed",
          "The workflow claim or attempt changed. Refresh before confirming this action again.",
        );
      }
      if (!observed.actions.includes(options.mode === "fresh" ? "start-fresh" : "takeover")) {
        return yield* startError(
          "claim-failed",
          `The ${options.mode} action is no longer available after refreshing workflow state.`,
        );
      }
      if (existing) existingEvidence = yield* readEvidence(existing);
      acceptedClaimLogin =
        observed.attempts.find(
          (attempt) => attempt.evidence === "accepted" && attempt.claimLogin !== null,
        )?.claimLogin ?? null;
      if (options.mode === "takeover") {
        confirmedTakeoverAssignees = observed.assignees;
        allowsClaimedReadiness = true;
      } else {
        const freshClaimLogin =
          existingEvidence?.evidence === "accepted" ||
          (existingEvidence?.evidence === "rejected" && existing?.claimOwned === 1)
            ? (existing?.claimLogin ?? null)
            : acceptedClaimLogin;
        allowsClaimedReadiness =
          freshClaimLogin !== null &&
          observed.assignees.length === 1 &&
          observed.assignees[0] === freshClaimLogin;
      }
    }

    const project = yield* selectedProject(input.projectId);
    const workspace = yield* fileSystem
      .stat(project.workspaceRoot)
      .pipe(
        Effect.mapError((error) =>
          startError(
            "workspace-unavailable",
            "The target workspace is not accessible in this environment.",
            String(error),
          ),
        ),
      );
    if (workspace.type !== "Directory") {
      return yield* startError(
        "workspace-unavailable",
        "The target workspace is not a directory in this environment.",
      );
    }
    const [root, issue] = yield* Effect.all([
      workflow.issueDetail({
        projectId: input.projectId,
        repository: input.repository,
        number: input.rootNumber,
      }),
      workflow.issueDetail({
        projectId: input.projectId,
        repository: input.repository,
        number: input.issueNumber,
      }),
    ]);
    if (issue.kind !== "decision" && !issue.labels.includes("wayfinder:task")) {
      return yield* startError(
        "unsupported-issue",
        "Start is available for workflow decisions and prerequisite tasks.",
      );
    }
    const processLabels = PROCESS_LABELS.filter((label) => issue.labels.includes(label));
    const kindLabels = WORKFLOW_KIND_LABELS.filter((label) => issue.labels.includes(label));
    if (processLabels.length !== 1 || kindLabels.length !== 1) {
      return yield* startError(
        "unsupported-issue",
        "The decision must have exactly one Wayfinder process label before it can start.",
      );
    }
    const located = yield* workflow.locate({
      projectId: input.projectId,
      repository: input.repository,
      id: issue.id,
      number: issue.number,
    });
    if (
      !located.ancestryComplete ||
      !located.ancestry.some((ancestor) => ancestor.id === root.id)
    ) {
      return yield* startError(
        "unsupported-issue",
        "The selected decision does not belong to this workflow root.",
      );
    }
    const selectedMap = located.ancestry.toReversed().find((ancestor) => ancestor.kind === "map");
    if (!selectedMap) {
      return yield* startError(
        "unsupported-issue",
        "The selected decision is not contained by a Wayfinder map.",
      );
    }
    if (
      issue.readiness?.status !== "ready" &&
      !(allowsClaimedReadiness && issue.readiness?.status === "claimed")
    ) {
      return yield* startError(
        "not-ready",
        "This decision is not ready to start.",
        issue.readiness?.reasons.map((reason) => reason.message).join(" ") ??
          "Refresh Workflow to load current readiness evidence.",
      );
    }

    const scopedProvider = yield* providerRegistry
      .probeWorkspaceSnapshot({
        instanceId: input.modelSelection.instanceId,
        cwd: project.workspaceRoot,
      })
      .pipe(
        Effect.mapError((error) =>
          startError(
            "provider-unavailable",
            "Codex workspace discovery failed. Check the provider and retry.",
            String(error),
          ),
        ),
      );
    if (
      !scopedProvider ||
      scopedProvider.driver !== ProviderDriverKind.make("codex") ||
      !scopedProvider.enabled ||
      !scopedProvider.installed ||
      scopedProvider.auth.status !== "authenticated" ||
      scopedProvider.status === "error" ||
      scopedProvider.status === "disabled"
    ) {
      return yield* startError(
        "provider-unavailable",
        "Choose an enabled, authenticated Codex provider in this environment.",
      );
    }
    if (!scopedProvider.models.some((model) => model.slug === input.modelSelection.model)) {
      return yield* startError(
        "model-unavailable",
        `Model '${input.modelSelection.model}' is not available from this Codex provider.`,
      );
    }
    const effort = getModelSelectionStringOptionValue(input.modelSelection, "reasoningEffort");
    if (!effort) {
      return yield* startError(
        "effort-required",
        "Choose a Codex reasoning effort before starting decision work.",
      );
    }
    const model = scopedProvider.models.find(
      (candidate) => candidate.slug === input.modelSelection.model,
    )!;
    const effortDescriptor = model.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort",
    );
    if (
      effortDescriptor?.type !== "select" ||
      !effortDescriptor.options.some((option) => option.id === effort)
    ) {
      return yield* startError(
        "effort-required",
        `Reasoning effort '${effort}' is not supported by model '${model.slug}'.`,
      );
    }
    const skills = requiredSkillNames(issue.labels).map((name) => {
      const matches = scopedProvider.skills.filter((skill) => skill.name === name && skill.enabled);
      return matches.length === 1 ? matches[0] : undefined;
    });
    const missingSkill = requiredSkillNames(issue.labels).find((_, index) => !skills[index]);
    if (missingSkill) {
      return yield* startError(
        "skill-unavailable",
        `Enable the '${missingSkill}' skill at one unambiguous path in the target workspace.`,
      );
    }

    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        startError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const attemptId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const threadId = ThreadId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          if (existing) {
            yield* sql`
            UPDATE workflow_start_attempts SET is_current = 0, updated_at = ${createdAt}
            WHERE attempt_id = ${existing.attemptId} AND is_current = 1
          `;
          }
          yield* sql`
          INSERT INTO workflow_start_attempts (
            attempt_id, environment_id, project_id, repository, root_number,
            issue_number, phase, thread_id, command_id, message_id, status,
            initial_turn_disposition, created_at, updated_at
          ) VALUES (
            ${attemptId}, ${environmentId}, ${input.projectId}, ${input.repository}, ${input.rootNumber},
            ${input.issueNumber}, 'decision', ${threadId}, ${commandId}, ${messageId}, 'claiming',
            'not-attempted', ${createdAt}, ${createdAt}
          )
        `;
        }),
      )
      .pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "The workflow attempt could not be saved while preserving its predecessor.",
            String(error),
          ),
        ),
      );

    const holdAttempt = Effect.fn("WorkflowStartService.holdAttempt")(function* (detail: string) {
      yield* sql`
        UPDATE workflow_start_attempts SET status = 'held', detail = ${detail},
          updated_at = ${createdAt}
        WHERE attempt_id = ${attemptId}
      `.pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "The held workflow attempt could not be saved.",
            String(error),
          ),
        ),
      );
    });

    const identity = yield* executeClaim(project.workspaceRoot, ["api", "user", "--jq", ".login"]);
    const login = identity.stdout.trim();
    if (!login) {
      return yield* startError("claim-failed", "GitHub did not identify the authenticated user.");
    }
    yield* sql`
      UPDATE workflow_start_attempts SET claim_login = ${login}, updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The GitHub claim identity could not be saved.",
          String(error),
        ),
      ),
    );
    const observedAssignees = yield* readAssignees({
      cwd: project.workspaceRoot,
      repository: input.repository,
      issueNumber: input.issueNumber,
    });
    if (
      confirmedTakeoverAssignees &&
      (confirmedTakeoverAssignees.length !== observedAssignees.length ||
        confirmedTakeoverAssignees.some((assignee, index) => observedAssignees[index] !== assignee))
    ) {
      const detail =
        "The GitHub assignment changed during takeover preflight. Refresh before confirming takeover again.";
      yield* holdAttempt(detail);
      return yield* startError("claim-failed", detail);
    }
    const releasesRejectedClaim =
      options?.mode === "fresh" &&
      existingEvidence?.evidence === "rejected" &&
      existing?.claimOwned === 1 &&
      existing.claimLogin === login &&
      observedAssignees.includes(login);
    if (releasesRejectedClaim) {
      yield* executeClaim(project.workspaceRoot, [
        "issue",
        "edit",
        String(input.issueNumber),
        "--repo",
        input.repository,
        "--remove-assignee",
        login,
      ]);
    }
    const beforeAssignees = releasesRejectedClaim
      ? observedAssignees.filter((assignee) => assignee !== login)
      : observedAssignees;
    const reusesAcceptedClaim =
      options?.mode === "fresh" &&
      ((existingEvidence?.evidence === "accepted" && existing?.claimLogin === login) ||
        acceptedClaimLogin === login) &&
      beforeAssignees.includes(login);
    if (options?.mode === "takeover") {
      for (const assignee of beforeAssignees) {
        yield* executeClaim(project.workspaceRoot, [
          "issue",
          "edit",
          String(input.issueNumber),
          "--repo",
          input.repository,
          "--remove-assignee",
          assignee,
        ]);
      }
    } else if (beforeAssignees.length > 0 && !reusesAcceptedClaim) {
      const detail = `This decision is already assigned to ${beforeAssignees.join(", ")}. Use an explicit handoff or takeover before starting it here.`;
      yield* holdAttempt(detail);
      return yield* startError("claim-failed", detail);
    }
    const shouldAddClaim = !reusesAcceptedClaim;
    yield* (
      shouldAddClaim
        ? executeClaim(project.workspaceRoot, [
            "issue",
            "edit",
            String(input.issueNumber),
            "--repo",
            input.repository,
            "--add-assignee",
            login,
          ]).pipe(Effect.asVoid)
        : Effect.void
    ).pipe(
      Effect.catch((error) =>
        sql`
          UPDATE workflow_start_attempts SET status = 'held',
            detail = 'Claim outcome is uncertain. Reconcile the GitHub assignment before retrying.',
            updated_at = ${createdAt}
          WHERE attempt_id = ${attemptId}
        `.pipe(
          Effect.mapError((cause) =>
            startError(
              "persistence-failed",
              "The uncertain claim could not be recorded.",
              String(cause),
            ),
          ),
          Effect.andThen(Effect.fail(error)),
        ),
      ),
    );
    const afterAssignees = yield* readAssignees({
      cwd: project.workspaceRoot,
      repository: input.repository,
      issueNumber: input.issueNumber,
    });
    const claimOwned = shouldAddClaim;
    const releaseOwnedClaim = () =>
      claimOwned
        ? executeClaim(project.workspaceRoot, [
            "issue",
            "edit",
            String(input.issueNumber),
            "--repo",
            input.repository,
            "--remove-assignee",
            login,
          ]).pipe(
            Effect.as(true),
            Effect.orElseSucceed(() => false),
          )
        : Effect.succeed(false);
    if (!afterAssignees.includes(login)) {
      yield* holdAttempt(
        "GitHub did not confirm the assignment. Reconcile the claim before retrying.",
      );
      return yield* startError(
        "claim-failed",
        "GitHub did not confirm the assignment. Reconcile the claim before retrying.",
      );
    }
    const competingAssignees = afterAssignees.filter((assignee) => assignee !== login);
    if (competingAssignees.length > 0) {
      const claimReleased = yield* releaseOwnedClaim();
      const detail = claimReleased
        ? `A competing assignment to ${competingAssignees.join(", ")} appeared while claiming. This attempt is held and its own claim was released.`
        : `A competing assignment to ${competingAssignees.join(", ")} appeared while claiming. This attempt is held, but its own claim could not be released.`;
      yield* holdAttempt(detail);
      return yield* startError("claim-failed", detail);
    }

    const instructions = workflowDecisionInstructions({
      repository: input.repository,
      mapNumber: selectedMap.number,
      mapTitle: selectedMap.title,
      issueNumber: input.issueNumber,
      issueTitle: issue.title,
      issueUrl: issue.url,
      labels: issue.labels,
    });
    yield* sql`
      UPDATE workflow_start_attempts SET status = 'submitting', claim_login = ${login},
        claim_owned = ${claimOwned ? 1 : 0}, initial_turn_disposition = 'unknown',
        updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The workflow submission intent could not be saved.",
          String(error),
        ),
      ),
      Effect.catch((error) => releaseOwnedClaim().pipe(Effect.andThen(Effect.fail(error)))),
    );

    const command = {
      type: "thread.turn.start" as const,
      commandId,
      threadId,
      message: { messageId, role: "user" as const, text: instructions, attachments: [] },
      modelSelection: input.modelSelection,
      skills: skills.map((skill) => ({ name: skill!.name, path: skill!.path })),
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      bootstrap: {
        createThread: {
          projectId: input.projectId,
          title: issue.title,
          modelSelection: input.modelSelection,
          runtimeMode: "approval-required" as const,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          branch: null,
          worktreePath: null,
          createdAt,
        },
      },
      createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;

    const dispatched = yield* dispatch(command).pipe(
      Effect.catch((error) => {
        const turnWasNotAccepted = error.bootstrapTurnDisposition === "not-accepted";
        const threadWasDeleted = error.bootstrapThreadDisposition === "deleted";
        const releaseClaim = turnWasNotAccepted ? releaseOwnedClaim() : Effect.succeed(false);

        return releaseClaim.pipe(
          Effect.flatMap((claimReleased) => {
            const detail = turnWasNotAccepted
              ? claimOwned && !claimReleased
                ? "No first turn was accepted, but the claim could not be released. Reconcile the assignment before retrying."
                : threadWasDeleted
                  ? "The bootstrap thread was deleted before a turn started. The claim added by this attempt was released."
                  : "No first turn was accepted. The claim added by this attempt was released."
              : "The first submission is uncertain. Reconcile the durable command before retrying.";
            return sql`
              UPDATE workflow_start_attempts SET status = 'held', detail = ${detail},
                initial_turn_disposition = ${turnWasNotAccepted ? "not-accepted" : "unknown"},
                updated_at = ${createdAt}
              WHERE attempt_id = ${attemptId}
            `.pipe(
              Effect.mapError((cause) =>
                startError(
                  "persistence-failed",
                  "The failed submission could not be recorded.",
                  String(cause),
                ),
              ),
              Effect.andThen(
                Effect.fail(
                  startError(
                    "dispatch-failed",
                    turnWasNotAccepted
                      ? detail
                      : "The first submission is uncertain and will not be sent again automatically.",
                    String(error),
                  ),
                ),
              ),
            );
          }),
        );
      }),
    );
    yield* sql`
      UPDATE workflow_start_attempts SET status = 'submitted', sequence = ${dispatched.sequence},
        detail = NULL, initial_turn_disposition = 'accepted', updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The submitted workflow attempt could not be saved.",
          String(error),
        ),
      ),
    );
    return resultFromRow(
      {
        attemptId,
        environmentId,
        projectId: input.projectId,
        repository: input.repository,
        rootNumber: input.rootNumber,
        issueNumber: input.issueNumber,
        phase: "decision",
        threadId,
        status: "submitted",
        createdAt,
        detail: null,
      },
      "started",
    );
  });

  const recoverUnlocked = Effect.fn("WorkflowStartService.recover")(function* (
    input: WorkflowRecoverInput,
    dispatch: Dispatch,
  ) {
    const state = yield* recoveryUnlocked(input);
    const sameResumeAttempt =
      input.action === "resume" &&
      input.attemptId !== undefined &&
      state.currentAttempt?.attemptId === input.attemptId;
    if (!input.observation || (state.observation !== input.observation && !sameResumeAttempt)) {
      return yield* startError(
        "claim-failed",
        "The workflow claim or attempt changed. Refresh before confirming this action again.",
      );
    }
    if (workflowPhase(input) !== "decision" && input.action !== "open") {
      return yield* startError(
        "dispatch-failed",
        "Planning phases can only reopen their linked thread.",
      );
    }
    const current = state.currentAttempt;
    if (input.action === "open") {
      if (!current || (input.attemptId && input.attemptId !== current.attemptId)) {
        return yield* startError(
          "dispatch-failed",
          "The linked workflow attempt changed. Refresh before opening it.",
        );
      }
      return {
        action: "open",
        attemptId: current.attemptId,
        environmentId: current.environmentId,
        projectId: current.projectId,
        repository: current.repository,
        rootNumber: current.rootNumber,
        issueNumber: current.issueNumber,
        threadId: current.threadId,
        message: "Opening the linked workflow thread.",
      } satisfies WorkflowRecoverResult;
    }
    if (!input.modelSelection) {
      return yield* startError(
        "model-unavailable",
        "Choose the target Codex model and reasoning effort for this action.",
      );
    }
    if (input.action === "resume") {
      if (!input.attemptId) {
        return yield* startError("dispatch-failed", "Choose the linked attempt to resume.");
      }
      return yield* resumeUnlocked(
        { ...input, attemptId: input.attemptId, modelSelection: input.modelSelection },
        dispatch,
      );
    }
    const mode = input.action === "start-fresh" ? "fresh" : input.action;
    const started = yield* startUnlocked(
      {
        projectId: input.projectId,
        repository: input.repository,
        rootNumber: input.rootNumber,
        issueNumber: input.issueNumber,
        modelSelection: input.modelSelection,
      },
      dispatch,
      { mode, expectedObservation: input.observation },
    );
    return {
      action: input.action === "start-fresh" ? "started-fresh" : "taken-over",
      attemptId: started.attemptId,
      environmentId: started.environmentId,
      projectId: started.projectId,
      repository: started.repository,
      rootNumber: started.rootNumber,
      issueNumber: started.issueNumber,
      threadId: started.threadId,
      message: started.message,
    } satisfies WorkflowRecoverResult;
  });

  return WorkflowStartService.of({
    start: (input, dispatch) =>
      lock.withPermits(1)(
        workflowPhase(input) === "decision"
          ? startUnlocked(input, dispatch)
          : startPlanningUnlocked(input, dispatch),
      ),
    recovery: (input) => lock.withPermits(1)(recoveryUnlocked(input)),
    recover: (input, dispatch) => lock.withPermits(1)(recoverUnlocked(input, dispatch)),
  });
});

export const layer = Layer.effect(WorkflowStartService, make).pipe(
  Layer.provide(OrchestrationCommandReceiptRepositoryLive),
);
