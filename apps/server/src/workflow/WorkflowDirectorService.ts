import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentId,
  MessageId,
  type OrchestrationCommand,
  type OrchestrationDispatchCommandError,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
  WorkflowDirectorError,
  type WorkflowDirectorAdmission,
  type WorkflowDirectorAdmissionInput,
  type WorkflowDirectorAdmissionResult,
  type WorkflowDirectorResumeInput,
  type WorkflowDirectorStartInput,
  type WorkflowDirectorStartResult,
  type WorkflowDirectorStatus,
  type WorkflowDirectorStatusInput,
  type WorkflowWorkerAssociateInput,
  type WorkflowWorkerHandoffInput,
  type WorkflowWorkerPrepareInput,
  type WorkflowWorkerPrepareResult,
  type WorkflowWorkerStatus,
  type WorkflowEvidenceRecord,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
  type WorkflowQueryError,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { parseGitHubRepositoryNameWithOwnerFromRemoteUrl } from "@t3tools/shared/git";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowService from "./WorkflowService.ts";

const ADMISSION_LIMIT = 10;
const DIRECTOR_MODEL = "gpt-6-astra";
const DIRECTOR_EFFORT = "high";
const WORKER_MODEL = "gpt-5.6-sol";
const WORKER_EFFORT = "high";
const REQUIRED_SKILLS = ["implement", "code-review"] as const;

type Dispatch = (
  command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;

const DirectorRow = Schema.Struct({
  directorId: Schema.String,
  batchId: Schema.String,
  environmentId: Schema.String,
  projectId: Schema.String,
  repository: Schema.String,
  rootNumber: Schema.Number,
  capabilityNumber: Schema.Number,
  threadId: Schema.String,
  commandId: Schema.String,
  messageId: Schema.String,
  worktreePath: Schema.String,
  worktreeBranch: Schema.String,
  status: Schema.String,
  requestedModel: Schema.String,
  requestedInstanceId: Schema.String,
  requestedEffort: Schema.String,
  observedModel: Schema.NullOr(Schema.String),
  observedEffort: Schema.NullOr(Schema.String),
  observedMatch: Schema.String,
  sequence: Schema.NullOr(Schema.Number),
  initialTurnDisposition: Schema.String,
  detail: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type DirectorRow = typeof DirectorRow.Type;
const decodeDirectorRow = Schema.decodeUnknownEffect(DirectorRow);

const AdmissionRow = Schema.Struct({
  admissionId: Schema.String,
  directorId: Schema.String,
  batchId: Schema.String,
  repository: Schema.String,
  ticketNumber: Schema.Number,
  slotTicketNumber: Schema.Number,
  purpose: Schema.String,
  ownership: Schema.String,
  claimLogin: Schema.NullOr(Schema.String),
  claimStatus: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type AdmissionRow = typeof AdmissionRow.Type;
const decodeAdmissionRow = Schema.decodeUnknownEffect(AdmissionRow);

const WorkerRow = Schema.Struct({
  dispatchId: Schema.NullOr(Schema.String),
  associationToken: Schema.NullOr(Schema.String),
  admissionId: Schema.NullOr(Schema.String),
  ticketNumber: Schema.NullOr(Schema.Number),
  providerThreadId: Schema.NullOr(Schema.String),
  parentProviderThreadId: Schema.NullOr(Schema.String),
  ownership: Schema.NullOr(Schema.String),
  writePathsJson: Schema.NullOr(Schema.String),
  requestedModel: Schema.NullOr(Schema.String),
  requestedEffort: Schema.NullOr(Schema.String),
  requestedSkillPath: Schema.NullOr(Schema.String),
  dispatchStatus: Schema.NullOr(Schema.String),
  providerStatus: Schema.String,
  observedModel: Schema.NullOr(Schema.String),
  observedEffort: Schema.NullOr(Schema.String),
  nativeLifecycle: Schema.NullOr(Schema.String),
  handoffSummary: Schema.NullOr(Schema.String),
  handoffCommitsJson: Schema.NullOr(Schema.String),
  handoffChecksJson: Schema.NullOr(Schema.String),
  title: Schema.NullOr(Schema.String),
  role: Schema.NullOr(Schema.String),
  updatedAt: Schema.String,
});
type WorkerRow = typeof WorkerRow.Type;
const decodeWorkerRow = Schema.decodeUnknownEffect(WorkerRow);
const StringArrayJson = Schema.fromJsonString(Schema.Array(Schema.String));
const decodeStringArrayJson = Schema.decodeUnknownEffect(StringArrayJson);
const encodeStringArrayJson = Schema.encodeUnknownSync(StringArrayJson);

function pathsOverlap(left: string, right: string) {
  return left === right || left.startsWith(`${right}/`) || right.startsWith(`${left}/`);
}

function writeReservationFor(
  worker: WorkerRow,
  workers: ReadonlyArray<WorkerRow>,
  writePathsByDispatch: ReadonlyMap<string, ReadonlyArray<string>>,
): "held" | "released" | null {
  // Native closure is enough only when every observation can be placed in a
  // closed descendant tree or a separately owned dispatch.
  if (!worker.dispatchId) return null;
  if (!worker.providerThreadId || worker.nativeLifecycle !== "closed") return "held";

  const observations = new Map(
    workers.flatMap((candidate) =>
      candidate.providerThreadId ? [[candidate.providerThreadId, candidate] as const] : [],
    ),
  );
  const workerPaths = writePathsByDispatch.get(worker.dispatchId) ?? [];
  const isKnownSeparate = (candidate: WorkerRow | null) => {
    if (!candidate?.dispatchId) return false;
    if (candidate.admissionId === worker.admissionId) return true;
    const candidatePaths = writePathsByDispatch.get(candidate.dispatchId) ?? [];
    if (workerPaths.length === 0 || candidatePaths.length === 0) return false;
    return !workerPaths.some((workerPath) =>
      candidatePaths.some((candidatePath) => pathsOverlap(workerPath, candidatePath)),
    );
  };

  for (const candidate of workers) {
    if (
      !candidate.providerThreadId ||
      candidate.providerThreadId === worker.providerThreadId ||
      candidate.nativeLifecycle === "closed"
    ) {
      continue;
    }

    let parentId = candidate.parentProviderThreadId;
    let associatedAncestor = candidate.dispatchId ? candidate : null;
    const visited = new Set<string>();
    while (parentId) {
      if (parentId === worker.providerThreadId) return "held";
      if (visited.has(parentId)) return "held";
      visited.add(parentId);
      const parent = observations.get(parentId);
      if (!parent) {
        if (!isKnownSeparate(associatedAncestor)) return "held";
        break;
      }
      if (parent.dispatchId) associatedAncestor = parent;
      parentId = parent.parentProviderThreadId;
    }
    if (!parentId && !isKnownSeparate(associatedAncestor)) return "held";
  }

  return "released";
}

function directorError(
  failure: WorkflowDirectorError["failure"],
  message: string,
  detail?: string,
) {
  return new WorkflowDirectorError({ failure, message, ...(detail ? { detail } : {}) });
}

function sameContent(left: string, right: string): boolean {
  return left.trim() === right.trim();
}

function durableT3SourceReference(source: string | undefined) {
  if (!source) return null;
  const threadIds = [...source.matchAll(/\bT3(?: Code)? thread\s+`([^`]+)`/giu)];
  const messageIds = [...source.matchAll(/\b(?:user\s+)?message\s+`([^`]+)`/giu)];
  if (threadIds.length !== 1 || messageIds.length !== 1) return null;
  return { threadId: threadIds[0]![1]!, messageId: messageIds[0]![1]! };
}

function breakdownUnits(content: string) {
  const details = [
    ...content.matchAll(
      /<details>\s*<summary>\s*(?:(T\d+)\s+[—-]\s+)?([^<]+?)\s*<\/summary>([\s\S]*?)<\/details>/giu,
    ),
  ].map((match) => ({
    id: match[1]?.toUpperCase() ?? null,
    title: match[2]!.replace(/\s+/gu, " ").trim(),
    content: match[3]!.trim(),
  }));
  const listTitles =
    details.length > 0
      ? details
      : [...content.matchAll(/^\s*\d+\.\s+\*\*(.+?)\*\*\s*$/gmu)].map((match) => ({
          id: null,
          title: match[1]!.replace(/\s+/gu, " ").trim(),
          content: "",
        }));
  return [...new Map(listTitles.map((unit) => [`${unit.id ?? ""}:${unit.title}`, unit])).values()];
}

function normalizedUnitTitle(title: string): string {
  return title.replace(/\s+/gu, " ").trim().toLocaleLowerCase();
}

function readinessDetail(issue: WorkflowIssueSummary): string {
  return (
    issue.readiness?.reasons.map((reason) => reason.message).join(" ") ||
    "Refresh Workflow to load current readiness evidence."
  );
}

function sourceIssueReferences(body: string) {
  const section = /(?:^|\n)#{2,6}\s+Source map\s*\n([\s\S]*?)(?=\n#{1,6}\s|$)/iu.exec(body)?.[1];
  if (!section || /^\s*none(?:\s*\(standalone\))?\.?\s*$/iu.test(section)) return [];
  return [
    ...new Map(
      [...section.matchAll(/https:\/\/github\.com\/([^/\s]+\/[^/\s)]+)\/issues\/(\d+)/giu)].map(
        (match) => [
          `${match[1]!.toLocaleLowerCase()}#${Number(match[2])}`,
          { repository: match[1]!, number: Number(match[2]) },
        ],
      ),
    ).values(),
  ];
}

function admissionFromRow(row: AdmissionRow): WorkflowDirectorAdmission {
  return {
    admissionId: row.admissionId,
    directorId: row.directorId,
    batchId: row.batchId,
    repository: row.repository as WorkflowDirectorAdmission["repository"],
    ticketNumber: row.ticketNumber,
    slotTicketNumber: row.slotTicketNumber,
    purpose: row.purpose as WorkflowDirectorAdmission["purpose"],
    ownership: row.ownership,
    claimLogin: row.claimLogin,
    claimStatus: row.claimStatus as WorkflowDirectorAdmission["claimStatus"],
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function ownedClaimLogins(rows: ReadonlyArray<AdmissionRow>): ReadonlyMap<number, string> {
  return new Map(
    rows.flatMap((row) =>
      row.claimLogin && row.claimStatus !== "conflict"
        ? ([[row.ticketNumber, row.claimLogin]] as const)
        : [],
    ),
  );
}

export function workflowDirectorInstructions(input: {
  readonly capability: WorkflowIssueDetail;
  readonly tickets: ReadonlyArray<WorkflowIssueSummary>;
  readonly approvalRecords: ReadonlyArray<WorkflowEvidenceRecord>;
  readonly sourceContext: ReadonlyArray<WorkflowIssueDetail>;
  readonly implementSkillPath: string;
  readonly reviewSkillPath: string;
}) {
  return [
    "Direct implementation of this approved capability from its isolated capability worktree.",
    "",
    `Capability: ${input.capability.repository}#${input.capability.number} — ${input.capability.title}`,
    `Source: ${input.capability.url}`,
    "Requested director profile: Astra/high.",
    `Delegate implementation in a fresh Sol/high role with $implement (${input.implementSkillPath}).`,
    `Delegate independent review in a separate fresh Astra/medium role with $code-review (${input.reviewSkillPath}).`,
    "The director coordinates those roles; it does not perform their child work itself.",
    "There is no workflow-director skill dependency.",
    "",
    "Approved capability:",
    input.capability.body,
    "",
    "Approval sources:",
    ...input.approvalRecords.map(
      (record) =>
        `- ${record.approvalKind}: ${record.url} by ${record.approvedBy ?? "unknown"}; source ${record.source ?? "unknown"}`,
    ),
    "",
    "Decisions and source context:",
    ...(input.sourceContext.length > 0
      ? input.sourceContext.map(
          (source) => `- ${source.repository}#${source.number} — ${source.title}\n${source.body}`,
        )
      : ["- Standalone capability; no source map was declared."]),
    "",
    "Live delivery frontier:",
    ...input.tickets.map(
      (ticket) =>
        `- ${ticket.repository}#${ticket.number} — ${ticket.title}: ${ticket.readiness?.status ?? "unknown"} (${ticket.url})`,
    ),
    "",
    "Before every implementation delegation, call the host workflow_prepare_worker MCP tool with the durable ticket number, a plain ownership summary, and exact repository-relative write paths. It persists readiness, admission, slot and ownership before returning native spawn instructions.",
    "After native spawn, call workflow_associate_worker with the returned token and exact child provider thread id. When the child returns, call workflow_report_worker_handoff with its result, commits and checks. Provider idle or a finished turn is not a handoff, ticket resolution or proof that descendants settled.",
    `This batch admits at most ${ADMISSION_LIMIT} distinct delivery slices. Failed or blocked admitted slices keep their slot; retry and review reuse it; nested tasks reuse their parent slice. At the limit, stop new admissions, finish or settle admitted work, and wait for a successor.`,
    "Re-read live tracker state before each admission. Do not infer approval from labels, assignment, closure, silence or unavailable evidence.",
    "GitHub assignment is observational and is not a cross-environment atomic lock.",
    "Arbitrary provider collaboration outside the admission RPC cannot be host-enforced; keep all directed delivery inside the callable boundary.",
  ].join("\n");
}

export class WorkflowDirectorService extends Context.Service<
  WorkflowDirectorService,
  {
    readonly start: (
      input: WorkflowDirectorStartInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowDirectorStartResult, WorkflowQueryError | WorkflowDirectorError>;
    readonly status: (
      input: WorkflowDirectorStatusInput,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly resume: (
      input: WorkflowDirectorResumeInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly admit: (
      input: WorkflowDirectorAdmissionInput,
    ) => Effect.Effect<WorkflowDirectorAdmissionResult, WorkflowQueryError | WorkflowDirectorError>;
    readonly prepareWorker: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowWorkerPrepareInput,
    ) => Effect.Effect<WorkflowWorkerPrepareResult, WorkflowQueryError | WorkflowDirectorError>;
    readonly associateWorker: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowWorkerAssociateInput,
    ) => Effect.Effect<WorkflowWorkerStatus, WorkflowDirectorError>;
    readonly reportWorkerHandoff: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowWorkerHandoffInput,
    ) => Effect.Effect<WorkflowWorkerStatus, WorkflowDirectorError>;
  }
>()("t3/workflow/WorkflowDirectorService") {}

export const make = Effect.gen(function* () {
  const workflow = yield* WorkflowService.WorkflowService;
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const git = yield* GitWorkflowService.GitWorkflowService;
  const github = yield* GitHubCli.GitHubCli;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* OrchestrationCommandReceiptRepository;
  const crypto = yield* Crypto.Crypto;
  const processRunner = yield* ProcessRunner.ProcessRunner;
  const lock = yield* Semaphore.make(1);
  const persistence = <A, E, R>(effect: Effect.Effect<A, E, R>, message: string) =>
    effect.pipe(
      Effect.mapError((error) => directorError("persistence-failed", message, String(error))),
    );

  const selectedProject = Effect.fn("WorkflowDirectorService.selectedProject")(function* (
    projectId: ProjectId,
  ) {
    const project = yield* projection
      .getProjectShellById(projectId)
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The target project could not be read.",
            error.message,
          ),
        ),
      );
    return yield* Option.match(project, {
      onNone: () =>
        Effect.fail(directorError("workspace-unavailable", "The target project is unavailable.")),
      onSome: Effect.succeed,
    });
  });

  const loadDirectorByCapability = Effect.fn("WorkflowDirectorService.loadDirectorByCapability")(
    function* (input: WorkflowDirectorStatusInput, environmentId: EnvironmentId) {
      if (!input.capabilityNumber && !input.ticketNumber) {
        return yield* directorError(
          "director-not-found",
          "A capability or admitted ticket identity is required to find its director.",
        );
      }
      const rows = input.capabilityNumber
        ? yield* sql<Record<string, unknown>>`
      SELECT director_id AS "directorId", batch_id AS "batchId", environment_id AS "environmentId",
        project_id AS "projectId", repository, root_number AS "rootNumber",
        capability_number AS "capabilityNumber", thread_id AS "threadId", command_id AS "commandId",
        message_id AS "messageId", worktree_path AS "worktreePath", worktree_branch AS "worktreeBranch",
        status, requested_model AS "requestedModel", requested_instance_id AS "requestedInstanceId",
        requested_effort AS "requestedEffort",
        observed_model AS "observedModel", observed_effort AS "observedEffort",
        observed_match AS "observedMatch", sequence, initial_turn_disposition AS "initialTurnDisposition",
        detail, created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_directors
      WHERE environment_id = ${environmentId} AND repository COLLATE NOCASE = ${input.repository}
        AND capability_number = ${input.capabilityNumber} AND is_current = 1
      LIMIT 1
    `.pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "The capability director could not be read.",
                String(error),
              ),
            ),
          )
        : yield* sql<Record<string, unknown>>`
      SELECT d.director_id AS "directorId", d.batch_id AS "batchId", d.environment_id AS "environmentId",
        d.project_id AS "projectId", d.repository, d.root_number AS "rootNumber",
        d.capability_number AS "capabilityNumber", d.thread_id AS "threadId", d.command_id AS "commandId",
        d.message_id AS "messageId", d.worktree_path AS "worktreePath", d.worktree_branch AS "worktreeBranch",
        d.status, d.requested_model AS "requestedModel", d.requested_instance_id AS "requestedInstanceId",
        d.requested_effort AS "requestedEffort", d.observed_model AS "observedModel",
        d.observed_effort AS "observedEffort", d.observed_match AS "observedMatch", d.sequence,
        d.initial_turn_disposition AS "initialTurnDisposition", d.detail,
        d.created_at AS "createdAt", d.updated_at AS "updatedAt"
      FROM workflow_directors d
      JOIN workflow_director_admissions a ON a.director_id = d.director_id
      WHERE d.environment_id = ${environmentId} AND d.project_id = ${input.projectId}
        AND d.repository COLLATE NOCASE = ${input.repository} AND d.is_current = 1
        AND a.ticket_number = ${input.ticketNumber}
      LIMIT 1
    `.pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "The admitted ticket director could not be read.",
                String(error),
              ),
            ),
          );
      return rows[0]
        ? yield* decodeDirectorRow(rows[0]).pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "The capability director record is invalid.",
                String(error),
              ),
            ),
          )
        : undefined;
    },
  );

  const loadDirectorById = Effect.fn("WorkflowDirectorService.loadDirectorById")(function* (
    directorId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT director_id AS "directorId", batch_id AS "batchId", environment_id AS "environmentId",
        project_id AS "projectId", repository, root_number AS "rootNumber",
        capability_number AS "capabilityNumber", thread_id AS "threadId", command_id AS "commandId",
        message_id AS "messageId", worktree_path AS "worktreePath", worktree_branch AS "worktreeBranch",
        status, requested_model AS "requestedModel", requested_instance_id AS "requestedInstanceId",
        requested_effort AS "requestedEffort",
        observed_model AS "observedModel", observed_effort AS "observedEffort",
        observed_match AS "observedMatch", sequence, initial_turn_disposition AS "initialTurnDisposition",
        detail, created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_directors WHERE director_id = ${directorId} AND is_current = 1 LIMIT 1
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The capability director could not be read.",
          String(error),
        ),
      ),
    );
    if (!rows[0])
      return yield* directorError(
        "director-not-found",
        "This capability director is no longer current in this environment.",
      );
    return yield* decodeDirectorRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The capability director record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const verifyApprovalSource = Effect.fn("WorkflowDirectorService.verifyApprovalSource")(function* (
    record: WorkflowEvidenceRecord,
  ) {
    if (record.sourceAccess === "verified") return true;
    if (record.sourceAccess === "unavailable") return false;
    const source = durableT3SourceReference(record.source);
    if (!source) return false;
    const detail = yield* projection
      .getThreadDetailById(ThreadId.make(source.threadId), {
        activityKinds: [],
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "An approval source thread could not be read.",
            String(error),
          ),
        ),
      );
    return (
      Option.isSome(detail) &&
      detail.value.messages.some(
        (message) => message.id === source.messageId && message.role === "user",
      )
    );
  });

  const currentApproval = Effect.fn("WorkflowDirectorService.currentApproval")(function* (
    capability: WorkflowIssueDetail,
    kind: "specification" | "ticket-breakdown",
  ) {
    const records =
      capability.evidence?.records.filter(
        (record) =>
          record.kind === "approval" &&
          record.approvalKind === kind &&
          record.state === "current" &&
          record.authority === "verified" &&
          typeof record.approvedBy === "string" &&
          typeof record.approvedContent === "string" &&
          (kind === "ticket-breakdown" ||
            (record.scope === "current" && sameContent(record.approvedContent, capability.body))),
      ) ?? [];
    for (const record of records.toReversed()) {
      if (yield* verifyApprovalSource(record)) return record;
    }
    return undefined;
  });

  const collectDeliveryTickets = Effect.fn("WorkflowDirectorService.collectDeliveryTickets")(
    function* (input: {
      readonly projectId: ProjectId;
      readonly repository: string;
      readonly capabilityNumber: number;
    }) {
      const tickets: WorkflowIssueSummary[] = [];
      const pending = [input.capabilityNumber];
      const visited = new Set<number>();
      while (pending.length > 0) {
        const parentNumber = pending.shift()!;
        if (visited.has(parentNumber)) continue;
        visited.add(parentNumber);
        const result = yield* workflow.children({
          projectId: input.projectId,
          repository: input.repository as WorkflowIssueSummary["repository"],
          parentNumber,
        });
        for (const child of result.children) {
          if (child.kind === "ticket") {
            tickets.push(child);
          } else if (child.kind === "container" || child.kind === "map") {
            pending.push(child.number);
          }
        }
      }
      return tickets;
    },
  );

  const verifyPublishedBreakdown = Effect.fn("WorkflowDirectorService.verifyPublishedBreakdown")(
    function* (approval: WorkflowEvidenceRecord, tickets: ReadonlyArray<WorkflowIssueDetail>) {
      const approved = breakdownUnits(approval.approvedContent ?? "");
      if (approved.length === 0) {
        return yield* directorError(
          "breakdown-incomplete",
          "The approved ticket breakdown does not identify any delivery units.",
        );
      }
      const matched = tickets.map((ticket) => {
        const approvedId = /Approved slice:\s*\*\*(T\d+)\*\*/iu
          .exec(ticket.body)?.[1]
          ?.toUpperCase();
        const unit = approvedId
          ? approved.find((candidate) => candidate.id === approvedId)
          : approved.find(
              (candidate) =>
                normalizedUnitTitle(candidate.title) === normalizedUnitTitle(ticket.title),
            );
        const approvalRecord = ticket.evidence?.records.find(
          (record) =>
            record.kind === "approval" &&
            record.approvalKind === "ticket-breakdown" &&
            record.state === "current" &&
            record.scope === "current" &&
            record.authority === "verified" &&
            sameContent(record.approvedContent ?? "", approval.approvedContent ?? ""),
        );
        return { ticket, unit, approvalRecord };
      });
      const matchedUnits = new Set(matched.flatMap(({ unit }) => (unit ? [unit] : [])));
      const missing = approved.filter((unit) => !matchedUnits.has(unit));
      const extra = matched.filter(({ unit }) => !unit).map(({ ticket }) => ticket.title);
      const changedScope: WorkflowIssueDetail[] = [];
      for (const entry of matched) {
        if (!entry.approvalRecord || !(yield* verifyApprovalSource(entry.approvalRecord))) {
          changedScope.push(entry.ticket);
        }
      }
      if (
        approved.length !== tickets.length ||
        missing.length > 0 ||
        extra.length > 0 ||
        changedScope.length > 0
      ) {
        return yield* directorError(
          "breakdown-incomplete",
          "Published delivery work does not match the complete approved breakdown.",
          [
            missing.length > 0 ? `Missing: ${missing.map((unit) => unit.title).join(", ")}.` : "",
            extra.length > 0 ? `Extra or renamed: ${extra.join(", ")}.` : "",
            changedScope.length > 0
              ? `Changed or unverified scope: ${changedScope.map((ticket) => `#${ticket.number}`).join(", ")}.`
              : "",
          ]
            .filter(Boolean)
            .join(" "),
        );
      }
    },
  );

  const providerPreflight = Effect.fn("WorkflowDirectorService.providerPreflight")(function* (
    cwd: string,
    modelSelection: WorkflowDirectorStartInput["modelSelection"],
  ) {
    if (modelSelection.model !== DIRECTOR_MODEL) {
      return yield* directorError(
        "model-unavailable",
        `Capability directors require '${DIRECTOR_MODEL}'. The requested model was '${modelSelection.model}'.`,
      );
    }
    const effort = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
    if (effort !== DIRECTOR_EFFORT) {
      return yield* directorError(
        "effort-required",
        `Capability directors require '${DIRECTOR_EFFORT}' reasoning effort.`,
      );
    }
    const provider = yield* providerRegistry
      .probeWorkspaceSnapshot({ instanceId: modelSelection.instanceId, cwd })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "provider-unavailable",
            "Codex workspace discovery failed. Check the selected provider and retry.",
            String(error),
          ),
        ),
      );
    if (
      !provider ||
      provider.driver !== ProviderDriverKind.make("codex") ||
      !provider.enabled ||
      !provider.installed ||
      provider.auth.status !== "authenticated" ||
      provider.status === "error" ||
      provider.status === "disabled"
    ) {
      return yield* directorError(
        "provider-unavailable",
        "Choose an enabled, installed and authenticated Codex provider.",
      );
    }
    const model = provider.models.find((candidate) => candidate.slug === DIRECTOR_MODEL);
    const descriptor = model?.capabilities?.optionDescriptors?.find(
      (candidate) => candidate.id === "reasoningEffort",
    );
    if (!model) {
      return yield* directorError(
        "model-unavailable",
        `Model '${DIRECTOR_MODEL}' is unavailable from the selected Codex provider.`,
      );
    }
    if (
      descriptor?.type !== "select" ||
      !descriptor.options.some((option) => option.id === DIRECTOR_EFFORT)
    ) {
      return yield* directorError(
        "effort-required",
        `Model '${DIRECTOR_MODEL}' does not support '${DIRECTOR_EFFORT}' reasoning effort.`,
      );
    }
    const skills = REQUIRED_SKILLS.map((name) => {
      const matches = provider.skills.filter((skill) => skill.name === name && skill.enabled);
      return matches.length === 1 ? matches[0] : undefined;
    });
    const missingSkill = REQUIRED_SKILLS.find((_, index) => !skills[index]);
    if (missingSkill) {
      return yield* directorError(
        "skill-unavailable",
        `Enable the '${missingSkill}' skill at one unambiguous path in the target workspace.`,
      );
    }
    return { provider, skills: skills.map((skill) => skill!) };
  });

  const verifyRepository = Effect.fn("WorkflowDirectorService.verifyRepository")(function* (
    cwd: string,
    repository: string,
  ) {
    const accessibleRepository = yield* github
      .getRepositoryCloneUrls({ cwd, repository })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The selected tracker repository could not be accessed.",
            error.message,
          ),
        ),
      );
    if (accessibleRepository.nameWithOwner.toLocaleLowerCase() !== repository.toLocaleLowerCase()) {
      return yield* directorError(
        "workspace-unavailable",
        "The selected tracker repository identity could not be verified.",
        `Expected ${repository}; observed ${accessibleRepository.nameWithOwner || "unknown"}.`,
      );
    }

    const result = yield* processRunner
      .run({
        command: "git",
        args: ["remote", "-v"],
        cwd,
        timeout: "5 seconds",
        maxOutputBytes: 100_000,
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The target repository remotes could not be read.",
            error.message,
          ),
        ),
      );
    if (result.code !== 0 || result.timedOut) {
      return yield* directorError(
        "workspace-unavailable",
        "The target repository remotes could not be read.",
        result.stderr.trim() || "git remote -v failed.",
      );
    }

    const remoteRepositories = result.stdout.split("\n").flatMap((line) => {
      const match = /^\S+\s+(\S+)\s+\(fetch\)$/.exec(line.trim());
      const remoteRepository = parseGitHubRepositoryNameWithOwnerFromRemoteUrl(match?.[1] ?? null);
      return remoteRepository ? [remoteRepository] : [];
    });
    if (
      !remoteRepositories.some(
        (remoteRepository) =>
          remoteRepository.toLocaleLowerCase() === repository.toLocaleLowerCase(),
      )
    ) {
      return yield* directorError(
        "workspace-unavailable",
        "The target workspace belongs to a different repository.",
        `Expected a ${repository} Git remote; observed ${remoteRepositories.join(", ") || "none"}.`,
      );
    }
  });

  const executeGitHub = Effect.fn("WorkflowDirectorService.executeGitHub")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
  ) {
    return yield* github
      .execute({ cwd, args, maxOutputBytes: 100_000 })
      .pipe(
        Effect.mapError((error) =>
          directorError("claim-failed", "GitHub ownership could not be verified.", error.message),
        ),
      );
  });

  const currentClaimIsOwned = Effect.fn("WorkflowDirectorService.currentClaimIsOwned")(function* (
    cwd: string,
    repository: string,
    ticketNumber: number,
    claimLogin: string,
  ) {
    const identity = yield* executeGitHub(cwd, ["api", "user", "--jq", ".login"]).pipe(
      Effect.result,
    );
    if (
      identity._tag === "Failure" ||
      identity.success.stdout.trim().toLocaleLowerCase() !== claimLogin.toLocaleLowerCase()
    ) {
      return false;
    }
    const assignees = yield* executeGitHub(cwd, [
      "issue",
      "view",
      String(ticketNumber),
      "--repo",
      repository,
      "--json",
      "assignees",
      "--jq",
      ".assignees[].login",
    ]).pipe(Effect.result);
    if (assignees._tag === "Failure") return false;
    const current = assignees.success.stdout
      .split("\n")
      .map((value) => value.trim().toLocaleLowerCase())
      .filter(Boolean);
    return current.length === 1 && current[0] === claimLogin.toLocaleLowerCase();
  });

  const prepareCapability = Effect.fn("WorkflowDirectorService.prepareCapability")(function* (
    input: Pick<
      WorkflowDirectorStartInput,
      "projectId" | "repository" | "capabilityNumber" | "modelSelection"
    >,
    cwd: string,
    options?: { readonly ownedClaims?: ReadonlyMap<number, string> },
  ) {
    const capability = yield* workflow.issueDetail({
      projectId: input.projectId,
      repository: input.repository,
      number: input.capabilityNumber,
    });
    if (capability.kind !== "capability" || capability.state !== "open") {
      return yield* directorError(
        "not-ready",
        "Start implementation is available for an open workflow capability.",
      );
    }
    const specificationApproval = yield* currentApproval(capability, "specification");
    const breakdownApproval = yield* currentApproval(capability, "ticket-breakdown");
    if (!specificationApproval || !breakdownApproval) {
      return yield* directorError(
        "approval-unavailable",
        "Current specification and ticket-breakdown approvals with available owner sources are required.",
      );
    }
    const ticketSummaries = yield* collectDeliveryTickets(input);
    const tickets = yield* Effect.forEach(ticketSummaries, (ticket) =>
      workflow.issueDetail({
        projectId: input.projectId,
        repository: input.repository,
        number: ticket.number,
      }),
    );
    yield* verifyPublishedBreakdown(breakdownApproval, tickets);
    yield* verifyRepository(cwd, input.repository);
    if (capability.readiness?.status !== "ready") {
      return yield* directorError(
        "not-ready",
        "This capability is not ready for implementation.",
        readinessDetail(capability),
      );
    }
    let hasReadyTicket = false;
    for (const ticket of tickets) {
      if (ticket.readiness?.status === "ready") {
        hasReadyTicket = true;
        break;
      }
      const claimLogin = options?.ownedClaims?.get(ticket.number);
      if (
        ticket.readiness?.status === "claimed" &&
        claimLogin &&
        (yield* currentClaimIsOwned(cwd, input.repository, ticket.number, claimLogin))
      ) {
        hasReadyTicket = true;
        break;
      }
    }
    if (!hasReadyTicket) {
      return yield* directorError(
        "not-ready",
        "No published delivery ticket is currently ready or owned by this capability.",
      );
    }
    const provider = yield* providerPreflight(cwd, input.modelSelection);
    return { capability, specificationApproval, breakdownApproval, tickets, ...provider };
  });

  const ensureWorktree = Effect.fn("WorkflowDirectorService.ensureWorktree")(function* (
    row: DirectorRow,
    projectCwd: string,
  ) {
    const validate = Effect.fn("WorkflowDirectorService.validateWorktree")(function* () {
      const status = yield* git
        .localStatus({ cwd: row.worktreePath })
        .pipe(Effect.orElseSucceed(() => null));
      return status?.isRepo === true && status.refName === row.worktreeBranch;
    });
    if (yield* validate()) {
      yield* verifyRepository(row.worktreePath, row.repository);
      return;
    }
    const projectStatus = yield* git
      .localStatus({ cwd: projectCwd })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "worktree-failed",
            "The project Git branch could not be read.",
            String(error),
          ),
        ),
      );
    if (!projectStatus.isRepo || !projectStatus.refName) {
      return yield* directorError(
        "worktree-failed",
        "The capability workspace must be a Git repository on a branch.",
      );
    }
    const created = yield* git
      .createWorktree({
        cwd: projectCwd,
        refName: projectStatus.refName,
        newRefName: row.worktreeBranch,
        baseRefName: projectStatus.refName,
        path: row.worktreePath,
      })
      .pipe(Effect.result);
    if (created._tag === "Failure" && !(yield* validate())) {
      return yield* directorError(
        "worktree-failed",
        "Capability worktree creation is uncertain. The recorded path was retained for recovery.",
        String(created.failure),
      );
    }
    if (!(yield* validate())) {
      return yield* directorError(
        "worktree-failed",
        "The created capability worktree does not match its recorded Git branch.",
      );
    }
    yield* verifyRepository(row.worktreePath, row.repository);
  });

  const sourceContext = Effect.fn("WorkflowDirectorService.sourceContext")(function* (
    input: Pick<WorkflowDirectorStartInput, "projectId" | "repository">,
    capability: WorkflowIssueDetail,
  ) {
    return yield* Effect.forEach(sourceIssueReferences(capability.body), (reference) =>
      workflow.issueDetail({
        projectId: input.projectId,
        repository: reference.repository as WorkflowIssueSummary["repository"],
        number: reference.number,
      }),
    );
  });

  const admissions = Effect.fn("WorkflowDirectorService.admissions")(function* (
    directorId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT admission_id AS "admissionId", director_id AS "directorId", batch_id AS "batchId",
        repository, ticket_number AS "ticketNumber", slot_ticket_number AS "slotTicketNumber",
        purpose, ownership, claim_login AS "claimLogin", claim_status AS "claimStatus",
        created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_director_admissions WHERE director_id = ${directorId} ORDER BY created_at
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "Director admissions could not be read.",
          String(error),
        ),
      ),
    );
    return yield* Effect.forEach(rows, (row) => decodeAdmissionRow(row)).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "A director admission record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const workerStatusFromRow = Effect.fn("WorkflowDirectorService.workerStatusFromRow")(
    function* (
      row: WorkerRow,
      rows: ReadonlyArray<WorkerRow>,
      writePathsByDispatch: ReadonlyMap<string, ReadonlyArray<string>>,
    ): Effect.fn.Return<WorkflowWorkerStatus, WorkflowDirectorError> {
      const decode = (value: string) =>
        decodeStringArrayJson(value).pipe(
          Effect.mapError((error) =>
            directorError("persistence-failed", "Worker history JSON is invalid.", String(error)),
          ),
        );
      const writePaths = row.writePathsJson ? yield* decode(row.writePathsJson) : [];
      const commits = row.handoffCommitsJson ? yield* decode(row.handoffCommitsJson) : [];
      const checks = row.handoffChecksJson ? yield* decode(row.handoffChecksJson) : [];
      const match =
        !row.requestedModel || !row.observedModel || !row.requestedEffort || !row.observedEffort
          ? "unknown"
          : row.requestedModel === row.observedModel && row.requestedEffort === row.observedEffort
            ? "match"
            : "mismatch";
      const outcome = row.dispatchStatus?.startsWith("reported-")
        ? (row.dispatchStatus.slice("reported-".length) as "succeeded" | "failed")
        : row.dispatchStatus === "unconfirmed"
          ? "unconfirmed"
          : null;
      return {
        dispatchId: row.dispatchId,
        admissionId: row.admissionId,
        ticketNumber: row.ticketNumber,
        providerThreadId: row.providerThreadId,
        parentProviderThreadId: row.parentProviderThreadId,
        ownership: row.ownership,
        writePaths,
        writeReservation: writeReservationFor(row, rows, writePathsByDispatch),
        settlementEvidence: row.nativeLifecycle === "closed" ? "native-closed" : null,
        association: row.dispatchId
          ? row.providerThreadId
            ? "associated"
            : "unconfirmed"
          : "unassociated",
        providerStatus: row.providerStatus,
        requestedProfile:
          row.requestedModel && row.requestedEffort && row.requestedSkillPath
            ? {
                model: row.requestedModel,
                effort: row.requestedEffort,
                skillPath: row.requestedSkillPath,
              }
            : null,
        observedProfile: {
          model: row.observedModel,
          effort: row.observedEffort,
          match,
        },
        handoff:
          outcome && row.handoffSummary
            ? { outcome, summary: row.handoffSummary, commits, checks }
            : null,
        title: row.title,
        role: row.role,
        updatedAt: row.updatedAt,
      };
    },
    Effect.mapError((error) =>
      directorError("persistence-failed", "A worker history record is invalid.", String(error)),
    ),
  );

  const workers = Effect.fn("WorkflowDirectorService.workers")(function* (directorId: string) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT d.dispatch_id AS "dispatchId", d.association_token AS "associationToken",
        d.admission_id AS "admissionId", d.ticket_number AS "ticketNumber",
        o.provider_thread_id AS "providerThreadId", o.parent_provider_thread_id AS "parentProviderThreadId",
        d.ownership, d.write_paths_json AS "writePathsJson", d.requested_model AS "requestedModel",
        d.requested_effort AS "requestedEffort", d.requested_skill_path AS "requestedSkillPath",
        d.status AS "dispatchStatus", COALESCE(o.provider_status, 'unconfirmed') AS "providerStatus",
        o.observed_model AS "observedModel", o.observed_effort AS "observedEffort",
        o.native_lifecycle AS "nativeLifecycle",
        d.handoff_summary AS "handoffSummary", d.handoff_commits_json AS "handoffCommitsJson",
        d.handoff_checks_json AS "handoffChecksJson", o.title, o.role,
        COALESCE(o.updated_at, d.updated_at) AS "updatedAt"
      FROM workflow_worker_dispatches d
      LEFT JOIN workflow_worker_observations o
        ON o.director_id = d.director_id AND o.provider_thread_id = d.provider_thread_id
      WHERE d.director_id = ${directorId}
      UNION ALL
      SELECT NULL AS "dispatchId", NULL AS "associationToken", NULL AS "admissionId",
        NULL AS "ticketNumber", o.provider_thread_id AS "providerThreadId",
        o.parent_provider_thread_id AS "parentProviderThreadId", NULL AS ownership,
        NULL AS "writePathsJson", NULL AS "requestedModel", NULL AS "requestedEffort",
        NULL AS "requestedSkillPath", NULL AS "dispatchStatus", o.provider_status AS "providerStatus",
        o.observed_model AS "observedModel", o.observed_effort AS "observedEffort",
        o.native_lifecycle AS "nativeLifecycle",
        NULL AS "handoffSummary", NULL AS "handoffCommitsJson", NULL AS "handoffChecksJson",
        o.title, o.role, o.updated_at AS "updatedAt"
      FROM workflow_worker_observations o
      WHERE o.director_id = ${directorId}
        AND NOT EXISTS (
          SELECT 1 FROM workflow_worker_dispatches d
          WHERE d.director_id = o.director_id AND d.provider_thread_id = o.provider_thread_id
        )
      ORDER BY "updatedAt"
    `.pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "Worker history could not be read.", String(error)),
      ),
    );
    const decoded = yield* Effect.forEach(rows, (row) => decodeWorkerRow(row)).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "A worker history row is invalid.", String(error)),
      ),
    );
    const writePathsByDispatch = new Map<string, ReadonlyArray<string>>();
    for (const row of decoded) {
      if (!row.dispatchId || !row.writePathsJson) continue;
      const paths = yield* decodeStringArrayJson(row.writePathsJson).pipe(
        Effect.mapError((error) =>
          directorError("persistence-failed", "Worker history JSON is invalid.", String(error)),
        ),
      );
      writePathsByDispatch.set(row.dispatchId, paths);
    }
    return yield* Effect.forEach(decoded, (row) =>
      workerStatusFromRow(row, decoded, writePathsByDispatch),
    );
  });

  const reconcileDirector = Effect.fn("WorkflowDirectorService.reconcileDirector")(function* (
    row: DirectorRow,
  ) {
    if (row.status !== "submitting" && row.status !== "held") return row;
    const receipt = yield* receipts
      .getByCommandId({ commandId: CommandId.make(row.commandId) })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Director command evidence could not be read.",
            String(error),
          ),
        ),
      );
    if (Option.isSome(receipt) && receipt.value.status === "accepted") {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`
        UPDATE workflow_directors SET status = 'active', sequence = ${receipt.value.resultSequence},
          initial_turn_disposition = 'accepted', detail = NULL, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Accepted director evidence could not be saved.",
            String(error),
          ),
        ),
      );
      return {
        ...row,
        status: "active",
        sequence: receipt.value.resultSequence,
        detail: null,
        updatedAt,
      };
    }
    if (row.status === "submitting") {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      const detail =
        "The director submission is uncertain. Inspect the linked thread; it will not be sent again automatically.";
      yield* sql`
        UPDATE workflow_directors SET status = 'held', detail = ${detail}, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The uncertain director could not be saved.",
            String(error),
          ),
        ),
      );
      return { ...row, status: "held", detail, updatedAt };
    }
    return row;
  });

  const statusFromRow = Effect.fn("WorkflowDirectorService.statusFromRow")(function* (
    original: DirectorRow,
  ) {
    const row = yield* reconcileDirector(original);
    const admissionRows = yield* admissions(row.directorId);
    const admissionCount = new Set(admissionRows.map((admission) => admission.slotTicketNumber))
      .size;
    const shell = yield* projection
      .getThreadShellById(ThreadId.make(row.threadId))
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director thread state could not be read.",
            String(error),
          ),
        ),
      );
    const actions: Array<WorkflowDirectorStatus["actions"][number]> = [];
    if (Option.isSome(shell)) actions.push("open");
    if (
      (row.status === "held" || row.status === "preparing-worktree") &&
      row.initialTurnDisposition === "not-attempted"
    ) {
      actions.push("retry");
    }
    if (
      row.status === "active" &&
      Option.isSome(shell) &&
      (shell.value.latestTurn?.state === "interrupted" ||
        shell.value.latestTurn?.state === "error") &&
      shell.value.session?.activeTurnId == null &&
      shell.value.session?.status !== "running" &&
      shell.value.session?.status !== "starting"
    ) {
      actions.push("resume");
    }
    const status =
      admissionCount >= ADMISSION_LIMIT && row.status === "active" ? "waiting" : row.status;
    const observation = [
      row.directorId,
      row.updatedAt,
      status,
      Option.isSome(shell) ? (shell.value.latestTurn?.turnId ?? "no-turn") : "no-thread",
      admissionCount,
    ].join("|");
    return {
      directorId: row.directorId,
      batchId: row.batchId,
      environmentId: EnvironmentId.make(row.environmentId),
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowDirectorStatus["repository"],
      rootNumber: row.rootNumber,
      capabilityNumber: row.capabilityNumber,
      threadId: ThreadId.make(row.threadId),
      worktreePath: row.worktreePath,
      worktreeBranch: row.worktreeBranch,
      status: status as WorkflowDirectorStatus["status"],
      requestedProfile: {
        instanceId: row.requestedInstanceId,
        model: row.requestedModel,
        effort: row.requestedEffort,
      },
      observedProfile: {
        model: row.observedModel,
        effort: row.observedEffort,
        match: row.observedMatch as WorkflowDirectorStatus["observedProfile"]["match"],
      },
      admissionCount,
      admissionLimit: ADMISSION_LIMIT,
      workers: yield* workers(row.directorId),
      observation,
      actions,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      message:
        status === "waiting"
          ? "This director has admitted ten delivery slices. Finish or settle admitted work, then wait for a successor."
          : status === "held"
            ? (row.detail ?? "Director startup is held for recovery.")
            : status === "preparing-worktree"
              ? "The recorded capability worktree needs recovery before the director can start."
              : status === "submitting"
                ? "The director submission is being reconciled."
                : "The capability director is active in its preserved worktree.",
    } satisfies WorkflowDirectorStatus;
  });

  const statusUnlocked = Effect.fn("WorkflowDirectorService.status")(function* (
    input: WorkflowDirectorStatusInput,
  ) {
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        directorError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const row = yield* loadDirectorByCapability(input, environmentId);
    if (!row) {
      return yield* directorError(
        "director-not-found",
        "No capability director is linked in this environment.",
      );
    }
    return yield* statusFromRow(row);
  });

  const continueInitialDirector = Effect.fn("WorkflowDirectorService.continueInitialDirector")(
    function* (
      row: DirectorRow,
      input: WorkflowDirectorStartInput,
      projectWorkspaceRoot: string,
      dispatch: Dispatch,
      options?: { readonly ownedClaims?: ReadonlyMap<number, string> },
    ) {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      const worktree = yield* ensureWorktree(row, projectWorkspaceRoot).pipe(Effect.result);
      if (worktree._tag === "Failure") {
        const detail = worktree.failure.message;
        yield* sql`
        UPDATE workflow_directors SET status = 'held', detail = ${detail}, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The held worktree state could not be saved.",
              String(error),
            ),
          ),
        );
        return {
          disposition: "held",
          director: yield* statusFromRow({ ...row, status: "held", detail, updatedAt }),
        } satisfies WorkflowDirectorStartResult;
      }
      const preparedResult = yield* prepareCapability(input, row.worktreePath, options).pipe(
        Effect.result,
      );
      if (preparedResult._tag === "Failure") {
        const detail = preparedResult.failure.message;
        yield* sql`
        UPDATE workflow_directors SET status = 'held', detail = ${detail}, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The held director state could not be saved.",
              String(error),
            ),
          ),
        );
        return {
          disposition: "held",
          director: yield* statusFromRow({ ...row, status: "held", detail, updatedAt }),
        } satisfies WorkflowDirectorStartResult;
      }
      const prepared = preparedResult.success;
      const sources = yield* sourceContext(input, prepared.capability);
      const instructions = workflowDirectorInstructions({
        capability: prepared.capability,
        tickets: prepared.tickets,
        approvalRecords: [prepared.specificationApproval, prepared.breakdownApproval],
        sourceContext: sources,
        implementSkillPath: prepared.skills[0]!.path,
        reviewSkillPath: prepared.skills[1]!.path,
      });
      yield* sql`
      UPDATE workflow_directors SET status = 'submitting', initial_turn_disposition = 'unknown',
        detail = NULL, updated_at = ${updatedAt} WHERE director_id = ${row.directorId}
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director submission intent could not be saved.",
            String(error),
          ),
        ),
      );
      const submitting = {
        ...row,
        status: "submitting",
        initialTurnDisposition: "unknown",
        detail: null,
        updatedAt,
      };
      const command = {
        type: "thread.turn.start" as const,
        commandId: CommandId.make(row.commandId),
        threadId: ThreadId.make(row.threadId),
        message: {
          messageId: MessageId.make(row.messageId),
          role: "user" as const,
          text: instructions,
          attachments: [],
        },
        modelSelection: input.modelSelection,
        runtimeMode: "approval-required" as const,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        bootstrap: {
          createThread: {
            projectId: ProjectId.make(row.projectId),
            title: `Director: ${prepared.capability.title}`,
            modelSelection: input.modelSelection,
            runtimeMode: "approval-required" as const,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: row.worktreeBranch,
            worktreePath: row.worktreePath,
            createdAt: updatedAt,
          },
          runSetupScript: true,
        },
        createdAt: updatedAt,
      } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
      const dispatched = yield* dispatch(command).pipe(Effect.result);
      if (dispatched._tag === "Failure") {
        const receipt = yield* receipts
          .getByCommandId({ commandId: command.commandId })
          .pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "Director command evidence could not be read.",
                String(error),
              ),
            ),
          );
        const accepted = Option.isSome(receipt) && receipt.value.status === "accepted";
        const notAccepted = dispatched.failure.bootstrapTurnDisposition === "not-accepted";
        const detail = accepted
          ? null
          : notAccepted
            ? "The director turn was not accepted. Its worktree and history were retained."
            : "The director submission is uncertain. Inspect the linked thread; it will not be sent again automatically.";
        yield* sql`
        UPDATE workflow_directors SET status = ${accepted ? "active" : "held"},
          sequence = ${accepted ? receipt.value.resultSequence : null},
          initial_turn_disposition = ${accepted ? "accepted" : notAccepted ? "not-accepted" : "unknown"},
          detail = ${detail}, updated_at = ${updatedAt} WHERE director_id = ${row.directorId}
      `.pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The director outcome could not be saved.",
              String(error),
            ),
          ),
        );
        return {
          disposition: accepted ? "started" : "held",
          director: yield* statusFromRow({
            ...submitting,
            status: accepted ? "active" : "held",
            sequence: accepted ? receipt.value.resultSequence : null,
            initialTurnDisposition: accepted
              ? "accepted"
              : notAccepted
                ? "not-accepted"
                : "unknown",
            detail,
          }),
        } satisfies WorkflowDirectorStartResult;
      }
      yield* sql`
      UPDATE workflow_directors SET status = 'active', sequence = ${dispatched.success.sequence},
        initial_turn_disposition = 'accepted', detail = NULL, updated_at = ${updatedAt}
      WHERE director_id = ${row.directorId}
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The accepted director could not be saved.",
            String(error),
          ),
        ),
      );
      return {
        disposition: "started",
        director: yield* statusFromRow({
          ...submitting,
          status: "active",
          sequence: dispatched.success.sequence,
          initialTurnDisposition: "accepted",
        }),
      } satisfies WorkflowDirectorStartResult;
    },
  );

  const startUnlocked = Effect.fn("WorkflowDirectorService.start")(function* (
    input: WorkflowDirectorStartInput,
    dispatch: Dispatch,
  ) {
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        directorError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const existing = yield* loadDirectorByCapability(input, environmentId);
    if (existing) {
      if (
        (existing.status === "held" || existing.status === "preparing-worktree") &&
        existing.initialTurnDisposition === "not-attempted"
      ) {
        if (existing.requestedInstanceId !== input.modelSelection.instanceId) {
          return yield* directorError(
            "provider-unavailable",
            "Retry with the Codex provider recorded for this director.",
          );
        }
        const project = yield* selectedProject(ProjectId.make(existing.projectId));
        const retryInput = {
          ...input,
          projectId: ProjectId.make(existing.projectId),
          repository: existing.repository as WorkflowDirectorStartInput["repository"],
          rootNumber: existing.rootNumber,
          capabilityNumber: existing.capabilityNumber,
        };
        const ownedClaims = ownedClaimLogins(yield* admissions(existing.directorId));
        yield* prepareCapability(retryInput, project.workspaceRoot, { ownedClaims });
        return yield* continueInitialDirector(
          existing,
          retryInput,
          project.workspaceRoot,
          dispatch,
          { ownedClaims },
        );
      }
      const director = yield* statusFromRow(existing);
      return {
        disposition: director.status === "held" ? "held" : "existing",
        director,
      } satisfies WorkflowDirectorStartResult;
    }
    const project = yield* selectedProject(input.projectId);
    yield* prepareCapability(input, project.workspaceRoot);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const directorId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const batchId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const threadId = ThreadId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const worktreeBranch = `t3code/workflow-${input.capabilityNumber}`;
    const repositoryDirectory = input.repository.replace(/[^a-z0-9._-]+/giu, "-");
    const worktreePath = path.join(
      config.worktreesDir,
      repositoryDirectory,
      `capability-${input.capabilityNumber}`,
    );
    yield* sql`
      INSERT INTO workflow_directors (
        director_id, batch_id, environment_id, project_id, repository, root_number,
        capability_number, thread_id, command_id, message_id, worktree_path, worktree_branch,
        status, requested_model, requested_instance_id, requested_effort, observed_match,
        initial_turn_disposition,
        created_at, updated_at
      ) VALUES (
        ${directorId}, ${batchId}, ${environmentId}, ${input.projectId}, ${input.repository}, ${input.rootNumber},
        ${input.capabilityNumber}, ${threadId}, ${commandId}, ${messageId}, ${worktreePath}, ${worktreeBranch},
        'preparing-worktree', ${DIRECTOR_MODEL}, ${input.modelSelection.instanceId},
        ${DIRECTOR_EFFORT}, 'unknown', 'not-attempted',
        ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The director and worktree intent could not be saved.",
          String(error),
        ),
      ),
    );
    const row = (yield* loadDirectorByCapability(input, environmentId))!;
    return yield* continueInitialDirector(row, input, project.workspaceRoot, dispatch);
  });

  const resumeUnlocked = Effect.fn("WorkflowDirectorService.resume")(function* (
    input: WorkflowDirectorResumeInput,
    dispatch: Dispatch,
  ) {
    const row = yield* loadDirectorById(input.directorId);
    if (
      row.projectId !== input.projectId ||
      row.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase() ||
      row.capabilityNumber !== input.capabilityNumber
    ) {
      return yield* directorError(
        "director-not-found",
        "The selected capability director changed.",
      );
    }
    const shell = yield* projection
      .getThreadShellById(ThreadId.make(row.threadId))
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director thread state could not be read.",
            String(error),
          ),
        ),
      );
    const latestTurn = Option.isSome(shell) ? shell.value.latestTurn : undefined;
    const observedSourceTurnId = input.observation.split("|")[3];
    if (
      observedSourceTurnId &&
      observedSourceTurnId !== "no-turn" &&
      observedSourceTurnId !== "no-thread"
    ) {
      const previous = yield* sql<{
        readonly commandId: string;
        readonly status: string;
        readonly sequence: number | null;
      }>`
        SELECT command_id AS "commandId", status, sequence FROM workflow_director_resumes
        WHERE director_id = ${row.directorId} AND source_turn_id = ${observedSourceTurnId} LIMIT 1
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Director resume history could not be read.",
            String(error),
          ),
        ),
      );
      if (previous[0]) {
        const receipt = yield* receipts
          .getByCommandId({ commandId: CommandId.make(previous[0].commandId) })
          .pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "Director resume evidence could not be read.",
                String(error),
              ),
            ),
          );
        if (
          (Option.isSome(receipt) && receipt.value.status === "accepted") ||
          (previous[0].status === "submitted" && previous[0].sequence !== null)
        ) {
          return yield* statusFromRow(row);
        }
        return yield* directorError(
          "dispatch-failed",
          "The director resume is uncertain and will not be sent again automatically.",
        );
      }
    }
    const current = yield* statusFromRow(row);
    if (
      current.observation !== input.observation ||
      current.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase() ||
      current.capabilityNumber !== input.capabilityNumber
    ) {
      return yield* directorError(
        "not-ready",
        "The director changed. Refresh its status before resuming.",
      );
    }
    if (!current.actions.includes("resume")) {
      return yield* directorError(
        "not-ready",
        "Resume is available only for an interrupted director with no active turn.",
      );
    }
    const project = yield* selectedProject(ProjectId.make(row.projectId));
    const ownedClaims = ownedClaimLogins(yield* admissions(row.directorId));
    yield* prepareCapability(
      {
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowDirectorStartInput["repository"],
        capabilityNumber: row.capabilityNumber,
        modelSelection: input.modelSelection,
      },
      row.worktreePath,
      { ownedClaims },
    );
    yield* ensureWorktree(row, project.workspaceRoot);
    if (Option.isNone(shell) || !latestTurn) {
      return yield* directorError("not-ready", "The interrupted director turn is unavailable.");
    }
    const sourceTurnId = latestTurn.turnId;
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const resumeId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    yield* sql`
      INSERT INTO workflow_director_resumes (
        resume_id, director_id, source_turn_id, command_id, message_id, status, created_at, updated_at
      ) VALUES (${resumeId}, ${row.directorId}, ${sourceTurnId}, ${commandId}, ${messageId}, 'submitting', ${createdAt}, ${createdAt})
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The director resume intent could not be saved.",
          String(error),
        ),
      ),
    );
    const command = {
      type: "thread.turn.start" as const,
      commandId,
      threadId: ThreadId.make(row.threadId),
      message: {
        messageId,
        role: "user" as const,
        text: `Resume the interrupted director for ${row.repository}#${row.capabilityNumber} in the preserved capability worktree. Re-read live readiness and use the host admission boundary before any new delegation.`,
        attachments: [],
      },
      modelSelection: input.modelSelection,
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
    const dispatched = yield* dispatch(command).pipe(Effect.result);
    if (dispatched._tag === "Failure") {
      const receipt = yield* receipts
        .getByCommandId({ commandId })
        .pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Director resume evidence could not be read.",
              String(error),
            ),
          ),
        );
      const accepted = Option.isSome(receipt) && receipt.value.status === "accepted";
      yield* sql`
        UPDATE workflow_director_resumes SET status = ${accepted ? "submitted" : "held"},
          sequence = ${accepted ? receipt.value.resultSequence : null},
          detail = ${accepted ? null : "The director resume is uncertain and will not be sent again automatically."},
          updated_at = ${createdAt} WHERE resume_id = ${resumeId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director resume outcome could not be saved.",
            String(error),
          ),
        ),
      );
      if (!accepted)
        return yield* directorError(
          "dispatch-failed",
          "The director resume is uncertain and will not be sent again automatically.",
        );
    } else {
      yield* sql`
        UPDATE workflow_director_resumes SET status = 'submitted', sequence = ${dispatched.success.sequence},
          detail = NULL, updated_at = ${createdAt} WHERE resume_id = ${resumeId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The accepted director resume could not be saved.",
            String(error),
          ),
        ),
      );
    }
    return yield* statusFromRow(row);
  });

  const requireDeliveryReadiness = Effect.fn("WorkflowDirectorService.requireDeliveryReadiness")(
    function* (
      row: DirectorRow,
      issue: WorkflowIssueSummary,
      ownedAdmission: AdmissionRow | undefined,
      allowOwnedClaim: boolean,
    ) {
      if (issue.readiness?.status === "ready") return;
      if (
        issue.readiness?.status === "claimed" &&
        allowOwnedClaim &&
        ownedAdmission?.claimLogin &&
        ownedAdmission.claimStatus !== "conflict" &&
        (yield* currentClaimIsOwned(
          row.worktreePath,
          row.repository,
          issue.number,
          ownedAdmission.claimLogin,
        ))
      ) {
        return;
      }
      return yield* directorError(
        "not-ready",
        "This delivery unit is not ready to admit.",
        readinessDetail(issue),
      );
    },
  );

  const reconcileExistingClaim = Effect.fn("WorkflowDirectorService.reconcileExistingClaim")(
    function* (row: DirectorRow, ticketNumber: number, existing: AdmissionRow) {
      let claimLogin = existing.claimLogin;
      let claimStatus: WorkflowDirectorAdmission["claimStatus"] = "uncertain";
      const identity = yield* executeGitHub(row.worktreePath, [
        "api",
        "user",
        "--jq",
        ".login",
      ]).pipe(Effect.result);
      if (identity._tag === "Success" && identity.success.stdout.trim()) {
        claimLogin = identity.success.stdout.trim();
        const assignees = yield* executeGitHub(row.worktreePath, [
          "issue",
          "view",
          String(ticketNumber),
          "--repo",
          row.repository,
          "--json",
          "assignees",
          "--jq",
          ".assignees[].login",
        ]).pipe(Effect.result);
        if (assignees._tag === "Success") {
          const current = assignees.success.stdout
            .split("\n")
            .map((value) => value.trim())
            .filter(Boolean);
          if (current.length === 0) {
            const claimed = yield* executeGitHub(row.worktreePath, [
              "issue",
              "edit",
              String(ticketNumber),
              "--repo",
              row.repository,
              "--add-assignee",
              claimLogin,
            ]).pipe(Effect.result);
            claimStatus = claimed._tag === "Success" ? "confirmed" : "uncertain";
          } else if (
            current.length === 1 &&
            current[0] === claimLogin &&
            existing.claimLogin === claimLogin
          ) {
            // A persisted local claim intent makes this observation safe to reconcile.
            claimStatus = "confirmed";
          } else {
            claimStatus = "conflict";
          }
        }
      }
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`
      UPDATE workflow_director_admissions SET claim_login = ${claimLogin}, claim_status = ${claimStatus},
        updated_at = ${updatedAt} WHERE admission_id = ${existing.admissionId}
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The ticket ownership result could not be saved.",
            String(error),
          ),
        ),
      );
      return { ...existing, claimLogin, claimStatus, updatedAt };
    },
  );

  const admitUnlocked = Effect.fn("WorkflowDirectorService.admit")(function* (
    input: WorkflowDirectorAdmissionInput,
  ) {
    const row = yield* loadDirectorById(input.directorId);
    if (
      row.projectId !== input.projectId ||
      row.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase()
    ) {
      return yield* directorError(
        "director-not-found",
        "The selected director does not own this project and repository.",
      );
    }
    const existingRows = yield* admissions(row.directorId);
    const existing = existingRows.find(
      (admission) =>
        admission.repository.toLocaleLowerCase() === input.repository.toLocaleLowerCase() &&
        admission.ticketNumber === input.ticketNumber,
    );
    const capabilityState = yield* prepareCapability(
      {
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowDirectorStartInput["repository"],
        capabilityNumber: row.capabilityNumber,
        modelSelection: {
          instanceId:
            row.requestedInstanceId as WorkflowDirectorStartInput["modelSelection"]["instanceId"],
          model: row.requestedModel,
          options: [{ id: "reasoningEffort", value: row.requestedEffort }],
        },
      },
      row.worktreePath,
      { ownedClaims: ownedClaimLogins(existingRows) },
    );
    const ticket = yield* workflow.issueDetail({
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowIssueSummary["repository"],
      number: input.ticketNumber,
    });
    const publishedTicket = capabilityState.tickets.find(
      (candidate) => candidate.id === ticket.id && candidate.number === ticket.number,
    );
    if (ticket.kind === "ticket" && !publishedTicket) {
      return yield* directorError(
        "not-ready",
        "This ticket is not a published delivery slice of the selected capability.",
      );
    }
    let slotTicketNumber: number;
    let parent: WorkflowIssueSummary | undefined;
    if (ticket.kind === "ticket") {
      if (input.parentTicketNumber && input.parentTicketNumber !== ticket.number) {
        return yield* directorError(
          "not-ready",
          "The supplied parent ticket does not match this delivery slice.",
        );
      }
      slotTicketNumber = ticket.number;
    } else if (ticket.kind === "task" && input.parentTicketNumber) {
      const located = yield* workflow.locate({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowIssueSummary["repository"],
        id: ticket.id,
        number: ticket.number,
      });
      const parentIdentity = located.ancestry.find(
        (ancestor) =>
          ancestor.number === input.parentTicketNumber &&
          ancestor.kind === "ticket" &&
          capabilityState.tickets.some(
            (candidate) => candidate.id === ancestor.id && candidate.number === ancestor.number,
          ),
      );
      parent = capabilityState.tickets.find(
        (candidate) =>
          candidate.id === parentIdentity?.id && candidate.number === parentIdentity?.number,
      );
      if (!located.ancestryComplete || !parentIdentity || !parent) {
        return yield* directorError(
          "not-ready",
          "The nested task does not belong to the supplied delivery slice.",
        );
      }
      slotTicketNumber = parent.number;
    } else {
      return yield* directorError(
        "not-ready",
        "Only delivery tickets, or their verified nested tasks, can be admitted.",
      );
    }
    yield* requireDeliveryReadiness(
      row,
      ticket,
      existing,
      Boolean(existing) && input.purpose !== "implement",
    );
    if (parent) {
      const parentAdmission = existingRows.find(
        (admission) =>
          admission.repository.toLocaleLowerCase() === input.repository.toLocaleLowerCase() &&
          admission.ticketNumber === parent!.number,
      );
      yield* requireDeliveryReadiness(row, parent, parentAdmission, Boolean(parentAdmission));
    }
    if (existing) {
      const reconciled = yield* reconcileExistingClaim(row, ticket.number, existing);
      return {
        disposition: "existing",
        admission: admissionFromRow(reconciled),
        admissionCount: new Set(existingRows.map((admission) => admission.slotTicketNumber)).size,
        admissionLimit: ADMISSION_LIMIT,
        directorStatus: (yield* statusFromRow(row)).status,
        message: "This delivery unit already uses its existing batch slot.",
      } satisfies WorkflowDirectorAdmissionResult;
    }
    const slots = new Set(existingRows.map((admission) => admission.slotTicketNumber));
    if (!slots.has(slotTicketNumber) && slots.size >= ADMISSION_LIMIT) {
      return {
        disposition: "limit-reached",
        admission: null,
        admissionCount: slots.size,
        admissionLimit: ADMISSION_LIMIT,
        directorStatus: "waiting",
        message:
          "This director has admitted ten delivery slices. Finish or settle admitted work, then wait for a successor.",
      } satisfies WorkflowDirectorAdmissionResult;
    }
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const admissionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    yield* sql`
      INSERT INTO workflow_director_admissions (
        admission_id, director_id, batch_id, repository, ticket_id, ticket_number,
        slot_ticket_number, purpose, ownership, claim_status, created_at, updated_at
      ) VALUES (
        ${admissionId}, ${row.directorId}, ${row.batchId}, ${row.repository}, ${ticket.id}, ${ticket.number},
        ${slotTicketNumber}, ${input.purpose}, ${input.ownership}, 'pending', ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The ticket admission could not be saved.",
          String(error),
        ),
      ),
    );
    let claimLogin: string | null = null;
    let claimStatus: WorkflowDirectorAdmission["claimStatus"] = "pending";
    const identity = yield* executeGitHub(row.worktreePath, ["api", "user", "--jq", ".login"]).pipe(
      Effect.result,
    );
    if (identity._tag === "Success" && identity.success.stdout.trim()) {
      claimLogin = identity.success.stdout.trim();
      const assignees = yield* executeGitHub(row.worktreePath, [
        "issue",
        "view",
        String(ticket.number),
        "--repo",
        row.repository,
        "--json",
        "assignees",
        "--jq",
        ".assignees[].login",
      ]).pipe(Effect.result);
      if (assignees._tag === "Failure") {
        claimStatus = "uncertain";
      } else {
        const current = assignees.success.stdout
          .split("\n")
          .map((value) => value.trim())
          .filter(Boolean);
        if (current.length === 0) {
          const claimed = yield* executeGitHub(row.worktreePath, [
            "issue",
            "edit",
            String(ticket.number),
            "--repo",
            row.repository,
            "--add-assignee",
            claimLogin,
          ]).pipe(Effect.result);
          claimStatus = claimed._tag === "Success" ? "confirmed" : "uncertain";
        } else {
          claimStatus = "conflict";
        }
      }
    } else {
      claimStatus = "uncertain";
    }
    yield* sql`
      UPDATE workflow_director_admissions SET claim_login = ${claimLogin}, claim_status = ${claimStatus},
        updated_at = ${createdAt} WHERE admission_id = ${admissionId}
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The ticket ownership result could not be saved.",
          String(error),
        ),
      ),
    );
    const admission = admissionFromRow({
      admissionId,
      directorId: row.directorId,
      batchId: row.batchId,
      repository: row.repository,
      ticketNumber: ticket.number,
      slotTicketNumber,
      purpose: input.purpose,
      ownership: input.ownership,
      claimLogin,
      claimStatus,
      createdAt,
      updatedAt: createdAt,
    });
    return {
      disposition: "admitted",
      admission,
      admissionCount: new Set([...slots, slotTicketNumber]).size,
      admissionLimit: ADMISSION_LIMIT,
      directorStatus:
        new Set([...slots, slotTicketNumber]).size >= ADMISSION_LIMIT ? "waiting" : "active",
      message:
        claimStatus === "confirmed"
          ? "Admission and explicit ownership were persisted. Delegation may proceed."
          : "Admission was persisted and consumes its slot, but ownership is not confirmed. Resolve the GitHub assignment, then repeat this admission to recheck it before delegation.",
    } satisfies WorkflowDirectorAdmissionResult;
  });

  const directorForMcpScope = Effect.fn("WorkflowDirectorService.directorForMcpScope")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT director_id AS "directorId", batch_id AS "batchId", environment_id AS "environmentId",
          project_id AS "projectId", repository, root_number AS "rootNumber",
          capability_number AS "capabilityNumber", thread_id AS "threadId", command_id AS "commandId",
          message_id AS "messageId", worktree_path AS "worktreePath", worktree_branch AS "worktreeBranch",
          status, requested_model AS "requestedModel", requested_instance_id AS "requestedInstanceId",
          requested_effort AS "requestedEffort", observed_model AS "observedModel",
          observed_effort AS "observedEffort", observed_match AS "observedMatch", sequence,
          initial_turn_disposition AS "initialTurnDisposition", detail,
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM workflow_directors
        WHERE environment_id = ${environmentId} AND thread_id = ${threadId} AND is_current = 1
        LIMIT 1
      `,
      "The scoped capability director could not be read.",
    );
    if (!rows[0]) {
      return yield* directorError(
        "director-not-found",
        "Workflow worker tools are available only to the current capability director thread.",
      );
    }
    const row = yield* decodeDirectorRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The scoped director record is invalid.",
          String(error),
        ),
      ),
    );
    if (row.requestedInstanceId !== providerInstanceId) {
      return yield* directorError(
        "provider-unavailable",
        "This MCP session does not use the Codex provider recorded for the capability director.",
      );
    }
    return row;
  });

  const normalizeWritePaths = Effect.fn("WorkflowDirectorService.normalizeWritePaths")(function* (
    paths: ReadonlyArray<string>,
  ) {
    const normalized = [
      ...new Set(
        paths.map((entry) =>
          path
            .normalize(entry.trim().replaceAll("\\", "/"))
            .replaceAll("\\", "/")
            .replace(/^\.\//u, "")
            .replace(/\/+$/u, ""),
        ),
      ),
    ];
    if (
      normalized.length === 0 ||
      normalized.some(
        (entry) =>
          entry.length === 0 ||
          entry === "." ||
          path.isAbsolute(entry) ||
          entry === ".." ||
          entry.startsWith("../"),
      )
    ) {
      return yield* directorError(
        "not-ready",
        "Write ownership requires one or more repository-relative file or directory paths.",
      );
    }
    return normalized;
  });

  const workerPreflight = Effect.fn("WorkflowDirectorService.workerPreflight")(function* (
    row: DirectorRow,
  ) {
    const provider = yield* providerRegistry
      .probeWorkspaceSnapshot({
        instanceId:
          row.requestedInstanceId as WorkflowDirectorStartInput["modelSelection"]["instanceId"],
        cwd: row.worktreePath,
      })
      .pipe(
        Effect.mapError((error) =>
          directorError("provider-unavailable", "Worker provider preflight failed.", String(error)),
        ),
      );
    const workerModel = provider?.models.find((model) => model.slug === WORKER_MODEL);
    const effort = workerModel?.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort",
    );
    const implementSkills = provider?.skills.filter(
      (skill) => skill.name === "implement" && skill.enabled,
    );
    if (
      provider?.driver !== ProviderDriverKind.make("codex") ||
      !workerModel ||
      effort?.type !== "select" ||
      !effort.options.some((option) => option.id === WORKER_EFFORT) ||
      implementSkills?.length !== 1
    ) {
      return yield* directorError(
        "provider-unavailable",
        "The requested Sol/high worker and one enabled implement skill must pass preflight before dispatch.",
      );
    }
    return { skillPath: implementSkills[0]!.path };
  });

  const prepareWorkerUnlocked = Effect.fn("WorkflowDirectorService.prepareWorker")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowWorkerPrepareInput,
  ) {
    const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    const writePaths = yield* normalizeWritePaths(input.writePaths);
    const currentWorkers = yield* workers(row.directorId);
    for (const active of currentWorkers) {
      if (
        !active.dispatchId ||
        active.writeReservation !== "held" ||
        active.ticketNumber === input.ticketNumber
      ) {
        continue;
      }
      const overlap = writePaths.find((candidate) =>
        active.writePaths.some((owned) => pathsOverlap(candidate, owned)),
      );
      if (overlap) {
        return yield* directorError(
          "not-ready",
          "Worker write ownership overlaps unsettled work.",
          `${overlap} overlaps ticket #${active.ticketNumber}.`,
        );
      }
    }

    const { skillPath } = yield* workerPreflight(row);

    const existingAdmission = (yield* admissions(row.directorId)).find(
      (admission) => admission.ticketNumber === input.ticketNumber,
    );
    const admissionResult = yield* admitUnlocked({
      projectId: ProjectId.make(row.projectId),
      directorId: row.directorId,
      repository: row.repository as WorkflowDirectorAdmissionInput["repository"],
      ticketNumber: input.ticketNumber,
      ...(input.parentTicketNumber ? { parentTicketNumber: input.parentTicketNumber } : {}),
      purpose: existingAdmission ? "retry" : "implement",
      ownership: input.ownership,
    });
    const admission = admissionResult.admission;
    if (!admission || admission.claimStatus !== "confirmed") {
      return yield* directorError(
        "claim-failed",
        "Ticket ownership is not confirmed, so child dispatch is held.",
        admissionResult.message,
      );
    }
    const existingRows = yield* persistence(
      sql<Record<string, unknown>>`
      SELECT d.dispatch_id AS "dispatchId", d.association_token AS "associationToken",
        d.admission_id AS "admissionId", d.ticket_number AS "ticketNumber",
        d.provider_thread_id AS "providerThreadId", o.parent_provider_thread_id AS "parentProviderThreadId",
        d.ownership, d.write_paths_json AS "writePathsJson", d.requested_model AS "requestedModel",
        d.requested_effort AS "requestedEffort", d.requested_skill_path AS "requestedSkillPath",
        d.status AS "dispatchStatus", COALESCE(o.provider_status, 'unconfirmed') AS "providerStatus",
        o.observed_model AS "observedModel", o.observed_effort AS "observedEffort",
        o.native_lifecycle AS "nativeLifecycle", d.handoff_summary AS "handoffSummary",
        d.handoff_commits_json AS "handoffCommitsJson", d.handoff_checks_json AS "handoffChecksJson",
        o.title, o.role, COALESCE(o.updated_at, d.updated_at) AS "updatedAt"
      FROM workflow_worker_dispatches d
      LEFT JOIN workflow_worker_observations o
        ON o.director_id = d.director_id AND o.provider_thread_id = d.provider_thread_id
      WHERE d.director_id = ${row.directorId} AND d.admission_id = ${admission.admissionId}
      ORDER BY COALESCE(o.updated_at, d.updated_at) DESC, d.created_at DESC
    `,
      "Prepared worker dispatches could not be read.",
    );
    const existing = yield* Effect.forEach(existingRows, (existingRow) =>
      decodeWorkerRow(existingRow).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The prepared worker dispatch is invalid.",
            String(error),
          ),
        ),
      ),
    );
    const refreshedWorkers = yield* workers(row.directorId);
    const existingWorker = refreshedWorkers.findLast(
      (worker) =>
        worker.admissionId === admission.admissionId && worker.writeReservation === "held",
    );
    const existingDispatch = existingWorker
      ? existing.find((candidate) => candidate.dispatchId === existingWorker.dispatchId)
      : undefined;
    if (existingWorker && existingDispatch) {
      return {
        dispatchId: existingDispatch.dispatchId!,
        associationToken: existingDispatch.associationToken!,
        admission,
        requestedProfile: {
          model: existingDispatch.requestedModel!,
          effort: existingDispatch.requestedEffort!,
          skillPath: existingDispatch.requestedSkillPath!,
        },
        taskName: `ticket-${admission.ticketNumber}-${existingDispatch.dispatchId!.slice(0, 8)}`,
        instructions: `Worker dispatch already exists (${existingWorker.association}, provider ${existingWorker.providerStatus}, write reservation held). Do not spawn another child for this dispatch.`,
        disposition: "existing",
        worker: existingWorker,
      } satisfies WorkflowWorkerPrepareResult;
    }

    const dispatchId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const associationToken = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql`
      INSERT INTO workflow_worker_dispatches (
        dispatch_id, association_token, director_id, batch_id, admission_id, repository,
        ticket_number, ownership, write_paths_json, requested_model, requested_effort,
        requested_skill_path, status, created_at, updated_at
      ) VALUES (
        ${dispatchId}, ${associationToken}, ${row.directorId}, ${row.batchId},
        ${admission.admissionId}, ${row.repository}, ${admission.ticketNumber}, ${input.ownership},
        ${encodeStringArrayJson(writePaths)}, ${WORKER_MODEL}, ${WORKER_EFFORT}, ${skillPath},
        'prepared', ${createdAt}, ${createdAt}
      )
    `,
      "The prepared worker dispatch could not be saved.",
    );
    const taskName = `ticket-${admission.ticketNumber}-${dispatchId.slice(0, 8)}`;
    const worker = (yield* workers(row.directorId)).find(
      (candidate) => candidate.dispatchId === dispatchId,
    )!;
    return {
      dispatchId,
      associationToken,
      admission,
      requestedProfile: { model: WORKER_MODEL, effort: WORKER_EFFORT, skillPath },
      taskName,
      instructions: [
        `Spawn one worker for ${row.repository}#${admission.ticketNumber} in ${row.worktreePath}.`,
        `Use model ${WORKER_MODEL}, reasoning effort ${WORKER_EFFORT}, and $implement at ${skillPath}.`,
        `Task name: ${taskName}.`,
        `Write ownership: ${input.ownership}. Paths: ${writePaths.join(", ")}.`,
        `Tell the worker it is not alone in the shared capability worktree and must preserve others' edits.`,
        `After the native spawn returns its child thread id, call workflow_associate_worker with token ${associationToken}.`,
      ].join("\n"),
      disposition: "prepared",
      worker,
    } satisfies WorkflowWorkerPrepareResult;
  });

  const associateWorkerUnlocked = Effect.fn("WorkflowDirectorService.associateWorker")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowWorkerAssociateInput,
  ) {
    const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    const observed = yield* persistence(
      sql<{ readonly providerThreadId: string }>`
      SELECT provider_thread_id AS "providerThreadId" FROM workflow_worker_observations
      WHERE director_id = ${row.directorId} AND provider_thread_id = ${input.providerThreadId}
      LIMIT 1
    `,
      "Observed child identity could not be read.",
    );
    if (!observed[0]) {
      return yield* directorError(
        "not-ready",
        "The child cannot be associated until its native provider activity is observed under this director.",
      );
    }
    const updatedAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql`
      UPDATE workflow_worker_dispatches SET provider_thread_id = ${input.providerThreadId},
        status = 'associated', updated_at = ${updatedAt}
      WHERE director_id = ${row.directorId} AND association_token = ${input.associationToken}
        AND provider_thread_id IS NULL
    `,
      "The worker association could not be saved.",
    );
    const association = yield* persistence(
      sql<{ readonly dispatchId: string }>`
      SELECT dispatch_id AS "dispatchId" FROM workflow_worker_dispatches
      WHERE director_id = ${row.directorId} AND association_token = ${input.associationToken}
        AND provider_thread_id = ${input.providerThreadId}
      LIMIT 1
    `,
      "The worker association could not be confirmed.",
    );
    if (!association[0]) {
      return yield* directorError(
        "not-ready",
        "This association token is unknown, already used, or belongs to another director.",
      );
    }
    return (yield* workers(row.directorId)).find(
      (worker) => worker.providerThreadId === input.providerThreadId,
    )!;
  });

  const reportWorkerHandoffUnlocked = Effect.fn("WorkflowDirectorService.reportWorkerHandoff")(
    function* (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowWorkerHandoffInput,
    ) {
      const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`
      UPDATE workflow_worker_dispatches SET status = ${
        input.outcome === "unconfirmed" ? "unconfirmed" : `reported-${input.outcome}`
      }, handoff_summary = ${input.summary}, handoff_commits_json = ${encodeStringArrayJson(input.commits)},
        handoff_checks_json = ${encodeStringArrayJson(input.checks)}, updated_at = ${updatedAt}
      WHERE director_id = ${row.directorId} AND provider_thread_id = ${input.providerThreadId}
    `,
        "The worker handoff could not be saved.",
      );
      const handoff = yield* persistence(
        sql<{ readonly dispatchId: string }>`
      SELECT dispatch_id AS "dispatchId" FROM workflow_worker_dispatches
      WHERE director_id = ${row.directorId} AND provider_thread_id = ${input.providerThreadId}
        AND handoff_summary = ${input.summary}
      LIMIT 1
    `,
        "The worker handoff could not be confirmed.",
      );
      if (!handoff[0]) {
        return yield* directorError(
          "not-ready",
          "Report a handoff only for an explicitly associated worker.",
        );
      }
      return (yield* workers(row.directorId)).find(
        (worker) => worker.providerThreadId === input.providerThreadId,
      )!;
    },
  );

  return WorkflowDirectorService.of({
    start: (input, dispatch) => lock.withPermits(1)(startUnlocked(input, dispatch)),
    status: (input) => lock.withPermits(1)(statusUnlocked(input)),
    resume: (input, dispatch) => lock.withPermits(1)(resumeUnlocked(input, dispatch)),
    admit: (input) => lock.withPermits(1)(admitUnlocked(input)),
    prepareWorker: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        prepareWorkerUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    associateWorker: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        associateWorkerUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    reportWorkerHandoff: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        reportWorkerHandoffUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
  });
});

export const layer = Layer.effect(WorkflowDirectorService, make).pipe(
  Layer.provide(Layer.merge(OrchestrationCommandReceiptRepositoryLive, ProcessRunner.layer)),
);
