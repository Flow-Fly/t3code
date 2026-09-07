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
  type WorkflowReviewDispositionInput,
  type WorkflowReviewCheckReceiptInput,
  type WorkflowReviewFinding,
  type WorkflowTicketResolveInput,
  type WorkflowTicketResolveResult,
  type WorkflowTicketResolutionStatus,
  type WorkflowTicketReviewAssociateInput,
  type WorkflowTicketReviewPrepareInput,
  type WorkflowTicketReviewPrepareResult,
  type WorkflowTicketReviewReportInput,
  type WorkflowTicketReviewStatus,
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
import { workflowEvidenceBodyFingerprint } from "./WorkflowEvidence.ts";
import * as WorkflowService from "./WorkflowService.ts";

const ADMISSION_LIMIT = 10;
const DIRECTOR_MODEL = "gpt-6-astra";
const DIRECTOR_EFFORT = "high";
const WORKER_MODEL = "gpt-5.6-sol";
const WORKER_EFFORT = "high";
const REVIEWER_MODEL = "gpt-6-astra";
const REVIEWER_EFFORT = "medium";
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
const decodeStringArrayJsonSync = Schema.decodeUnknownSync(StringArrayJson);

const ReviewRow = Schema.Struct({
  reviewId: Schema.String,
  associationToken: Schema.String,
  directorId: Schema.String,
  admissionId: Schema.String,
  implementationDispatchId: Schema.String,
  ticketNumber: Schema.Number,
  fixedBase: Schema.String,
  implementationHead: Schema.String,
  scopeBody: Schema.String,
  requestedModel: Schema.String,
  requestedEffort: Schema.String,
  requestedSkillPath: Schema.String,
  providerThreadId: Schema.NullOr(Schema.String),
  status: Schema.String,
  reportSummary: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type ReviewRow = typeof ReviewRow.Type;
const decodeReviewRow = Schema.decodeUnknownEffect(ReviewRow);

const ObservationRow = Schema.Struct({
  providerThreadId: Schema.String,
  parentProviderThreadId: Schema.NullOr(Schema.String),
  observedModel: Schema.NullOr(Schema.String),
  observedEffort: Schema.NullOr(Schema.String),
  providerStatus: Schema.String,
  nativeLifecycle: Schema.NullOr(Schema.String),
  firstObservedAt: Schema.String,
});
type ObservationRow = typeof ObservationRow.Type;
const decodeObservationRow = Schema.decodeUnknownEffect(ObservationRow);

const CheckRow = Schema.Struct({
  reviewId: Schema.String,
  label: Schema.String,
  command: Schema.String,
  toolCallId: Schema.NullOr(Schema.String),
  exitCode: Schema.NullOr(Schema.Number),
  output: Schema.String,
  startedHead: Schema.String,
  finishedHead: Schema.NullOr(Schema.String),
  startedClean: Schema.Number,
  finishedClean: Schema.NullOr(Schema.Number),
  verificationStatus: Schema.String,
  verificationError: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
});
type CheckRow = typeof CheckRow.Type;
const decodeCheckRow = Schema.decodeUnknownEffect(CheckRow);

const FindingRow = Schema.Struct({
  reviewId: Schema.String,
  findingId: Schema.String,
  axis: Schema.String,
  severity: Schema.String,
  summary: Schema.String,
  location: Schema.NullOr(Schema.String),
  disposition: Schema.NullOr(Schema.String),
  dispositionRationale: Schema.NullOr(Schema.String),
  dispositionEvidenceSource: Schema.NullOr(Schema.String),
  dispositionEvidenceQuote: Schema.NullOr(Schema.String),
  resultingReviewId: Schema.NullOr(Schema.String),
});
type FindingRow = typeof FindingRow.Type;
const decodeFindingRow = Schema.decodeUnknownEffect(FindingRow);

const ResolutionRow = Schema.Struct({
  resolutionId: Schema.String,
  reviewId: Schema.String,
  ticketNumber: Schema.Number,
  finalHead: Schema.String,
  commentBody: Schema.String,
  status: Schema.String,
  commentUrl: Schema.NullOr(Schema.String),
  frontierJson: Schema.NullOr(Schema.String),
  lastError: Schema.NullOr(Schema.String),
  updatedAt: Schema.String,
});
type ResolutionRow = typeof ResolutionRow.Type;
const decodeResolutionRow = Schema.decodeUnknownEffect(ResolutionRow);

const NativeCommandRow = Schema.Struct({
  lifecycle: Schema.String,
  command: Schema.String,
  cwd: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  exitCode: Schema.NullOr(Schema.Number),
  output: Schema.String,
  createdAt: Schema.String,
});
type NativeCommandRow = typeof NativeCommandRow.Type;
const decodeNativeCommandRow = Schema.decodeUnknownEffect(NativeCommandRow);

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
    "After a successful implementation handoff, immediately call workflow_prepare_ticket_review with the fixed base, exact final implementation head and agreed command checks. Run its registered checks through the normal Codex command and approval path, bind their exact native item ids with workflow_record_review_checks, then repeat preparation. The host never runs those checks for you.",
    "Use the prepared instructions to spawn one fresh Astra/medium $code-review coordinator, associate its exact child identity with workflow_associate_ticket_review, and require it to delegate fresh independent Standards and Spec axes. Report the coordinator and both exact axis identities with workflow_report_ticket_review.",
    "Validate every reported finding against the source. Record each director judgment separately with workflow_record_review_dispositions. A fixed finding cites the later fresh review of its changed head. For owner acceptance, first send a readable decision prompt that includes the returned review and finding references, then cite the owner's exact user reply. Close the implementation and review child trees natively, then call workflow_resolve_ticket; retry a pending result so its stable GitHub evidence and closure can reconcile before treating the ticket as resolved.",
    `This batch admits at most ${ADMISSION_LIMIT} distinct delivery slices. Failed or blocked admitted slices keep their slot; retry and review reuse it; nested tasks reuse their parent slice. At the limit, stop new admissions, finish or settle admitted work, and wait for a successor.`,
    "Re-read live tracker state before each admission. Do not infer approval from labels, assignment, closure, silence or unavailable evidence.",
    "GitHub assignment is observational and is not a cross-environment atomic lock.",
    "Arbitrary provider collaboration outside the admission RPC cannot be host-enforced; keep all directed delivery inside the callable boundary.",
  ].join("\n");
}

export function workflowTicketResolutionBody(input: {
  readonly resolutionId: string;
  readonly repository: string;
  readonly ticketNumber: number;
  readonly review: WorkflowTicketReviewStatus;
  readonly dispositionReviewIds?: ReadonlyArray<string>;
}) {
  const coordinator = input.review.providerThreadId!;
  const standards = input.review.axes.find((axis) => axis.axis === "standards")!;
  const spec = input.review.axes.find((axis) => axis.axis === "spec")!;
  const dispositions = input.review.findings.map(
    (finding) =>
      `- ${finding.id} (${finding.severity}, ${finding.axis}): ${finding.disposition?.outcome} — ${finding.disposition?.rationale}`,
  );
  return [
    "## Resolution",
    "<!-- t3-workflow:v1 resolution -->",
    "Outcome: resolved",
    `Source: https://github.com/${input.repository}/issues/${input.ticketNumber}`,
    "",
    "### Summary",
    `Committed implementation at ${input.review.implementationHead} passed the registered checks and fresh independent Standards and Spec review.`,
    "",
    "### Evidence",
    `- [Implementation commit](https://github.com/${input.repository}/commit/${input.review.implementationHead})`,
    `- artifact: \`workflow-review:${input.review.reviewId}/checks\``,
    `- thread: \`${coordinator}\` (review coordinator)`,
    `- thread: \`${standards.providerThreadId}\` (Standards)`,
    `- thread: \`${spec.providerThreadId}\` (Spec)`,
    ...[
      ...new Set([
        ...(input.dispositionReviewIds ?? []),
        ...(input.review.findings.length > 0 ? [input.review.reviewId] : []),
      ]),
    ].map((reviewId) => `- artifact: \`workflow-review:${reviewId}/dispositions\``),
    `- artifact: \`workflow-resolution:${input.resolutionId}\``,
    "",
    "### Review results",
    input.review.summary ?? "Independent review completed.",
    ...(dispositions.length > 0 ? ["", ...dispositions] : []),
    "",
    "### Limits and staffing",
    `Requested reviewer ${input.review.requestedProfile.model}/${input.review.requestedProfile.effort}; observed ${input.review.observedProfile.model ?? "unavailable"}/${input.review.observedProfile.effort ?? "unavailable"} (${input.review.observedProfile.match}).`,
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
    readonly prepareTicketReview: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketReviewPrepareInput,
    ) => Effect.Effect<
      WorkflowTicketReviewPrepareResult,
      WorkflowQueryError | WorkflowDirectorError
    >;
    readonly associateTicketReview: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketReviewAssociateInput,
    ) => Effect.Effect<WorkflowTicketReviewStatus, WorkflowDirectorError>;
    readonly recordReviewChecks: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowReviewCheckReceiptInput,
    ) => Effect.Effect<WorkflowTicketReviewStatus, WorkflowDirectorError>;
    readonly reportTicketReview: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketReviewReportInput,
    ) => Effect.Effect<WorkflowTicketReviewStatus, WorkflowDirectorError>;
    readonly recordReviewDispositions: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowReviewDispositionInput,
    ) => Effect.Effect<WorkflowTicketReviewStatus, WorkflowDirectorError>;
    readonly resolveTicket: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketResolveInput,
    ) => Effect.Effect<WorkflowTicketResolveResult, WorkflowQueryError | WorkflowDirectorError>;
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

  const verifiedUserSourceMessage = Effect.fn("WorkflowDirectorService.verifiedUserSourceMessage")(
    function* (source: string) {
      const reference = durableT3SourceReference(source);
      if (!reference) return null;
      const detail = yield* projection
        .getThreadDetailById(ThreadId.make(reference.threadId), { activityKinds: [] })
        .pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "A disposition source thread could not be read.",
              String(error),
            ),
          ),
        );
      if (Option.isNone(detail)) return null;
      const index = detail.value.messages.findIndex(
        (message) => message.id === reference.messageId && message.role === "user",
      );
      if (index < 0) return null;
      return {
        message: detail.value.messages[index]!,
        prompt:
          detail.value.messages
            .slice(0, index)
            .findLast((message) => message.role === "assistant") ?? null,
      };
    },
  );

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

  const observations = Effect.fn("WorkflowDirectorService.observations")(function* (
    directorId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT provider_thread_id AS "providerThreadId",
          parent_provider_thread_id AS "parentProviderThreadId",
          observed_model AS "observedModel", observed_effort AS "observedEffort",
          provider_status AS "providerStatus", native_lifecycle AS "nativeLifecycle",
          first_observed_at AS "firstObservedAt"
        FROM workflow_worker_observations WHERE director_id = ${directorId}
      `,
      "Provider child observations could not be read.",
    );
    return yield* Effect.forEach(rows, (candidate) => decodeObservationRow(candidate)).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "A provider child observation is invalid.",
          String(error),
        ),
      ),
    );
  });

  const verifyIndependentReviewAncestry = Effect.fn(
    "WorkflowDirectorService.verifyIndependentReviewAncestry",
  )(function* (
    row: DirectorRow,
    observationRows: ReadonlyArray<ObservationRow>,
    coordinatorProviderThreadId: string,
    implementationProviderThreadId: string,
  ) {
    const directorIdentities = yield* persistence(
      sql<{ readonly providerThreadId: string | null }>`
        SELECT provider_thread_id AS "providerThreadId"
        FROM projection_thread_sessions
        WHERE thread_id = ${row.threadId}
          AND provider_instance_id = ${row.requestedInstanceId}
        LIMIT 1
      `,
      "The director's native provider identity could not be read.",
    );
    const directorProviderThreadId = directorIdentities[0]?.providerThreadId;
    if (!directorProviderThreadId) {
      return yield* directorError(
        "review-incomplete",
        "The director's exact native provider identity is unavailable.",
      );
    }
    const byId = new Map(
      observationRows.map((observation) => [observation.providerThreadId, observation]),
    );
    let current = byId.get(coordinatorProviderThreadId);
    const visited = new Set([coordinatorProviderThreadId]);
    while (current?.parentProviderThreadId) {
      const parentId = current.parentProviderThreadId;
      if (parentId === implementationProviderThreadId) {
        return yield* directorError(
          "review-incomplete",
          "The review coordinator cannot be an implementation-worker descendant.",
        );
      }
      if (parentId === directorProviderThreadId) return;
      if (visited.has(parentId)) {
        return yield* directorError("review-incomplete", "The review ancestry is cyclic.");
      }
      visited.add(parentId);
      current = byId.get(parentId);
      if (!current) {
        return yield* directorError(
          "review-incomplete",
          "The review ancestry is incomplete and does not reach the director's native identity.",
        );
      }
    }
    return yield* directorError(
      "review-incomplete",
      "The review ancestry does not reach the director's native identity.",
    );
  });

  const reviewStatusFromRow = Effect.fn("WorkflowDirectorService.reviewStatusFromRow")(function* (
    row: ReviewRow,
  ): Effect.fn.Return<WorkflowTicketReviewStatus, WorkflowDirectorError> {
    const observationRows = yield* observations(row.directorId);
    const observationById = new Map(
      observationRows.map((observation) => [observation.providerThreadId, observation]),
    );
    const coordinator = row.providerThreadId
      ? (observationById.get(row.providerThreadId) ?? null)
      : null;
    const rawChecks = yield* persistence(
      sql<Record<string, unknown>>`
          SELECT review_id AS "reviewId", label, command, tool_call_id AS "toolCallId",
            exit_code AS "exitCode", output, started_head AS "startedHead",
            finished_head AS "finishedHead", started_clean AS "startedClean",
            finished_clean AS "finishedClean", verification_status AS "verificationStatus",
            verification_error AS "verificationError", created_at AS "createdAt"
          FROM workflow_review_checks WHERE review_id = ${row.reviewId} ORDER BY rowid
        `,
      "Review check evidence could not be read.",
    );
    const checkRows = yield* Effect.forEach(rawChecks, (candidate) =>
      decodeCheckRow(candidate),
    ).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "Review check evidence is invalid.", String(error)),
      ),
    );
    const checks = checkRows.map((check) => {
      return {
        label: check.label,
        command: check.command,
        toolCallId: check.toolCallId,
        exitCode: check.exitCode,
        output: check.output,
        startedHead: check.startedHead,
        finishedHead: check.finishedHead,
        startedClean: check.startedClean === 1,
        finishedClean: check.finishedClean === null ? null : check.finishedClean === 1,
        status: check.verificationStatus as "pending" | "passed" | "failed",
        verificationError: check.verificationError,
      };
    });
    const rawAxes = yield* persistence(
      sql<{ readonly axis: string; readonly providerThreadId: string }>`
          SELECT axis, provider_thread_id AS "providerThreadId"
          FROM workflow_review_axes WHERE review_id = ${row.reviewId} ORDER BY axis
        `,
      "Review identities could not be read.",
    );
    const axes = rawAxes.flatMap((axis) => {
      const observed = observationById.get(axis.providerThreadId);
      if (!observed || (axis.axis !== "standards" && axis.axis !== "spec")) return [];
      const match =
        !observed.observedModel || !observed.observedEffort
          ? "unknown"
          : observed.observedModel === REVIEWER_MODEL && observed.observedEffort === REVIEWER_EFFORT
            ? "match"
            : "mismatch";
      return [
        {
          axis: axis.axis,
          providerThreadId: axis.providerThreadId,
          parentProviderThreadId: observed.parentProviderThreadId,
          providerStatus: observed.providerStatus,
          settlementEvidence: observed.nativeLifecycle === "closed" ? "native-closed" : null,
          observedProfile: {
            model: observed.observedModel,
            effort: observed.observedEffort,
            match,
          },
        } as const,
      ];
    });
    const rawFindings = yield* persistence(
      sql<Record<string, unknown>>`
          SELECT review_id AS "reviewId", finding_id AS "findingId", axis, severity, summary,
            location, disposition, disposition_rationale AS "dispositionRationale",
            disposition_evidence_source AS "dispositionEvidenceSource",
            disposition_evidence_quote AS "dispositionEvidenceQuote",
            resulting_review_id AS "resultingReviewId"
          FROM workflow_review_findings WHERE review_id = ${row.reviewId} ORDER BY rowid
        `,
      "Review findings could not be read.",
    );
    const findingRows = yield* Effect.forEach(rawFindings, (candidate) =>
      decodeFindingRow(candidate),
    ).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "Review findings are invalid.", String(error)),
      ),
    );
    const findings = findingRows.map(
      (finding) =>
        ({
          id: finding.findingId,
          axis: finding.axis as WorkflowReviewFinding["axis"],
          severity: finding.severity as WorkflowReviewFinding["severity"],
          summary: finding.summary,
          location: finding.location,
          disposition:
            finding.disposition && finding.dispositionRationale
              ? {
                  outcome: finding.disposition as NonNullable<
                    WorkflowReviewFinding["disposition"]
                  >["outcome"],
                  rationale: finding.dispositionRationale,
                  evidenceSource: finding.dispositionEvidenceSource,
                  evidenceQuote: finding.dispositionEvidenceQuote,
                  resultingReviewId: finding.resultingReviewId,
                }
              : null,
        }) satisfies WorkflowReviewFinding,
    );
    const match =
      !coordinator?.observedModel || !coordinator.observedEffort
        ? "unknown"
        : coordinator.observedModel === REVIEWER_MODEL &&
            coordinator.observedEffort === REVIEWER_EFFORT
          ? "match"
          : "mismatch";
    return {
      reviewId: row.reviewId,
      admissionId: row.admissionId,
      ticketNumber: row.ticketNumber,
      implementationProviderThreadId: yield* persistence(
        sql<{ readonly providerThreadId: string }>`
            SELECT provider_thread_id AS "providerThreadId" FROM workflow_worker_dispatches
            WHERE dispatch_id = ${row.implementationDispatchId} LIMIT 1
          `,
        "The implementation identity for this review could not be read.",
      ).pipe(
        Effect.flatMap((identity) =>
          identity[0]?.providerThreadId
            ? Effect.succeed(identity[0].providerThreadId)
            : Effect.fail(
                directorError(
                  "persistence-failed",
                  "The reviewed implementation identity is missing.",
                ),
              ),
        ),
      ),
      fixedBase: row.fixedBase,
      implementationHead: row.implementationHead,
      status: row.status as WorkflowTicketReviewStatus["status"],
      association: row.providerThreadId ? "associated" : "unconfirmed",
      providerThreadId: row.providerThreadId,
      parentProviderThreadId: coordinator?.parentProviderThreadId ?? null,
      providerStatus: coordinator?.providerStatus ?? "unconfirmed",
      settlementEvidence: coordinator?.nativeLifecycle === "closed" ? "native-closed" : null,
      requestedProfile: {
        model: row.requestedModel,
        effort: row.requestedEffort,
        skillPath: row.requestedSkillPath,
      },
      observedProfile: {
        model: coordinator?.observedModel ?? null,
        effort: coordinator?.observedEffort ?? null,
        match,
      },
      checks,
      axes,
      findings,
      summary: row.reportSummary,
      updatedAt: row.updatedAt,
    };
  });

  const reviews = Effect.fn("WorkflowDirectorService.reviews")(function* (directorId: string) {
    const rawRows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT review_id AS "reviewId", association_token AS "associationToken",
          director_id AS "directorId", admission_id AS "admissionId",
          implementation_dispatch_id AS "implementationDispatchId", ticket_number AS "ticketNumber",
          fixed_base AS "fixedBase", implementation_head AS "implementationHead",
          scope_body AS "scopeBody", requested_model AS "requestedModel",
          requested_effort AS "requestedEffort", requested_skill_path AS "requestedSkillPath",
          provider_thread_id AS "providerThreadId", status, report_summary AS "reportSummary",
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM workflow_ticket_reviews WHERE director_id = ${directorId} ORDER BY created_at
      `,
      "Review history could not be read.",
    );
    const rows = yield* Effect.forEach(rawRows, (candidate) => decodeReviewRow(candidate)).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "A review history row is invalid.", String(error)),
      ),
    );
    return yield* Effect.forEach(rows, reviewStatusFromRow);
  });

  const loadReview = Effect.fn("WorkflowDirectorService.loadReview")(function* (
    directorId: string,
    reviewId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT review_id AS "reviewId", association_token AS "associationToken",
          director_id AS "directorId", admission_id AS "admissionId",
          implementation_dispatch_id AS "implementationDispatchId", ticket_number AS "ticketNumber",
          fixed_base AS "fixedBase", implementation_head AS "implementationHead",
          scope_body AS "scopeBody", requested_model AS "requestedModel",
          requested_effort AS "requestedEffort", requested_skill_path AS "requestedSkillPath",
          provider_thread_id AS "providerThreadId", status, report_summary AS "reportSummary",
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM workflow_ticket_reviews WHERE director_id = ${directorId} AND review_id = ${reviewId}
        LIMIT 1
      `,
      "The ticket review could not be read.",
    );
    if (!rows[0]) {
      return yield* directorError(
        "review-incomplete",
        "The ticket review is unknown to this director.",
      );
    }
    return yield* decodeReviewRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "The ticket review record is invalid.", String(error)),
      ),
    );
  });

  const resolutionStatusFromRow = (row: ResolutionRow): WorkflowTicketResolutionStatus => ({
    resolutionId: row.resolutionId,
    reviewId: row.reviewId,
    ticketNumber: row.ticketNumber,
    finalHead: row.finalHead,
    status: row.status as WorkflowTicketResolutionStatus["status"],
    commentUrl: row.commentUrl,
    lastError: row.lastError,
    readyIssueIds: row.frontierJson ? decodeStringArrayJsonSync(row.frontierJson) : [],
    updatedAt: row.updatedAt,
  });

  const resolutions = Effect.fn("WorkflowDirectorService.resolutions")(function* (
    directorId: string,
  ) {
    const rawRows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT resolution_id AS "resolutionId", review_id AS "reviewId",
          ticket_number AS "ticketNumber", final_head AS "finalHead",
          comment_body AS "commentBody", status,
          comment_url AS "commentUrl", frontier_json AS "frontierJson",
          last_error AS "lastError", updated_at AS "updatedAt"
        FROM workflow_ticket_resolution_intents WHERE director_id = ${directorId} ORDER BY created_at
      `,
      "Ticket resolution history could not be read.",
    );
    const rows = yield* Effect.forEach(rawRows, (candidate) => decodeResolutionRow(candidate)).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "A ticket resolution row is invalid.", String(error)),
      ),
    );
    return rows.map(resolutionStatusFromRow);
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
      reviews: yield* reviews(row.directorId),
      resolutions: yield* resolutions(row.directorId),
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

  const reviewerPreflight = Effect.fn("WorkflowDirectorService.reviewerPreflight")(function* (
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
          directorError("provider-unavailable", "Review provider preflight failed.", String(error)),
        ),
      );
    const model = provider?.models.find((candidate) => candidate.slug === REVIEWER_MODEL);
    const effort = model?.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort",
    );
    const skills = provider?.skills.filter(
      (skill) => skill.name === "code-review" && skill.enabled,
    );
    if (
      provider?.driver !== ProviderDriverKind.make("codex") ||
      !model ||
      effort?.type !== "select" ||
      !effort.options.some((option) => option.id === REVIEWER_EFFORT) ||
      skills?.length !== 1
    ) {
      return yield* directorError(
        "provider-unavailable",
        "The requested Astra/medium reviewer and one enabled code-review skill must pass preflight before dispatch.",
      );
    }
    return { skillPath: skills[0]!.path };
  });

  const gitObservation = Effect.fn("WorkflowDirectorService.gitObservation")(function* (
    cwd: string,
  ) {
    const head = yield* processRunner
      .run({
        command: "git",
        args: ["rev-parse", "HEAD"],
        cwd,
        maxOutputBytes: 4_096,
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The implementation HEAD could not be read.",
            error.message,
          ),
        ),
      );
    const status = yield* processRunner
      .run({
        command: "git",
        args: ["status", "--porcelain", "--untracked-files=normal"],
        cwd,
        maxOutputBytes: 100_000,
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The capability worktree status could not be read.",
            error.message,
          ),
        ),
      );
    if (head.code !== 0 || head.timedOut || status.code !== 0 || status.timedOut) {
      return yield* directorError(
        "workspace-unavailable",
        "The capability worktree could not be verified.",
        head.stderr.trim() || status.stderr.trim(),
      );
    }
    return { head: head.stdout.trim(), clean: status.stdout.trim().length === 0 };
  });

  const verifyGitRange = Effect.fn("WorkflowDirectorService.verifyGitRange")(function* (
    cwd: string,
    fixedBase: string,
    implementationHead: string,
  ) {
    if (!/^[0-9a-f]{40}$/iu.test(fixedBase) || !/^[0-9a-f]{40}$/iu.test(implementationHead)) {
      return yield* directorError(
        "stale-review",
        "Review requires full fixed-base and implementation commit hashes.",
      );
    }
    const range = yield* processRunner
      .run({
        command: "git",
        args: ["merge-base", "--is-ancestor", fixedBase, implementationHead],
        cwd,
        maxOutputBytes: 4_096,
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The review commit range could not be verified.",
            error.message,
          ),
        ),
      );
    const observed = yield* gitObservation(cwd);
    if (
      range.code !== 0 ||
      range.timedOut ||
      observed.head !== implementationHead ||
      !observed.clean
    ) {
      return yield* directorError(
        "stale-review",
        "Review requires a committed, clean implementation at the exact final head.",
        `Expected ${implementationHead}; observed ${observed.head}${observed.clean ? "" : " with uncommitted changes"}.`,
      );
    }
    return observed;
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

  const reviewPreparationInstructions = (
    review: WorkflowTicketReviewStatus,
    associationToken: string,
  ) =>
    [
      `Spawn one fresh review coordinator for ticket #${review.ticketNumber}.`,
      `Request model ${REVIEWER_MODEL}, reasoning effort ${REVIEWER_EFFORT}, and $code-review at ${review.requestedProfile.skillPath}.`,
      `Review exactly ${review.fixedBase}...${review.implementationHead} with independent Standards and Spec axes.`,
      "Both axes must use fresh native children of the coordinator. Record their exact provider thread ids; titles are not identities.",
      `After native spawn, call workflow_associate_ticket_review with token ${associationToken}.`,
      "After both axes return, call workflow_report_ticket_review. Then record director dispositions separately before resolution.",
    ].join("\n");

  const prepareTicketReviewUnlocked = Effect.fn("WorkflowDirectorService.prepareTicketReview")(
    function* (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketReviewPrepareInput,
    ) {
      const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
      const labels = input.checks.map((check) => check.label);
      if (new Set(labels).size !== labels.length) {
        return yield* directorError("checks-failed", "Agreed review check labels must be unique.");
      }
      const currentWorkers = yield* workers(row.directorId);
      const implementation = currentWorkers.find(
        (worker) =>
          worker.providerThreadId === input.implementationProviderThreadId &&
          worker.ticketNumber === input.ticketNumber &&
          worker.association === "associated",
      );
      if (
        !implementation?.dispatchId ||
        !implementation.admissionId ||
        implementation.handoff?.outcome !== "succeeded" ||
        !implementation.handoff.commits.includes(input.implementationHead)
      ) {
        return yield* directorError(
          "review-incomplete",
          "Review requires an associated successful implementation handoff that names the final commit.",
        );
      }
      const admission = (yield* admissions(row.directorId)).find(
        (candidate) => candidate.admissionId === implementation.admissionId,
      );
      if (!admission) {
        return yield* directorError(
          "review-incomplete",
          "The implementation admission is missing.",
        );
      }
      const admissionResult = yield* admitUnlocked({
        projectId: ProjectId.make(row.projectId),
        directorId: row.directorId,
        repository: row.repository as WorkflowDirectorAdmissionInput["repository"],
        ticketNumber: input.ticketNumber,
        purpose: "review",
        ownership: admission.ownership,
      });
      if (!admissionResult.admission || admissionResult.admission.claimStatus !== "confirmed") {
        return yield* directorError(
          "claim-failed",
          "Ticket ownership must remain confirmed before review.",
          admissionResult.message,
        );
      }
      const issue = yield* workflow.issueDetail({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowIssueSummary["repository"],
        number: input.ticketNumber,
      });
      if (issue.kind !== "ticket") {
        return yield* directorError(
          "not-ready",
          "Only an approved delivery ticket can be reviewed.",
        );
      }
      yield* verifyGitRange(row.worktreePath, input.fixedBase, input.implementationHead);
      const { skillPath } = yield* reviewerPreflight(row);

      const existingRows = yield* persistence(
        sql<Record<string, unknown>>`
          SELECT review_id AS "reviewId", association_token AS "associationToken",
            director_id AS "directorId", admission_id AS "admissionId",
            implementation_dispatch_id AS "implementationDispatchId", ticket_number AS "ticketNumber",
            fixed_base AS "fixedBase", implementation_head AS "implementationHead",
            scope_body AS "scopeBody", requested_model AS "requestedModel",
            requested_effort AS "requestedEffort", requested_skill_path AS "requestedSkillPath",
            provider_thread_id AS "providerThreadId", status, report_summary AS "reportSummary",
            created_at AS "createdAt", updated_at AS "updatedAt"
          FROM workflow_ticket_reviews
          WHERE director_id = ${row.directorId} AND admission_id = ${admission.admissionId}
            AND implementation_head = ${input.implementationHead}
          ORDER BY created_at DESC LIMIT 1
        `,
        "Existing ticket review could not be read.",
      );
      if (existingRows[0]) {
        const existing = yield* decodeReviewRow(existingRows[0]).pipe(
          Effect.mapError((error) =>
            directorError("persistence-failed", "The existing review is invalid.", String(error)),
          ),
        );
        if (existing.fixedBase !== input.fixedBase || existing.scopeBody !== issue.body) {
          return yield* directorError(
            "stale-review",
            "The existing review does not cover the current base or ticket scope.",
          );
        }
        const storedChecks = (yield* reviewStatusFromRow(existing)).checks;
        if (
          storedChecks.length !== input.checks.length ||
          storedChecks.some(
            (check) =>
              !input.checks.some(
                (candidate) =>
                  candidate.label === check.label && candidate.command === check.command,
              ),
          )
        ) {
          return yield* directorError(
            "checks-failed",
            "The agreed checks differ from the checks already registered for this review.",
          );
        }
        const review = yield* reviewStatusFromRow(existing);
        const checksReady = review.checks.every((check) => check.status === "passed");
        if (checksReady && existing.status === "prepared") {
          const updatedAt = DateTime.formatIso(yield* DateTime.now);
          yield* persistence(
            sql`
              UPDATE workflow_ticket_reviews SET status = 'spawn-issued', updated_at = ${updatedAt}
              WHERE review_id = ${existing.reviewId}
            `,
            "The durable review launch instruction could not be saved.",
          );
          const issued = yield* reviewStatusFromRow(
            yield* loadReview(row.directorId, existing.reviewId),
          );
          return {
            disposition: "prepared",
            associationToken: existing.associationToken,
            taskName: `review-${input.ticketNumber}-${existing.reviewId.slice(0, 8)}`,
            instructions: reviewPreparationInstructions(issued, existing.associationToken),
            review: issued,
          } satisfies WorkflowTicketReviewPrepareResult;
        }
        return {
          disposition: checksReady && existing.providerThreadId ? "existing" : "held",
          associationToken: existing.associationToken,
          taskName: `review-${input.ticketNumber}-${existing.reviewId.slice(0, 8)}`,
          instructions:
            existing.status === "spawn-issued"
              ? `The review launch was already issued with token ${existing.associationToken}. Reconcile the exact native child and call workflow_associate_ticket_review; do not spawn a duplicate.`
              : existing.status === "associated"
                ? `Review coordinator ${existing.providerThreadId} is already associated. Wait for its fresh Standards and Spec axes, then call workflow_report_ticket_review.`
                : existing.status === "reported"
                  ? "The independent report is already durable. Validate findings, record remaining dispositions, close the child trees natively, and resolve the ticket."
                  : [
                      "Run each registered check through the normal Codex command path and approve it according to the provider policy.",
                      ...review.checks
                        .filter((check) => check.status !== "passed")
                        .map((check) => `- ${check.label}: ${check.command}`),
                      `Then call workflow_record_review_checks for review ${review.reviewId} with each exact native toolCallId, and repeat workflow_prepare_ticket_review.`,
                    ].join("\n"),
          review,
        } satisfies WorkflowTicketReviewPrepareResult;
      }

      const previousReviews = yield* reviews(row.directorId);
      const unsettled = previousReviews.find(
        (review) =>
          review.ticketNumber === input.ticketNumber &&
          review.implementationHead !== input.implementationHead &&
          (review.status === "spawn-issued" ||
            review.status === "associated" ||
            review.status === "reported" ||
            review.providerThreadId !== null ||
            review.axes.length > 0) &&
          (review.settlementEvidence !== "native-closed" ||
            review.axes.some((axis) => axis.settlementEvidence !== "native-closed")),
      );
      if (unsettled) {
        return yield* directorError(
          "review-incomplete",
          "A prior review attempt for this ticket still has live or unconfirmed provider work.",
        );
      }

      const git = yield* gitObservation(row.worktreePath);
      const reviewId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const associationToken = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              INSERT INTO workflow_ticket_reviews (
                review_id, association_token, director_id, batch_id, admission_id,
                implementation_dispatch_id, repository, ticket_number, fixed_base,
                implementation_head, scope_body, requested_model, requested_effort,
                requested_skill_path, status, created_at, updated_at
              ) VALUES (
                ${reviewId}, ${associationToken}, ${row.directorId}, ${row.batchId},
                ${admission.admissionId}, ${implementation.dispatchId}, ${row.repository},
                ${input.ticketNumber}, ${input.fixedBase}, ${input.implementationHead}, ${issue.body},
                ${REVIEWER_MODEL}, ${REVIEWER_EFFORT}, ${skillPath}, 'checks-pending',
                ${createdAt}, ${createdAt}
              )
            `;
            for (const check of input.checks) {
              yield* sql`
                INSERT INTO workflow_review_checks (
                  review_id, label, command, output, started_head, started_clean,
                  verification_status, thread_id, provider_instance_id, created_at, updated_at
                ) VALUES (
                  ${reviewId}, ${check.label}, ${check.command}, '', ${git.head},
                  ${git.clean ? 1 : 0}, 'pending', ${row.threadId}, ${row.requestedInstanceId},
                  ${createdAt}, ${createdAt}
                )
              `;
            }
          }),
        ),
        "The review and its agreed checks could not be registered.",
      );
      const review = (yield* reviews(row.directorId)).find(
        (candidate) => candidate.reviewId === reviewId,
      )!;
      return {
        disposition: "held",
        associationToken,
        taskName: `review-${input.ticketNumber}-${reviewId.slice(0, 8)}`,
        instructions: [
          "Run each registered check through the normal Codex command path and approve it according to the provider policy.",
          ...input.checks.map((check) => `- ${check.label}: ${check.command}`),
          `Then call workflow_record_review_checks for review ${reviewId} with each exact native toolCallId, and repeat workflow_prepare_ticket_review.`,
        ].join("\n"),
        review,
      } satisfies WorkflowTicketReviewPrepareResult;
    },
  );

  const recordReviewChecksUnlocked = Effect.fn("WorkflowDirectorService.recordReviewChecks")(
    function* (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowReviewCheckReceiptInput,
    ) {
      const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
      const reviewRow = yield* loadReview(row.directorId, input.reviewId);
      const receiptLabels = input.receipts.map((receipt) => receipt.label);
      const receiptIds = input.receipts.map((receipt) => receipt.toolCallId);
      if (
        new Set(receiptLabels).size !== receiptLabels.length ||
        new Set(receiptIds).size !== receiptIds.length
      ) {
        return yield* directorError(
          "checks-failed",
          "Each check label and native toolCallId may be bound only once per request.",
        );
      }
      for (const receipt of input.receipts) {
        const checkRows = yield* persistence(
          sql<Record<string, unknown>>`
            SELECT review_id AS "reviewId", label, command, tool_call_id AS "toolCallId",
              exit_code AS "exitCode", output, started_head AS "startedHead",
              finished_head AS "finishedHead", started_clean AS "startedClean",
              finished_clean AS "finishedClean", verification_status AS "verificationStatus",
              verification_error AS "verificationError", created_at AS "createdAt"
            FROM workflow_review_checks
            WHERE review_id = ${reviewRow.reviewId} AND label = ${receipt.label} LIMIT 1
          `,
          "The registered check could not be read.",
        );
        if (!checkRows[0]) {
          return yield* directorError(
            "checks-failed",
            `No registered check is named ${receipt.label}.`,
          );
        }
        const check = yield* decodeCheckRow(checkRows[0]).pipe(
          Effect.mapError((error) =>
            directorError("persistence-failed", "The registered check is invalid.", String(error)),
          ),
        );
        if (
          check.verificationStatus === "passed" &&
          check.toolCallId &&
          check.toolCallId !== receipt.toolCallId
        ) {
          return yield* directorError(
            "checks-failed",
            `Check ${receipt.label} is already bound to a different native command receipt.`,
          );
        }
        if (check.verificationStatus === "passed" && check.toolCallId === receipt.toolCallId) {
          continue;
        }
        const observationRows = yield* persistence(
          sql<Record<string, unknown>>`
            SELECT lifecycle, command, cwd, status, exit_code AS "exitCode", output,
              created_at AS "createdAt"
            FROM workflow_native_command_observations
            WHERE thread_id = ${row.threadId} AND provider_instance_id = ${providerInstanceId}
              AND tool_call_id = ${receipt.toolCallId}
              AND created_at > ${check.createdAt}
            ORDER BY created_at
          `,
          "The native check receipt could not be read.",
        );
        const nativeRows = yield* Effect.forEach(observationRows, (candidate) =>
          decodeNativeCommandRow(candidate),
        ).pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Native command evidence is invalid.",
              String(error),
            ),
          ),
        );
        const started = nativeRows.find((candidate) => candidate.lifecycle === "started");
        const completed = nativeRows.findLast((candidate) => candidate.lifecycle === "completed");
        if (!started || !completed || started.createdAt > completed.createdAt) {
          return yield* directorError(
            "review-incomplete",
            `Native start and completion for check ${receipt.label} are not both durably observed after registration.`,
          );
        }
        const git = yield* gitObservation(row.worktreePath);
        const exitCode = completed.exitCode;
        const accepted =
          started.command === check.command &&
          completed.command === check.command &&
          started.cwd === row.worktreePath &&
          completed.cwd === row.worktreePath &&
          completed.status === "completed" &&
          exitCode === 0 &&
          check.startedHead === reviewRow.implementationHead &&
          check.startedClean === 1 &&
          git.head === reviewRow.implementationHead &&
          git.clean;
        const verificationError = accepted
          ? null
          : started.command !== check.command || completed.command !== check.command
            ? "The observed native command does not match the registered command."
            : started.cwd !== row.worktreePath || completed.cwd !== row.worktreePath
              ? "The observed native command ran outside the capability worktree."
              : completed.status !== "completed" || exitCode === null
                ? "The native command completion status or exit code is unknown."
                : exitCode !== 0
                  ? `The native command exited with code ${exitCode}.`
                  : check.startedHead !== reviewRow.implementationHead ||
                      git.head !== reviewRow.implementationHead
                    ? "The implementation HEAD changed while checks ran."
                    : "The capability worktree was dirty before or after the check.";
        const updatedAt = DateTime.formatIso(yield* DateTime.now);
        yield* persistence(
          sql`
            UPDATE workflow_review_checks SET tool_call_id = ${receipt.toolCallId},
              exit_code = ${exitCode}, output = ${completed.output},
              finished_head = ${git.head}, finished_clean = ${git.clean ? 1 : 0},
              verification_status = ${accepted ? "passed" : "failed"},
              verification_error = ${verificationError}, native_started_at = ${started.createdAt},
              native_completed_at = ${completed.createdAt},
              updated_at = ${updatedAt}
            WHERE review_id = ${reviewRow.reviewId} AND label = ${receipt.label}
          `,
          "The verified native check receipt could not be saved.",
        );
        yield* persistence(
          sql`
            DELETE FROM workflow_native_command_observations
            WHERE thread_id = ${row.threadId} AND provider_instance_id = ${providerInstanceId}
              AND tool_call_id = ${receipt.toolCallId}
          `,
          "The compacted native check observation could not be released.",
        );
        if (!accepted) {
          yield* persistence(
            sql`
              UPDATE workflow_ticket_reviews SET status = 'checks-failed', updated_at = ${updatedAt}
              WHERE review_id = ${reviewRow.reviewId}
            `,
            "The failed check state could not be saved.",
          );
        }
      }
      const review = yield* reviewStatusFromRow(yield* loadReview(row.directorId, input.reviewId));
      const status = review.checks.every((check) => check.status === "passed")
        ? reviewRow.status === "checks-pending" || reviewRow.status === "checks-failed"
          ? "prepared"
          : reviewRow.status
        : review.checks.some((check) => check.status === "failed")
          ? "checks-failed"
          : "checks-pending";
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`
          UPDATE workflow_ticket_reviews SET status = ${status}, updated_at = ${updatedAt}
          WHERE review_id = ${review.reviewId}
        `,
        "The review check state could not be saved.",
      );
      return yield* reviewStatusFromRow(yield* loadReview(row.directorId, input.reviewId));
    },
  );

  const associateTicketReviewUnlocked = Effect.fn("WorkflowDirectorService.associateTicketReview")(
    function* (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketReviewAssociateInput,
    ) {
      const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
      const reviewRows = yield* persistence(
        sql<{ readonly reviewId: string }>`
        SELECT review_id AS "reviewId" FROM workflow_ticket_reviews
        WHERE director_id = ${row.directorId} AND association_token = ${input.associationToken}
        LIMIT 1
      `,
        "The review association token could not be read.",
      );
      if (!reviewRows[0]) {
        return yield* directorError(
          "review-incomplete",
          "This review association token is unknown or belongs to another director.",
        );
      }
      const reviewRow = yield* loadReview(row.directorId, reviewRows[0].reviewId);
      if (reviewRow.providerThreadId && reviewRow.providerThreadId !== input.providerThreadId) {
        return yield* directorError(
          "review-incomplete",
          "This review is already associated with another exact provider child.",
        );
      }
      if (
        !reviewRow.providerThreadId &&
        reviewRow.status !== "spawn-issued" &&
        reviewRow.status !== "associated"
      ) {
        return yield* directorError(
          "checks-failed",
          "A reviewer cannot be associated until every registered check is verified.",
        );
      }
      const observationRows = yield* observations(row.directorId);
      const observed = observationRows.find(
        (candidate) => candidate.providerThreadId === input.providerThreadId,
      );
      const implementation = (yield* workers(row.directorId)).find(
        (worker) => worker.dispatchId === reviewRow.implementationDispatchId,
      );
      if (
        !observed ||
        !implementation?.providerThreadId ||
        input.providerThreadId === implementation.providerThreadId ||
        observed.firstObservedAt < reviewRow.createdAt ||
        observed.observedModel !== REVIEWER_MODEL ||
        observed.observedEffort !== REVIEWER_EFFORT
      ) {
        return yield* directorError(
          "review-incomplete",
          `Review association requires a fresh exact ${REVIEWER_MODEL}/${REVIEWER_EFFORT} child observed under this director and distinct from its implementation worker.`,
        );
      }
      yield* verifyIndependentReviewAncestry(
        row,
        observationRows,
        input.providerThreadId,
        implementation.providerThreadId,
      );
      const workerReuse = (yield* workers(row.directorId)).some(
        (worker) => worker.dispatchId && worker.providerThreadId === input.providerThreadId,
      );
      const reviewReuse = yield* persistence(
        sql<{ readonly reviewId: string }>`
        SELECT review_id AS "reviewId" FROM workflow_ticket_reviews
        WHERE director_id = ${row.directorId} AND provider_thread_id = ${input.providerThreadId}
          AND review_id <> ${reviewRow.reviewId} LIMIT 1
      `,
        "Existing reviewer identities could not be read.",
      );
      if (workerReuse || reviewReuse[0]) {
        return yield* directorError(
          "review-incomplete",
          "Reviewer identity must be fresh and independent of prior implementation or review work.",
        );
      }
      if (reviewRow.providerThreadId) return yield* reviewStatusFromRow(reviewRow);
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`
        UPDATE workflow_ticket_reviews SET provider_thread_id = ${input.providerThreadId},
          status = 'associated', updated_at = ${updatedAt}
        WHERE review_id = ${reviewRow.reviewId}
      `,
        "The exact reviewer identity could not be saved.",
      );
      return yield* reviewStatusFromRow(yield* loadReview(row.directorId, reviewRow.reviewId));
    },
  );

  const reportTicketReviewUnlocked = Effect.fn("WorkflowDirectorService.reportTicketReview")(
    function* (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowTicketReviewReportInput,
    ) {
      const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
      const reviewRows = yield* persistence(
        sql<{ readonly reviewId: string }>`
          SELECT review_id AS "reviewId" FROM workflow_ticket_reviews
          WHERE director_id = ${row.directorId} AND provider_thread_id = ${input.providerThreadId}
          LIMIT 1
        `,
        "The associated ticket review could not be read.",
      );
      if (!reviewRows[0]) {
        return yield* directorError(
          "review-incomplete",
          "Report review evidence only for an explicitly associated reviewer.",
        );
      }
      const reviewRow = yield* loadReview(row.directorId, reviewRows[0].reviewId);
      if (reviewRow.status !== "associated" && reviewRow.status !== "reported") {
        return yield* directorError("review-incomplete", "This reviewer is not ready to report.");
      }
      if (
        input.standardsReviewerThreadId === input.specReviewerThreadId ||
        input.standardsReviewerThreadId === input.providerThreadId ||
        input.specReviewerThreadId === input.providerThreadId
      ) {
        return yield* directorError(
          "review-incomplete",
          "Standards and Spec require distinct fresh child reviewer identities.",
        );
      }
      const findingIds = input.findings.map((finding) => finding.id);
      if (new Set(findingIds).size !== findingIds.length) {
        return yield* directorError("review-incomplete", "Review finding ids must be unique.");
      }
      const observationRows = yield* observations(row.directorId);
      const byId = new Map(
        observationRows.map((observation) => [observation.providerThreadId, observation]),
      );
      const standards = byId.get(input.standardsReviewerThreadId);
      const spec = byId.get(input.specReviewerThreadId);
      if (
        !standards ||
        !spec ||
        standards.parentProviderThreadId !== input.providerThreadId ||
        spec.parentProviderThreadId !== input.providerThreadId ||
        standards.firstObservedAt < reviewRow.createdAt ||
        spec.firstObservedAt < reviewRow.createdAt ||
        standards.observedModel !== REVIEWER_MODEL ||
        standards.observedEffort !== REVIEWER_EFFORT ||
        spec.observedModel !== REVIEWER_MODEL ||
        spec.observedEffort !== REVIEWER_EFFORT
      ) {
        return yield* directorError(
          "review-incomplete",
          "Both review axes must be fresh exact native children of this review coordinator.",
        );
      }
      const implementationIds = new Set(
        (yield* workers(row.directorId)).flatMap((worker) =>
          worker.dispatchId && worker.providerThreadId ? [worker.providerThreadId] : [],
        ),
      );
      if (
        implementationIds.has(input.standardsReviewerThreadId) ||
        implementationIds.has(input.specReviewerThreadId)
      ) {
        return yield* directorError(
          "review-incomplete",
          "Review axes cannot reuse implementation identities.",
        );
      }
      const reusedAxes = yield* persistence(
        sql<{ readonly providerThreadId: string }>`
          SELECT a.provider_thread_id AS "providerThreadId" FROM workflow_review_axes a
          JOIN workflow_ticket_reviews r ON r.review_id = a.review_id
          WHERE r.director_id = ${row.directorId} AND a.review_id <> ${reviewRow.reviewId}
            AND a.provider_thread_id IN (${input.standardsReviewerThreadId}, ${input.specReviewerThreadId})
        `,
        "Prior review axis identities could not be read.",
      );
      if (reusedAxes.length > 0) {
        return yield* directorError(
          "review-incomplete",
          "Standards and Spec reviewer identities must be fresh for this implementation head.",
        );
      }
      if (reviewRow.status === "reported") {
        const existing = yield* reviewStatusFromRow(reviewRow);
        const sameAxes =
          existing.axes.some(
            (axis) =>
              axis.axis === "standards" &&
              axis.providerThreadId === input.standardsReviewerThreadId,
          ) &&
          existing.axes.some(
            (axis) => axis.axis === "spec" && axis.providerThreadId === input.specReviewerThreadId,
          );
        const sameFindings =
          existing.findings.length === input.findings.length &&
          existing.findings.every((finding) =>
            input.findings.some(
              (candidate) =>
                candidate.id === finding.id &&
                candidate.axis === finding.axis &&
                candidate.severity === finding.severity &&
                candidate.summary === finding.summary &&
                (candidate.location ?? null) === finding.location,
            ),
          );
        if (sameAxes && sameFindings && existing.summary === input.summary) return existing;
        return yield* directorError(
          "review-incomplete",
          "This review already has different durable report evidence.",
        );
      }
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              INSERT INTO workflow_review_axes (review_id, axis, provider_thread_id)
              VALUES (${reviewRow.reviewId}, 'standards', ${input.standardsReviewerThreadId})
            `;
            yield* sql`
              INSERT INTO workflow_review_axes (review_id, axis, provider_thread_id)
              VALUES (${reviewRow.reviewId}, 'spec', ${input.specReviewerThreadId})
            `;
            for (const finding of input.findings) {
              yield* sql`
                INSERT INTO workflow_review_findings (
                  review_id, finding_id, axis, severity, summary, location, updated_at
                ) VALUES (
                  ${reviewRow.reviewId}, ${finding.id}, ${finding.axis}, ${finding.severity},
                  ${finding.summary}, ${finding.location ?? null}, ${updatedAt}
                )
              `;
            }
            yield* sql`
              UPDATE workflow_ticket_reviews SET status = 'reported',
                report_summary = ${input.summary}, updated_at = ${updatedAt}
              WHERE review_id = ${reviewRow.reviewId}
            `;
          }),
        ),
        "The independent review report could not be saved.",
      );
      return yield* reviewStatusFromRow(yield* loadReview(row.directorId, reviewRow.reviewId));
    },
  );

  const recordReviewDispositionsUnlocked = Effect.fn(
    "WorkflowDirectorService.recordReviewDispositions",
  )(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowReviewDispositionInput,
  ) {
    const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    const reviewRow = yield* loadReview(row.directorId, input.reviewId);
    if (reviewRow.status !== "reported") {
      return yield* directorError(
        "review-incomplete",
        "Director dispositions require a durable independent review report.",
      );
    }
    const ids = input.dispositions.map((disposition) => disposition.findingId);
    if (new Set(ids).size !== ids.length) {
      return yield* directorError(
        "review-incomplete",
        "Each finding may be disposed once per request.",
      );
    }
    const existing = yield* reviewStatusFromRow(reviewRow);
    for (const disposition of input.dispositions) {
      const finding = existing.findings.find((candidate) => candidate.id === disposition.findingId);
      if (!finding) {
        return yield* directorError(
          "review-incomplete",
          `Finding ${disposition.findingId} is not part of this review.`,
        );
      }
      if (
        finding.disposition &&
        (finding.disposition.outcome !== disposition.outcome ||
          finding.disposition.rationale !== disposition.rationale ||
          finding.disposition.evidenceSource !== (disposition.evidenceSource ?? null) ||
          finding.disposition.evidenceQuote !== (disposition.evidenceQuote ?? null) ||
          finding.disposition.resultingReviewId !== (disposition.resultingReviewId ?? null))
      ) {
        return yield* directorError(
          "review-incomplete",
          `Finding ${disposition.findingId} already has a different durable disposition.`,
        );
      }
      if (disposition.outcome === "fixed") {
        if (
          !disposition.resultingReviewId ||
          disposition.resultingReviewId === reviewRow.reviewId
        ) {
          return yield* directorError(
            "review-incomplete",
            "A fixed finding requires a distinct later review of the resulting implementation head.",
          );
        }
        const resultingRow = yield* loadReview(row.directorId, disposition.resultingReviewId);
        const resulting = yield* reviewStatusFromRow(resultingRow);
        if (
          resulting.ticketNumber !== existing.ticketNumber ||
          resultingRow.createdAt <= reviewRow.createdAt ||
          resulting.implementationHead === existing.implementationHead ||
          resulting.status !== "reported" ||
          resulting.checks.length === 0 ||
          !resulting.checks.every((check) => check.status === "passed") ||
          resulting.axes.length !== 2
        ) {
          return yield* directorError(
            "review-incomplete",
            "The cited fix review must be a later independent review with verified checks on a changed head.",
          );
        }
      } else if (disposition.resultingReviewId) {
        return yield* directorError(
          "review-incomplete",
          "Only a fixed finding may cite a resulting review.",
        );
      }
      if (disposition.outcome === "owner-accepted") {
        if (!disposition.evidenceSource || !disposition.evidenceQuote) {
          return yield* directorError(
            "review-incomplete",
            "Owner acceptance requires a durable user message source and retained quote.",
          );
        }
        const message = yield* verifiedUserSourceMessage(disposition.evidenceSource);
        if (
          !message ||
          !message.message.text.includes(disposition.evidenceQuote) ||
          message.message.createdAt <= reviewRow.createdAt ||
          !message.prompt ||
          message.prompt.createdAt < reviewRow.createdAt ||
          !message.prompt.text.includes(reviewRow.reviewId) ||
          !message.prompt.text.includes(disposition.findingId)
        ) {
          return yield* directorError(
            "review-incomplete",
            "The cited owner reply must match its retained quote and follow the recorded decision prompt for this finding.",
          );
        }
      } else if (disposition.evidenceSource || disposition.evidenceQuote) {
        return yield* directorError(
          "review-incomplete",
          "Only owner acceptance may cite a user decision source.",
        );
      }
    }
    const updatedAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql.withTransaction(
        Effect.forEach(
          input.dispositions,
          (disposition) =>
            sql`
            UPDATE workflow_review_findings SET disposition = ${disposition.outcome},
              disposition_rationale = ${disposition.rationale},
              disposition_evidence_source = ${disposition.evidenceSource ?? null},
              disposition_evidence_quote = ${disposition.evidenceQuote ?? null},
              resulting_review_id = ${disposition.resultingReviewId ?? null},
              updated_at = ${updatedAt}
            WHERE review_id = ${reviewRow.reviewId} AND finding_id = ${disposition.findingId}
          `,
        ),
      ),
      "Director review dispositions could not be saved.",
    );
    return yield* reviewStatusFromRow(yield* loadReview(row.directorId, reviewRow.reviewId));
  });

  const resolveTicketUnlocked = Effect.fn("WorkflowDirectorService.resolveTicket")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowTicketResolveInput,
  ) {
    const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    const reviewRow = yield* loadReview(row.directorId, input.reviewId);
    const review = yield* reviewStatusFromRow(reviewRow);
    if (
      review.status !== "reported" ||
      review.checks.length === 0 ||
      !review.checks.every((check) => check.status === "passed") ||
      review.axes.length !== 2 ||
      !review.axes.some((axis) => axis.axis === "standards") ||
      !review.axes.some((axis) => axis.axis === "spec")
    ) {
      return yield* directorError(
        "review-incomplete",
        "Resolution requires verified checks and a durable independent Standards and Spec report.",
      );
    }
    const unresolved = review.findings.filter((finding) => !finding.disposition);
    const priorUnresolved = yield* persistence(
      sql<{ readonly reviewId: string; readonly findingId: string }>`
        SELECT f.review_id AS "reviewId", f.finding_id AS "findingId"
        FROM workflow_review_findings f
        JOIN workflow_ticket_reviews r ON r.review_id = f.review_id
        WHERE r.director_id = ${row.directorId} AND r.ticket_number = ${review.ticketNumber}
          AND r.created_at <= ${reviewRow.createdAt} AND f.disposition IS NULL
      `,
      "Prior review finding dispositions could not be read.",
    );
    if (unresolved.length > 0 || priorUnresolved.length > 0) {
      return yield* directorError(
        "review-incomplete",
        "Every finding from this review history needs a separate director disposition before resolution.",
        priorUnresolved.map((finding) => `${finding.reviewId}/${finding.findingId}`).join(", "),
      );
    }
    const dispositionRows = yield* persistence(
      sql<{ readonly reviewId: string }>`
        SELECT DISTINCT f.review_id AS "reviewId"
        FROM workflow_review_findings f
        JOIN workflow_ticket_reviews r ON r.review_id = f.review_id
        WHERE r.director_id = ${row.directorId} AND r.ticket_number = ${review.ticketNumber}
          AND r.created_at <= ${reviewRow.createdAt} AND f.disposition IS NOT NULL
        ORDER BY r.created_at
      `,
      "Review disposition evidence could not be read.",
    );
    for (const finding of review.findings) {
      const disposition = finding.disposition!;
      if (disposition.outcome === "fixed") {
        if (!disposition.resultingReviewId) {
          return yield* directorError(
            "review-incomplete",
            `Finding ${finding.id} has no resulting review evidence.`,
          );
        }
        const resultingRow = yield* loadReview(row.directorId, disposition.resultingReviewId);
        const resulting = yield* reviewStatusFromRow(resultingRow);
        if (
          resulting.ticketNumber !== review.ticketNumber ||
          resultingRow.createdAt <= reviewRow.createdAt ||
          resulting.implementationHead === review.implementationHead ||
          resulting.status !== "reported" ||
          resulting.checks.length === 0 ||
          !resulting.checks.every((check) => check.status === "passed") ||
          resulting.axes.length !== 2
        ) {
          return yield* directorError(
            "review-incomplete",
            `Finding ${finding.id} no longer has valid later review evidence.`,
          );
        }
      }
      if (disposition.outcome === "owner-accepted") {
        if (!disposition.evidenceSource || !disposition.evidenceQuote) {
          return yield* directorError(
            "review-incomplete",
            `Finding ${finding.id} has no owner decision evidence.`,
          );
        }
        const message = yield* verifiedUserSourceMessage(disposition.evidenceSource);
        if (
          !message ||
          !message.message.text.includes(disposition.evidenceQuote) ||
          message.message.createdAt <= reviewRow.createdAt ||
          !message.prompt ||
          message.prompt.createdAt < reviewRow.createdAt ||
          !message.prompt.text.includes(review.reviewId) ||
          !message.prompt.text.includes(finding.id)
        ) {
          return yield* directorError(
            "review-incomplete",
            `Finding ${finding.id} no longer has a verified owner decision.`,
          );
        }
      }
    }
    const currentWorkers = yield* workers(row.directorId);
    const implementation = currentWorkers.find(
      (worker) => worker.dispatchId === reviewRow.implementationDispatchId,
    );
    if (!implementation?.providerThreadId || implementation.writeReservation !== "released") {
      return yield* directorError(
        "review-incomplete",
        "The implementation worker is still live, idle, or has unconfirmed descendants.",
      );
    }
    const observationRows = yield* observations(row.directorId);
    const coordinator = review.providerThreadId;
    if (!coordinator) {
      return yield* directorError(
        "review-incomplete",
        "The review coordinator identity is unavailable.",
      );
    }
    yield* verifyIndependentReviewAncestry(
      row,
      observationRows,
      coordinator,
      implementation.providerThreadId,
    );
    const coordinatorObservation = observationRows.find(
      (observation) => observation.providerThreadId === coordinator,
    );
    if (coordinatorObservation?.nativeLifecycle !== "closed") {
      return yield* directorError(
        "review-incomplete",
        "The review coordinator has not reached exact native closure.",
      );
    }
    const byId = new Map(
      observationRows.map((observation) => [observation.providerThreadId, observation]),
    );
    for (const observation of observationRows) {
      let parentId = observation.parentProviderThreadId;
      const visited = new Set<string>();
      while (parentId) {
        if (parentId === coordinator) {
          if (observation.nativeLifecycle !== "closed") {
            return yield* directorError(
              "review-incomplete",
              "A review descendant remains live or unconfirmed.",
              observation.providerThreadId,
            );
          }
          break;
        }
        if (visited.has(parentId)) {
          return yield* directorError("review-incomplete", "Review ancestry is cyclic.");
        }
        visited.add(parentId);
        parentId = byId.get(parentId)?.parentProviderThreadId ?? null;
      }
    }
    yield* verifyGitRange(row.worktreePath, review.fixedBase, review.implementationHead);
    const issue = yield* workflow.issueDetail({
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowIssueSummary["repository"],
      number: review.ticketNumber,
    });
    if (issue.body !== reviewRow.scopeBody) {
      return yield* directorError(
        "stale-review",
        "The ticket scope changed after review preparation and requires renewed approval and review.",
      );
    }

    const existingIntentRows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT resolution_id AS "resolutionId", review_id AS "reviewId",
          ticket_number AS "ticketNumber", final_head AS "finalHead",
          comment_body AS "commentBody", status,
          comment_url AS "commentUrl", frontier_json AS "frontierJson",
          last_error AS "lastError", updated_at AS "updatedAt"
        FROM workflow_ticket_resolution_intents WHERE review_id = ${review.reviewId} LIMIT 1
      `,
      "The ticket resolution intent could not be read.",
    );
    let resolutionRow: ResolutionRow;
    if (existingIntentRows[0]) {
      resolutionRow = yield* decodeResolutionRow(existingIntentRows[0]).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The ticket resolution intent is invalid.",
            String(error),
          ),
        ),
      );
    } else {
      const admission = (yield* admissions(row.directorId)).find(
        (candidate) => candidate.admissionId === review.admissionId,
      );
      if (!admission) {
        return yield* directorError(
          "review-incomplete",
          "The reviewed ticket admission is missing.",
        );
      }
      const admissionResult = yield* admitUnlocked({
        projectId: ProjectId.make(row.projectId),
        directorId: row.directorId,
        repository: row.repository as WorkflowDirectorAdmissionInput["repository"],
        ticketNumber: review.ticketNumber,
        purpose: "review",
        ownership: admission.ownership,
      });
      if (!admissionResult.admission || admissionResult.admission.claimStatus !== "confirmed") {
        return yield* directorError(
          "claim-failed",
          "Ticket ownership must remain confirmed before resolution.",
          admissionResult.message,
        );
      }
      const resolutionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const commentBody = workflowTicketResolutionBody({
        resolutionId,
        repository: row.repository,
        ticketNumber: review.ticketNumber,
        review,
        dispositionReviewIds: dispositionRows.map((candidate) => candidate.reviewId),
      });
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`
          INSERT INTO workflow_ticket_resolution_intents (
            resolution_id, director_id, admission_id, review_id, repository, ticket_number,
            scope_body, final_head, comment_body, status, created_at, updated_at
          ) VALUES (
            ${resolutionId}, ${row.directorId}, ${review.admissionId}, ${review.reviewId},
            ${row.repository}, ${review.ticketNumber}, ${reviewRow.scopeBody},
            ${review.implementationHead}, ${commentBody}, 'comment-pending', ${createdAt}, ${createdAt}
          )
        `,
        "The pending ticket resolution could not be saved before its GitHub write.",
      );
      resolutionRow = {
        resolutionId,
        reviewId: review.reviewId,
        ticketNumber: review.ticketNumber,
        finalHead: review.implementationHead,
        commentBody,
        status: "comment-pending",
        commentUrl: null,
        frontierJson: null,
        lastError: null,
        updatedAt: createdAt,
      };
    }

    const evidenceMarker = `workflow-resolution:${resolutionRow.resolutionId}`;
    const matchingResolution = (detail: WorkflowIssueDetail) =>
      detail.evidence?.records.find(
        (record) =>
          record.kind === "resolution" &&
          record.state === "current" &&
          record.scope === "current" &&
          record.sourceAccess !== "unavailable" &&
          record.outcome === "resolved" &&
          record.bodyFingerprint === workflowEvidenceBodyFingerprint(resolutionRow.commentBody) &&
          record.evidence?.includes(evidenceMarker),
      );
    let refreshed = issue;
    let record = matchingResolution(refreshed);
    const confirmResolutionAuthority = Effect.fn(
      "WorkflowDirectorService.confirmResolutionAuthority",
    )(function* () {
      const admission = (yield* admissions(row.directorId)).find(
        (candidate) => candidate.admissionId === review.admissionId,
      );
      if (!admission) {
        return yield* directorError(
          "review-incomplete",
          "The reviewed ticket admission is missing.",
        );
      }
      const result = yield* admitUnlocked({
        projectId: ProjectId.make(row.projectId),
        directorId: row.directorId,
        repository: row.repository as WorkflowDirectorAdmissionInput["repository"],
        ticketNumber: review.ticketNumber,
        purpose: "review",
        ownership: admission.ownership,
      });
      if (!result.admission || result.admission.claimStatus !== "confirmed") {
        return yield* directorError(
          "claim-failed",
          "Current ticket ownership and readiness must be confirmed before a new GitHub write.",
          result.message,
        );
      }
    });
    if (
      resolutionRow.status === "comment-pending" ||
      resolutionRow.status === "comment-uncertain"
    ) {
      if (refreshed.evidence?.historyComplete === false) {
        return {
          disposition: "pending",
          resolution: resolutionStatusFromRow(resolutionRow),
        } satisfies WorkflowTicketResolveResult;
      }
      if (!record && resolutionRow.status === "comment-uncertain") {
        return {
          disposition: "pending",
          resolution: resolutionStatusFromRow(resolutionRow),
        } satisfies WorkflowTicketResolveResult;
      }
      if (!record) {
        yield* confirmResolutionAuthority();
        const attemptedAt = DateTime.formatIso(yield* DateTime.now);
        yield* persistence(
          sql`
            UPDATE workflow_ticket_resolution_intents SET status = 'comment-uncertain',
              updated_at = ${attemptedAt} WHERE resolution_id = ${resolutionRow.resolutionId}
          `,
          "The pending comment attempt could not be retained before its GitHub write.",
        );
        resolutionRow = { ...resolutionRow, status: "comment-uncertain", updatedAt: attemptedAt };
        const comment = yield* executeGitHub(row.worktreePath, [
          "issue",
          "comment",
          String(review.ticketNumber),
          "--repo",
          row.repository,
          "--body",
          resolutionRow.commentBody,
        ]).pipe(Effect.result);
        refreshed = yield* workflow.issueDetail({
          projectId: ProjectId.make(row.projectId),
          repository: row.repository as WorkflowIssueSummary["repository"],
          number: review.ticketNumber,
        });
        record = matchingResolution(refreshed);
        if (!record) {
          const updatedAt = DateTime.formatIso(yield* DateTime.now);
          const lastError =
            comment._tag === "Failure"
              ? comment.failure.message
              : "GitHub did not return the persisted resolution evidence yet.";
          yield* persistence(
            sql`
              UPDATE workflow_ticket_resolution_intents SET status = 'comment-uncertain',
                last_error = ${lastError},
                updated_at = ${updatedAt} WHERE resolution_id = ${resolutionRow.resolutionId}
            `,
            "The pending resolution result could not be retained.",
          );
          return {
            disposition: "pending",
            resolution: {
              ...resolutionStatusFromRow(resolutionRow),
              status: "comment-uncertain",
              lastError,
              updatedAt,
            },
          } satisfies WorkflowTicketResolveResult;
        }
      }
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`
          UPDATE workflow_ticket_resolution_intents SET status = 'close-pending',
            comment_url = ${record.url}, last_error = NULL, updated_at = ${updatedAt}
          WHERE resolution_id = ${resolutionRow.resolutionId}
        `,
        "The reconciled resolution comment could not be saved.",
      );
      resolutionRow = {
        ...resolutionRow,
        status: "close-pending",
        commentUrl: record.url,
        updatedAt,
      };
    }

    if (
      resolutionRow.status === "close-uncertain" &&
      (refreshed.state !== "closed" || refreshed.stateReason !== "completed")
    ) {
      return {
        disposition: "pending",
        resolution: resolutionStatusFromRow(resolutionRow),
      } satisfies WorkflowTicketResolveResult;
    }
    if (refreshed.state !== "closed" || refreshed.stateReason !== "completed") {
      yield* confirmResolutionAuthority();
      const attemptedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`
          UPDATE workflow_ticket_resolution_intents SET status = 'close-uncertain',
            updated_at = ${attemptedAt} WHERE resolution_id = ${resolutionRow.resolutionId}
        `,
        "The pending closure attempt could not be retained before its GitHub write.",
      );
      resolutionRow = { ...resolutionRow, status: "close-uncertain", updatedAt: attemptedAt };
      yield* executeGitHub(row.worktreePath, [
        "issue",
        "close",
        String(review.ticketNumber),
        "--repo",
        row.repository,
        "--reason",
        "completed",
      ]).pipe(Effect.result);
    }
    refreshed = yield* workflow.issueDetail({
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowIssueSummary["repository"],
      number: review.ticketNumber,
    });
    record = matchingResolution(refreshed);
    if (
      refreshed.state !== "closed" ||
      refreshed.stateReason !== "completed" ||
      refreshed.readiness?.status !== "resolved" ||
      !record
    ) {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      const lastError = "GitHub resolution closure is still unconfirmed.";
      yield* persistence(
        sql`
          UPDATE workflow_ticket_resolution_intents SET last_error = ${lastError},
            updated_at = ${updatedAt} WHERE resolution_id = ${resolutionRow.resolutionId}
        `,
        "The unconfirmed closure could not be retained.",
      );
      return {
        disposition: "pending",
        resolution: {
          ...resolutionStatusFromRow(resolutionRow),
          lastError,
          updatedAt,
        },
      } satisfies WorkflowTicketResolveResult;
    }
    const frontier = yield* workflow.children({
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowIssueSummary["repository"],
      parentNumber: row.capabilityNumber,
    });
    const readyIssueIds = frontier.frontier?.readyIssueIds ?? [];
    const updatedAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql`
        UPDATE workflow_ticket_resolution_intents SET status = 'resolved',
          comment_url = ${record.url}, frontier_json = ${encodeStringArrayJson(readyIssueIds)},
          last_error = NULL, updated_at = ${updatedAt}
        WHERE resolution_id = ${resolutionRow.resolutionId}
      `,
      "The confirmed ticket resolution and refreshed frontier could not be saved.",
    );
    return {
      disposition: "resolved",
      resolution: {
        ...resolutionStatusFromRow(resolutionRow),
        status: "resolved",
        commentUrl: record.url,
        lastError: null,
        readyIssueIds,
        updatedAt,
      },
    } satisfies WorkflowTicketResolveResult;
  });

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
    prepareTicketReview: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        prepareTicketReviewUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    recordReviewChecks: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        recordReviewChecksUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    associateTicketReview: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        associateTicketReviewUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    reportTicketReview: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        reportTicketReviewUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    recordReviewDispositions: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        recordReviewDispositionsUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    resolveTicket: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        resolveTicketUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
  });
});

export const layer = Layer.effect(WorkflowDirectorService, make).pipe(
  Layer.provide(Layer.merge(OrchestrationCommandReceiptRepositoryLive, ProcessRunner.layer)),
);
