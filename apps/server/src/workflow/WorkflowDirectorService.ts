import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentId,
  MessageId,
  type OrchestrationCommand,
  type OrchestrationDispatchCommandError,
  type OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
  WorkflowDirectorError,
  type WorkflowCapabilityCompleteInput,
  type WorkflowCapabilityCompleteResult,
  type WorkflowCapabilityCompletionStatus,
  type WorkflowDirectorAdmission,
  type WorkflowDirectorAdmissionInput,
  type WorkflowDirectorAdmissionResult,
  type WorkflowDirectorHandoffPrepareInput,
  type WorkflowDirectorHandoffOwnerReconcileInput,
  type WorkflowDirectorHandoffRecoveryTarget,
  type WorkflowDirectorHandoffReconcileInput,
  type WorkflowDirectorHandoffReconciliationActor,
  type WorkflowDirectorHandoffReconciliation,
  type WorkflowDirectorHandoffStatus,
  type WorkflowDirectorResumeInput,
  type WorkflowDirectorReassessmentRetryInput,
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

export function workflowSuccessorCreateCommandId(commandId: CommandId): CommandId {
  return CommandId.make(`workflow:successor:create:${commandId}`);
}

function rootTurnIsSettled(shell: OrchestrationThreadShell) {
  return (
    shell.latestTurn != null &&
    shell.latestTurn.state !== "running" &&
    shell.session?.activeTurnId == null &&
    shell.session?.status !== "running" &&
    shell.session?.status !== "starting"
  );
}

type Dispatch = (
  command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;

type SuccessorDispatchPreflight =
  | { readonly disposition: "ready" }
  | {
      readonly disposition: "blocked";
      readonly error: WorkflowDirectorError | WorkflowQueryError;
      readonly hold?: {
        readonly detail: string;
        readonly restoreSource: boolean;
      };
    };

type InterruptDispatch = (
  command: Extract<OrchestrationCommand, { type: "thread.turn.interrupt" }>,
) => Effect.Effect<{ readonly sequence: number }, WorkflowDirectorError>;

type ReassessmentInput = {
  readonly projectId: ProjectId;
  readonly issue: WorkflowIssueDetail;
};

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
  specificationFingerprint: Schema.NullOr(Schema.String),
  breakdownFingerprint: Schema.NullOr(Schema.String),
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
  scopeBody: Schema.NullOr(Schema.String),
  scopeFingerprint: Schema.NullOr(Schema.String),
  currentScopeBody: Schema.NullOr(Schema.String),
  currentScopeFingerprint: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type AdmissionRow = typeof AdmissionRow.Type;
const decodeAdmissionRow = Schema.decodeUnknownEffect(AdmissionRow);

const ReassessmentRow = Schema.Struct({
  reassessmentId: Schema.String,
  directorId: Schema.String,
  triggerKind: Schema.String,
  triggerIssueNumber: Schema.Number,
  triggerSource: Schema.String,
  status: Schema.String,
  requiredAction: Schema.String,
  stopRequestStatus: Schema.String,
  trackerStatus: Schema.String,
  trackerUrl: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type ReassessmentRow = typeof ReassessmentRow.Type;
const decodeReassessmentRow = Schema.decodeUnknownEffect(ReassessmentRow);

const InterruptionSubjectRow = Schema.Struct({
  subjectId: Schema.String,
  subjectKind: Schema.String,
  providerThreadId: Schema.NullOr(Schema.String),
  parentProviderThreadId: Schema.NullOr(Schema.String),
  nativeSessionId: Schema.NullOr(Schema.String),
  nativeTurnId: Schema.NullOr(Schema.String),
  interruptAttemptId: Schema.NullOr(Schema.String),
  requestStatus: Schema.String,
  outcome: Schema.String,
  detail: Schema.NullOr(Schema.String),
  updatedAt: Schema.String,
});
type InterruptionSubjectRow = typeof InterruptionSubjectRow.Type;
const decodeInterruptionSubjectRow = Schema.decodeUnknownEffect(InterruptionSubjectRow);

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
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const ResumeAdmissionScope = Schema.Struct({
  admissionId: Schema.String,
  body: Schema.String,
  fingerprint: Schema.String,
});
const ResumeAdmissionScopesJson = Schema.fromJsonString(Schema.Array(ResumeAdmissionScope));
const decodeResumeAdmissionScopesJson = Schema.decodeUnknownEffect(ResumeAdmissionScopesJson);
const encodeResumeAdmissionScopesJson = Schema.encodeUnknownSync(ResumeAdmissionScopesJson);

const ResumeRow = Schema.Struct({
  resumeId: Schema.String,
  directorId: Schema.String,
  sourceTurnId: Schema.String,
  commandId: Schema.String,
  status: Schema.String,
  sequence: Schema.NullOr(Schema.Number),
  reassessmentId: Schema.NullOr(Schema.String),
  specificationFingerprint: Schema.NullOr(Schema.String),
  breakdownFingerprint: Schema.NullOr(Schema.String),
  admissionScopesJson: Schema.NullOr(Schema.String),
  reassessmentTriggerCount: Schema.NullOr(Schema.Number),
});
type ResumeRow = typeof ResumeRow.Type;
const decodeResumeRow = Schema.decodeUnknownEffect(ResumeRow);

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

const CompletionRow = Schema.Struct({
  completionId: Schema.String,
  directorId: Schema.String,
  repository: Schema.String,
  capabilityNumber: Schema.Number,
  resultingHead: Schema.String,
  specificationFingerprint: Schema.String,
  breakdownFingerprint: Schema.String,
  commentBody: Schema.String,
  status: Schema.String,
  commentUrl: Schema.NullOr(Schema.String),
  closeConfirmed: Schema.Number,
  closeOwned: Schema.Number,
  requiredAction: Schema.String,
  lastError: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type CompletionRow = typeof CompletionRow.Type;
const decodeCompletionRow = Schema.decodeUnknownEffect(CompletionRow);

const CompletionCheckRow = Schema.Struct({
  completionId: Schema.String,
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
type CompletionCheckRow = typeof CompletionCheckRow.Type;
const decodeCompletionCheckRow = Schema.decodeUnknownEffect(CompletionCheckRow);

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

const HandoffRow = Schema.Struct({
  handoffId: Schema.String,
  sourceDirectorId: Schema.String,
  successorDirectorId: Schema.NullOr(Schema.String),
  sourceThreadId: Schema.String,
  sourceBatchId: Schema.String,
  admissionsJson: Schema.String,
  settlementsJson: Schema.String,
  implementationHead: Schema.NullOr(Schema.String),
  worktreePath: Schema.String,
  worktreeBranch: Schema.String,
  specificationLinksJson: Schema.String,
  issueLinksJson: Schema.String,
  reviewLinksJson: Schema.String,
  commitLinksJson: Schema.String,
  suggestedSkillsJson: Schema.String,
  suggestedStaffingJson: Schema.String,
  lessonsJson: Schema.String,
  unresolvedContextJson: Schema.String,
  successorThreadId: Schema.NullOr(Schema.String),
  successorCommandId: Schema.NullOr(Schema.String),
  successorMessageId: Schema.NullOr(Schema.String),
  successorPrompt: Schema.NullOr(Schema.String),
  status: Schema.String,
  detail: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type HandoffRow = typeof HandoffRow.Type;
const decodeHandoffRow = Schema.decodeUnknownEffect(HandoffRow);

const HandoffReconciliationRow = Schema.Struct({
  sequence: Schema.Number,
  reconciliationId: Schema.String,
  handoffId: Schema.String,
  acknowledgedByDirectorId: Schema.String,
  actorKind: Schema.Literals(["director", "owner-session"]),
  actorSubject: Schema.NullOr(Schema.String),
  settlementsJson: Schema.String,
  implementationHead: Schema.String,
  summary: Schema.String,
  createdAt: Schema.String,
});
type HandoffReconciliationRow = typeof HandoffReconciliationRow.Type;
const decodeHandoffReconciliationRow = Schema.decodeUnknownEffect(HandoffReconciliationRow);

const HandoffRecoveryTargetRow = Schema.Struct({
  handoffId: Schema.String,
  sourceDirectorId: Schema.String,
  sourceBatchId: Schema.String,
  sourceThreadId: Schema.String,
  sourceBatchNumber: Schema.Number,
  status: Schema.String,
  detail: Schema.NullOr(Schema.String),
  settlementsJson: Schema.String,
  latestReconciliationSequence: Schema.NullOr(Schema.Number),
  latestSettlementsJson: Schema.NullOr(Schema.String),
});
type HandoffRecoveryTargetRow = typeof HandoffRecoveryTargetRow.Type;
const decodeHandoffRecoveryTargetRow = Schema.decodeUnknownEffect(HandoffRecoveryTargetRow);

const HandoffNativeEvidenceRow = Schema.Struct({
  directorId: Schema.String,
  kind: Schema.Literals(["director", "child"]),
  providerThreadId: Schema.NullOr(Schema.String),
  nativeSessionId: Schema.NullOr(Schema.String),
  nativeTurnId: Schema.NullOr(Schema.String),
  nativeStatus: Schema.NullOr(Schema.String),
  nativeLifecycle: Schema.NullOr(Schema.String),
  associated: Schema.Number,
  updatedAt: Schema.String,
});
type HandoffNativeEvidenceRow = typeof HandoffNativeEvidenceRow.Type;
const decodeHandoffNativeEvidenceRow = Schema.decodeUnknownEffect(HandoffNativeEvidenceRow);

function handoffRecoveryTargetObservation(input: {
  readonly currentDirectorId: string;
  readonly handoffId: string;
  readonly handoffStatus: string;
  readonly latestReconciliationSequence: number | null;
  readonly evidence: ReadonlyArray<HandoffNativeEvidenceRow>;
}) {
  return workflowEvidenceBodyFingerprint(encodeUnknownJson(input));
}

const HandoffAdmission = Schema.Struct({
  admissionId: Schema.String,
  ticketNumber: Schema.Number,
  slotTicketNumber: Schema.Number,
  claimStatus: Schema.String,
  outcome: Schema.String,
});
const HandoffAdmissionsJson = Schema.fromJsonString(Schema.Array(HandoffAdmission));
const encodeHandoffAdmissionsJson = Schema.encodeUnknownSync(HandoffAdmissionsJson);
const decodeHandoffAdmissionsJson = Schema.decodeUnknownEffect(HandoffAdmissionsJson);
const HandoffSettlement = Schema.Struct({
  kind: Schema.Literals(["director", "child"]),
  providerThreadId: Schema.String,
  nativeSessionId: Schema.NullOr(Schema.String),
  nativeTurnId: Schema.NullOr(Schema.String),
  mode: Schema.Literals(["closed", "interrupted"]),
  observedAt: Schema.String,
});
const HandoffSettlementsJson = Schema.fromJsonString(Schema.Array(HandoffSettlement));
const encodeHandoffSettlementsJson = Schema.encodeUnknownSync(HandoffSettlementsJson);
const decodeHandoffSettlementsJson = Schema.decodeUnknownEffect(HandoffSettlementsJson);

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

function completionEvidenceChanged(error: WorkflowDirectorError | WorkflowQueryError) {
  return (
    error._tag === "WorkflowDirectorError" &&
    (error.failure === "completion-pending" || error.failure === "breakdown-incomplete")
  );
}

function requiredIssueCompletionEvidence(
  issue: WorkflowIssueDetail,
): "resolved" | "changed" | "unknown" {
  if (issue.readiness?.status === "resolved") return "resolved";
  if (issue.state === "open") return "changed";
  if (
    issue.readiness?.status === "cancelled" ||
    issue.readiness?.status === "out-of-scope" ||
    issue.readiness?.status === "superseded"
  ) {
    return "changed";
  }
  if (issue.readiness?.status !== "closed-unverified") {
    return issue.readiness ? "changed" : "unknown";
  }
  if (!issue.evidence) return "unknown";
  if (issue.evidence?.historyComplete === false) return "unknown";
  const uncertainResolution = issue.evidence?.records.some(
    (record) =>
      record.kind === "resolution" &&
      record.state === "current" &&
      (record.scope === "unknown" || record.sourceAccess === "unavailable"),
  );
  return uncertainResolution ? "unknown" : "changed";
}

function closeCommandWasAcknowledged(
  result: { readonly stdout: string; readonly stderr: string },
  repository: string,
  issueNumber: number,
) {
  const acknowledgment = `closed issue ${repository.toLocaleLowerCase()}#${issueNumber} (`;
  return `${result.stdout}\n${result.stderr}`.split(/\r?\n/gu).some((line) =>
    line
      .replace(/^[\p{P}\p{S}\s]*/u, "")
      .toLocaleLowerCase()
      .startsWith(acknowledgment),
  );
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

function invalidatesPrerequisite(issue: WorkflowIssueSummary): boolean {
  const status = issue.readiness?.status;
  if (status === "resolved" || status === "closed-unverified") {
    return false;
  }
  if (issue.state === "open") return true;
  if (status === undefined) return false;
  return status === "cancelled" || status === "out-of-scope" || status === "superseded";
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
    "After every approved ticket and nested task has current resolution evidence and every native child is closed, call workflow_complete_capability with the exact clean result head, the combined acceptance commands, and no receipts. Run those registered commands through the normal provider path, then repeat the call with each exact native toolCallId. Retry pending tracker results so saved comment, close, or compensating reopen intent can reconcile; only completed/current is present completion authority.",
    `This batch admits at most ${ADMISSION_LIMIT} distinct delivery slices. Failed or blocked admitted slices keep their slot; retry and review reuse it; nested tasks reuse their parent slice. At the limit, stop new admissions, finish or exactly interrupt admitted child work, call workflow_prepare_director_handoff with useful lessons, unresolved context, and suggested skills/staffing, then settle this turn. T3 persists and verifies the handoff before starting a successor.`,
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

export function workflowCapabilityCompletionBody(input: {
  readonly completionId: string;
  readonly repository: string;
  readonly capabilityNumber: number;
  readonly resultingHead: string;
  readonly requiredIssues: ReadonlyArray<WorkflowIssueDetail>;
  readonly checks: WorkflowCapabilityCompleteInput["checks"];
}) {
  return [
    "## Resolution",
    "<!-- t3-workflow:v1 resolution -->",
    "Outcome: resolved",
    `Source: https://github.com/${input.repository}/issues/${input.capabilityNumber}`,
    "",
    "### Summary",
    `The complete approved delivery hierarchy passed combined acceptance at ${input.resultingHead}.`,
    "",
    "### Evidence",
    `- [Integrated implementation](https://github.com/${input.repository}/commit/${input.resultingHead})`,
    `- artifact: \`workflow-capability-completion:${input.completionId}\``,
    ...input.checks.map(
      (check) =>
        `- artifact: \`workflow-capability-completion:${input.completionId}/check/${check.label}\``,
    ),
    "",
    "### Required delivery",
    ...input.requiredIssues.map(
      (issue) => `- [#${issue.number} — ${issue.title}](${issue.url}): resolved`,
    ),
    "",
    "### Combined acceptance",
    ...input.checks.map((check) => `- ${check.label}: \`${check.command}\``),
    "",
    "### Limits",
    "This records capability acceptance only. Merge and release remain separate actions.",
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
    readonly reassess: (
      input: ReassessmentInput,
      dispatch: InterruptDispatch,
    ) => Effect.Effect<void, WorkflowDirectorError>;
    readonly retryReassessment: (
      input: WorkflowDirectorReassessmentRetryInput,
      dispatch: InterruptDispatch,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly resume: (
      input: WorkflowDirectorResumeInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly admit: (
      input: WorkflowDirectorAdmissionInput,
    ) => Effect.Effect<WorkflowDirectorAdmissionResult, WorkflowQueryError | WorkflowDirectorError>;
    readonly prepareHandoff: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowDirectorHandoffPrepareInput,
    ) => Effect.Effect<WorkflowDirectorHandoffStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly reconcileHandoff: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowDirectorHandoffReconcileInput,
    ) => Effect.Effect<WorkflowDirectorHandoffStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly reconcileHandoffAsOwner: (
      input: WorkflowDirectorHandoffOwnerReconcileInput,
      actorSubject: string,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly rotateReady: (
      input: WorkflowDirectorStatusInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
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
    readonly completeCapability: (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowCapabilityCompleteInput,
    ) => Effect.Effect<
      WorkflowCapabilityCompleteResult,
      WorkflowQueryError | WorkflowDirectorError
    >;
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
        detail, specification_fingerprint AS "specificationFingerprint",
        breakdown_fingerprint AS "breakdownFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt"
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
        d.specification_fingerprint AS "specificationFingerprint",
        d.breakdown_fingerprint AS "breakdownFingerprint",
        d.created_at AS "createdAt", d.updated_at AS "updatedAt"
      FROM workflow_director_admissions a
      JOIN workflow_directors admitted ON admitted.director_id = a.director_id
      JOIN workflow_directors d
        ON d.environment_id = admitted.environment_id
          AND d.repository COLLATE NOCASE = admitted.repository
          AND d.capability_number = admitted.capability_number
          AND d.is_current = 1
      WHERE d.environment_id = ${environmentId} AND d.project_id = ${input.projectId}
        AND d.repository COLLATE NOCASE = ${input.repository}
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
        detail, specification_fingerprint AS "specificationFingerprint",
        breakdown_fingerprint AS "breakdownFingerprint",
        created_at AS "createdAt", updated_at AS "updatedAt"
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

  const loadDirectorHistoryById = Effect.fn("WorkflowDirectorService.loadDirectorHistoryById")(
    function* (directorId: string) {
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
            specification_fingerprint AS "specificationFingerprint",
            breakdown_fingerprint AS "breakdownFingerprint",
            created_at AS "createdAt", updated_at AS "updatedAt"
          FROM workflow_directors WHERE director_id = ${directorId} LIMIT 1
        `,
        "The historical capability director could not be read.",
      );
      if (!rows[0]) {
        return yield* directorError(
          "director-not-found",
          "The historical director is unavailable.",
        );
      }
      return yield* decodeDirectorRow(rows[0]).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The historical director record is invalid.",
            String(error),
          ),
        ),
      );
    },
  );

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
        const approvalRecords =
          ticket.evidence?.records.filter(
            (record) =>
              record.kind === "approval" &&
              record.approvalKind === "ticket-breakdown" &&
              record.state === "current" &&
              sameContent(record.approvedContent ?? "", approval.approvedContent ?? ""),
          ) ?? [];
        const approvalRecord = approvalRecords.find(
          (record) =>
            record.scope === "current" &&
            record.authority === "verified" &&
            record.sourceAccess !== "unavailable",
        );
        const evidenceUnknown =
          !approvalRecord &&
          approvalRecords.some(
            (record) =>
              record.scope === "unknown" ||
              record.sourceAccess === "unavailable" ||
              record.authority === "unknown",
          );
        const evidenceChanged = approvalRecords.some((record) => record.scope === "changed");
        return { ticket, unit, approvalRecord, evidenceUnknown, evidenceChanged };
      });
      const matchedUnits = new Set(matched.flatMap(({ unit }) => (unit ? [unit] : [])));
      const missing = approved.filter((unit) => !matchedUnits.has(unit));
      const extra = matched.filter(({ unit }) => !unit).map(({ ticket }) => ticket.title);
      const changedScope: WorkflowIssueDetail[] = [];
      const unknownScope: WorkflowIssueDetail[] = [];
      for (const entry of matched) {
        if (entry.approvalRecord && (yield* verifyApprovalSource(entry.approvalRecord))) continue;
        if (entry.evidenceChanged || !entry.evidenceUnknown) {
          changedScope.push(entry.ticket);
        } else {
          unknownScope.push(entry.ticket);
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
      return unknownScope;
    },
  );

  const approvedDeliveryHierarchy = Effect.fn("WorkflowDirectorService.approvedDeliveryHierarchy")(
    function* (input: {
      readonly projectId: ProjectId;
      readonly repository: string;
      readonly capabilityNumber: number;
      readonly breakdownApproval: WorkflowEvidenceRecord;
    }) {
      const ticketSummaries = yield* collectDeliveryTickets(input);
      const approvedTickets = yield* Effect.forEach(ticketSummaries, (ticket) =>
        workflow.issueDetail({
          projectId: input.projectId,
          repository: input.repository as WorkflowIssueSummary["repository"],
          number: ticket.number,
        }),
      );
      const unknownPublishedTickets = yield* verifyPublishedBreakdown(
        input.breakdownApproval,
        approvedTickets,
      );

      const required = new Map(approvedTickets.map((ticket) => [ticket.number, ticket]));
      const pending = approvedTickets.map((ticket) => ticket.number);
      const visited = new Set<number>();
      while (pending.length > 0) {
        const parentNumber = pending.shift()!;
        if (visited.has(parentNumber)) continue;
        visited.add(parentNumber);
        const children = yield* workflow.children({
          projectId: input.projectId,
          repository: input.repository as WorkflowIssueSummary["repository"],
          parentNumber,
        });
        for (const child of children.children) {
          if (child.kind === "ticket" || child.kind === "task") {
            if (!required.has(child.number)) {
              required.set(
                child.number,
                yield* workflow.issueDetail({
                  projectId: input.projectId,
                  repository: input.repository as WorkflowIssueSummary["repository"],
                  number: child.number,
                }),
              );
            }
          }
          if (
            child.kind === "ticket" ||
            child.kind === "task" ||
            child.kind === "container" ||
            child.kind === "map"
          ) {
            pending.push(child.number);
          }
        }
      }
      return {
        approvedTickets,
        requiredIssues: [...required.values()],
        unknownPublishedTickets,
      };
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
    stdin?: string,
  ) {
    return yield* github
      .execute({ cwd, args, ...(stdin === undefined ? {} : { stdin }), maxOutputBytes: 100_000 })
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
    options?: {
      readonly ownedClaims?: ReadonlyMap<number, string>;
      readonly admittedTicketNumbers?: ReadonlySet<number>;
      readonly requireActionableUnfinishedTicket?: boolean;
    },
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
    const unknownPublishedTickets = yield* verifyPublishedBreakdown(breakdownApproval, tickets);
    if (unknownPublishedTickets.length > 0) {
      return yield* directorError(
        "breakdown-incomplete",
        "Published delivery scope cannot be verified from the available approval evidence.",
        `Unavailable or unknown scope: ${unknownPublishedTickets.map((ticket) => `#${ticket.number}`).join(", ")}.`,
      );
    }
    yield* verifyRepository(cwd, input.repository);
    if (capability.readiness?.status !== "ready") {
      return yield* directorError(
        "not-ready",
        "This capability is not ready for implementation.",
        readinessDetail(capability),
      );
    }
    let hasReadyTicket = false;
    let hasActionableUnfinishedTicket = false;
    for (const ticket of tickets) {
      if (ticket.readiness?.status === "ready") {
        hasReadyTicket = true;
        hasActionableUnfinishedTicket = true;
        break;
      }
      if (
        ticket.readiness?.status === "resolved" &&
        options?.admittedTicketNumbers?.has(ticket.number)
      ) {
        hasReadyTicket = true;
        if (!options.requireActionableUnfinishedTicket) break;
        continue;
      }
      const claimLogin = options?.ownedClaims?.get(ticket.number);
      if (
        ticket.readiness?.status === "claimed" &&
        claimLogin &&
        (yield* currentClaimIsOwned(cwd, input.repository, ticket.number, claimLogin))
      ) {
        hasReadyTicket = true;
        hasActionableUnfinishedTicket = true;
        break;
      }
    }
    if (!hasReadyTicket) {
      return yield* directorError(
        "not-ready",
        "No published delivery ticket is currently ready or owned by this capability.",
      );
    }
    if (options?.requireActionableUnfinishedTicket && !hasActionableUnfinishedTicket) {
      return yield* directorError(
        "not-ready",
        "No unfinished published delivery ticket is currently actionable for a successor batch.",
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
        scope_body AS "scopeBody", scope_fingerprint AS "scopeFingerprint",
        current_scope_body AS "currentScopeBody",
        current_scope_fingerprint AS "currentScopeFingerprint",
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

  const handoffForDirector = Effect.fn("WorkflowDirectorService.handoffForDirector")(function* (
    directorId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT handoff_id AS "handoffId", source_director_id AS "sourceDirectorId",
          successor_director_id AS "successorDirectorId", source_thread_id AS "sourceThreadId",
          source_batch_id AS "sourceBatchId", admissions_json AS "admissionsJson",
          settlements_json AS "settlementsJson",
          implementation_head AS "implementationHead", worktree_path AS "worktreePath",
          worktree_branch AS "worktreeBranch",
          specification_links_json AS "specificationLinksJson",
          issue_links_json AS "issueLinksJson", review_links_json AS "reviewLinksJson",
          commit_links_json AS "commitLinksJson", suggested_skills_json AS "suggestedSkillsJson",
          suggested_staffing_json AS "suggestedStaffingJson", lessons_json AS "lessonsJson",
          unresolved_context_json AS "unresolvedContextJson",
          successor_thread_id AS "successorThreadId",
          successor_command_id AS "successorCommandId",
          successor_message_id AS "successorMessageId", status, detail,
          successor_prompt AS "successorPrompt",
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM workflow_director_handoffs
        WHERE source_director_id = ${directorId} OR successor_director_id = ${directorId}
        ORDER BY created_at DESC LIMIT 1
      `,
      "The director handoff could not be read.",
    );
    if (!rows[0]) return null;
    return yield* decodeHandoffRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The director handoff record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const outgoingHandoff = Effect.fn("WorkflowDirectorService.outgoingHandoff")(function* (
    directorId: string,
  ) {
    const handoff = yield* handoffForDirector(directorId);
    return handoff?.sourceDirectorId === directorId ? handoff : null;
  });

  const loadHandoffById = Effect.fn("WorkflowDirectorService.loadHandoffById")(function* (
    handoffId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT handoff_id AS "handoffId", source_director_id AS "sourceDirectorId",
          successor_director_id AS "successorDirectorId", source_thread_id AS "sourceThreadId",
          source_batch_id AS "sourceBatchId", admissions_json AS "admissionsJson",
          settlements_json AS "settlementsJson", implementation_head AS "implementationHead",
          worktree_path AS "worktreePath", worktree_branch AS "worktreeBranch",
          specification_links_json AS "specificationLinksJson",
          issue_links_json AS "issueLinksJson", review_links_json AS "reviewLinksJson",
          commit_links_json AS "commitLinksJson", suggested_skills_json AS "suggestedSkillsJson",
          suggested_staffing_json AS "suggestedStaffingJson", lessons_json AS "lessonsJson",
          unresolved_context_json AS "unresolvedContextJson",
          successor_thread_id AS "successorThreadId", successor_command_id AS "successorCommandId",
          successor_message_id AS "successorMessageId", successor_prompt AS "successorPrompt",
          status, detail, created_at AS "createdAt", updated_at AS "updatedAt"
        FROM workflow_director_handoffs WHERE handoff_id = ${handoffId} LIMIT 1
      `,
      "The requested director handoff could not be read.",
    );
    if (!rows[0]) {
      return yield* directorError(
        "not-ready",
        "The requested predecessor handoff does not exist.",
        handoffId,
      );
    }
    return yield* decodeHandoffRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The requested handoff record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const latestHandoffReconciliation = Effect.fn(
    "WorkflowDirectorService.latestHandoffReconciliation",
  )(function* (handoffId: string) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT sequence, reconciliation_id AS "reconciliationId", handoff_id AS "handoffId",
          acknowledged_by_director_id AS "acknowledgedByDirectorId",
          actor_kind AS "actorKind", actor_subject AS "actorSubject",
          settlements_json AS "settlementsJson", implementation_head AS "implementationHead",
          summary, created_at AS "createdAt"
        FROM workflow_director_handoff_reconciliations
        WHERE handoff_id = ${handoffId} ORDER BY sequence DESC LIMIT 1
      `,
      "Handoff reconciliation evidence could not be read.",
    );
    if (!rows[0]) return null;
    return yield* decodeHandoffReconciliationRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The latest handoff reconciliation is invalid.",
          String(error),
        ),
      ),
    );
  });

  const handoffReconciliationStatus = Effect.fn(
    "WorkflowDirectorService.handoffReconciliationStatus",
  )(function* (row: HandoffReconciliationRow) {
    const settlements = yield* decodeHandoffSettlementsJson(row.settlementsJson).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The handoff reconciliation snapshot is invalid.",
          String(error),
        ),
      ),
    );
    return {
      reconciliationId: row.reconciliationId,
      sequence: row.sequence,
      acknowledgedByDirectorId: row.acknowledgedByDirectorId,
      acknowledgementActor: {
        kind: row.actorKind,
        subject: row.actorSubject ?? row.acknowledgedByDirectorId,
      },
      implementationHead: row.implementationHead,
      settlementCount: settlements.length,
      summary: row.summary,
      createdAt: row.createdAt,
    } satisfies WorkflowDirectorHandoffReconciliation;
  });

  const effectiveHandoffSettlement = Effect.fn(
    "WorkflowDirectorService.effectiveHandoffSettlement",
  )(function* (handoff: HandoffRow) {
    const reconciliation = yield* latestHandoffReconciliation(handoff.handoffId);
    return {
      settlementsJson: reconciliation?.settlementsJson ?? handoff.settlementsJson,
      implementationHead: reconciliation?.implementationHead ?? handoff.implementationHead,
      reconciliation,
    };
  });

  const handoffStatusFromRow = Effect.fn("WorkflowDirectorService.handoffStatusFromRow")(function* (
    row: HandoffRow,
  ) {
    const admissions = yield* decodeHandoffAdmissionsJson(row.admissionsJson).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The handoff admission snapshot is invalid.",
          String(error),
        ),
      ),
    );
    const lessons = yield* decodeStringArrayJson(row.lessonsJson).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "The handoff lessons are invalid.", String(error)),
      ),
    );
    const unresolvedContext = yield* decodeStringArrayJson(row.unresolvedContextJson).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "The handoff context is invalid.", String(error)),
      ),
    );
    const reconciliation = yield* latestHandoffReconciliation(row.handoffId);
    const requiredAction =
      row.status === "submitted"
        ? "The successor owns the current batch and must reread live tracker authority before admitting work."
        : row.status === "held"
          ? (row.detail ?? "Refresh the saved handoff after its source evidence changes.")
          : row.status === "submitting"
            ? "The saved successor first turn is being reconciled through its durable command receipt."
            : "Finish or exactly interrupt every source child; T3 will rotate after a current tracker refresh.";
    return {
      handoffId: row.handoffId,
      sourceDirectorId: row.sourceDirectorId,
      sourceBatchId: row.sourceBatchId,
      sourceThreadId: ThreadId.make(row.sourceThreadId),
      successorDirectorId: row.successorDirectorId,
      successorThreadId: row.successorThreadId ? ThreadId.make(row.successorThreadId) : null,
      implementationHead: row.implementationHead,
      status: row.status as WorkflowDirectorHandoffStatus["status"],
      admissionCount: new Set(admissions.map((admission) => admission.slotTicketNumber)).size,
      lessons,
      unresolvedContext,
      latestReconciliation: reconciliation
        ? yield* handoffReconciliationStatus(reconciliation)
        : null,
      requiredAction,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    } satisfies WorkflowDirectorHandoffStatus;
  });

  const recoveryTargetsForDirector = Effect.fn("WorkflowDirectorService.handoffRecoveryTargets")(
    function* (current: DirectorRow) {
      const targetRows = yield* persistence(
        sql<Record<string, unknown>>`
        WITH RECURSIVE predecessor_handoffs(handoff_id, source_director_id) AS (
          SELECT handoff_id, source_director_id FROM workflow_director_handoffs
          WHERE successor_director_id = ${current.directorId}
          UNION ALL
          SELECT h.handoff_id, h.source_director_id FROM workflow_director_handoffs h
          JOIN predecessor_handoffs p ON h.successor_director_id = p.source_director_id
        )
        SELECT h.handoff_id AS "handoffId", h.source_director_id AS "sourceDirectorId",
          h.source_batch_id AS "sourceBatchId", h.source_thread_id AS "sourceThreadId",
          (SELECT count(*) FROM workflow_directors d2
            WHERE d2.environment_id = d.environment_id
              AND d2.repository COLLATE NOCASE = d.repository
              AND d2.capability_number = d.capability_number
              AND (d2.created_at < d.created_at OR
                (d2.created_at = d.created_at AND d2.director_id <= d.director_id)))
            AS "sourceBatchNumber",
          h.status, h.detail, h.settlements_json AS "settlementsJson",
          r.sequence AS "latestReconciliationSequence",
          r.settlements_json AS "latestSettlementsJson"
        FROM predecessor_handoffs p
        JOIN workflow_director_handoffs h ON h.handoff_id = p.handoff_id
        JOIN workflow_directors d ON d.director_id = h.source_director_id
        LEFT JOIN workflow_director_handoff_reconciliations r ON r.sequence = (
          SELECT max(candidate.sequence) FROM workflow_director_handoff_reconciliations candidate
          WHERE candidate.handoff_id = h.handoff_id
        )
        ORDER BY d.created_at DESC, d.director_id DESC
      `,
        "Predecessor handoff recovery targets could not be read.",
      );
      const targets = yield* Effect.forEach(targetRows, (row) =>
        decodeHandoffRecoveryTargetRow(row),
      ).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Predecessor handoff recovery targets are invalid.",
            String(error),
          ),
        ),
      );
      if (targets.length === 0) return [];
      const evidenceRows = yield* persistence(
        sql<Record<string, unknown>>`
        WITH RECURSIVE predecessor_directors(director_id) AS (
          SELECT source_director_id FROM workflow_director_handoffs
          WHERE successor_director_id = ${current.directorId}
          UNION ALL
          SELECT h.source_director_id FROM workflow_director_handoffs h
          JOIN predecessor_directors p ON h.successor_director_id = p.director_id
        )
        SELECT t.director_id AS "directorId", 'director' AS kind,
          NULL AS "providerThreadId", t.native_session_id AS "nativeSessionId",
          t.native_turn_id AS "nativeTurnId", t.status AS "nativeStatus",
          NULL AS "nativeLifecycle", 1 AS associated, t.updated_at AS "updatedAt"
        FROM workflow_director_native_turns t
        JOIN predecessor_directors p ON p.director_id = t.director_id
        UNION ALL
        SELECT o.director_id AS "directorId", 'child' AS kind,
          o.provider_thread_id AS "providerThreadId", o.native_session_id AS "nativeSessionId",
          o.native_turn_id AS "nativeTurnId", o.native_turn_status AS "nativeStatus",
          o.native_lifecycle AS "nativeLifecycle",
          CASE WHEN EXISTS (
            SELECT 1 FROM workflow_worker_dispatches d
            WHERE d.director_id = o.director_id AND d.provider_thread_id = o.provider_thread_id
          ) OR EXISTS (
            SELECT 1 FROM workflow_ticket_reviews r
            WHERE r.director_id = o.director_id AND r.provider_thread_id = o.provider_thread_id
          ) OR EXISTS (
            SELECT 1 FROM workflow_review_axes a JOIN workflow_ticket_reviews r ON r.review_id = a.review_id
            WHERE r.director_id = o.director_id AND a.provider_thread_id = o.provider_thread_id
          ) THEN 1 ELSE 0 END AS associated,
          o.updated_at AS "updatedAt"
        FROM workflow_worker_observations o
        JOIN predecessor_directors p ON p.director_id = o.director_id
        ORDER BY "directorId", kind, "providerThreadId"
      `,
        "Predecessor native recovery evidence could not be read.",
      );
      const evidence = yield* Effect.forEach(evidenceRows, (row) =>
        decodeHandoffNativeEvidenceRow(row),
      ).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Predecessor native recovery evidence is invalid.",
            String(error),
          ),
        ),
      );
      const recoveryTargets: Array<WorkflowDirectorHandoffRecoveryTarget> = [];
      for (const target of targets) {
        const saved = yield* decodeHandoffSettlementsJson(
          target.latestSettlementsJson ?? target.settlementsJson,
        ).pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Predecessor settlement recovery evidence is invalid.",
              String(error),
            ),
          ),
        );
        const targetEvidence = evidence.filter(
          (candidate) => candidate.directorId === target.sourceDirectorId,
        );
        const root = targetEvidence.find((candidate) => candidate.kind === "director");
        const savedRoot = saved.find((settlement) => settlement.kind === "director");
        const children = targetEvidence.filter((candidate) => candidate.kind === "child");
        const savedChildren = new Map(
          saved
            .filter((settlement) => settlement.kind === "child")
            .map((settlement) => [settlement.providerThreadId, settlement]),
        );
        const rootChanged =
          !root ||
          !savedRoot ||
          savedRoot.nativeSessionId !== root.nativeSessionId ||
          savedRoot.nativeTurnId !== root.nativeTurnId ||
          savedRoot.observedAt !== root.updatedAt ||
          (savedRoot.mode === "interrupted"
            ? root.nativeStatus !== "interrupted"
            : !["completed", "failed"].includes(root.nativeStatus ?? ""));
        const childChanged =
          savedChildren.size !== children.length ||
          children.some((child) => {
            const savedChild = savedChildren.get(child.providerThreadId ?? "");
            return (
              child.associated !== 1 ||
              !savedChild ||
              savedChild.nativeSessionId !== child.nativeSessionId ||
              savedChild.nativeTurnId !== child.nativeTurnId ||
              savedChild.observedAt !== child.updatedAt ||
              (savedChild.mode === "closed"
                ? child.nativeLifecycle !== "closed"
                : child.nativeStatus !== "interrupted")
            );
          });
        if (!rootChanged && !childChanged && target.status !== "held") continue;
        const settledChildCount = children.filter(
          (child) =>
            child.associated === 1 &&
            (child.nativeLifecycle === "closed" || child.nativeStatus === "interrupted"),
        ).length;
        const latestReconciliation = yield* latestHandoffReconciliation(target.handoffId);
        recoveryTargets.push({
          handoffId: target.handoffId,
          sourceDirectorId: target.sourceDirectorId,
          sourceBatchId: target.sourceBatchId,
          sourceBatchNumber: target.sourceBatchNumber,
          sourceThreadId: ThreadId.make(target.sourceThreadId),
          status: target.status as WorkflowDirectorHandoffRecoveryTarget["status"],
          detail: target.detail,
          rootSettlement:
            root?.nativeStatus === "running" ||
            root?.nativeStatus === "completed" ||
            root?.nativeStatus === "failed" ||
            root?.nativeStatus === "interrupted"
              ? { status: root.nativeStatus, observedAt: root.updatedAt }
              : null,
          childSettlements: {
            observedCount: children.length,
            settledCount: settledChildCount,
          },
          targetObservation: handoffRecoveryTargetObservation({
            currentDirectorId: current.directorId,
            handoffId: target.handoffId,
            handoffStatus: target.status,
            latestReconciliationSequence: target.latestReconciliationSequence,
            evidence: targetEvidence,
          }),
          latestReconciliation: latestReconciliation
            ? yield* handoffReconciliationStatus(latestReconciliation)
            : null,
        });
      }
      return recoveryTargets;
    },
  );

  const activeReassessment = Effect.fn("WorkflowDirectorService.activeReassessment")(function* (
    directorId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
      SELECT reassessment_id AS "reassessmentId", director_id AS "directorId",
        trigger_kind AS "triggerKind", trigger_issue_number AS "triggerIssueNumber",
        trigger_source AS "triggerSource", status, required_action AS "requiredAction",
        stop_request_status AS "stopRequestStatus", tracker_status AS "trackerStatus",
        tracker_url AS "trackerUrl", created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_reassessments
      WHERE director_id = ${directorId} AND status != 'cleared'
      LIMIT 1
    `,
      "The active reassessment could not be read.",
    );
    if (!rows[0]) return null;
    return yield* decodeReassessmentRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "The active reassessment is invalid.", String(error)),
      ),
    );
  });

  const unclearedCapabilityReassessments = Effect.fn(
    "WorkflowDirectorService.unclearedCapabilityReassessments",
  )(function* (row: DirectorRow) {
    return yield* persistence(
      sql<{ readonly directorId: string }>`
        SELECT r.director_id AS "directorId" FROM workflow_reassessments r
        JOIN workflow_directors d ON d.director_id = r.director_id
        WHERE d.environment_id = ${row.environmentId}
          AND d.repository COLLATE NOCASE = ${row.repository}
          AND d.capability_number = ${row.capabilityNumber}
          AND r.status != 'cleared'
      `,
      "Capability reassessment history could not be read.",
    );
  });

  const interruptionSubjects = Effect.fn("WorkflowDirectorService.interruptionSubjects")(function* (
    reassessmentId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
      SELECT subject_id AS "subjectId", subject_kind AS "subjectKind",
        provider_thread_id AS "providerThreadId",
        parent_provider_thread_id AS "parentProviderThreadId",
        native_session_id AS "nativeSessionId", native_turn_id AS "nativeTurnId",
        interrupt_attempt_id AS "interruptAttemptId", request_status AS "requestStatus",
        outcome, detail, updated_at AS "updatedAt"
      FROM workflow_interruption_subjects
      WHERE reassessment_id = ${reassessmentId}
      ORDER BY discovered_at, subject_id
    `,
      "Interruption evidence could not be read.",
    );
    return yield* Effect.forEach(rows, (row) => decodeInterruptionSubjectRow(row)).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "Interruption evidence is invalid.", String(error)),
      ),
    );
  });

  const reassessmentStatus = Effect.fn("WorkflowDirectorService.reassessmentStatus")(function* (
    directorId: string,
  ) {
    const reassessment = yield* activeReassessment(directorId);
    if (!reassessment) return null;
    const triggers = yield* persistence(
      sql<{
        readonly kind: "scope-change" | "prerequisite";
        readonly issueNumber: number;
        readonly source: string;
        readonly requiredAction: string;
        readonly discoveredAt: string;
      }>`SELECT trigger_kind AS kind, trigger_issue_number AS "issueNumber",
          trigger_source AS source, required_action AS "requiredAction",
          discovered_at AS "discoveredAt"
        FROM workflow_reassessment_triggers
        WHERE reassessment_id = ${reassessment.reassessmentId}
        ORDER BY discovered_at, trigger_issue_number, trigger_source`,
      "Reassessment trigger history could not be read.",
    );
    return {
      reassessmentId: reassessment.reassessmentId,
      triggerKind: reassessment.triggerKind as "scope-change" | "prerequisite",
      triggerIssueNumber: reassessment.triggerIssueNumber,
      triggerSource: reassessment.triggerSource,
      status: reassessment.status as "stopping" | "held" | "clearing" | "cleared",
      requiredAction: reassessment.requiredAction,
      stopRequestStatus: reassessment.stopRequestStatus as
        | "not-issued"
        | "submitted"
        | "failed"
        | "unknown",
      trackerStatus: reassessment.trackerStatus as
        | "not-written"
        | "pending"
        | "uncertain"
        | "confirmed",
      trackerUrl: reassessment.trackerUrl,
      triggers,
      subjects: (yield* interruptionSubjects(reassessment.reassessmentId)).map((subject) => ({
        subjectId: subject.subjectId,
        kind: subject.subjectKind as "director" | "worker" | "reviewer" | "unknown-child",
        providerThreadId: subject.providerThreadId,
        parentProviderThreadId: subject.parentProviderThreadId,
        nativeSessionId: subject.nativeSessionId,
        nativeTurnId: subject.nativeTurnId,
        interruptAttemptId: subject.interruptAttemptId,
        requestStatus: subject.requestStatus as
          | "not-issued"
          | "requested"
          | "acknowledged"
          | "failed"
          | "unknown",
        outcome: subject.outcome as
          | "stopping"
          | "stopped"
          | "failed"
          | "unknown"
          | "closed"
          | "resumed",
        detail: subject.detail,
        updatedAt: subject.updatedAt,
      })),
      createdAt: reassessment.createdAt,
      updatedAt: reassessment.updatedAt,
    };
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

  const completionChecks = Effect.fn("WorkflowDirectorService.completionChecks")(function* (
    completionId: string,
  ) {
    const rawRows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT completion_id AS "completionId", label, command,
          tool_call_id AS "toolCallId", exit_code AS "exitCode", output,
          started_head AS "startedHead", finished_head AS "finishedHead",
          started_clean AS "startedClean", finished_clean AS "finishedClean",
          verification_status AS "verificationStatus",
          verification_error AS "verificationError", created_at AS "createdAt"
        FROM workflow_capability_checks
        WHERE completion_id = ${completionId}
        ORDER BY rowid
      `,
      "Capability acceptance checks could not be read.",
    );
    return yield* Effect.forEach(rawRows, (candidate) => decodeCompletionCheckRow(candidate)).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "Capability acceptance check evidence is invalid.",
          String(error),
        ),
      ),
    );
  });

  const completionStatusFromRow = Effect.fn("WorkflowDirectorService.completionStatusFromRow")(
    function* (row: CompletionRow, authority: WorkflowCapabilityCompletionStatus["authority"]) {
      const checks = (yield* completionChecks(row.completionId)).map((check) => ({
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
      }));
      return {
        completionId: row.completionId,
        resultingHead: row.resultingHead,
        status: row.status as WorkflowCapabilityCompletionStatus["status"],
        authority,
        checks,
        evidenceUrl: row.commentUrl,
        requiredAction: row.requiredAction,
        lastError: row.lastError,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
      } satisfies WorkflowCapabilityCompletionStatus;
    },
  );

  const latestCompletion = Effect.fn("WorkflowDirectorService.latestCompletion")(function* (
    directorId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT completion_id AS "completionId", director_id AS "directorId", repository,
          capability_number AS "capabilityNumber", resulting_head AS "resultingHead",
          specification_fingerprint AS "specificationFingerprint",
          breakdown_fingerprint AS "breakdownFingerprint", comment_body AS "commentBody",
          status, comment_url AS "commentUrl", close_confirmed AS "closeConfirmed",
          close_owned AS "closeOwned",
          required_action AS "requiredAction", last_error AS "lastError",
          created_at AS "createdAt", updated_at AS "updatedAt"
        FROM workflow_capability_completions
        WHERE director_id = ${directorId}
        ORDER BY created_at DESC, rowid DESC
        LIMIT 1
      `,
      "Capability completion history could not be read.",
    );
    if (!rows[0]) return null;
    return yield* decodeCompletionRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The capability completion record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const reconcileDirector = Effect.fn("WorkflowDirectorService.reconcileDirector")(function* (
    row: DirectorRow,
  ) {
    if (row.status !== "submitting" && row.status !== "held") return row;
    if (row.status === "held" && (yield* activeReassessment(row.directorId))) return row;
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
      const handoff = yield* handoffForDirector(row.directorId);
      if (handoff?.successorDirectorId === row.directorId) return row;
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

  const completionForStatus = Effect.fn("WorkflowDirectorService.completionForStatus")(function* (
    director: DirectorRow,
  ) {
    let completion = yield* latestCompletion(director.directorId);
    if (!completion) return null;
    if (completion.status === "completed") {
      const current = yield* capabilityCompletionGate(
        director,
        completion.resultingHead,
        true,
      ).pipe(Effect.result);
      const authoritative =
        current._tag === "Success" &&
        current.success.capability.state === "closed" &&
        current.success.capability.stateReason === "completed" &&
        matchingCapabilityCompletion(current.success.capability, completion);
      if (authoritative) return yield* completionStatusFromRow(completion, "current");
      const reason =
        current._tag === "Failure"
          ? (current.failure.detail ?? current.failure.message)
          : "The saved capability completion evidence is no longer current.";
      if (current._tag === "Success" || completionEvidenceChanged(current.failure)) {
        completion = yield* compensateCapabilityClose(director, completion, reason);
        return yield* completionStatusFromRow(
          completion,
          completion.status === "invalidated" ? "historical" : "unknown",
        );
      }
      return {
        ...(yield* completionStatusFromRow(completion, "unknown")),
        requiredAction:
          "Refresh live tracker and workspace evidence before relying on this historical completion.",
        lastError: reason,
      };
    }
    return yield* completionStatusFromRow(
      completion,
      completion.status === "invalidated" || completion.status === "checks-failed"
        ? "historical"
        : "unknown",
    );
  });

  const statusFromRow = Effect.fn("WorkflowDirectorService.statusFromRow")(function* (
    original: DirectorRow,
  ) {
    const row = yield* reconcileDirector(original);
    const reassessment = yield* reassessmentStatus(row.directorId);
    const completion = yield* completionForStatus(row);
    const handoffRow = yield* handoffForDirector(row.directorId);
    const handoff = handoffRow ? yield* handoffStatusFromRow(handoffRow) : null;
    const handoffRecoveryTargets = yield* recoveryTargetsForDirector(row);
    const admissionRows = yield* admissions(row.directorId);
    const admissionCount = new Set(admissionRows.map((admission) => admission.slotTicketNumber))
      .size;
    const historyIds = yield* persistence(
      sql<{ readonly directorId: string }>`
        SELECT director_id AS "directorId" FROM workflow_directors
        WHERE environment_id = ${row.environmentId}
          AND repository COLLATE NOCASE = ${row.repository}
          AND capability_number = ${row.capabilityNumber}
        ORDER BY created_at
      `,
      "Capability director history could not be read for status.",
    ).pipe(Effect.map((rows) => rows.map((candidate) => candidate.directorId)));
    const workerHistory = (yield* Effect.forEach(historyIds, workers)).flat();
    const reviewHistory = (yield* Effect.forEach(historyIds, reviews)).flat();
    const resolutionHistory = (yield* Effect.forEach(historyIds, resolutions)).flat();
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
    const hasSettledRoot = Option.isSome(shell) && rootTurnIsSettled(shell.value);
    if (
      (row.status === "held" || row.status === "preparing-worktree") &&
      row.initialTurnDisposition === "not-attempted"
    ) {
      actions.push("retry");
    }
    if (
      reassessment &&
      hasSettledRoot &&
      reassessment.subjects.every(
        (subject) => subject.outcome === "stopped" || subject.outcome === "closed",
      )
    ) {
      actions.push("resume");
    } else if (reassessment) {
      actions.push("stop");
    } else if (
      row.status === "active" &&
      hasSettledRoot &&
      Option.isSome(shell) &&
      (shell.value.latestTurn?.state === "interrupted" || shell.value.latestTurn?.state === "error")
    ) {
      actions.push("resume");
    }
    const status =
      completion?.status === "completed" && completion.authority === "current"
        ? "completed"
        : admissionCount >= ADMISSION_LIMIT && row.status === "active"
          ? "waiting"
          : row.status;
    if (status === "completed") {
      actions.splice(0, actions.length, ...(Option.isSome(shell) ? (["open"] as const) : []));
    }
    const observation = [
      row.directorId,
      row.updatedAt,
      status,
      Option.isSome(shell) ? (shell.value.latestTurn?.turnId ?? "no-turn") : "no-thread",
      admissionCount,
      reassessment?.updatedAt ?? "no-reassessment",
      completion?.updatedAt ?? "no-completion",
      completion?.authority ?? "no-completion-authority",
      handoff?.updatedAt ?? "no-handoff",
      handoff?.latestReconciliation?.sequence ?? "no-handoff-reconciliation",
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
      workers: workerHistory,
      reviews: reviewHistory,
      resolutions: resolutionHistory,
      reassessment,
      completion,
      handoff,
      handoffRecoveryTargets,
      observation,
      actions,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      message:
        status === "completed"
          ? "Combined acceptance is current and the capability is closed completed."
          : status === "waiting"
            ? "This director has admitted ten delivery slices. Finish or settle admitted work, then wait for a successor."
            : reassessment
              ? "Work is held for reassessment. Resume only after every native turn is settled and the tracker records the current decision."
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
    yield* reconcileAcceptedResume(row.directorId);
    return yield* statusFromRow(yield* loadDirectorById(row.directorId));
  });

  const reassessmentTriggers = Effect.fn("WorkflowDirectorService.reassessmentTriggers")(function* (
    row: DirectorRow,
    input: ReassessmentInput,
  ) {
    const triggers: Array<{
      readonly kind: "scope-change" | "prerequisite";
      readonly issueNumber: number;
      readonly source: string;
      readonly previousFingerprint: string | null;
      readonly currentFingerprint: string | null;
      readonly requiredAction: string;
    }> = [];
    const records = input.issue.evidence?.records ?? [];
    for (const approvalKind of ["specification", "ticket-breakdown"] as const) {
      const baseline =
        approvalKind === "specification" ? row.specificationFingerprint : row.breakdownFingerprint;
      const changed = records.findLast(
        (record) =>
          record.kind === "approval" &&
          record.approvalKind === approvalKind &&
          record.state === "current" &&
          record.sourceAccess === "verified" &&
          record.scope === "changed" &&
          record.authority === "verified",
      );
      const current = records.findLast(
        (record) =>
          record.kind === "approval" &&
          record.approvalKind === approvalKind &&
          record.state === "current" &&
          record.sourceAccess === "verified" &&
          record.scope === "current" &&
          record.authority === "verified" &&
          record.approvedContent,
      );
      const currentFingerprint = current?.approvedContent
        ? workflowEvidenceBodyFingerprint(current.approvedContent)
        : null;
      if (changed || (baseline && currentFingerprint && baseline !== currentFingerprint)) {
        const source = changed?.url ?? current!.url;
        triggers.push({
          kind: "scope-change",
          issueNumber: input.issue.number,
          source,
          previousFingerprint: baseline,
          currentFingerprint: changed?.bodyFingerprint ?? currentFingerprint,
          requiredAction: `Reapprove the current ${approvalKind} and record a cleared reassessment that supersedes ${source}.`,
        });
      }
    }

    for (const blocker of input.issue.blockedBy.filter(invalidatesPrerequisite)) {
      triggers.push({
        kind: "prerequisite",
        issueNumber: blocker.number,
        source: blocker.url,
        previousFingerprint: null,
        currentFingerprint: null,
        requiredAction: `Restore or explicitly replace prerequisite #${blocker.number}, then record a cleared reassessment.`,
      });
    }
    if (
      input.issue.labels.includes("workflow:needs-reassessment") &&
      !triggers.some((trigger) => trigger.kind === "prerequisite")
    ) {
      triggers.push({
        kind: "prerequisite",
        issueNumber: input.issue.number,
        source: input.issue.url,
        previousFingerprint: null,
        currentFingerprint: null,
        requiredAction:
          "Record a current cleared reassessment for the live trigger before resuming.",
      });
    }

    for (const admission of yield* admissions(row.directorId)) {
      const admittedFingerprint = admission.currentScopeFingerprint ?? admission.scopeFingerprint;
      if (!admittedFingerprint) continue;
      const detail = yield* workflow
        .issueDetail({
          projectId: input.projectId,
          repository: row.repository as WorkflowIssueSummary["repository"],
          number: admission.ticketNumber,
        })
        .pipe(Effect.result);
      if (detail._tag === "Failure") continue;
      const currentFingerprint = workflowEvidenceBodyFingerprint(detail.success.body);
      if (currentFingerprint !== admittedFingerprint) {
        triggers.push({
          kind: "scope-change",
          issueNumber: admission.ticketNumber,
          source: detail.success.url,
          previousFingerprint: admittedFingerprint,
          currentFingerprint,
          requiredAction: `Reapprove changed ticket #${admission.ticketNumber}, then record a cleared reassessment that supersedes its scope-change record.`,
        });
      }
      for (const blocker of detail.success.blockedBy.filter(invalidatesPrerequisite)) {
        triggers.push({
          kind: "prerequisite",
          issueNumber: blocker.number,
          source: blocker.url,
          previousFingerprint: null,
          currentFingerprint: null,
          requiredAction: `Restore or explicitly replace prerequisite #${blocker.number}, then record a cleared reassessment.`,
        });
      }
    }
    return [
      ...new Map(
        triggers.map((trigger) => [
          `${trigger.kind}:${trigger.issueNumber}:${trigger.source}`,
          trigger,
        ]),
      ).values(),
    ];
  });

  const reassessUnlocked = Effect.fn("WorkflowDirectorService.reassess")(function* (
    input: ReassessmentInput,
    dispatch: InterruptDispatch,
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
    const found = yield* loadDirectorByCapability(
      {
        projectId: input.projectId,
        repository: input.issue.repository,
        capabilityNumber: input.issue.number,
      },
      environmentId,
    );
    if (!found || (found.status !== "active" && found.status !== "held")) return;
    yield* reconcileAcceptedResume(found.directorId);
    const row = yield* loadDirectorById(found.directorId);
    const liveIssue = yield* workflow
      .issueDetail({
        projectId: input.projectId,
        repository: row.repository as WorkflowIssueSummary["repository"],
        number: row.capabilityNumber,
      })
      .pipe(Effect.result);
    if (liveIssue._tag === "Failure") return;
    const triggers = yield* reassessmentTriggers(row, {
      projectId: input.projectId,
      issue: liveIssue.success,
    });
    if (triggers.length === 0) return;
    const primary = triggers.find((trigger) => trigger.kind === "scope-change") ?? triggers[0]!;
    const requiredAction = triggers.map((trigger) => trigger.requiredAction).join(" ");
    const existingReassessment = yield* activeReassessment(row.directorId);
    if (existingReassessment) {
      const discoveredAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* Effect.forEach(
              triggers,
              (trigger) =>
                sql`INSERT INTO workflow_reassessment_triggers (
                  reassessment_id, trigger_kind, trigger_issue_number, trigger_source,
                  previous_fingerprint, current_fingerprint, required_action, discovered_at
                ) VALUES (${existingReassessment.reassessmentId}, ${trigger.kind},
                  ${trigger.issueNumber}, ${trigger.source}, ${trigger.previousFingerprint},
                  ${trigger.currentFingerprint}, ${trigger.requiredAction}, ${discoveredAt})
                ON CONFLICT DO NOTHING`,
              { discard: true },
            );
            yield* sql`UPDATE workflow_reassessments SET required_action = ${requiredAction},
              updated_at = ${discoveredAt} WHERE reassessment_id = ${existingReassessment.reassessmentId}`;
          }),
        ),
        "The additional reassessment trigger could not be saved.",
      );
      return;
    }
    const reassessmentId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const now = DateTime.formatIso(yield* DateTime.now);
    const trackerBody = triggers.some((trigger) => trigger.kind === "scope-change")
      ? [
          "## Reassessment",
          "<!-- t3-workflow:v1 reassessment -->",
          `Trigger: ${primary.source}`,
          "Outcome: scope-change",
          "",
          "### Changes",
          `Approved scope changed while capability #${row.capabilityNumber} had active work.`,
          "",
          "### Evidence",
          ...triggers.map((trigger) => `- ${trigger.source}`),
          `- reassessment: \`${reassessmentId}\``,
        ].join("\n")
      : null;
    const nativeRows = yield* persistence(
      sql<{
        readonly nativeSessionId: string;
        readonly nativeTurnId: string;
        readonly status: string;
      }>`SELECT native_session_id AS "nativeSessionId", native_turn_id AS "nativeTurnId", status
          FROM workflow_director_native_turns WHERE director_id = ${row.directorId}`,
      "Native director evidence could not be read.",
    );
    const childRows = yield* persistence(
      sql<{
        readonly providerThreadId: string;
        readonly parentProviderThreadId: string | null;
        readonly nativeLifecycle: string | null;
        readonly nativeSessionId: string | null;
        readonly nativeTurnId: string | null;
        readonly nativeTurnStatus: string | null;
        readonly dispatchId: string | null;
        readonly reviewId: string | null;
      }>`SELECT o.provider_thread_id AS "providerThreadId",
          o.parent_provider_thread_id AS "parentProviderThreadId",
          o.native_lifecycle AS "nativeLifecycle",
          o.native_session_id AS "nativeSessionId", o.native_turn_id AS "nativeTurnId",
          o.native_turn_status AS "nativeTurnStatus", d.dispatch_id AS "dispatchId",
          r.review_id AS "reviewId"
        FROM workflow_worker_observations o
        LEFT JOIN workflow_worker_dispatches d ON d.provider_thread_id = o.provider_thread_id
          AND d.director_id = o.director_id
        LEFT JOIN workflow_ticket_reviews r ON r.provider_thread_id = o.provider_thread_id
          AND r.director_id = o.director_id
        WHERE o.director_id = ${row.directorId}`,
      "Known director children could not be read.",
    );
    const native = nativeRows[0];
    const rootSettled =
      native?.status === "completed" ||
      native?.status === "failed" ||
      native?.status === "interrupted";
    yield* persistence(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO workflow_reassessments (
              reassessment_id, director_id, trigger_kind, trigger_issue_number, trigger_source,
              previous_fingerprint, current_fingerprint, status, required_action, stop_command_id,
              stop_request_status, tracker_status, tracker_body, created_at, updated_at
            ) VALUES (
              ${reassessmentId}, ${row.directorId}, ${primary.kind}, ${primary.issueNumber},
              ${primary.source}, ${primary.previousFingerprint}, ${primary.currentFingerprint},
              'stopping', ${requiredAction}, ${commandId}, 'not-issued', 'pending', ${trackerBody}, ${now}, ${now}
            )
          `;
          yield* Effect.forEach(
            triggers,
            (trigger) =>
              sql`INSERT INTO workflow_reassessment_triggers (
                reassessment_id, trigger_kind, trigger_issue_number, trigger_source,
                previous_fingerprint, current_fingerprint, required_action, discovered_at
              ) VALUES (${reassessmentId}, ${trigger.kind}, ${trigger.issueNumber},
                ${trigger.source}, ${trigger.previousFingerprint}, ${trigger.currentFingerprint},
                ${trigger.requiredAction}, ${now})`,
            { discard: true },
          );
          yield* sql`UPDATE workflow_directors SET status = 'held', detail = ${requiredAction},
            updated_at = ${now} WHERE director_id = ${row.directorId}`;
          yield* sql`INSERT INTO workflow_interruption_subjects (
              reassessment_id, subject_id, subject_kind, provider_thread_id, native_session_id,
              native_turn_id, request_status, outcome, detail, discovered_at, updated_at
            ) VALUES (${reassessmentId}, 'director', 'director', ${row.threadId},
              ${native?.nativeSessionId ?? null}, ${native?.nativeTurnId ?? null}, 'not-issued',
              ${rootSettled ? "stopped" : "unknown"},
              ${rootSettled ? "The current native director turn had already ended before the hold." : null},
              ${now}, ${now})`;
          yield* Effect.forEach(
            childRows,
            (child) => {
              const childTurnSettled =
                child.nativeTurnStatus === "completed" ||
                child.nativeTurnStatus === "failed" ||
                child.nativeTurnStatus === "interrupted";
              return sql`INSERT INTO workflow_interruption_subjects (
                  reassessment_id, subject_id, subject_kind, provider_thread_id,
                  parent_provider_thread_id, native_session_id, native_turn_id,
                  request_status, outcome, detail, discovered_at, updated_at
                ) VALUES (${reassessmentId}, ${child.providerThreadId},
                  ${child.reviewId ? "reviewer" : child.dispatchId ? "worker" : "unknown-child"},
                  ${child.providerThreadId}, ${child.parentProviderThreadId},
                  ${child.nativeSessionId}, ${child.nativeTurnId}, 'not-issued',
                  ${child.nativeLifecycle === "closed" ? "closed" : childTurnSettled ? "stopped" : "unknown"},
                  ${child.nativeLifecycle === "closed" ? "The native child thread closed." : childTurnSettled ? `The matching native child turn had already ended with status ${child.nativeTurnStatus}.` : null},
                  ${now}, ${now})
                ON CONFLICT(reassessment_id, subject_id) DO NOTHING`;
            },
            { discard: true },
          );
        }),
      ),
      "The reassessment hold could not be saved.",
    );

    const dispatched = yield* dispatch({
      type: "thread.turn.interrupt",
      commandId,
      threadId: ThreadId.make(row.threadId),
      createdAt: now,
    }).pipe(Effect.result);
    const updatedAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql`UPDATE workflow_reassessments SET stop_request_status =
          ${dispatched._tag === "Success" ? "submitted" : "failed"},
          status = 'held', updated_at = ${updatedAt} WHERE reassessment_id = ${reassessmentId}`,
      "The interruption request result could not be saved.",
    );

    const trackerWrite = yield* Effect.gen(function* () {
      if (trackerBody) {
        yield* github.execute({
          cwd: row.worktreePath,
          args: [
            "issue",
            "comment",
            String(row.capabilityNumber),
            "--repo",
            row.repository,
            "--body-file",
            "-",
          ],
          stdin: trackerBody,
          maxOutputBytes: 100_000,
        });
      }
      yield* github.execute({
        cwd: row.worktreePath,
        args: [
          "issue",
          "edit",
          String(row.capabilityNumber),
          "--repo",
          row.repository,
          "--add-label",
          "workflow:needs-reassessment",
          "--remove-label",
          "ready-for-agent",
        ],
        maxOutputBytes: 100_000,
      });
      const verified = yield* workflow.issueDetail({
        projectId: input.projectId,
        repository: row.repository as WorkflowIssueSummary["repository"],
        number: row.capabilityNumber,
      });
      const record = trackerBody
        ? verified.evidence?.records.find(
            (candidate) =>
              candidate.kind === "reassessment" &&
              candidate.outcome === "scope-change" &&
              candidate.source === primary.source &&
              candidate.bodyFingerprint === workflowEvidenceBodyFingerprint(trackerBody),
          )
        : undefined;
      if (!verified.labels.includes("workflow:needs-reassessment") || (trackerBody && !record)) {
        return yield* Effect.fail("Tracker readback did not match the saved mutation intent.");
      }
      return record?.url ?? null;
    }).pipe(Effect.result);
    const trackerUpdatedAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql`UPDATE workflow_reassessments SET tracker_body = ${trackerBody},
          tracker_status = ${trackerWrite._tag === "Success" ? "confirmed" : "uncertain"},
          tracker_url = ${trackerWrite._tag === "Success" ? trackerWrite.success : null},
          updated_at = ${trackerUpdatedAt} WHERE reassessment_id = ${reassessmentId}`,
      "The tracker mutation result could not be saved.",
    );
  });

  const retryReassessmentUnlocked = Effect.fn("WorkflowDirectorService.retryReassessment")(
    function* (input: WorkflowDirectorReassessmentRetryInput, dispatch: InterruptDispatch) {
      const row = yield* loadDirectorById(input.directorId);
      if (
        row.projectId !== input.projectId ||
        row.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase() ||
        row.capabilityNumber !== input.capabilityNumber
      ) {
        return yield* directorError("director-not-found", "The selected director changed.");
      }
      const current = yield* statusFromRow(row);
      if (current.observation !== input.observation || !current.actions.includes("stop")) {
        return yield* directorError(
          "not-ready",
          "Refresh the reassessment before retrying its interruption.",
        );
      }
      const reassessment = current.reassessment!;
      const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
      const requestedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE workflow_reassessments SET status = 'stopping',
            stop_command_id = ${commandId}, stop_request_status = 'not-issued',
            updated_at = ${requestedAt} WHERE reassessment_id = ${reassessment.reassessmentId}`;
            yield* sql`UPDATE workflow_interruption_subjects SET request_status = 'not-issued',
            outcome = 'unknown', detail = 'An explicit interruption retry was requested.',
            updated_at = ${requestedAt}
            WHERE reassessment_id = ${reassessment.reassessmentId}
              AND outcome NOT IN ('stopped', 'closed')`;
          }),
        ),
        "The interruption retry intent could not be saved.",
      );
      const dispatched = yield* dispatch({
        type: "thread.turn.interrupt",
        commandId,
        threadId: ThreadId.make(row.threadId),
        createdAt: requestedAt,
      }).pipe(Effect.result);
      const dispatchedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`UPDATE workflow_reassessments SET status = 'held',
        stop_request_status = ${dispatched._tag === "Success" ? "submitted" : "failed"},
        updated_at = ${dispatchedAt} WHERE reassessment_id = ${reassessment.reassessmentId}`,
        "The interruption retry result could not be saved.",
      );

      const trackerRows = yield* persistence(
        sql<{ readonly body: string | null; readonly status: string }>`
        SELECT tracker_body AS body, tracker_status AS status FROM workflow_reassessments
        WHERE reassessment_id = ${reassessment.reassessmentId}
      `,
        "The reassessment tracker intent could not be read.",
      );
      const tracker = trackerRows[0];
      if (tracker?.body && tracker.status === "uncertain") {
        const readback = yield* workflow
          .issueDetail({
            projectId: ProjectId.make(row.projectId),
            repository: row.repository as WorkflowIssueSummary["repository"],
            number: row.capabilityNumber,
          })
          .pipe(Effect.result);
        if (readback._tag === "Success") {
          const record = readback.success.evidence?.records.find(
            (candidate) =>
              candidate.kind === "reassessment" &&
              candidate.bodyFingerprint === workflowEvidenceBodyFingerprint(tracker.body!),
          );
          if (record) {
            yield* persistence(
              sql`UPDATE workflow_reassessments SET tracker_status = 'confirmed',
              tracker_url = ${record.url}, updated_at = ${dispatchedAt}
              WHERE reassessment_id = ${reassessment.reassessmentId}`,
              "Confirmed tracker evidence could not be saved.",
            );
          }
        }
      }
      return yield* statusFromRow(row);
    },
  );

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
    const preparedCapability = yield* prepareCapability(input, project.workspaceRoot);
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
        initial_turn_disposition, specification_fingerprint, breakdown_fingerprint,
        created_at, updated_at
      ) VALUES (
        ${directorId}, ${batchId}, ${environmentId}, ${input.projectId}, ${input.repository}, ${input.rootNumber},
        ${input.capabilityNumber}, ${threadId}, ${commandId}, ${messageId}, ${worktreePath}, ${worktreeBranch},
        'preparing-worktree', ${DIRECTOR_MODEL}, ${input.modelSelection.instanceId},
        ${DIRECTOR_EFFORT}, 'unknown', 'not-attempted',
        ${workflowEvidenceBodyFingerprint(preparedCapability.specificationApproval.approvedContent ?? "")},
        ${workflowEvidenceBodyFingerprint(preparedCapability.breakdownApproval.approvedContent ?? "")},
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

  const loadResume = Effect.fn("WorkflowDirectorService.loadResume")(function* (
    directorId: string,
    sourceTurnId: string,
  ) {
    const rows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT resume_id AS "resumeId", director_id AS "directorId",
          source_turn_id AS "sourceTurnId", command_id AS "commandId", status, sequence,
          reassessment_id AS "reassessmentId",
          specification_fingerprint AS "specificationFingerprint",
          breakdown_fingerprint AS "breakdownFingerprint",
          admission_scopes_json AS "admissionScopesJson",
          reassessment_trigger_count AS "reassessmentTriggerCount"
        FROM workflow_director_resumes
        WHERE director_id = ${directorId} AND source_turn_id = ${sourceTurnId}
        LIMIT 1
      `,
      "Director resume history could not be read.",
    );
    if (!rows[0]) return null;
    return yield* decodeResumeRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError("persistence-failed", "A director resume record is invalid.", String(error)),
      ),
    );
  });

  const finalizeAcceptedResume = Effect.fn("WorkflowDirectorService.finalizeAcceptedResume")(
    function* (resume: ResumeRow, sequence: number) {
      const admissionScopes = resume.admissionScopesJson
        ? yield* decodeResumeAdmissionScopesJson(resume.admissionScopesJson).pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "The director resume authority record is invalid.",
                String(error),
              ),
            ),
          )
        : [];
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      return yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE workflow_director_resumes SET status = 'submitted',
              sequence = ${sequence}, detail = NULL, updated_at = ${updatedAt}
              WHERE resume_id = ${resume.resumeId}`;
            if (!resume.reassessmentId) return "cleared" as const;

            const reassessmentRows = yield* sql<{
              readonly status: string;
              readonly triggerCount: number;
              readonly unsettledCount: number;
              readonly projectedUnsettledCount: number;
            }>`
              SELECT r.status,
                (SELECT COUNT(*) FROM workflow_reassessment_triggers t
                  WHERE t.reassessment_id = r.reassessment_id) AS "triggerCount",
                (SELECT COUNT(*) FROM workflow_interruption_subjects s
                  WHERE s.reassessment_id = r.reassessment_id
                    AND s.outcome NOT IN ('stopped', 'closed')
                    AND NOT (
                      s.subject_kind = 'director'
                      AND EXISTS (
                        SELECT 1 FROM projection_turns turn
                        JOIN orchestration_command_receipts receipt
                          ON receipt.command_id = ${resume.commandId}
                            AND receipt.status = 'accepted'
                        WHERE turn.thread_id = (
                          SELECT thread_id FROM workflow_directors
                          WHERE director_id = ${resume.directorId}
                        )
                          AND turn.turn_id = s.native_turn_id
                          AND turn.pending_message_id = (
                            SELECT message_id FROM workflow_director_resumes
                            WHERE resume_id = ${resume.resumeId}
                          )
                      )
                    )) AS "unsettledCount",
                (SELECT COUNT(*) FROM projection_thread_sessions session
                  WHERE session.thread_id = (
                    SELECT thread_id FROM workflow_directors
                    WHERE director_id = ${resume.directorId}
                  )
                    AND (session.status IN ('running', 'starting')
                      OR session.active_turn_id IS NOT NULL)
                    AND NOT EXISTS (
                      SELECT 1 FROM projection_turns turn
                      JOIN orchestration_command_receipts receipt
                        ON receipt.command_id = ${resume.commandId}
                          AND receipt.status = 'accepted'
                      WHERE turn.thread_id = session.thread_id
                        AND turn.pending_message_id = (
                          SELECT message_id FROM workflow_director_resumes
                          WHERE resume_id = ${resume.resumeId}
                        )
                        AND (
                          (session.status = 'starting' AND session.active_turn_id IS NULL
                            AND turn.turn_id IS NULL AND turn.state = 'pending'
                            AND turn.checkpoint_turn_count IS NULL)
                          OR (session.status = 'running'
                            AND session.active_turn_id IS NOT NULL
                            AND turn.turn_id = session.active_turn_id)
                        )
                    )) AS "projectedUnsettledCount"
              FROM workflow_reassessments r
              WHERE r.reassessment_id = ${resume.reassessmentId}
              LIMIT 1
            `;
            const reassessment = reassessmentRows[0];
            if (!reassessment || reassessment.status === "cleared") return "cleared" as const;

            const canClear =
              resume.specificationFingerprint !== null &&
              resume.breakdownFingerprint !== null &&
              resume.admissionScopesJson !== null &&
              resume.reassessmentTriggerCount !== null &&
              reassessment.triggerCount === resume.reassessmentTriggerCount &&
              reassessment.unsettledCount === 0 &&
              reassessment.projectedUnsettledCount === 0;
            if (!canClear) {
              const detail =
                "The accepted resume remains held because newer native activity or reassessment evidence needs interruption and review.";
              yield* sql`UPDATE workflow_director_resumes SET detail = ${detail},
                updated_at = ${updatedAt} WHERE resume_id = ${resume.resumeId}`;
              yield* sql`UPDATE workflow_directors SET status = 'held', detail = ${detail},
                updated_at = ${updatedAt} WHERE director_id = ${resume.directorId}`;
              return "held" as const;
            }

            yield* sql`UPDATE workflow_reassessments SET status = 'cleared',
              tracker_status = 'confirmed', updated_at = ${updatedAt}
              WHERE reassessment_id = ${resume.reassessmentId}`;
            yield* sql`UPDATE workflow_directors SET status = 'active', detail = NULL,
              specification_fingerprint = ${resume.specificationFingerprint},
              breakdown_fingerprint = ${resume.breakdownFingerprint},
              updated_at = ${updatedAt} WHERE director_id = ${resume.directorId}`;
            yield* Effect.forEach(
              admissionScopes,
              (scope) => sql`UPDATE workflow_director_admissions
                SET current_scope_body = ${scope.body},
                  current_scope_fingerprint = ${scope.fingerprint},
                  updated_at = ${updatedAt}
                WHERE admission_id = ${scope.admissionId} AND director_id = ${resume.directorId}`,
              { discard: true },
            );
            return "cleared" as const;
          }),
        ),
        "The accepted director resume could not be reconciled.",
      );
    },
  );

  const reconcileAcceptedResume = Effect.fn("WorkflowDirectorService.reconcileAcceptedResume")(
    function* (directorId: string) {
      const rows = yield* persistence(
        sql<Record<string, unknown>>`
          SELECT resume.resume_id AS "resumeId", resume.director_id AS "directorId",
            resume.source_turn_id AS "sourceTurnId", resume.command_id AS "commandId",
            resume.status, resume.sequence, resume.reassessment_id AS "reassessmentId",
            resume.specification_fingerprint AS "specificationFingerprint",
            resume.breakdown_fingerprint AS "breakdownFingerprint",
            resume.admission_scopes_json AS "admissionScopesJson",
            resume.reassessment_trigger_count AS "reassessmentTriggerCount"
          FROM workflow_director_resumes resume
          JOIN workflow_reassessments reassessment
            ON reassessment.reassessment_id = resume.reassessment_id
          WHERE resume.director_id = ${directorId} AND reassessment.status = 'clearing'
          ORDER BY resume.created_at DESC LIMIT 1
        `,
        "Pending director resume recovery could not be read.",
      );
      if (!rows[0]) return;
      const resume = yield* decodeResumeRow(rows[0]).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "A director resume record is invalid.",
            String(error),
          ),
        ),
      );
      const receipt = yield* receipts
        .getByCommandId({ commandId: CommandId.make(resume.commandId) })
        .pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Director resume evidence could not be read.",
              String(error),
            ),
          ),
        );
      if (Option.isSome(receipt) && receipt.value.status === "accepted") {
        yield* finalizeAcceptedResume(resume, receipt.value.resultSequence);
      }
    },
  );

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
      const previous = yield* loadResume(row.directorId, observedSourceTurnId);
      if (previous) {
        const receipt = yield* receipts
          .getByCommandId({ commandId: CommandId.make(previous.commandId) })
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
          (previous.status === "submitted" && previous.sequence !== null)
        ) {
          const sequence = Option.isSome(receipt)
            ? receipt.value.resultSequence
            : previous.sequence!;
          yield* finalizeAcceptedResume(previous, sequence);
          return yield* statusFromRow(yield* loadDirectorById(row.directorId));
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
    const reassessment = current.reassessment;
    let reassessmentTriggerCount: number | null = null;
    if (reassessment) {
      const unsettled = reassessment.subjects.filter(
        (subject) => subject.outcome !== "stopped" && subject.outcome !== "closed",
      );
      if (unsettled.length > 0) {
        return yield* directorError(
          "not-ready",
          `Resume is held while ${unsettled.length} native director or child turn remains unsettled.`,
        );
      }
      const triggerRows = yield* persistence(
        sql<{
          readonly latestAt: string;
          readonly scopeChanges: number;
          readonly triggerCount: number;
        }>`
          SELECT MAX(discovered_at) AS "latestAt",
            SUM(CASE WHEN trigger_kind = 'scope-change' THEN 1 ELSE 0 END) AS "scopeChanges",
            COUNT(*) AS "triggerCount"
          FROM workflow_reassessment_triggers
          WHERE reassessment_id = ${reassessment.reassessmentId}
        `,
        "Reassessment triggers could not be read.",
      );
      const latestTriggerAt = triggerRows[0]?.latestAt ?? reassessment.createdAt;
      const hasScopeChange = (triggerRows[0]?.scopeChanges ?? 0) > 0;
      reassessmentTriggerCount = triggerRows[0]?.triggerCount ?? 0;
      const live = yield* workflow.issueDetail({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowIssueSummary["repository"],
        number: row.capabilityNumber,
      });
      const cleared = live.evidence?.records.findLast(
        (record) =>
          record.kind === "reassessment" &&
          record.state === "current" &&
          record.scope === "current" &&
          record.sourceAccess === "verified" &&
          record.outcome === "cleared" &&
          record.createdAt > latestTriggerAt,
      );
      const currentScopeChange = live.evidence?.records.some(
        (record) =>
          record.kind === "reassessment" &&
          record.state === "current" &&
          record.outcome === "scope-change",
      );
      if (!cleared || currentScopeChange) {
        return yield* directorError(
          "not-ready",
          hasScopeChange
            ? "Resume requires a current cleared reassessment that supersedes every scope-change record and renewed approval for current scope."
            : "Resume requires a current cleared reassessment for the restored prerequisite.",
        );
      }
      const clearingAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`UPDATE workflow_reassessments SET status = 'clearing', tracker_status = 'pending',
          tracker_url = ${cleared.url}, updated_at = ${clearingAt}
          WHERE reassessment_id = ${reassessment.reassessmentId}`,
        "The reassessment clearing intent could not be saved.",
      );
      const labelClear = yield* github
        .execute({
          cwd: row.worktreePath,
          args: [
            "issue",
            "edit",
            String(row.capabilityNumber),
            "--repo",
            row.repository,
            "--remove-label",
            "workflow:needs-reassessment",
          ],
          maxOutputBytes: 100_000,
        })
        .pipe(Effect.result);
      if (labelClear._tag === "Failure") {
        yield* persistence(
          sql`UPDATE workflow_reassessments SET tracker_status = 'uncertain'
            WHERE reassessment_id = ${reassessment.reassessmentId}`,
          "The uncertain reassessment tracker result could not be saved.",
        );
        return yield* directorError(
          "not-ready",
          "The reassessment label clear is uncertain. Refresh GitHub before resuming.",
        );
      }
    }
    const project = yield* selectedProject(ProjectId.make(row.projectId));
    const resumeAdmissions = yield* admissions(row.directorId);
    const ownedClaims = ownedClaimLogins(resumeAdmissions);
    const prepared = yield* prepareCapability(
      {
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowDirectorStartInput["repository"],
        capabilityNumber: row.capabilityNumber,
        modelSelection: input.modelSelection,
      },
      row.worktreePath,
      {
        ownedClaims,
        admittedTicketNumbers: new Set(resumeAdmissions.map((admission) => admission.ticketNumber)),
      },
    );
    const legacyAdmission = resumeAdmissions.find(
      (admission) => !(admission.currentScopeFingerprint ?? admission.scopeFingerprint),
    );
    if (legacyAdmission) {
      return yield* directorError(
        "not-ready",
        `Admission for ticket #${legacyAdmission.ticketNumber} has no recoverable approved scope baseline.`,
        "Start fresh delivery authority instead of inferring historical scope from the current issue body.",
      );
    }
    const refreshedAdmissionScopes = yield* Effect.forEach(resumeAdmissions, (admission) =>
      Effect.gen(function* () {
        const issue = yield* workflow.issueDetail({
          projectId: ProjectId.make(row.projectId),
          repository: row.repository as WorkflowIssueSummary["repository"],
          number: admission.ticketNumber,
        });
        if (issue.readiness?.status !== "resolved") {
          yield* requireDeliveryReadiness(row, issue, admission, true);
        }
        return {
          admissionId: admission.admissionId,
          body: issue.body,
          fingerprint: workflowEvidenceBodyFingerprint(issue.body),
        };
      }),
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
    const specificationFingerprint = workflowEvidenceBodyFingerprint(
      prepared.specificationApproval.approvedContent ?? "",
    );
    const breakdownFingerprint = workflowEvidenceBodyFingerprint(
      prepared.breakdownApproval.approvedContent ?? "",
    );
    const admissionScopesJson = encodeResumeAdmissionScopesJson(refreshedAdmissionScopes);
    const resumeDecision = yield* persistence(
      sql.withTransaction(
        Effect.gen(function* () {
          const currentShell = yield* projection.getThreadShellById(ThreadId.make(row.threadId));
          if (Option.isNone(currentShell) || !rootTurnIsSettled(currentShell.value)) {
            return "root-active" as const;
          }
          if (reassessment) {
            const validationRows = yield* sql<{
              readonly triggerCount: number;
              readonly unsettledCount: number;
            }>`
              SELECT
                (SELECT COUNT(*) FROM workflow_reassessment_triggers t
                  WHERE t.reassessment_id = r.reassessment_id) AS "triggerCount",
                (SELECT COUNT(*) FROM workflow_interruption_subjects s
                  WHERE s.reassessment_id = r.reassessment_id
                    AND s.outcome NOT IN ('stopped', 'closed')) AS "unsettledCount"
              FROM workflow_reassessments r
              WHERE r.reassessment_id = ${reassessment.reassessmentId}
                AND r.director_id = ${row.directorId} AND r.status != 'cleared'
              LIMIT 1
            `;
            const validation = validationRows[0];
            if (
              !validation ||
              validation.triggerCount !== reassessmentTriggerCount ||
              validation.unsettledCount > 0
            ) {
              return "reassessment-changed" as const;
            }
          }
          yield* sql`
            INSERT INTO workflow_director_resumes (
              resume_id, director_id, source_turn_id, command_id, message_id, status,
              reassessment_id, specification_fingerprint, breakdown_fingerprint,
              admission_scopes_json, reassessment_trigger_count, created_at, updated_at
            ) VALUES (${resumeId}, ${row.directorId}, ${sourceTurnId}, ${commandId}, ${messageId},
              'submitting', ${reassessment?.reassessmentId ?? null}, ${specificationFingerprint},
              ${breakdownFingerprint}, ${admissionScopesJson}, ${reassessmentTriggerCount},
              ${createdAt}, ${createdAt})
          `;
          return "saved" as const;
        }),
      ),
      "The director resume decision could not be saved.",
    );
    if (resumeDecision === "root-active") {
      return yield* directorError(
        "not-ready",
        "The director thread started new work while Resume was being prepared.",
        "Interrupt and settle the current root turn, then refresh before retrying Resume.",
      );
    }
    if (resumeDecision === "reassessment-changed") {
      return yield* directorError(
        "not-ready",
        "Native activity or reassessment evidence changed while Resume was being prepared.",
        "Interrupt and settle every current subject, then reassess before retrying Resume.",
      );
    }
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
    let acceptedSequence: number;
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
      if (!accepted) {
        yield* persistence(
          sql`UPDATE workflow_director_resumes SET status = 'held', sequence = NULL,
            detail = 'The director resume is uncertain and will not be sent again automatically.',
            updated_at = ${createdAt} WHERE resume_id = ${resumeId}`,
          "The director resume outcome could not be saved.",
        );
        return yield* directorError(
          "dispatch-failed",
          "The director resume is uncertain and will not be sent again automatically.",
        );
      }
      acceptedSequence = receipt.value.resultSequence;
    } else {
      acceptedSequence = dispatched.success.sequence;
    }
    const persistedResume = (yield* loadResume(row.directorId, sourceTurnId))!;
    yield* finalizeAcceptedResume(persistedResume, acceptedSequence);
    return yield* statusFromRow(yield* loadDirectorById(row.directorId));
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
    yield* ensurePredecessorExecutionSettled(row);
    if (yield* activeReassessment(row.directorId)) {
      return yield* directorError(
        "not-ready",
        "New admissions are held until the active reassessment is explicitly cleared.",
      );
    }
    const existingRows = yield* admissions(row.directorId);
    const historicalAdmissionRows = yield* persistence(
      sql<Record<string, unknown>>`
        SELECT a.admission_id AS "admissionId", a.director_id AS "directorId",
          a.batch_id AS "batchId", a.repository, a.ticket_number AS "ticketNumber",
          a.slot_ticket_number AS "slotTicketNumber", a.purpose, a.ownership,
          a.claim_login AS "claimLogin", a.claim_status AS "claimStatus",
          a.scope_body AS "scopeBody", a.scope_fingerprint AS "scopeFingerprint",
          a.current_scope_body AS "currentScopeBody",
          a.current_scope_fingerprint AS "currentScopeFingerprint",
          a.created_at AS "createdAt", a.updated_at AS "updatedAt"
        FROM workflow_director_admissions a
        JOIN workflow_directors d ON d.director_id = a.director_id
        WHERE d.environment_id = ${row.environmentId}
          AND d.repository COLLATE NOCASE = ${row.repository}
          AND d.capability_number = ${row.capabilityNumber}
      `,
      "Capability admission history could not be read before admission.",
    );
    const historicalAdmissions = yield* Effect.forEach(historicalAdmissionRows, (candidate) =>
      decodeAdmissionRow(candidate),
    ).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "Capability admission history is invalid.",
          String(error),
        ),
      ),
    );
    const existing = existingRows.find(
      (admission) =>
        admission.repository.toLocaleLowerCase() === input.repository.toLocaleLowerCase() &&
        admission.ticketNumber === input.ticketNumber,
    );
    if (existing && !existing.scopeFingerprint) {
      return yield* directorError(
        "not-ready",
        "This legacy admission has no recoverable approved scope snapshot.",
        "Reassess the delivery unit before preparing or retrying work.",
      );
    }
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
      { ownedClaims: ownedClaimLogins(historicalAdmissions) },
    );
    if (!row.specificationFingerprint || !row.breakdownFingerprint) {
      return yield* directorError(
        "not-ready",
        "This legacy director has no recoverable approved scope baseline.",
        "Explicitly reassess the capability before admitting or preparing work.",
      );
    }
    if (
      workflowEvidenceBodyFingerprint(
        capabilityState.specificationApproval.approvedContent ?? "",
      ) !== row.specificationFingerprint ||
      workflowEvidenceBodyFingerprint(capabilityState.breakdownApproval.approvedContent ?? "") !==
        row.breakdownFingerprint
    ) {
      return yield* directorError(
        "not-ready",
        "Approved capability scope changed while this director remained active.",
        "Record and clear a reassessment before admitting or preparing more work.",
      );
    }
    const ticket = yield* workflow.issueDetail({
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowIssueSummary["repository"],
      number: input.ticketNumber,
    });
    const publishedTicket = capabilityState.tickets.find(
      (candidate) => candidate.id === ticket.id && candidate.number === ticket.number,
    );
    const admittedFingerprint = existing?.currentScopeFingerprint ?? existing?.scopeFingerprint;
    if (existing && admittedFingerprint !== workflowEvidenceBodyFingerprint(ticket.body)) {
      return yield* directorError(
        "not-ready",
        "Approved delivery scope changed while this admission remained active.",
        "Record and clear a reassessment before retrying or preparing this work.",
      );
    }
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
        slot_ticket_number, purpose, ownership, claim_status, scope_body, scope_fingerprint,
        current_scope_body, current_scope_fingerprint,
        created_at, updated_at
      ) VALUES (
        ${admissionId}, ${row.directorId}, ${row.batchId}, ${row.repository}, ${ticket.id}, ${ticket.number},
        ${slotTicketNumber}, ${input.purpose}, ${input.ownership}, 'pending', ${ticket.body},
        ${workflowEvidenceBodyFingerprint(ticket.body)}, ${ticket.body},
        ${workflowEvidenceBodyFingerprint(ticket.body)}, ${createdAt}, ${createdAt}
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
          const inheritedClaim = historicalAdmissions.find(
            (admission) =>
              admission.directorId !== row.directorId &&
              admission.ticketNumber === ticket.number &&
              admission.claimStatus === "confirmed" &&
              admission.claimLogin === claimLogin,
          );
          claimStatus =
            current.length === 1 && current[0] === claimLogin && inheritedClaim
              ? "confirmed"
              : "conflict";
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
      scopeBody: ticket.body,
      scopeFingerprint: workflowEvidenceBodyFingerprint(ticket.body),
      currentScopeBody: ticket.body,
      currentScopeFingerprint: workflowEvidenceBodyFingerprint(ticket.body),
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
          specification_fingerprint AS "specificationFingerprint",
          breakdown_fingerprint AS "breakdownFingerprint",
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

  const prepareHandoffUnlocked = Effect.fn("WorkflowDirectorService.prepareHandoff")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowDirectorHandoffPrepareInput,
  ) {
    const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    const admissionRows = yield* admissions(row.directorId);
    const slotCount = new Set(admissionRows.map((admission) => admission.slotTicketNumber)).size;
    if (slotCount < ADMISSION_LIMIT) {
      return yield* directorError(
        "not-ready",
        `A director handoff is available only after ${ADMISSION_LIMIT} distinct delivery slots are admitted.`,
      );
    }
    if (input.suggestedSkills.length === 0 || input.suggestedStaffing.length === 0) {
      return yield* directorError(
        "not-ready",
        "A durable handoff requires suggested skills and staffing for the successor.",
      );
    }
    if (yield* activeReassessment(row.directorId)) {
      return yield* directorError(
        "not-ready",
        "The director must finish reassessment before preparing succession.",
      );
    }
    const existing = yield* outgoingHandoff(row.directorId);
    if (existing) {
      const sameInput =
        existing.lessonsJson === encodeStringArrayJson(input.lessons) &&
        existing.unresolvedContextJson === encodeStringArrayJson(input.unresolvedContext) &&
        existing.suggestedSkillsJson === encodeStringArrayJson(input.suggestedSkills) &&
        existing.suggestedStaffingJson === encodeStringArrayJson(input.suggestedStaffing);
      if (!sameInput) {
        return yield* directorError(
          "not-ready",
          "The saved handoff differs from this request. Refresh its current status before replacing context.",
        );
      }
      if (existing.status === "held" && existing.successorDirectorId !== null) {
        const recoveredAt = DateTime.formatIso(yield* DateTime.now);
        yield* persistence(
          sql`UPDATE workflow_director_handoffs SET successor_director_id = NULL,
            successor_thread_id = NULL, successor_command_id = NULL, successor_message_id = NULL,
            successor_prompt = NULL, settlements_json = '[]', implementation_head = NULL,
            specification_links_json = '[]', issue_links_json = '[]', review_links_json = '[]',
            commit_links_json = '[]', status = 'waiting-settlement', detail = NULL,
            updated_at = ${recoveredAt} WHERE handoff_id = ${existing.handoffId} AND status = 'held'`,
          "The held handoff could not be reset for an explicit source retry.",
        );
        return yield* handoffStatusFromRow((yield* outgoingHandoff(row.directorId))!);
      }
      return yield* handoffStatusFromRow(existing);
    }
    const handoffId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const admissionSnapshot = admissionRows.map((admission) => ({
      admissionId: admission.admissionId,
      ticketNumber: admission.ticketNumber,
      slotTicketNumber: admission.slotTicketNumber,
      claimStatus: admission.claimStatus,
      outcome: "pending",
    }));
    yield* persistence(
      sql`
        INSERT INTO workflow_director_handoffs (
          handoff_id, source_director_id, source_thread_id, source_batch_id,
          admissions_json, settlements_json, implementation_head, worktree_path, worktree_branch,
          specification_links_json, issue_links_json, review_links_json, commit_links_json,
          suggested_skills_json, suggested_staffing_json, lessons_json, unresolved_context_json,
          status, created_at, updated_at
        ) VALUES (
          ${handoffId}, ${row.directorId}, ${row.threadId}, ${row.batchId},
          ${encodeHandoffAdmissionsJson(admissionSnapshot)}, ${encodeHandoffSettlementsJson([])}, NULL,
          ${row.worktreePath}, ${row.worktreeBranch}, ${encodeStringArrayJson([])},
          ${encodeStringArrayJson([])}, ${encodeStringArrayJson([])}, ${encodeStringArrayJson([])},
          ${encodeStringArrayJson(input.suggestedSkills)},
          ${encodeStringArrayJson(input.suggestedStaffing)}, ${encodeStringArrayJson(input.lessons)},
          ${encodeStringArrayJson(input.unresolvedContext)}, 'waiting-settlement',
          ${createdAt}, ${createdAt}
        )
      `,
      "The director handoff intent could not be saved.",
    );
    return yield* handoffStatusFromRow((yield* handoffForDirector(row.directorId))!);
  });

  const settledHandoffSnapshot = Effect.fn("WorkflowDirectorService.settledHandoffSnapshot")(
    function* (row: DirectorRow, handoff: HandoffRow, acceptedImplementationHead?: string) {
      const shell = yield* projection
        .getThreadShellById(ThreadId.make(row.threadId))
        .pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The source director turn could not be read.",
              String(error),
            ),
          ),
        );
      if (Option.isNone(shell) || !rootTurnIsSettled(shell.value)) {
        return yield* directorError(
          "not-ready",
          "The source director turn must settle before succession.",
        );
      }
      const nativeRoot = yield* persistence(
        sql<{
          readonly nativeSessionId: string;
          readonly nativeTurnId: string;
          readonly status: string;
          readonly updatedAt: string;
        }>`
          SELECT native_session_id AS "nativeSessionId", native_turn_id AS "nativeTurnId",
            status, updated_at AS "updatedAt" FROM workflow_director_native_turns
          WHERE director_id = ${row.directorId} LIMIT 1
        `,
        "The source director native turn could not be read.",
      );
      if (
        !nativeRoot[0] ||
        !["completed", "failed", "interrupted"].includes(nativeRoot[0].status)
      ) {
        return yield* directorError(
          "not-ready",
          "The source director has no exact terminal native turn evidence.",
        );
      }
      const children = yield* persistence(
        sql<{
          readonly providerThreadId: string;
          readonly nativeLifecycle: string | null;
          readonly nativeTurnStatus: string | null;
          readonly nativeSessionId: string | null;
          readonly nativeTurnId: string | null;
          readonly updatedAt: string;
          readonly associated: number;
        }>`
          SELECT o.provider_thread_id AS "providerThreadId",
            o.native_lifecycle AS "nativeLifecycle", o.native_turn_status AS "nativeTurnStatus",
            o.native_session_id AS "nativeSessionId", o.native_turn_id AS "nativeTurnId",
            o.updated_at AS "updatedAt",
            CASE WHEN EXISTS (
              SELECT 1 FROM workflow_worker_dispatches d
              WHERE d.director_id = o.director_id AND d.provider_thread_id = o.provider_thread_id
            ) OR EXISTS (
              SELECT 1 FROM workflow_ticket_reviews r
              WHERE r.director_id = o.director_id AND r.provider_thread_id = o.provider_thread_id
            ) OR EXISTS (
              SELECT 1 FROM workflow_review_axes a
              JOIN workflow_ticket_reviews r ON r.review_id = a.review_id
              WHERE r.director_id = o.director_id AND a.provider_thread_id = o.provider_thread_id
            ) THEN 1 ELSE 0 END AS associated
          FROM workflow_worker_observations o WHERE o.director_id = ${row.directorId}
        `,
        "Source child settlement evidence could not be read.",
      );
      const unsettled = children.filter(
        (child) =>
          child.associated !== 1 ||
          (child.nativeLifecycle !== "closed" && child.nativeTurnStatus !== "interrupted"),
      );
      const unassociatedLaunches = yield* persistence(
        sql<{ readonly launchId: string }>`
          SELECT dispatch_id AS "launchId" FROM workflow_worker_dispatches
          WHERE director_id = ${row.directorId} AND provider_thread_id IS NULL
          UNION ALL
          SELECT review_id AS "launchId" FROM workflow_ticket_reviews
          WHERE director_id = ${row.directorId} AND status = 'spawn-issued'
            AND provider_thread_id IS NULL
        `,
        "Source child association evidence could not be read.",
      );
      if (unsettled.length > 0 || unassociatedLaunches.length > 0) {
        return yield* directorError(
          "not-ready",
          "Every source worker and reviewer must be associated and exactly closed or interrupted before succession.",
          [
            ...unsettled.map((child) => child.providerThreadId),
            ...unassociatedLaunches.map((entry) => entry.launchId),
          ].join(", "),
        );
      }
      const observed = acceptedImplementationHead
        ? { head: acceptedImplementationHead, clean: true }
        : yield* gitObservation(row.worktreePath);
      if (!observed.clean || !/^[0-9a-f]{40}$/iu.test(observed.head)) {
        return yield* directorError(
          "not-ready",
          "Succession requires a clean capability worktree at one exact implementation commit.",
          observed.head,
        );
      }
      const admissionRows = yield* admissions(row.directorId);
      const workerRows = yield* workers(row.directorId);
      const resolutionRows = yield* resolutions(row.directorId);
      const outcomeByTicket = new Map<number, string>();
      for (const resolution of resolutionRows)
        outcomeByTicket.set(resolution.ticketNumber, resolution.status);
      for (const worker of workerRows) {
        if (worker.ticketNumber && worker.handoff)
          outcomeByTicket.set(worker.ticketNumber, worker.handoff.outcome);
      }
      const admissionSnapshot = admissionRows.map((admission) => ({
        admissionId: admission.admissionId,
        ticketNumber: admission.ticketNumber,
        slotTicketNumber: admission.slotTicketNumber,
        claimStatus: admission.claimStatus,
        outcome: outcomeByTicket.get(admission.ticketNumber) ?? admission.claimStatus,
      }));
      const capability = yield* workflow.issueDetail({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowIssueSummary["repository"],
        number: row.capabilityNumber,
      });
      const specificationLinks = (capability.evidence?.records ?? [])
        .filter((record) => record.kind === "approval")
        .flatMap((record) => [record.url, ...(record.source ? [record.source] : [])]);
      const issueLinks = [
        capability.url,
        ...admissionRows.map(
          (admission) => `https://github.com/${row.repository}/issues/${admission.ticketNumber}`,
        ),
      ];
      const reviewLinks = resolutionRows.flatMap((resolution) =>
        resolution.commentUrl ? [resolution.commentUrl] : [],
      );
      const commitLinks = [
        ...new Set(workerRows.flatMap((worker) => worker.handoff?.commits ?? [])),
      ].map((commit) => `https://github.com/${row.repository}/commit/${commit}`);
      const settlements = [
        {
          kind: "director" as const,
          providerThreadId: row.threadId,
          nativeSessionId: nativeRoot[0]!.nativeSessionId,
          nativeTurnId: nativeRoot[0]!.nativeTurnId,
          mode:
            nativeRoot[0]!.status === "interrupted"
              ? ("interrupted" as const)
              : ("closed" as const),
          observedAt: nativeRoot[0]!.updatedAt,
        },
        ...children.map((child) => ({
          kind: "child" as const,
          providerThreadId: child.providerThreadId,
          nativeSessionId: child.nativeSessionId,
          nativeTurnId: child.nativeTurnId,
          mode: child.nativeLifecycle === "closed" ? ("closed" as const) : ("interrupted" as const),
          observedAt: child.updatedAt,
        })),
      ];
      return {
        admissionsJson: encodeHandoffAdmissionsJson(admissionSnapshot),
        settlementsJson: encodeHandoffSettlementsJson(settlements),
        implementationHead: observed.head,
        specificationLinksJson: encodeStringArrayJson([...new Set(specificationLinks)]),
        issueLinksJson: encodeStringArrayJson([...new Set(issueLinks)]),
        reviewLinksJson: encodeStringArrayJson([...new Set(reviewLinks)]),
        commitLinksJson: encodeStringArrayJson(commitLinks),
      };
    },
  );

  const reconcileHandoffForCurrent = Effect.fn(
    "WorkflowDirectorService.reconcileHandoffForCurrent",
  )(function* (
    current: DirectorRow,
    input: WorkflowDirectorHandoffReconcileInput,
    actor: WorkflowDirectorHandoffReconciliationActor,
    expectedTargetObservation?: string,
  ) {
    const handoff = yield* loadHandoffById(input.handoffId);
    const source = yield* loadDirectorHistoryById(handoff.sourceDirectorId);
    if (
      source.directorId === current.directorId ||
      source.environmentId !== current.environmentId ||
      source.repository.toLocaleLowerCase() !== current.repository.toLocaleLowerCase() ||
      source.capabilityNumber !== current.capabilityNumber ||
      source.worktreePath !== current.worktreePath ||
      source.worktreeBranch !== current.worktreeBranch ||
      handoff.worktreePath !== current.worktreePath ||
      handoff.worktreeBranch !== current.worktreeBranch
    ) {
      return yield* directorError(
        "not-ready",
        "Only an exact predecessor handoff in this director's capability worktree can be reconciled.",
        input.handoffId,
      );
    }
    const ancestry = yield* persistence(
      sql<{ readonly count: number }>`
        WITH RECURSIVE predecessor_handoffs(handoff_id, source_director_id) AS (
          SELECT handoff_id, source_director_id FROM workflow_director_handoffs
          WHERE successor_director_id = ${current.directorId}
          UNION ALL
          SELECT h.handoff_id, h.source_director_id FROM workflow_director_handoffs h
          JOIN predecessor_handoffs p ON h.successor_director_id = p.source_director_id
        )
        SELECT count(*) AS count FROM predecessor_handoffs WHERE handoff_id = ${handoff.handoffId}
      `,
      "The predecessor handoff chain could not be verified.",
    );
    if (ancestry[0]?.count !== 1) {
      return yield* directorError(
        "not-ready",
        "The requested handoff is not a predecessor of the current director.",
        input.handoffId,
      );
    }
    if (handoff.successorDirectorId === current.directorId) {
      const receiptState = yield* successorReceiptState(handoff);
      if (
        receiptState.disposition === "rejected" ||
        (receiptState.disposition !== "accepted" &&
          current.initialTurnDisposition === "not-accepted")
      ) {
        return yield* directorError(
          "not-ready",
          "Handoff reconciliation cannot adopt a rejected successor with separate native activity.",
          `Keep handoff ${handoff.handoffId} held until a maintainer explicitly adopts or recovers the unverified execution.`,
        );
      }
    }
    const unclearedReassessments = yield* unclearedCapabilityReassessments(current);
    if (unclearedReassessments.length > 0) {
      return yield* directorError(
        "not-ready",
        "Clear every capability director reassessment before acknowledging predecessor evidence.",
        unclearedReassessments.map((entry) => entry.directorId).join(", "),
      );
    }
    const capability = yield* workflow.issueDetail({
      projectId: ProjectId.make(current.projectId),
      repository: current.repository as WorkflowIssueSummary["repository"],
      number: current.capabilityNumber,
    });
    if (capability.kind !== "capability" || capability.state !== "open") {
      return yield* directorError(
        "not-ready",
        "The current capability must remain open before predecessor evidence can be acknowledged.",
      );
    }
    const authorityTriggers = yield* reassessmentTriggers(current, {
      projectId: ProjectId.make(current.projectId),
      issue: capability,
    });
    if (authorityTriggers.length > 0) {
      return yield* directorError(
        "not-ready",
        "Current capability authority requires reassessment before handoff reconciliation.",
        authorityTriggers.map((trigger) => trigger.source).join(", "),
      );
    }
    const specification = yield* currentApproval(capability, "specification");
    const breakdown = yield* currentApproval(capability, "ticket-breakdown");
    if (
      !specification ||
      !breakdown ||
      workflowEvidenceBodyFingerprint(specification.approvedContent ?? "") !==
        current.specificationFingerprint ||
      workflowEvidenceBodyFingerprint(breakdown.approvedContent ?? "") !==
        current.breakdownFingerprint
    ) {
      return yield* directorError(
        "not-ready",
        "Current approved capability scope changed and requires reassessment before handoff reconciliation.",
      );
    }
    const snapshot = yield* settledHandoffSnapshot(source, handoff);
    const reconciliationId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const saveDecision = yield* persistence(
      sql.withTransaction(
        Effect.gen(function* () {
          if (expectedTargetObservation) {
            // Acquire SQLite's writer lock before the final evidence read. Native ingestion writes
            // through another service and does not share the director semaphore.
            yield* sql`UPDATE workflow_directors SET director_id = director_id
              WHERE director_id = ${current.directorId}`;
            const lockedHandoffRows = yield* sql<{
              readonly status: string;
              readonly latestReconciliationSequence: number | null;
            }>`SELECT h.status,
                (SELECT max(r.sequence) FROM workflow_director_handoff_reconciliations r
                  WHERE r.handoff_id = h.handoff_id) AS "latestReconciliationSequence"
              FROM workflow_director_handoffs h WHERE h.handoff_id = ${handoff.handoffId}`;
            const lockedEvidenceRows = yield* sql<Record<string, unknown>>`
              SELECT t.director_id AS "directorId", 'director' AS kind,
                NULL AS "providerThreadId", t.native_session_id AS "nativeSessionId",
                t.native_turn_id AS "nativeTurnId", t.status AS "nativeStatus",
                NULL AS "nativeLifecycle", 1 AS associated, t.updated_at AS "updatedAt"
              FROM workflow_director_native_turns t
              WHERE t.director_id = ${source.directorId}
              UNION ALL
              SELECT o.director_id AS "directorId", 'child' AS kind,
                o.provider_thread_id AS "providerThreadId", o.native_session_id AS "nativeSessionId",
                o.native_turn_id AS "nativeTurnId", o.native_turn_status AS "nativeStatus",
                o.native_lifecycle AS "nativeLifecycle",
                CASE WHEN EXISTS (
                  SELECT 1 FROM workflow_worker_dispatches d
                  WHERE d.director_id = o.director_id AND d.provider_thread_id = o.provider_thread_id
                ) OR EXISTS (
                  SELECT 1 FROM workflow_ticket_reviews r
                  WHERE r.director_id = o.director_id AND r.provider_thread_id = o.provider_thread_id
                ) OR EXISTS (
                  SELECT 1 FROM workflow_review_axes a JOIN workflow_ticket_reviews r ON r.review_id = a.review_id
                  WHERE r.director_id = o.director_id AND a.provider_thread_id = o.provider_thread_id
                ) THEN 1 ELSE 0 END AS associated,
                o.updated_at AS "updatedAt"
              FROM workflow_worker_observations o
              WHERE o.director_id = ${source.directorId}
              ORDER BY kind, "providerThreadId"`;
            const lockedEvidence = yield* Effect.forEach(lockedEvidenceRows, (row) =>
              decodeHandoffNativeEvidenceRow(row),
            );
            const lockedHandoff = lockedHandoffRows[0];
            if (
              !lockedHandoff ||
              handoffRecoveryTargetObservation({
                currentDirectorId: current.directorId,
                handoffId: handoff.handoffId,
                handoffStatus: lockedHandoff.status,
                latestReconciliationSequence: lockedHandoff.latestReconciliationSequence,
                evidence: lockedEvidence,
              }) !== expectedTargetObservation
            ) {
              return "target-changed" as const;
            }
            const lockedRoot = lockedEvidence.find((entry) => entry.kind === "director");
            const lockedChildren = lockedEvidence.filter((entry) => entry.kind === "child");
            if (
              !lockedRoot ||
              !["completed", "failed", "interrupted"].includes(lockedRoot.nativeStatus ?? "") ||
              lockedChildren.some(
                (child) =>
                  child.providerThreadId === null ||
                  child.associated !== 1 ||
                  (child.nativeLifecycle !== "closed" && child.nativeStatus !== "interrupted"),
              )
            ) {
              return "target-changed" as const;
            }
            const lockedSettlements = [
              {
                kind: "director" as const,
                providerThreadId: source.threadId,
                nativeSessionId: lockedRoot.nativeSessionId,
                nativeTurnId: lockedRoot.nativeTurnId,
                mode: lockedRoot.nativeStatus === "interrupted" ? "interrupted" : "closed",
                observedAt: lockedRoot.updatedAt,
              },
              ...lockedChildren.map((child) => ({
                kind: "child" as const,
                providerThreadId: child.providerThreadId!,
                nativeSessionId: child.nativeSessionId,
                nativeTurnId: child.nativeTurnId,
                mode:
                  child.nativeLifecycle === "closed"
                    ? ("closed" as const)
                    : ("interrupted" as const),
                observedAt: child.updatedAt,
              })),
            ].toSorted((left, right) =>
              `${left.kind}:${left.providerThreadId}`.localeCompare(
                `${right.kind}:${right.providerThreadId}`,
              ),
            );
            const snapshotSettlements = (yield* decodeHandoffSettlementsJson(
              snapshot.settlementsJson,
            )).toSorted((left, right) =>
              `${left.kind}:${left.providerThreadId}`.localeCompare(
                `${right.kind}:${right.providerThreadId}`,
              ),
            );
            if (
              encodeHandoffSettlementsJson(lockedSettlements) !==
              encodeHandoffSettlementsJson(snapshotSettlements)
            ) {
              return "target-changed" as const;
            }
          }
          yield* sql`
            INSERT INTO workflow_director_handoff_reconciliations (
              reconciliation_id, handoff_id, acknowledged_by_director_id, settlements_json,
              implementation_head, summary, created_at, actor_kind, actor_subject
            ) VALUES (
              ${reconciliationId}, ${handoff.handoffId}, ${current.directorId},
              ${snapshot.settlementsJson}, ${snapshot.implementationHead}, ${input.summary}, ${createdAt},
              ${actor.kind}, ${actor.subject}
            )
          `;
          if (handoff.successorDirectorId === current.directorId && handoff.status === "held") {
            yield* sql`UPDATE workflow_director_handoffs SET status = 'submitting', detail = NULL,
              updated_at = ${createdAt} WHERE handoff_id = ${handoff.handoffId}`;
            yield* sql`UPDATE workflow_directors SET status = 'submitting', detail = NULL,
              updated_at = ${createdAt} WHERE director_id = ${current.directorId} AND is_current = 1`;
          }
          return "saved" as const;
        }),
      ),
      "The handoff reconciliation acknowledgement could not be saved.",
    );
    if (saveDecision === "target-changed") {
      return yield* directorError(
        "not-ready",
        "The selected predecessor evidence changed before handoff acknowledgement.",
        "Refresh Workflow and review the current batch history before trying again.",
      );
    }
    return yield* handoffStatusFromRow(yield* loadHandoffById(handoff.handoffId));
  });

  const reconcileHandoffUnlocked = Effect.fn("WorkflowDirectorService.reconcileHandoff")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowDirectorHandoffReconcileInput,
  ) {
    const current = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    return yield* reconcileHandoffForCurrent(current, input, {
      kind: "director",
      subject: current.directorId,
    });
  });

  const reconcileHandoffAsOwnerUnlocked = Effect.fn(
    "WorkflowDirectorService.reconcileHandoffAsOwner",
  )(function* (input: WorkflowDirectorHandoffOwnerReconcileInput, actorSubject: string) {
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        directorError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const current = yield* loadDirectorByCapability(input, environmentId);
    if (
      !current ||
      current.projectId !== input.projectId ||
      current.directorId !== input.expectedDirectorId
    ) {
      return yield* directorError(
        "not-ready",
        "The selected capability director changed before handoff acknowledgement.",
      );
    }
    const currentStatus = yield* statusFromRow(current);
    if (currentStatus.observation !== input.expectedObservation) {
      return yield* directorError(
        "not-ready",
        "The capability director changed before handoff acknowledgement.",
        "Refresh Workflow and review the current director status before trying again.",
      );
    }
    const target = currentStatus.handoffRecoveryTargets?.find(
      (candidate) => candidate.handoffId === input.handoffId,
    );
    if (!target || target.targetObservation !== input.expectedTargetObservation) {
      return yield* directorError(
        "not-ready",
        "The selected predecessor evidence changed before handoff acknowledgement.",
        "Refresh Workflow and review the current batch history before trying again.",
      );
    }
    yield* reconcileHandoffForCurrent(
      current,
      { handoffId: input.handoffId, summary: input.summary },
      { kind: "owner-session", subject: actorSubject },
      input.expectedTargetObservation,
    );
    return yield* statusFromRow(yield* loadDirectorById(current.directorId));
  });

  const successorReceiptState = Effect.fn("WorkflowDirectorService.successorReceiptState")(
    function* (handoff: HandoffRow) {
      if (!handoff.successorCommandId) {
        return yield* directorError(
          "persistence-failed",
          "The successor submission command identity is incomplete.",
        );
      }
      const readReceipt = (commandId: CommandId) =>
        receipts
          .getByCommandId({ commandId })
          .pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "Successor command evidence could not be read.",
                String(error),
              ),
            ),
          );
      const turnReceipt = yield* readReceipt(CommandId.make(handoff.successorCommandId));
      const createReceipt = yield* readReceipt(
        workflowSuccessorCreateCommandId(CommandId.make(handoff.successorCommandId)),
      );
      if (Option.isSome(turnReceipt) && turnReceipt.value.status === "accepted") {
        return { disposition: "accepted" as const, sequence: turnReceipt.value.resultSequence };
      }
      if (
        (Option.isSome(turnReceipt) && turnReceipt.value.status === "rejected") ||
        (Option.isNone(turnReceipt) &&
          Option.isSome(createReceipt) &&
          createReceipt.value.status === "rejected")
      ) {
        return { disposition: "rejected" as const };
      }
      return { disposition: "unknown" as const };
    },
  );

  const finalizeAcceptedSuccessor = Effect.fn("WorkflowDirectorService.finalizeAcceptedSuccessor")(
    function* (current: DirectorRow, handoff: HandoffRow, sequence: number) {
      const completedAt = DateTime.formatIso(yield* DateTime.now);
      const reassessment = yield* activeReassessment(current.directorId);
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            if (reassessment) {
              yield* sql`UPDATE workflow_directors SET status = 'held', sequence = ${sequence},
                initial_turn_disposition = 'accepted', updated_at = ${completedAt}
                WHERE director_id = ${current.directorId} AND is_current = 1`;
            } else {
              yield* sql`UPDATE workflow_directors SET status = 'active', sequence = ${sequence},
                initial_turn_disposition = 'accepted', detail = NULL, updated_at = ${completedAt}
                WHERE director_id = ${current.directorId} AND is_current = 1`;
            }
            yield* sql`UPDATE workflow_director_handoffs SET status = 'submitted', detail = NULL,
            updated_at = ${completedAt} WHERE handoff_id = ${handoff.handoffId}`;
          }),
        ),
        "The accepted successor receipt could not be saved.",
      );
      return yield* statusFromRow(yield* loadDirectorById(current.directorId));
    },
  );

  const restoreRejectedSuccessor = Effect.fn("WorkflowDirectorService.restoreRejectedSuccessor")(
    function* (current: DirectorRow, handoff: HandoffRow) {
      const source = yield* loadDirectorHistoryById(handoff.sourceDirectorId);
      const restoredAt = DateTime.formatIso(yield* DateTime.now);
      const nativeActivity = yield* persistence(
        sql<{ readonly count: number }>`SELECT
        (SELECT count(*) FROM workflow_director_native_turns WHERE director_id = ${current.directorId}) +
        (SELECT count(*) FROM workflow_worker_observations WHERE director_id = ${current.directorId}) AS count`,
        "Rejected successor native evidence could not be read.",
      );
      if (nativeActivity[0]?.count !== 0) {
        const detail = `The saved successor command was rejected, but the successor has separate native activity. Keep handoff ${handoff.handoffId} held until a maintainer explicitly adopts or recovers that unverified execution.`;
        yield* persistence(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`UPDATE workflow_directors SET status = 'held',
              initial_turn_disposition = 'not-accepted', detail = ${detail},
              updated_at = ${restoredAt} WHERE director_id = ${current.directorId} AND is_current = 1`;
              yield* sql`UPDATE workflow_director_handoffs SET status = 'held', detail = ${detail},
              updated_at = ${restoredAt} WHERE handoff_id = ${handoff.handoffId}`;
            }),
          ),
          "The rejected successor with native activity could not be held.",
        );
        return yield* directorError(
          "not-ready",
          "The rejected successor cannot be abandoned.",
          detail,
        );
      }
      const detail =
        "The successor turn was definitively rejected. Call workflow_prepare_director_handoff from the restored source to retry with a fresh identity.";
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE workflow_directors SET is_current = 0, status = 'held',
            initial_turn_disposition = 'not-accepted', detail = ${detail}, updated_at = ${restoredAt}
            WHERE director_id = ${current.directorId} AND is_current = 1`;
            yield* sql`UPDATE workflow_directors SET is_current = 1, status = 'waiting',
            detail = ${detail}, updated_at = ${restoredAt}
            WHERE director_id = ${source.directorId} AND is_current = 0`;
            yield* sql`UPDATE workflow_director_handoffs SET status = 'held', detail = ${detail},
            updated_at = ${restoredAt} WHERE handoff_id = ${handoff.handoffId}`;
          }),
        ),
        "The definitively rejected successor could not restore its source director.",
      );
    },
  );

  const successorDispatchDecision = Effect.fn("WorkflowDirectorService.successorDispatchDecision")(
    function* (
      current: DirectorRow,
      sourceDirectorId: string,
      handoff: HandoffRow,
      attemptedAt: string,
      preflight: SuccessorDispatchPreflight,
    ) {
      return yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            // Native ingestion and projection writes do not share the director semaphore. Take the
            // SQLite writer lock before the last local evidence read and the persisted attempt.
            yield* sql`UPDATE workflow_directors SET director_id = director_id
            WHERE director_id = ${current.directorId}`;
            const receiptRows = yield* sql<{
              readonly commandId: string;
              readonly resultSequence: number;
              readonly status: string;
            }>`SELECT command_id AS "commandId", result_sequence AS "resultSequence", status
            FROM orchestration_command_receipts
            WHERE command_id IN (
              ${handoff.successorCommandId},
              ${workflowSuccessorCreateCommandId(CommandId.make(handoff.successorCommandId!))}
            )`;
            const turnReceipt = receiptRows.find(
              (receipt) => receipt.commandId === handoff.successorCommandId,
            );
            const createReceipt = receiptRows.find(
              (receipt) => receipt.commandId !== handoff.successorCommandId,
            );
            if (turnReceipt?.status === "accepted") {
              return {
                disposition: "accepted" as const,
                sequence: turnReceipt.resultSequence,
              };
            }
            if (
              turnReceipt?.status === "rejected" ||
              (!turnReceipt && createReceipt?.status === "rejected")
            ) {
              return { disposition: "rejected" as const };
            }
            if (preflight.disposition === "blocked" && preflight.hold) {
              yield* sql`UPDATE workflow_director_handoffs SET status = 'held',
                detail = ${preflight.hold.detail}, updated_at = ${attemptedAt}
                WHERE handoff_id = ${handoff.handoffId}`;
              if (preflight.hold.restoreSource) {
                yield* sql`UPDATE workflow_directors SET is_current = 0, status = 'held',
                  detail = ${preflight.hold.detail}, updated_at = ${attemptedAt}
                  WHERE director_id = ${current.directorId} AND is_current = 1`;
                yield* sql`UPDATE workflow_directors SET is_current = 1, status = 'waiting',
                  detail = 'Call workflow_prepare_director_handoff again after reviewing the changed source evidence.',
                  updated_at = ${attemptedAt}
                  WHERE director_id = ${sourceDirectorId} AND is_current = 0`;
              } else {
                yield* sql`UPDATE workflow_directors SET status = 'held',
                  detail = ${preflight.hold.detail}, updated_at = ${attemptedAt}
                  WHERE director_id = ${current.directorId} AND is_current = 1`;
              }
            }
            if (preflight.disposition === "blocked") {
              return { disposition: "blocked" as const, error: preflight.error };
            }
            const predecessorDecision = yield* ensurePredecessorExecutionSettled(
              current,
              sourceDirectorId,
            ).pipe(Effect.result);
            if (predecessorDecision._tag === "Failure") {
              return {
                disposition: "blocked" as const,
                error: predecessorDecision.failure,
              };
            }
            yield* sql`UPDATE workflow_directors SET initial_turn_disposition = 'unknown',
            updated_at = ${attemptedAt} WHERE director_id = ${current.directorId}
              AND initial_turn_disposition = 'not-attempted'`;
            return { disposition: "dispatch" as const };
          }),
        ),
        "The successor dispatch decision could not be persisted.",
      );
    },
  );

  const prepareRotationCapability = Effect.fn("WorkflowDirectorService.prepareRotationCapability")(
    function* (current: DirectorRow, requireActionableUnfinishedTicket: boolean) {
      const unclearedReassessments = yield* unclearedCapabilityReassessments(current);
      if (unclearedReassessments.length > 0) {
        return yield* directorError(
          "not-ready",
          "Clear every capability director reassessment before dispatching the saved successor turn.",
          unclearedReassessments.map((entry) => entry.directorId).join(", "),
        );
      }

      const capabilityAdmissionsRaw = yield* persistence(
        sql<Record<string, unknown>>`
        SELECT a.admission_id AS "admissionId", a.director_id AS "directorId",
          a.batch_id AS "batchId", a.repository, a.ticket_number AS "ticketNumber",
          a.slot_ticket_number AS "slotTicketNumber", a.purpose, a.ownership,
          a.claim_login AS "claimLogin", a.claim_status AS "claimStatus",
          a.scope_body AS "scopeBody", a.scope_fingerprint AS "scopeFingerprint",
          a.current_scope_body AS "currentScopeBody",
          a.current_scope_fingerprint AS "currentScopeFingerprint",
          a.created_at AS "createdAt", a.updated_at AS "updatedAt"
        FROM workflow_director_admissions a
        JOIN workflow_directors d ON d.director_id = a.director_id
        WHERE d.environment_id = ${current.environmentId}
          AND d.repository COLLATE NOCASE = ${current.repository}
          AND d.capability_number = ${current.capabilityNumber}
        ORDER BY a.created_at
      `,
        "Capability admission history could not be read before succession.",
      );
      const capabilityAdmissions = yield* Effect.forEach(capabilityAdmissionsRaw, (candidate) =>
        decodeAdmissionRow(candidate),
      ).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Capability admission history is invalid.",
            String(error),
          ),
        ),
      );
      const modelSelection: WorkflowDirectorStartInput["modelSelection"] = {
        instanceId: ProviderInstanceId.make(current.requestedInstanceId),
        model: DIRECTOR_MODEL,
        options: [{ id: "reasoningEffort", value: DIRECTOR_EFFORT }],
      };
      const prepared = yield* prepareCapability(
        {
          projectId: ProjectId.make(current.projectId),
          repository: current.repository as WorkflowDirectorStartInput["repository"],
          capabilityNumber: current.capabilityNumber,
          modelSelection,
        },
        current.worktreePath,
        {
          ownedClaims: ownedClaimLogins(capabilityAdmissions),
          admittedTicketNumbers: new Set(
            capabilityAdmissions.map((admission) => admission.ticketNumber),
          ),
          requireActionableUnfinishedTicket,
        },
      );
      return { modelSelection, prepared };
    },
  );
  type PreparedRotationCapability = Effect.Success<ReturnType<typeof prepareRotationCapability>>;

  const rotateReadyUnlocked = Effect.fn("WorkflowDirectorService.rotateReady")(function* (
    input: WorkflowDirectorStatusInput,
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
    const loadedCurrent = yield* loadDirectorByCapability(input, environmentId);
    if (!loadedCurrent) {
      return yield* directorError(
        "director-not-found",
        "No capability director is linked in this environment.",
      );
    }
    let current: DirectorRow = loadedCurrent;
    const outgoing = yield* outgoingHandoff(current.directorId);
    let selectedHandoff = outgoing;
    if (!selectedHandoff) {
      const incoming = yield* handoffForDirector(current.directorId);
      if (
        incoming?.successorDirectorId !== current.directorId ||
        incoming.status !== "submitting"
      ) {
        return yield* statusFromRow(current);
      }
      selectedHandoff = incoming;
    }
    let handoff: HandoffRow = selectedHandoff;

    if (
      handoff.successorDirectorId !== null &&
      handoff.successorDirectorId !== current.directorId
    ) {
      return yield* statusFromRow(current);
    }
    if (handoff.successorDirectorId === current.directorId) {
      if (
        !handoff.successorCommandId ||
        !handoff.successorMessageId ||
        !handoff.successorThreadId ||
        !handoff.successorPrompt
      ) {
        return yield* directorError(
          "persistence-failed",
          "The successor submission identity is incomplete.",
        );
      }
      const receiptState = yield* successorReceiptState(handoff);
      if (receiptState.disposition === "accepted") {
        return yield* finalizeAcceptedSuccessor(current, handoff, receiptState.sequence);
      }
      if (receiptState.disposition === "rejected") {
        yield* restoreRejectedSuccessor(current, handoff);
        return yield* directorError(
          "dispatch-failed",
          "The successor was definitively rejected. The source director was restored for explicit handoff retry.",
        );
      }
    }

    let rotationPrepared: PreparedRotationCapability | undefined;
    if (handoff.successorDirectorId === null) {
      yield* ensurePredecessorExecutionSettled(current);
      rotationPrepared = yield* prepareRotationCapability(current, true);
    }

    if (handoff.successorDirectorId === null) {
      const { prepared } = rotationPrepared!;
      const snapshot = yield* settledHandoffSnapshot(current, handoff);
      const sourceContextDetails = yield* sourceContext(
        {
          projectId: ProjectId.make(current.projectId),
          repository: current.repository as WorkflowDirectorStartInput["repository"],
        },
        prepared.capability,
      );
      const decodeLinks = (value: string) =>
        decodeStringArrayJson(value).pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The finalized handoff links are invalid.",
              String(error),
            ),
          ),
        );
      const lessons = yield* decodeLinks(handoff.lessonsJson);
      const unresolvedContext = yield* decodeLinks(handoff.unresolvedContextJson);
      const suggestedSkills = yield* decodeLinks(handoff.suggestedSkillsJson);
      const suggestedStaffing = yield* decodeLinks(handoff.suggestedStaffingJson);
      const specificationLinks = yield* decodeLinks(snapshot.specificationLinksJson);
      const issueLinks = yield* decodeLinks(snapshot.issueLinksJson);
      const reviewLinks = yield* decodeLinks(snapshot.reviewLinksJson);
      const commitLinks = yield* decodeLinks(snapshot.commitLinksJson);
      const successorInstructions = [
        workflowDirectorInstructions({
          capability: prepared.capability,
          tickets: prepared.tickets,
          approvalRecords: [prepared.specificationApproval, prepared.breakdownApproval],
          sourceContext: sourceContextDetails,
          implementSkillPath: prepared.skills[0]!.path,
          reviewSkillPath: prepared.skills[1]!.path,
        }),
        "",
        "## Durable predecessor handoff",
        `Source director ${current.directorId}, batch ${current.batchId}, thread ${current.threadId}.`,
        `The source settled at exact implementation HEAD ${snapshot.implementationHead} in ${current.worktreePath}.`,
        `Admitted work: ${snapshot.admissionsJson}.`,
        `Specification sources: ${specificationLinks.join(", ") || "none"}.`,
        `Issue sources: ${issueLinks.join(", ") || "none"}.`,
        `Review sources: ${reviewLinks.join(", ") || "none"}.`,
        `Commit sources: ${commitLinks.join(", ") || "none"}.`,
        `Suggested skills: ${suggestedSkills.join(", ")}.`,
        `Suggested staffing: ${suggestedStaffing.join(", ")}.`,
        `Lessons: ${lessons.join(" | ") || "none recorded"}.`,
        `Unresolved context: ${unresolvedContext.join(" | ") || "none recorded"}.`,
        "This handoff was persisted before this successor identity. Reread current GitHub tracker authority before every admission; the snapshot is context, not present authority.",
      ].join("\n");
      const successorDirectorId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const successorBatchId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const successorThreadId = ThreadId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
      const successorCommandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
      const successorMessageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      const sourceCurrent = current;
      const sourceHandoff = handoff;
      yield* persistence(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE workflow_directors SET is_current = 0, status = 'waiting',
              detail = 'A durable successor owns the current capability batch.', updated_at = ${createdAt}
              WHERE director_id = ${sourceCurrent.directorId} AND is_current = 1`;
            yield* sql`
              INSERT INTO workflow_directors (
                director_id, batch_id, environment_id, project_id, repository, root_number,
                capability_number, thread_id, command_id, message_id, worktree_path, worktree_branch,
                status, requested_model, requested_instance_id, requested_effort, observed_match,
                initial_turn_disposition, specification_fingerprint, breakdown_fingerprint,
                created_at, updated_at
              ) VALUES (
                ${successorDirectorId}, ${successorBatchId}, ${sourceCurrent.environmentId}, ${sourceCurrent.projectId},
                ${sourceCurrent.repository}, ${sourceCurrent.rootNumber}, ${sourceCurrent.capabilityNumber}, ${successorThreadId},
                ${successorCommandId}, ${successorMessageId}, ${sourceCurrent.worktreePath}, ${sourceCurrent.worktreeBranch},
                'submitting', ${DIRECTOR_MODEL}, ${sourceCurrent.requestedInstanceId}, ${DIRECTOR_EFFORT},
                'unknown', 'not-attempted', ${workflowEvidenceBodyFingerprint(prepared.specificationApproval.approvedContent ?? "")},
                ${workflowEvidenceBodyFingerprint(prepared.breakdownApproval.approvedContent ?? "")},
                ${createdAt}, ${createdAt}
              )
            `;
            yield* sql`UPDATE workflow_director_handoffs SET
              successor_director_id = ${successorDirectorId}, successor_thread_id = ${successorThreadId},
              successor_command_id = ${successorCommandId}, successor_message_id = ${successorMessageId},
              successor_prompt = ${successorInstructions}, admissions_json = ${snapshot.admissionsJson},
              settlements_json = ${snapshot.settlementsJson}, implementation_head = ${snapshot.implementationHead},
              specification_links_json = ${snapshot.specificationLinksJson},
              issue_links_json = ${snapshot.issueLinksJson}, review_links_json = ${snapshot.reviewLinksJson},
              commit_links_json = ${snapshot.commitLinksJson}, status = 'submitting', detail = NULL,
              updated_at = ${createdAt} WHERE handoff_id = ${sourceHandoff.handoffId}
                AND successor_director_id IS NULL`;
          }),
        ),
        "The durable handoff could not transfer current director ownership.",
      );
      current = yield* loadDirectorById(successorDirectorId);
      handoff = (yield* handoffForDirector(successorDirectorId))!;
    }

    if (
      !handoff.successorCommandId ||
      !handoff.successorMessageId ||
      !handoff.successorThreadId ||
      !handoff.successorPrompt
    ) {
      return yield* directorError(
        "persistence-failed",
        "The successor submission identity is incomplete.",
      );
    }
    const externalPreflight = yield* Effect.gen(function* () {
      const preparedRotation =
        rotationPrepared ?? (yield* prepareRotationCapability(current, false));
      const source = yield* loadDirectorHistoryById(handoff.sourceDirectorId);
      yield* ensurePredecessorExecutionSettled(source);
      const expectedSnapshot = yield* effectiveHandoffSettlement(handoff);
      const currentSnapshot = yield* settledHandoffSnapshot(source, handoff).pipe(Effect.result);
      const snapshotChanged =
        currentSnapshot._tag === "Failure" ||
        currentSnapshot.success.admissionsJson !== handoff.admissionsJson ||
        currentSnapshot.success.settlementsJson !== expectedSnapshot.settlementsJson ||
        currentSnapshot.success.implementationHead !== expectedSnapshot.implementationHead ||
        currentSnapshot.success.specificationLinksJson !== handoff.specificationLinksJson ||
        currentSnapshot.success.issueLinksJson !== handoff.issueLinksJson ||
        currentSnapshot.success.reviewLinksJson !== handoff.reviewLinksJson ||
        currentSnapshot.success.commitLinksJson !== handoff.commitLinksJson;
      if (!snapshotChanged) {
        return {
          preparedRotation,
          dispatchPreflight: { disposition: "ready" } as SuccessorDispatchPreflight,
        };
      }
      const detail =
        currentSnapshot._tag === "Failure"
          ? (currentSnapshot.failure.detail ?? currentSnapshot.failure.message)
          : "The source handoff changed after successor identity creation. Review the source batch and explicitly retry succession.";
      return {
        preparedRotation,
        dispatchPreflight: {
          disposition: "blocked",
          error: directorError(
            "not-ready",
            "The saved handoff needs explicit recovery before succession.",
            detail,
          ),
          hold: {
            detail,
            restoreSource: ["not-attempted", "not-accepted"].includes(
              current.initialTurnDisposition,
            ),
          },
        } as SuccessorDispatchPreflight,
      };
    }).pipe(Effect.result);
    const dispatchPreflight: SuccessorDispatchPreflight =
      externalPreflight._tag === "Failure"
        ? { disposition: "blocked", error: externalPreflight.failure }
        : externalPreflight.success.dispatchPreflight;
    if (externalPreflight._tag === "Success") {
      rotationPrepared = externalPreflight.success.preparedRotation;
    }
    const submittedAt = DateTime.formatIso(yield* DateTime.now);
    const dispatchDecision = yield* successorDispatchDecision(
      current,
      handoff.sourceDirectorId,
      handoff,
      submittedAt,
      dispatchPreflight,
    );
    if (dispatchDecision.disposition === "accepted") {
      return yield* finalizeAcceptedSuccessor(current, handoff, dispatchDecision.sequence);
    }
    if (dispatchDecision.disposition === "rejected") {
      yield* restoreRejectedSuccessor(current, handoff);
      return yield* directorError(
        "dispatch-failed",
        "The successor was definitively rejected. The source director was restored for explicit handoff retry.",
      );
    }
    if (dispatchDecision.disposition === "blocked") {
      return yield* dispatchDecision.error;
    }
    current = { ...current, initialTurnDisposition: "unknown", updatedAt: submittedAt };
    const { modelSelection, prepared } = rotationPrepared!;
    const dispatched = yield* dispatch({
      type: "thread.turn.start",
      commandId: CommandId.make(handoff.successorCommandId),
      threadId: ThreadId.make(handoff.successorThreadId),
      message: {
        messageId: MessageId.make(handoff.successorMessageId),
        role: "user",
        text: handoff.successorPrompt,
        attachments: [],
      },
      modelSelection,
      runtimeMode: "approval-required",
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      bootstrap: {
        createThread: {
          projectId: ProjectId.make(current.projectId),
          title: `Director: ${prepared.capability.title}`,
          modelSelection,
          runtimeMode: "approval-required",
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          branch: current.worktreeBranch,
          worktreePath: current.worktreePath,
          createdAt: submittedAt,
        },
        runSetupScript: false,
      },
      createdAt: submittedAt,
    }).pipe(Effect.result);
    if (dispatched._tag === "Failure") {
      const afterFailure = yield* successorReceiptState(handoff).pipe(Effect.result);
      if (afterFailure._tag === "Success" && afterFailure.success.disposition === "accepted") {
        yield* finalizeAcceptedSuccessor(current, handoff, afterFailure.success.sequence);
        return yield* directorError(
          "dispatch-failed",
          "The successor turn was accepted and reconciled after the local dispatch failure.",
          String(dispatched.failure),
        );
      }
      if (
        (afterFailure._tag === "Success" && afterFailure.success.disposition === "rejected") ||
        dispatched.failure.bootstrapTurnDisposition === "not-accepted"
      ) {
        yield* restoreRejectedSuccessor(current, handoff);
        return yield* directorError(
          "dispatch-failed",
          "The successor was definitively rejected. The source director was restored for explicit handoff retry.",
          String(dispatched.failure),
        );
      }
      return yield* directorError(
        "dispatch-failed",
        "The saved successor first turn remains pending receipt reconciliation.",
        afterFailure._tag === "Failure"
          ? `${String(dispatched.failure)} Receipt read failed: ${String(afterFailure.failure)}`
          : String(dispatched.failure),
      );
    }
    return yield* finalizeAcceptedSuccessor(current, handoff, dispatched.success.sequence);
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

  const capabilityCompletionGate = Effect.fn("WorkflowDirectorService.capabilityCompletionGate")(
    function* (row: DirectorRow, resultingHead: string, allowClosed: boolean) {
      if (!/^[0-9a-f]{40}$/iu.test(resultingHead)) {
        return yield* directorError(
          "completion-pending",
          "Capability completion requires a full resulting commit hash.",
        );
      }
      const capability = yield* workflow.issueDetail({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowIssueSummary["repository"],
        number: row.capabilityNumber,
      });
      if (
        capability.kind !== "capability" ||
        (capability.state !== "open" && (!allowClosed || capability.stateReason !== "completed"))
      ) {
        return yield* directorError(
          "completion-pending",
          "The capability must be open, or be the completed capability currently being reconciled.",
        );
      }
      const specification = yield* currentApproval(capability, "specification");
      const breakdown = yield* currentApproval(capability, "ticket-breakdown");
      if (!specification || !breakdown) {
        return yield* directorError(
          "approval-unavailable",
          "Current verified specification and ticket-breakdown approvals are required for completion.",
        );
      }
      const specificationFingerprint = workflowEvidenceBodyFingerprint(
        specification.approvedContent ?? "",
      );
      const breakdownFingerprint = workflowEvidenceBodyFingerprint(breakdown.approvedContent ?? "");
      if (
        specificationFingerprint !== row.specificationFingerprint ||
        breakdownFingerprint !== row.breakdownFingerprint
      ) {
        return yield* directorError(
          "completion-pending",
          "Approved capability scope changed and requires reassessment before completion.",
        );
      }
      const capabilityDirectorRows = yield* persistence(
        sql<{
          readonly directorId: string;
          readonly commandId: string;
          readonly initialTurnDisposition: string;
        }>`
          SELECT director_id AS "directorId", command_id AS "commandId",
            initial_turn_disposition AS "initialTurnDisposition" FROM workflow_directors
          WHERE environment_id = ${row.environmentId}
            AND repository COLLATE NOCASE = ${row.repository}
            AND capability_number = ${row.capabilityNumber}
        `,
        "Capability director history could not be read before completion.",
      );
      const capabilityDirectorIds = capabilityDirectorRows.map((candidate) => candidate.directorId);
      for (const historical of capabilityDirectorRows) {
        if (historical.directorId === row.directorId) continue;
        const handoff = yield* outgoingHandoff(historical.directorId);
        if (handoff?.status === "submitted") continue;
        const receipt = yield* receipts
          .getByCommandId({ commandId: CommandId.make(historical.commandId) })
          .pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "Historical director receipt evidence could not be read.",
                String(error),
              ),
            ),
          );
        const activity = yield* persistence(
          sql<{ readonly count: number }>`SELECT
            (SELECT count(*) FROM workflow_director_native_turns WHERE director_id = ${historical.directorId}) +
            (SELECT count(*) FROM workflow_worker_observations WHERE director_id = ${historical.directorId}) AS count`,
          "Historical director native evidence could not be read.",
        );
        if (
          !["not-attempted", "not-accepted"].includes(historical.initialTurnDisposition) ||
          (Option.isSome(receipt) && receipt.value.status !== "rejected") ||
          activity[0]?.count !== 0
        ) {
          return yield* directorError(
            "completion-pending",
            "Every predecessor director needs an accepted durable handoff or definitive abandonment evidence.",
            historical.directorId,
          );
        }
      }
      const activeReassessments = yield* persistence(
        sql<{ readonly directorId: string }>`
          SELECT director_id AS "directorId" FROM workflow_reassessments
          WHERE ${sql.in("director_id", capabilityDirectorIds)} AND status != 'cleared'
        `,
        "Capability reassessment history could not be read before completion.",
      );
      if (activeReassessments.length > 0) {
        return yield* directorError(
          "completion-pending",
          "Capability completion is held until the active reassessment is cleared.",
        );
      }
      const hierarchy = yield* approvedDeliveryHierarchy({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository,
        capabilityNumber: row.capabilityNumber,
        breakdownApproval: breakdown,
      });
      const requiredEvidence = hierarchy.requiredIssues.map((issue) => ({
        issue,
        state: requiredIssueCompletionEvidence(issue),
      }));
      const invalidated = requiredEvidence.filter((entry) => entry.state === "changed");
      if (invalidated.length > 0) {
        return yield* directorError(
          "completion-pending",
          "Every approved delivery ticket and nested task needs current resolution evidence.",
          invalidated.map(({ issue }) => `#${issue.number}: ${readinessDetail(issue)}`).join(" "),
        );
      }
      if (hierarchy.unknownPublishedTickets.length > 0) {
        return yield* directorError(
          "completion-unavailable",
          "Published delivery scope cannot be verified from the available approval evidence.",
          `Unavailable or unknown scope: ${hierarchy.unknownPublishedTickets.map((ticket) => `#${ticket.number}`).join(", ")}.`,
        );
      }
      const uncertain = requiredEvidence.filter((entry) => entry.state === "unknown");
      if (uncertain.length > 0) {
        return yield* directorError(
          "completion-unavailable",
          "Required delivery resolution evidence is unavailable or has unknown scope.",
          uncertain.map(({ issue }) => `#${issue.number}: ${readinessDetail(issue)}`).join(" "),
        );
      }
      const requiredNumbers = hierarchy.requiredIssues.map((issue) => issue.number);
      const pendingResolutions =
        requiredNumbers.length === 0
          ? []
          : yield* persistence(
              sql<{ readonly ticketNumber: number }>`
              SELECT ticket_number AS "ticketNumber"
              FROM workflow_ticket_resolution_intents
              WHERE ${sql.in("director_id", capabilityDirectorIds)}
                AND ${sql.in("ticket_number", requiredNumbers)}
                AND status <> 'resolved'
            `,
              "Pending ticket writes could not be checked before capability completion.",
            );
      if (pendingResolutions.length > 0) {
        return yield* directorError(
          "completion-pending",
          "A required ticket still has a pending or uncertain tracker write.",
          pendingResolutions.map((entry) => `#${entry.ticketNumber}`).join(", "),
        );
      }
      const unconfirmedDispatches = yield* persistence(
        sql<{ readonly ticketNumber: number }>`
        SELECT ticket_number AS "ticketNumber" FROM workflow_worker_dispatches
        WHERE ${sql.in("director_id", capabilityDirectorIds)} AND provider_thread_id IS NULL
      `,
        "Worker association evidence could not be checked before capability completion.",
      );
      if (unconfirmedDispatches.length > 0) {
        return yield* directorError(
          "completion-pending",
          "A prepared worker has no confirmed native child identity.",
        );
      }
      const childRows = yield* persistence(
        sql<{
          readonly providerThreadId: string;
          readonly directorId: string;
          readonly nativeLifecycle: string | null;
          readonly nativeTurnStatus: string | null;
          readonly nativeSessionId: string | null;
          readonly nativeTurnId: string | null;
          readonly updatedAt: string;
          readonly associated: number;
        }>`
        SELECT o.provider_thread_id AS "providerThreadId", o.director_id AS "directorId",
          o.native_lifecycle AS "nativeLifecycle", o.native_turn_status AS "nativeTurnStatus",
          o.native_session_id AS "nativeSessionId", o.native_turn_id AS "nativeTurnId",
          o.updated_at AS "updatedAt",
          CASE WHEN d.dispatch_id IS NOT NULL OR r.review_id IS NOT NULL OR ar.review_id IS NOT NULL
            THEN 1 ELSE 0 END AS associated
        FROM workflow_worker_observations o
        LEFT JOIN workflow_worker_dispatches d
          ON d.director_id = o.director_id AND d.provider_thread_id = o.provider_thread_id
        LEFT JOIN workflow_ticket_reviews r
          ON r.director_id = o.director_id AND r.provider_thread_id = o.provider_thread_id
        LEFT JOIN workflow_review_axes a ON a.provider_thread_id = o.provider_thread_id
        LEFT JOIN workflow_ticket_reviews ar
          ON ar.review_id = a.review_id AND ar.director_id = o.director_id
        WHERE ${sql.in("o.director_id", capabilityDirectorIds)}
      `,
        "Native child settlement evidence could not be checked before capability completion.",
      );
      const predecessorSettlements = new Map<string, Map<string, typeof HandoffSettlement.Type>>();
      for (const directorId of capabilityDirectorIds) {
        if (directorId === row.directorId) continue;
        const handoff = yield* outgoingHandoff(directorId);
        if (!handoff || handoff.status !== "submitted") continue;
        const effective = yield* effectiveHandoffSettlement(handoff);
        const settlements = yield* decodeHandoffSettlementsJson(effective.settlementsJson).pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Historical handoff settlement evidence is invalid.",
              String(error),
            ),
          ),
        );
        predecessorSettlements.set(
          directorId,
          new Map(
            settlements
              .filter((settlement) => settlement.kind === "child")
              .map((settlement) => [settlement.providerThreadId, settlement]),
          ),
        );
      }
      const historicalDirectorIds = capabilityDirectorIds.filter((id) => id !== row.directorId);
      const historicalRoots =
        historicalDirectorIds.length === 0
          ? []
          : yield* persistence(
              sql<{
                readonly directorId: string;
                readonly nativeSessionId: string;
                readonly nativeTurnId: string;
                readonly status: string;
                readonly updatedAt: string;
              }>`
          SELECT director_id AS "directorId", native_session_id AS "nativeSessionId",
            native_turn_id AS "nativeTurnId", status, updated_at AS "updatedAt"
          FROM workflow_director_native_turns
          WHERE ${sql.in("director_id", historicalDirectorIds)}
        `,
              "Historical director settlement evidence could not be read before completion.",
            );
      const unsettledRoots = [] as string[];
      for (const root of historicalRoots) {
        const handoff = yield* outgoingHandoff(root.directorId);
        if (!handoff || handoff.status !== "submitted") {
          unsettledRoots.push(root.directorId);
          continue;
        }
        const effective = yield* effectiveHandoffSettlement(handoff);
        const settlements = yield* decodeHandoffSettlementsJson(effective.settlementsJson).pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Historical root settlement evidence is invalid.",
              String(error),
            ),
          ),
        );
        const saved = settlements.find((settlement) => settlement.kind === "director");
        if (
          !saved ||
          saved.nativeSessionId !== root.nativeSessionId ||
          saved.nativeTurnId !== root.nativeTurnId ||
          saved.observedAt !== root.updatedAt ||
          (saved.mode === "interrupted"
            ? root.status !== "interrupted"
            : !["completed", "failed"].includes(root.status))
        ) {
          unsettledRoots.push(root.directorId);
        }
      }
      const unsettledChildren = childRows.filter((child) => {
        if (child.associated !== 1) return true;
        if (child.directorId === row.directorId) return child.nativeLifecycle !== "closed";
        const settlement = predecessorSettlements
          .get(child.directorId)
          ?.get(child.providerThreadId);
        if (
          !settlement ||
          settlement.nativeSessionId !== child.nativeSessionId ||
          settlement.nativeTurnId !== child.nativeTurnId ||
          settlement.observedAt !== child.updatedAt
        )
          return true;
        return settlement.mode === "closed"
          ? child.nativeLifecycle !== "closed"
          : child.nativeTurnStatus !== "interrupted";
      });
      if (unsettledChildren.length > 0 || unsettledRoots.length > 0) {
        return yield* directorError(
          "completion-pending",
          "Every observed native child must be associated and exactly closed before completion.",
          [...unsettledRoots, ...unsettledChildren.map((child) => child.providerThreadId)].join(
            ", ",
          ),
        );
      }
      const observed = yield* gitObservation(row.worktreePath);
      if (observed.head !== resultingHead || !observed.clean) {
        return yield* directorError(
          "completion-pending",
          "Combined acceptance requires the clean worktree at the exact registered result head.",
          `Expected ${resultingHead}; observed ${observed.head}${observed.clean ? "" : " with uncommitted changes"}.`,
        );
      }
      return {
        capability,
        requiredIssues: hierarchy.requiredIssues,
        specificationFingerprint,
        breakdownFingerprint,
      };
    },
  );

  const ensurePredecessorExecutionSettled = Effect.fn(
    "WorkflowDirectorService.ensurePredecessorExecutionSettled",
  )(function* (row: DirectorRow, requiredProjectedSourceDirectorId?: string) {
    const predecessors = yield* persistence(
      sql<{
        readonly directorId: string;
        readonly commandId: string;
        readonly initialTurnDisposition: string;
      }>`
        SELECT director_id AS "directorId", command_id AS "commandId",
          initial_turn_disposition AS "initialTurnDisposition" FROM workflow_directors
        WHERE environment_id = ${row.environmentId}
          AND repository COLLATE NOCASE = ${row.repository}
          AND capability_number = ${row.capabilityNumber}
          AND is_current = 0
          AND director_id != ${row.directorId}
      `,
      "Predecessor execution history could not be read.",
    );
    for (const predecessor of predecessors) {
      const handoff = yield* outgoingHandoff(predecessor.directorId);
      const isPendingImmediateSource =
        predecessor.directorId === requiredProjectedSourceDirectorId &&
        handoff?.successorDirectorId === row.directorId &&
        handoff.status === "submitting";
      if (!handoff || (handoff.status !== "submitted" && !isPendingImmediateSource)) {
        const receipt = yield* persistence(
          sql<{ readonly status: string }>`SELECT status FROM orchestration_command_receipts
            WHERE command_id = ${predecessor.commandId} LIMIT 1`,
          "Abandoned predecessor receipt evidence could not be read.",
        );
        const nativeActivity = yield* persistence(
          sql<{ readonly count: number }>`SELECT
            (SELECT count(*) FROM workflow_director_native_turns WHERE director_id = ${predecessor.directorId}) +
            (SELECT count(*) FROM workflow_worker_observations WHERE director_id = ${predecessor.directorId}) AS count`,
          "Abandoned predecessor native evidence could not be read.",
        );
        const safelyAbandoned =
          ["not-attempted", "not-accepted"].includes(predecessor.initialTurnDisposition) &&
          (!receipt[0] || receipt[0].status === "rejected") &&
          nativeActivity[0]?.count === 0;
        if (safelyAbandoned) continue;
        return yield* directorError(
          "not-ready",
          "A predecessor batch has no accepted durable settlement handoff.",
          predecessor.directorId,
        );
      }
      const projectedRoots = yield* persistence(
        sql<{
          readonly latestTurnState: string | null;
          readonly sessionStatus: string | null;
          readonly activeTurnId: string | null;
          readonly unsettledTurnCount: number;
        }>`
          SELECT turn.state AS "latestTurnState", session.status AS "sessionStatus",
            session.active_turn_id AS "activeTurnId",
            (SELECT count(*) FROM projection_turns unsettled
              WHERE unsettled.thread_id = thread.thread_id
                AND (unsettled.turn_id IS NULL OR unsettled.state = 'running'))
              AS "unsettledTurnCount"
          FROM projection_threads thread
          LEFT JOIN projection_turns turn
            ON turn.thread_id = thread.thread_id AND turn.turn_id = thread.latest_turn_id
          LEFT JOIN projection_thread_sessions session ON session.thread_id = thread.thread_id
          WHERE thread.thread_id = (
            SELECT thread_id FROM workflow_directors WHERE director_id = ${predecessor.directorId}
          ) AND thread.deleted_at IS NULL AND thread.archived_at IS NULL
          LIMIT 1
        `,
        "Predecessor projected director state could not be read.",
      );
      const projectedRoot = projectedRoots[0];
      if (
        (!projectedRoot && predecessor.directorId === requiredProjectedSourceDirectorId) ||
        (projectedRoot &&
          (projectedRoot.latestTurnState === null ||
            projectedRoot.latestTurnState === "running" ||
            projectedRoot.unsettledTurnCount > 0 ||
            projectedRoot.activeTurnId !== null ||
            projectedRoot.sessionStatus === "running" ||
            projectedRoot.sessionStatus === "starting"))
      ) {
        return yield* directorError(
          "not-ready",
          "New predecessor director activity requires explicit handoff reconciliation.",
          predecessor.directorId,
        );
      }
      const effective = yield* effectiveHandoffSettlement(handoff);
      const settlements = yield* decodeHandoffSettlementsJson(effective.settlementsJson).pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Predecessor settlement history is invalid.",
            String(error),
          ),
        ),
      );
      const savedRoot = settlements.find((settlement) => settlement.kind === "director");
      const roots = yield* persistence(
        sql<{
          readonly nativeSessionId: string;
          readonly nativeTurnId: string;
          readonly status: string;
          readonly updatedAt: string;
        }>`
          SELECT native_session_id AS "nativeSessionId", native_turn_id AS "nativeTurnId",
            status, updated_at AS "updatedAt" FROM workflow_director_native_turns
          WHERE director_id = ${predecessor.directorId} LIMIT 1
        `,
        "Predecessor native director evidence could not be read.",
      );
      const root = roots[0];
      if (
        !root ||
        !savedRoot ||
        savedRoot.nativeSessionId !== root.nativeSessionId ||
        savedRoot.nativeTurnId !== root.nativeTurnId ||
        savedRoot.observedAt !== root.updatedAt ||
        (savedRoot.mode === "interrupted"
          ? root.status !== "interrupted"
          : !["completed", "failed"].includes(root.status))
      ) {
        return yield* directorError(
          "not-ready",
          "New predecessor director activity requires explicit handoff reconciliation.",
          `Call workflow_reconcile_director_handoff with handoffId ${handoff.handoffId}.`,
        );
      }
      const children = yield* persistence(
        sql<{
          readonly providerThreadId: string;
          readonly nativeLifecycle: string | null;
          readonly nativeTurnStatus: string | null;
          readonly nativeSessionId: string | null;
          readonly nativeTurnId: string | null;
          readonly updatedAt: string;
          readonly associated: number;
        }>`
          SELECT o.provider_thread_id AS "providerThreadId",
            o.native_lifecycle AS "nativeLifecycle", o.native_turn_status AS "nativeTurnStatus",
            o.native_session_id AS "nativeSessionId", o.native_turn_id AS "nativeTurnId",
            o.updated_at AS "updatedAt",
            CASE WHEN EXISTS (
              SELECT 1 FROM workflow_worker_dispatches d
              WHERE d.director_id = o.director_id AND d.provider_thread_id = o.provider_thread_id
            ) OR EXISTS (
              SELECT 1 FROM workflow_ticket_reviews r
              WHERE r.director_id = o.director_id AND r.provider_thread_id = o.provider_thread_id
            ) OR EXISTS (
              SELECT 1 FROM workflow_review_axes a JOIN workflow_ticket_reviews r ON r.review_id = a.review_id
              WHERE r.director_id = o.director_id AND a.provider_thread_id = o.provider_thread_id
            ) THEN 1 ELSE 0 END AS associated
          FROM workflow_worker_observations o WHERE o.director_id = ${predecessor.directorId}
        `,
        "Predecessor native child evidence could not be read.",
      );
      const savedChildren = new Map(
        settlements
          .filter((settlement) => settlement.kind === "child")
          .map((settlement) => [settlement.providerThreadId, settlement]),
      );
      const invalid = children.find((child) => {
        const saved = savedChildren.get(child.providerThreadId);
        return (
          child.associated !== 1 ||
          !saved ||
          saved.observedAt !== child.updatedAt ||
          saved.nativeSessionId !== child.nativeSessionId ||
          saved.nativeTurnId !== child.nativeTurnId ||
          (saved.mode === "closed"
            ? child.nativeLifecycle !== "closed"
            : child.nativeTurnStatus !== "interrupted")
        );
      });
      if (invalid || savedChildren.size !== children.length) {
        return yield* directorError(
          "not-ready",
          "New, unknown, or unsettled predecessor child activity blocks delegation.",
          `Handoff ${handoff.handoffId}; call workflow_reconcile_director_handoff after ${invalid?.providerThreadId ?? predecessor.directorId} settles.`,
        );
      }
    }
  });

  const prepareWorkerUnlocked = Effect.fn("WorkflowDirectorService.prepareWorker")(function* (
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowWorkerPrepareInput,
  ) {
    const row = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
    yield* ensurePredecessorExecutionSettled(row);
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
      yield* ensurePredecessorExecutionSettled(row);
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

  const updateCompletion = Effect.fn("WorkflowDirectorService.updateCompletion")(function* (
    row: CompletionRow,
    update: {
      readonly status: CompletionRow["status"];
      readonly requiredAction: string;
      readonly lastError?: string | null;
      readonly commentUrl?: string | null;
      readonly closeConfirmed?: boolean;
      readonly closeOwned?: boolean;
    },
  ) {
    const updatedAt = DateTime.formatIso(yield* DateTime.now);
    yield* persistence(
      sql`UPDATE workflow_capability_completions SET status = ${update.status},
        required_action = ${update.requiredAction},
        last_error = ${update.lastError ?? null},
        comment_url = ${update.commentUrl === undefined ? row.commentUrl : update.commentUrl},
        close_confirmed = ${update.closeConfirmed === undefined ? row.closeConfirmed : update.closeConfirmed ? 1 : 0},
        close_owned = ${update.closeOwned === undefined ? row.closeOwned : update.closeOwned ? 1 : 0},
        updated_at = ${updatedAt}
        WHERE completion_id = ${row.completionId}`,
      "Capability completion state could not be saved.",
    );
    return {
      ...row,
      status: update.status,
      requiredAction: update.requiredAction,
      lastError: update.lastError ?? null,
      commentUrl: update.commentUrl === undefined ? row.commentUrl : update.commentUrl,
      closeConfirmed:
        update.closeConfirmed === undefined ? row.closeConfirmed : update.closeConfirmed ? 1 : 0,
      closeOwned: update.closeOwned === undefined ? row.closeOwned : update.closeOwned ? 1 : 0,
      updatedAt,
    };
  });

  const matchingCapabilityCompletion = (detail: WorkflowIssueDetail, row: CompletionRow) =>
    detail.evidence?.records.find(
      (record) =>
        record.kind === "resolution" &&
        record.state === "current" &&
        record.scope === "current" &&
        record.sourceAccess !== "unavailable" &&
        record.outcome === "resolved" &&
        record.bodyFingerprint === workflowEvidenceBodyFingerprint(row.commentBody) &&
        record.evidence?.includes(`workflow-capability-completion:${row.completionId}`),
    );

  const compensateCapabilityClose = Effect.fn("WorkflowDirectorService.compensateCapabilityClose")(
    function* (director: DirectorRow, completion: CompletionRow, reason: string) {
      if (completion.closeOwned !== 1) {
        const requiredAction =
          "This attempt has no attributable director close. Reconcile the tracker state manually before retrying combined acceptance.";
        return yield* updateCompletion(completion, {
          status: "invalidated",
          requiredAction,
          lastError: reason,
        });
      }
      const current = yield* workflow.issueDetail({
        projectId: ProjectId.make(director.projectId),
        repository: director.repository as WorkflowIssueSummary["repository"],
        number: director.capabilityNumber,
      });
      if (current.state === "open") {
        return yield* updateCompletion(completion, {
          status: "invalidated",
          requiredAction: reason,
          lastError: reason,
          closeConfirmed: false,
        });
      }
      let row = yield* updateCompletion(completion, {
        status: "reopen-uncertain",
        requiredAction:
          "Reconcile the compensating capability reopen before retrying combined acceptance.",
        lastError: reason,
      });
      yield* executeGitHub(director.worktreePath, [
        "issue",
        "reopen",
        String(director.capabilityNumber),
        "--repo",
        director.repository,
      ]).pipe(Effect.result);
      const refreshed = yield* workflow.issueDetail({
        projectId: ProjectId.make(director.projectId),
        repository: director.repository as WorkflowIssueSummary["repository"],
        number: director.capabilityNumber,
      });
      if (refreshed.state === "open") {
        row = yield* updateCompletion(row, {
          status: "invalidated",
          requiredAction: reason,
          lastError: reason,
          closeConfirmed: false,
        });
      }
      return row;
    },
  );

  const bindCapabilityCheckReceipts = Effect.fn(
    "WorkflowDirectorService.bindCapabilityCheckReceipts",
  )(function* (
    director: DirectorRow,
    completion: CompletionRow,
    providerInstanceId: ProviderInstanceId,
    input: WorkflowCapabilityCompleteInput,
  ) {
    const labels = input.receipts.map((receipt) => receipt.label);
    const toolCallIds = input.receipts.map((receipt) => receipt.toolCallId);
    if (
      new Set(labels).size !== labels.length ||
      new Set(toolCallIds).size !== toolCallIds.length
    ) {
      return yield* directorError(
        "checks-failed",
        "Each capability check label and native toolCallId may be bound only once per request.",
      );
    }
    for (const receipt of input.receipts) {
      const check = (yield* completionChecks(completion.completionId)).find(
        (candidate) => candidate.label === receipt.label,
      );
      if (!check) {
        return yield* directorError(
          "checks-failed",
          `No registered capability check is named ${receipt.label}.`,
        );
      }
      if (check.toolCallId === receipt.toolCallId && check.verificationStatus !== "pending") {
        continue;
      }
      if (check.toolCallId && check.toolCallId !== receipt.toolCallId) {
        return yield* directorError(
          "checks-failed",
          `Capability check ${receipt.label} is already bound to a different native receipt.`,
        );
      }
      const reused = yield* persistence(
        sql<{ readonly used: number }>`
          SELECT 1 AS used FROM workflow_review_checks
          WHERE thread_id = ${director.threadId}
            AND provider_instance_id = ${providerInstanceId}
            AND tool_call_id = ${receipt.toolCallId}
          UNION ALL
          SELECT 1 AS used FROM workflow_capability_checks
          WHERE completion_id <> ${completion.completionId}
            AND thread_id = ${director.threadId}
            AND provider_instance_id = ${providerInstanceId}
            AND tool_call_id = ${receipt.toolCallId}
          LIMIT 1
        `,
        "Prior native receipt use could not be checked.",
      );
      if (reused.length > 0) {
        return yield* directorError(
          "checks-failed",
          "A native command receipt cannot be reused for capability acceptance.",
        );
      }
      const rawNative = yield* persistence(
        sql<Record<string, unknown>>`
          SELECT lifecycle, command, cwd, status, exit_code AS "exitCode", output,
            created_at AS "createdAt"
          FROM workflow_native_command_observations
          WHERE thread_id = ${director.threadId}
            AND provider_instance_id = ${providerInstanceId}
            AND tool_call_id = ${receipt.toolCallId}
            AND created_at > ${check.createdAt}
          ORDER BY created_at
        `,
        "Capability native check receipts could not be read.",
      );
      const native = yield* Effect.forEach(rawNative, (candidate) =>
        decodeNativeCommandRow(candidate),
      ).pipe(
        Effect.mapError((error) =>
          directorError("persistence-failed", "Native command evidence is invalid.", String(error)),
        ),
      );
      const started = native.find((candidate) => candidate.lifecycle === "started");
      const completed = native.findLast((candidate) => candidate.lifecycle === "completed");
      if (!started || !completed || started.createdAt > completed.createdAt) {
        return yield* directorError(
          "checks-failed",
          `Native start and completion for ${receipt.label} are not both observed after registration.`,
        );
      }
      const git = yield* gitObservation(director.worktreePath);
      const accepted =
        started.command === check.command &&
        completed.command === check.command &&
        started.cwd === director.worktreePath &&
        completed.cwd === director.worktreePath &&
        completed.status === "completed" &&
        completed.exitCode === 0 &&
        check.startedHead === completion.resultingHead &&
        check.startedClean === 1 &&
        git.head === completion.resultingHead &&
        git.clean;
      const verificationError = accepted
        ? null
        : started.command !== check.command || completed.command !== check.command
          ? "The observed native command does not match the registered command."
          : started.cwd !== director.worktreePath || completed.cwd !== director.worktreePath
            ? "The observed native command ran outside the capability worktree."
            : completed.status !== "completed" || completed.exitCode === null
              ? "The native command completion status or exit code is unknown."
              : completed.exitCode !== 0
                ? `The native command exited with code ${completed.exitCode}.`
                : git.head !== completion.resultingHead
                  ? "The resulting HEAD changed while combined acceptance ran."
                  : "The capability worktree was dirty before or after combined acceptance.";
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* persistence(
        sql`UPDATE workflow_capability_checks SET tool_call_id = ${receipt.toolCallId},
          exit_code = ${completed.exitCode}, output = ${completed.output},
          finished_head = ${git.head}, finished_clean = ${git.clean ? 1 : 0},
          verification_status = ${accepted ? "passed" : "failed"},
          verification_error = ${verificationError}, native_started_at = ${started.createdAt},
          native_completed_at = ${completed.createdAt}, updated_at = ${updatedAt}
          WHERE completion_id = ${completion.completionId} AND label = ${receipt.label}`,
        "Capability native check evidence could not be saved.",
      );
      yield* persistence(
        sql`DELETE FROM workflow_native_command_observations
          WHERE thread_id = ${director.threadId}
            AND provider_instance_id = ${providerInstanceId}
            AND tool_call_id = ${receipt.toolCallId}`,
        "Compacted capability check observations could not be released.",
      );
    }
  });

  const completeCapabilityUnlocked = Effect.fn("WorkflowDirectorService.completeCapability")(
    function* (
      environmentId: EnvironmentId,
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      input: WorkflowCapabilityCompleteInput,
    ) {
      const director = yield* directorForMcpScope(environmentId, threadId, providerInstanceId);
      const labels = input.checks.map((check) => check.label);
      if (new Set(labels).size !== labels.length) {
        return yield* directorError("checks-failed", "Capability check labels must be unique.");
      }
      let completion = yield* latestCompletion(director.directorId);
      if (completion?.status === "completed") {
        const status = yield* completionForStatus(director);
        return {
          disposition:
            status?.status === "completed" && status.authority === "current" ? "completed" : "held",
          completion: status!,
        } satisfies WorkflowCapabilityCompleteResult;
      }
      if (completion?.status === "reopen-uncertain") {
        completion = yield* compensateCapabilityClose(
          director,
          completion,
          completion.lastError ?? "The owned capability close no longer has current acceptance.",
        );
        return {
          disposition: "held",
          completion: yield* completionStatusFromRow(
            completion,
            completion.status === "invalidated" ? "historical" : "unknown",
          ),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      if (
        completion?.status === "comment-uncertain" ||
        completion?.status === "close-pending" ||
        completion?.status === "close-uncertain"
      ) {
        const tracker = yield* workflow.issueDetail({
          projectId: ProjectId.make(director.projectId),
          repository: director.repository as WorkflowIssueSummary["repository"],
          number: director.capabilityNumber,
        });
        const record = matchingCapabilityCompletion(tracker, completion);
        if (completion.status === "comment-uncertain" && !record) {
          return {
            disposition: "pending",
            completion: yield* completionStatusFromRow(completion, "unknown"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        if (record && completion.status === "comment-uncertain") {
          completion = yield* updateCompletion(completion, {
            status: "close-pending",
            requiredAction:
              "Confirm current acceptance state, then close the capability completed.",
            commentUrl: record.url,
          });
        }
        if (tracker.state === "closed" && tracker.stateReason === "completed" && record) {
          completion = yield* updateCompletion(completion, {
            status: "close-pending",
            requiredAction: "Confirm the post-close acceptance gate.",
            commentUrl: record.url,
            closeConfirmed: true,
          });
          const postClose = yield* capabilityCompletionGate(
            director,
            completion.resultingHead,
            true,
          ).pipe(Effect.result);
          if (postClose._tag === "Failure") {
            if (!completionEvidenceChanged(postClose.failure)) {
              return {
                disposition: "pending",
                completion: yield* completionStatusFromRow(completion, "unknown"),
              } satisfies WorkflowCapabilityCompleteResult;
            }
            completion = yield* compensateCapabilityClose(
              director,
              completion,
              postClose.failure.detail ?? postClose.failure.message,
            );
            return {
              disposition: "held",
              completion: yield* completionStatusFromRow(
                completion,
                completion.status === "invalidated" ? "historical" : "unknown",
              ),
            } satisfies WorkflowCapabilityCompleteResult;
          }
          completion = yield* updateCompletion(completion, {
            status: "completed",
            requiredAction: "No capability completion action is required.",
            commentUrl: record.url,
            closeConfirmed: true,
          });
          return {
            disposition: "completed",
            completion: yield* completionStatusFromRow(completion, "current"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        if (completion.status === "close-uncertain") {
          return {
            disposition: "pending",
            completion: yield* completionStatusFromRow(completion, "unknown"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
      }
      if (completion?.status === "invalidated") completion = null;
      const gated = yield* capabilityCompletionGate(director, input.resultingHead, false).pipe(
        Effect.result,
      );
      if (gated._tag === "Failure") {
        if (completion) {
          if (!completionEvidenceChanged(gated.failure)) {
            return {
              disposition: "pending",
              completion: yield* completionStatusFromRow(completion, "unknown"),
            } satisfies WorkflowCapabilityCompleteResult;
          }
          completion = yield* updateCompletion(completion, {
            status: "invalidated",
            requiredAction: gated.failure.message,
            lastError: gated.failure.detail ?? gated.failure.message,
          });
          return {
            disposition: "held",
            completion: yield* completionStatusFromRow(completion, "historical"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        return yield* gated.failure;
      }
      const gate = gated.success;
      const sameAttempt =
        completion?.resultingHead === input.resultingHead &&
        completion.specificationFingerprint === gate.specificationFingerprint &&
        completion.breakdownFingerprint === gate.breakdownFingerprint &&
        (yield* completionChecks(completion.completionId)).length === input.checks.length &&
        (yield* completionChecks(completion.completionId)).every((check) =>
          input.checks.some(
            (candidate) => candidate.label === check.label && candidate.command === check.command,
          ),
        );
      if (completion?.status === "checks-failed" && sameAttempt && input.receipts.length === 0) {
        completion = yield* updateCompletion(completion, {
          status: "invalidated",
          requiredAction: "A fresh immutable retry replaced this failed acceptance attempt.",
        });
        completion = null;
      } else if (completion && !sameAttempt) {
        if (
          (completion.status === "checks-pending" || completion.status === "checks-failed") &&
          !completion.commentUrl &&
          completion.closeConfirmed === 0
        ) {
          completion = yield* updateCompletion(completion, {
            status: "invalidated",
            requiredAction: "A corrected head or combined check set replaced this attempt.",
          });
          completion = null;
        } else {
          return yield* directorError(
            "completion-pending",
            "Reconcile the existing capability completion write before registering a replacement.",
          );
        }
      }
      if (!completion) {
        const completionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const commentBody = workflowCapabilityCompletionBody({
          completionId,
          repository: director.repository,
          capabilityNumber: director.capabilityNumber,
          resultingHead: input.resultingHead,
          requiredIssues: gate.requiredIssues,
          checks: input.checks,
        });
        yield* persistence(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`INSERT INTO workflow_capability_completions (
              completion_id, director_id, repository, capability_number, resulting_head,
              specification_fingerprint, breakdown_fingerprint, comment_body, status,
              required_action, created_at, updated_at
            ) VALUES (
              ${completionId}, ${director.directorId}, ${director.repository},
              ${director.capabilityNumber}, ${input.resultingHead},
              ${gate.specificationFingerprint}, ${gate.breakdownFingerprint}, ${commentBody},
              'checks-pending', 'Run every registered combined acceptance command through the native provider path, then bind its exact receipt.',
              ${createdAt}, ${createdAt}
            )`;
              yield* Effect.forEach(
                input.checks,
                (check) => sql`INSERT INTO workflow_capability_checks (
                completion_id, label, command, output, started_head, started_clean,
                verification_status, thread_id, provider_instance_id, created_at, updated_at
              ) VALUES (
                ${completionId}, ${check.label}, ${check.command}, '', ${input.resultingHead}, 1,
                'pending', ${director.threadId}, ${providerInstanceId}, ${createdAt}, ${createdAt}
              )`,
                { discard: true },
              );
            }),
          ),
          "The capability completion attempt could not be registered.",
        );
        completion = (yield* latestCompletion(director.directorId))!;
      }
      if (completion.status === "completed") {
        return {
          disposition: "completed",
          completion: yield* completionStatusFromRow(completion, "current"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      if (input.receipts.length === 0) {
        return {
          disposition: completion.status === "checks-failed" ? "held" : "pending",
          completion: yield* completionStatusFromRow(
            completion,
            completion.status === "checks-failed" ? "historical" : "unknown",
          ),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      yield* bindCapabilityCheckReceipts(director, completion, providerInstanceId, input);
      const checks = yield* completionChecks(completion.completionId);
      if (checks.some((check) => check.verificationStatus === "failed")) {
        completion = yield* updateCompletion(completion, {
          status: "checks-failed",
          requiredAction:
            "Correct the failure, then register a fresh immutable acceptance attempt with a corrected head or check set.",
          lastError:
            checks.find((check) => check.verificationStatus === "failed")?.verificationError ??
            "Combined acceptance failed.",
        });
        return {
          disposition: "held",
          completion: yield* completionStatusFromRow(completion, "historical"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      if (!checks.every((check) => check.verificationStatus === "passed")) {
        return {
          disposition: "pending",
          completion: yield* completionStatusFromRow(completion, "unknown"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      const afterChecks = yield* capabilityCompletionGate(
        director,
        completion.resultingHead,
        false,
      ).pipe(Effect.result);
      if (afterChecks._tag === "Failure") {
        if (!completionEvidenceChanged(afterChecks.failure)) {
          return {
            disposition: "pending",
            completion: yield* completionStatusFromRow(completion, "unknown"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        completion = yield* updateCompletion(completion, {
          status: "invalidated",
          requiredAction: afterChecks.failure.message,
          lastError: afterChecks.failure.detail ?? afterChecks.failure.message,
        });
        return {
          disposition: "held",
          completion: yield* completionStatusFromRow(completion, "historical"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      let refreshed = afterChecks.success.capability;
      let record = matchingCapabilityCompletion(refreshed, completion);
      if (completion.status === "comment-uncertain" && !record) {
        return {
          disposition: "pending",
          completion: yield* completionStatusFromRow(completion, "unknown"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      if (!record) {
        completion = yield* updateCompletion(completion, {
          status: "comment-uncertain",
          requiredAction: "Reconcile the exact saved capability evidence comment before retrying.",
        });
        const write = yield* executeGitHub(
          director.worktreePath,
          [
            "issue",
            "comment",
            String(director.capabilityNumber),
            "--repo",
            director.repository,
            "--body-file",
            "-",
          ],
          completion.commentBody,
        ).pipe(Effect.result);
        refreshed = yield* workflow.issueDetail({
          projectId: ProjectId.make(director.projectId),
          repository: director.repository as WorkflowIssueSummary["repository"],
          number: director.capabilityNumber,
        });
        record = matchingCapabilityCompletion(refreshed, completion);
        if (!record) {
          completion = yield* updateCompletion(completion, {
            status: "comment-uncertain",
            requiredAction:
              "Reconcile the exact saved capability evidence comment before retrying.",
            lastError:
              write._tag === "Failure"
                ? write.failure.message
                : "GitHub has not exposed the saved completion evidence yet.",
          });
          return {
            disposition: "pending",
            completion: yield* completionStatusFromRow(completion, "unknown"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        completion = yield* updateCompletion(completion, {
          status: "close-pending",
          requiredAction: "Confirm current acceptance state, then close the capability completed.",
          commentUrl: record.url,
        });
      }
      const beforeClose = yield* capabilityCompletionGate(
        director,
        completion.resultingHead,
        false,
      ).pipe(Effect.result);
      if (beforeClose._tag === "Failure") {
        if (!completionEvidenceChanged(beforeClose.failure)) {
          return {
            disposition: "pending",
            completion: yield* completionStatusFromRow(completion, "unknown"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        completion = yield* updateCompletion(completion, {
          status: "invalidated",
          requiredAction: beforeClose.failure.message,
          lastError: beforeClose.failure.detail ?? beforeClose.failure.message,
        });
        return {
          disposition: "held",
          completion: yield* completionStatusFromRow(completion, "historical"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      if (refreshed.state !== "closed" || refreshed.stateReason !== "completed") {
        completion = yield* updateCompletion(completion, {
          status: "close-uncertain",
          requiredAction: "Reconcile the capability close result before retrying.",
        });
        const close = yield* executeGitHub(director.worktreePath, [
          "issue",
          "close",
          String(director.capabilityNumber),
          "--repo",
          director.repository,
          "--reason",
          "completed",
        ]).pipe(Effect.result);
        if (
          close._tag === "Success" &&
          closeCommandWasAcknowledged(close.success, director.repository, director.capabilityNumber)
        ) {
          completion = yield* updateCompletion(completion, {
            status: "close-uncertain",
            requiredAction: "Confirm the acknowledged capability close from live tracker state.",
            closeOwned: true,
          });
        }
      }
      refreshed = yield* workflow.issueDetail({
        projectId: ProjectId.make(director.projectId),
        repository: director.repository as WorkflowIssueSummary["repository"],
        number: director.capabilityNumber,
      });
      record = matchingCapabilityCompletion(refreshed, completion);
      if (refreshed.state !== "closed" || refreshed.stateReason !== "completed" || !record) {
        completion = yield* updateCompletion(completion, {
          status: "close-uncertain",
          requiredAction: "Reconcile the capability close outcome before retrying.",
          lastError: "GitHub capability closure is still unconfirmed.",
        });
        return {
          disposition: "pending",
          completion: yield* completionStatusFromRow(completion, "unknown"),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      completion = yield* updateCompletion(completion, {
        status: "close-pending",
        requiredAction: "Confirm the post-close acceptance gate.",
        commentUrl: record.url,
        closeConfirmed: true,
      });
      const afterClose = yield* capabilityCompletionGate(
        director,
        completion.resultingHead,
        true,
      ).pipe(Effect.result);
      if (afterClose._tag === "Failure") {
        if (!completionEvidenceChanged(afterClose.failure)) {
          return {
            disposition: "pending",
            completion: yield* completionStatusFromRow(completion, "unknown"),
          } satisfies WorkflowCapabilityCompleteResult;
        }
        completion = yield* compensateCapabilityClose(
          director,
          completion,
          afterClose.failure.detail ?? afterClose.failure.message,
        );
        return {
          disposition: "held",
          completion: yield* completionStatusFromRow(
            completion,
            completion.status === "invalidated" ? "historical" : "unknown",
          ),
        } satisfies WorkflowCapabilityCompleteResult;
      }
      completion = yield* updateCompletion(completion, {
        status: "completed",
        requiredAction: "No capability completion action is required.",
        commentUrl: record.url,
        closeConfirmed: true,
      });
      return {
        disposition: "completed",
        completion: yield* completionStatusFromRow(completion, "current"),
      } satisfies WorkflowCapabilityCompleteResult;
    },
  );

  return WorkflowDirectorService.of({
    start: (input, dispatch) => lock.withPermits(1)(startUnlocked(input, dispatch)),
    status: (input) => lock.withPermits(1)(statusUnlocked(input)),
    reassess: (input, dispatch) => lock.withPermits(1)(reassessUnlocked(input, dispatch)),
    retryReassessment: (input, dispatch) =>
      lock.withPermits(1)(retryReassessmentUnlocked(input, dispatch)),
    resume: (input, dispatch) => lock.withPermits(1)(resumeUnlocked(input, dispatch)),
    admit: (input) => lock.withPermits(1)(admitUnlocked(input)),
    prepareHandoff: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        prepareHandoffUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    reconcileHandoff: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        reconcileHandoffUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
    reconcileHandoffAsOwner: (input, actorSubject) =>
      lock.withPermits(1)(reconcileHandoffAsOwnerUnlocked(input, actorSubject)),
    rotateReady: (input, dispatch) => lock.withPermits(1)(rotateReadyUnlocked(input, dispatch)),
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
    completeCapability: (environmentId, threadId, providerInstanceId, input) =>
      lock.withPermits(1)(
        completeCapabilityUnlocked(environmentId, threadId, providerInstanceId, input),
      ),
  });
});

export const layer = Layer.effect(WorkflowDirectorService, make).pipe(
  Layer.provide(Layer.merge(OrchestrationCommandReceiptRepositoryLive, ProcessRunner.layer)),
);
