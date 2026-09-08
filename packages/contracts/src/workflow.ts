import * as Schema from "effect/Schema";

import {
  EnvironmentId,
  IsoDateTime,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection } from "./orchestration.ts";

export const WorkflowIssueKind = Schema.Literals([
  "map",
  "decision",
  "capability",
  "container",
  "ticket",
  "task",
]);
export type WorkflowIssueKind = typeof WorkflowIssueKind.Type;

export const WorkflowIssueState = Schema.Literals(["open", "closed"]);
export type WorkflowIssueState = typeof WorkflowIssueState.Type;

export const WorkflowIssueStateReason = Schema.Literals(["completed", "not_planned", "reopened"]);
export type WorkflowIssueStateReason = typeof WorkflowIssueStateReason.Type;

export const WorkflowRepositoryNameWithOwner = TrimmedNonEmptyString.check(
  Schema.isPattern(/^[^/\s]+\/[^/\s]+$/u),
);
export type WorkflowRepositoryNameWithOwner = typeof WorkflowRepositoryNameWithOwner.Type;

export const WorkflowRepository = Schema.Struct({
  nameWithOwner: WorkflowRepositoryNameWithOwner,
  remoteNames: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowRepository = typeof WorkflowRepository.Type;

export const WorkflowReadinessStatus = Schema.Literals([
  "ready",
  "claimed",
  "blocked",
  "needs-review",
  "unapproved",
  "resolved",
  "closed-unverified",
  "cancelled",
  "out-of-scope",
  "superseded",
]);
export type WorkflowReadinessStatus = typeof WorkflowReadinessStatus.Type;

export const WorkflowReadinessReasonKind = Schema.Literals([
  "approved-scope",
  "missing-approval",
  "scope-changed",
  "claimed",
  "open-blocker",
  "unverified-blocker",
  "cancelled-blocker",
  "superseded-blocker",
  "reassessment",
  "manual-condition",
  "resolution",
  "missing-resolution",
  "cancelled",
  "out-of-scope",
  "superseded",
]);
export type WorkflowReadinessReasonKind = typeof WorkflowReadinessReasonKind.Type;

export const WorkflowReadinessReason = Schema.Struct({
  kind: WorkflowReadinessReasonKind,
  message: TrimmedNonEmptyString,
  source: Schema.optionalKey(TrimmedNonEmptyString),
});
export type WorkflowReadinessReason = typeof WorkflowReadinessReason.Type;

export const WorkflowReadiness = Schema.Struct({
  status: WorkflowReadinessStatus,
  reasons: Schema.Array(WorkflowReadinessReason),
});
export type WorkflowReadiness = typeof WorkflowReadiness.Type;

export const WorkflowFrontierStatus = Schema.Literals([
  "available",
  "complete",
  "empty-claimed",
  "empty-blocked",
  "empty-reassessment",
  "empty-unapproved",
  "empty-review",
  "empty-inactive",
  "empty",
]);
export type WorkflowFrontierStatus = typeof WorkflowFrontierStatus.Type;

export const WorkflowFrontier = Schema.Struct({
  status: WorkflowFrontierStatus,
  message: TrimmedNonEmptyString,
  readyIssueIds: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowFrontier = typeof WorkflowFrontier.Type;

export const WorkflowEvidenceRecordKind = Schema.Literals([
  "approval",
  "resolution",
  "reassessment",
]);
export type WorkflowEvidenceRecordKind = typeof WorkflowEvidenceRecordKind.Type;

export const WorkflowEvidenceRecordState = Schema.Literals(["current", "superseded", "invalid"]);
export type WorkflowEvidenceRecordState = typeof WorkflowEvidenceRecordState.Type;

export const WorkflowEvidenceSourceAccess = Schema.Literals([
  "verified",
  "reported",
  "unavailable",
]);
export type WorkflowEvidenceSourceAccess = typeof WorkflowEvidenceSourceAccess.Type;

export const WorkflowEvidenceScope = Schema.Literals([
  "current",
  "changed",
  "unknown",
  "not-applicable",
]);
export type WorkflowEvidenceScope = typeof WorkflowEvidenceScope.Type;

export const WorkflowApprovalAuthority = Schema.Literals(["verified", "reported", "unknown"]);
export type WorkflowApprovalAuthority = typeof WorkflowApprovalAuthority.Type;

export const WorkflowEvidenceRecord = Schema.Struct({
  id: TrimmedNonEmptyString,
  url: TrimmedNonEmptyString,
  createdAt: TrimmedNonEmptyString,
  kind: WorkflowEvidenceRecordKind,
  state: WorkflowEvidenceRecordState,
  sourceAccess: WorkflowEvidenceSourceAccess,
  scope: WorkflowEvidenceScope,
  summary: TrimmedNonEmptyString,
  approvalKind: Schema.optionalKey(Schema.Literals(["specification", "ticket-breakdown"])),
  authority: Schema.optionalKey(WorkflowApprovalAuthority),
  approvedBy: Schema.optionalKey(TrimmedNonEmptyString),
  source: Schema.optionalKey(TrimmedNonEmptyString),
  approvedContent: Schema.optionalKey(TrimmedNonEmptyString),
  outcome: Schema.optionalKey(
    Schema.Literals(["resolved", "cancelled", "out-of-scope", "cleared", "scope-change"]),
  ),
  evidence: Schema.optionalKey(TrimmedNonEmptyString),
  bodyFingerprint: Schema.optionalKey(TrimmedNonEmptyString),
});
export type WorkflowEvidenceRecord = typeof WorkflowEvidenceRecord.Type;

export const WorkflowManualCondition = Schema.Struct({
  description: TrimmedNonEmptyString,
  source: TrimmedNonEmptyString,
  status: Schema.Literals(["review-required", "satisfied"]),
  evidence: Schema.optionalKey(TrimmedNonEmptyString),
});
export type WorkflowManualCondition = typeof WorkflowManualCondition.Type;

export const WorkflowEvidence = Schema.Struct({
  records: Schema.Array(WorkflowEvidenceRecord),
  manualConditions: Schema.Array(WorkflowManualCondition),
  historyComplete: Schema.optionalKey(Schema.Boolean),
});
export type WorkflowEvidence = typeof WorkflowEvidence.Type;

export const WorkflowIssueSummary = Schema.Struct({
  id: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  number: PositiveInt,
  title: TrimmedNonEmptyString,
  url: TrimmedNonEmptyString,
  kind: WorkflowIssueKind,
  state: WorkflowIssueState,
  stateReason: Schema.NullOr(WorkflowIssueStateReason),
  updatedAt: TrimmedNonEmptyString,
  childCount: Schema.Number,
  parentNumber: Schema.NullOr(PositiveInt),
  labels: Schema.Array(TrimmedNonEmptyString),
  readiness: Schema.optionalKey(WorkflowReadiness),
});
export type WorkflowIssueSummary = typeof WorkflowIssueSummary.Type;

export const WorkflowRepositoriesInput = Schema.Struct({ projectId: ProjectId });
export type WorkflowRepositoriesInput = typeof WorkflowRepositoriesInput.Type;

export const WorkflowMonitorInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
});
export type WorkflowMonitorInput = typeof WorkflowMonitorInput.Type;

export const WorkflowSyncStatus = Schema.Literals([
  "refreshing",
  "fresh",
  "stale",
  "access-denied",
  "unavailable",
  "rate-limited",
]);
export type WorkflowSyncStatus = typeof WorkflowSyncStatus.Type;

export const WorkflowSyncState = Schema.Struct({
  repository: WorkflowRepositoryNameWithOwner,
  status: WorkflowSyncStatus,
  lastAttemptAt: Schema.NullOr(IsoDateTime),
  lastSuccessfulAt: Schema.NullOr(IsoDateTime),
  cacheAgeMs: Schema.NullOr(Schema.Number),
  retryAt: Schema.NullOr(IsoDateTime),
  revision: Schema.Number,
  message: TrimmedNonEmptyString,
});
export type WorkflowSyncState = typeof WorkflowSyncState.Type;

export const WorkflowRepositoriesResult = Schema.Struct({
  projectId: ProjectId,
  projectTitle: TrimmedNonEmptyString,
  repositories: Schema.Array(WorkflowRepository),
});
export type WorkflowRepositoriesResult = typeof WorkflowRepositoriesResult.Type;

export const WorkflowRootsInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
});
export type WorkflowRootsInput = typeof WorkflowRootsInput.Type;

export const WorkflowRootsResult = Schema.Struct({
  repository: WorkflowRepositoryNameWithOwner,
  roots: Schema.Array(WorkflowIssueSummary),
});
export type WorkflowRootsResult = typeof WorkflowRootsResult.Type;

export const WorkflowChildrenInput = Schema.Struct({
  ...WorkflowRootsInput.fields,
  parentNumber: PositiveInt,
});
export type WorkflowChildrenInput = typeof WorkflowChildrenInput.Type;

export const WorkflowChildrenResult = Schema.Struct({
  parentNumber: PositiveInt,
  children: Schema.Array(WorkflowIssueSummary),
  frontier: Schema.optionalKey(WorkflowFrontier),
});
export type WorkflowChildrenResult = typeof WorkflowChildrenResult.Type;

export const WorkflowIssueDetailInput = Schema.Struct({
  ...WorkflowRootsInput.fields,
  number: PositiveInt,
});
export type WorkflowIssueDetailInput = typeof WorkflowIssueDetailInput.Type;

export const WorkflowIssueDetail = Schema.Struct({
  ...WorkflowIssueSummary.fields,
  body: Schema.String,
  blockedBy: Schema.Array(WorkflowIssueSummary),
  evidence: Schema.optionalKey(WorkflowEvidence),
});
export type WorkflowIssueDetail = typeof WorkflowIssueDetail.Type;

export const WorkflowSearchInput = Schema.Struct({
  ...WorkflowRootsInput.fields,
  query: TrimmedNonEmptyString,
});
export type WorkflowSearchInput = typeof WorkflowSearchInput.Type;

export const WorkflowSearchMatch = Schema.Struct({
  issue: WorkflowIssueSummary,
  ancestry: Schema.Array(WorkflowIssueSummary),
  ancestryComplete: Schema.Boolean,
});
export type WorkflowSearchMatch = typeof WorkflowSearchMatch.Type;

export const WorkflowSearchResult = Schema.Struct({
  matches: Schema.Array(WorkflowSearchMatch),
  hasMore: Schema.Boolean,
});
export type WorkflowSearchResult = typeof WorkflowSearchResult.Type;

export const WorkflowLocateInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  id: Schema.optional(TrimmedNonEmptyString),
  number: PositiveInt,
});
export type WorkflowLocateInput = typeof WorkflowLocateInput.Type;

export const WorkflowLocateResult = WorkflowSearchMatch;
export type WorkflowLocateResult = typeof WorkflowLocateResult.Type;

export const WorkflowPhase = Schema.Literals(["decision", "specification", "ticket-breakdown"]);
export type WorkflowPhase = typeof WorkflowPhase.Type;

export const WorkflowStartInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  issueNumber: PositiveInt,
  phase: Schema.optional(WorkflowPhase),
  planningThreadId: Schema.optional(ThreadId),
  modelSelection: ModelSelection,
});
export type WorkflowStartInput = typeof WorkflowStartInput.Type;

export const WorkflowStartAttemptStatus = Schema.Literals(["submitted", "held"]);
export type WorkflowStartAttemptStatus = typeof WorkflowStartAttemptStatus.Type;

export const WorkflowStartResult = Schema.Struct({
  disposition: Schema.Literals(["started", "existing", "held"]),
  attemptId: TrimmedNonEmptyString,
  environmentId: EnvironmentId,
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  issueNumber: PositiveInt,
  phase: WorkflowPhase,
  threadId: ThreadId,
  status: WorkflowStartAttemptStatus,
  createdAt: IsoDateTime,
  message: TrimmedNonEmptyString,
});
export type WorkflowStartResult = typeof WorkflowStartResult.Type;

export const WorkflowRecoveryInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  issueNumber: PositiveInt,
  phase: Schema.optional(WorkflowPhase),
});
export type WorkflowRecoveryInput = typeof WorkflowRecoveryInput.Type;

export const WorkflowRecoveryEvidence = Schema.Literals(["accepted", "rejected", "unknown"]);
export type WorkflowRecoveryEvidence = typeof WorkflowRecoveryEvidence.Type;

export const WorkflowRecoveryAttempt = Schema.Struct({
  attemptId: TrimmedNonEmptyString,
  environmentId: EnvironmentId,
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  issueNumber: PositiveInt,
  phase: WorkflowPhase,
  threadId: ThreadId,
  status: Schema.Literals(["claiming", "submitting", "submitted", "held"]),
  evidence: WorkflowRecoveryEvidence,
  claimLogin: Schema.NullOr(TrimmedNonEmptyString),
  isCurrent: Schema.Boolean,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  detail: Schema.NullOr(TrimmedNonEmptyString),
});
export type WorkflowRecoveryAttempt = typeof WorkflowRecoveryAttempt.Type;

export const WorkflowRecoveryResult = Schema.Struct({
  environmentId: EnvironmentId,
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  issueNumber: PositiveInt,
  attempts: Schema.Array(WorkflowRecoveryAttempt),
  currentAttempt: Schema.NullOr(WorkflowRecoveryAttempt),
  assignees: Schema.Array(TrimmedNonEmptyString),
  observation: TrimmedNonEmptyString,
  actions: Schema.Array(Schema.Literals(["open", "resume", "start-fresh", "takeover"])),
  message: TrimmedNonEmptyString,
});
export type WorkflowRecoveryResult = typeof WorkflowRecoveryResult.Type;

export const WorkflowRecoverInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  issueNumber: PositiveInt,
  phase: Schema.optional(WorkflowPhase),
  attemptId: Schema.optional(TrimmedNonEmptyString),
  action: Schema.Literals(["open", "resume", "start-fresh", "takeover"]),
  observation: Schema.optional(TrimmedNonEmptyString),
  modelSelection: Schema.optional(ModelSelection),
});
export type WorkflowRecoverInput = typeof WorkflowRecoverInput.Type;

export const WorkflowRecoverResult = Schema.Struct({
  action: Schema.Literals(["open", "resumed", "started-fresh", "taken-over"]),
  attemptId: TrimmedNonEmptyString,
  environmentId: EnvironmentId,
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  issueNumber: PositiveInt,
  threadId: ThreadId,
  message: TrimmedNonEmptyString,
});
export type WorkflowRecoverResult = typeof WorkflowRecoverResult.Type;

export const WorkflowDirectorStartInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  capabilityNumber: PositiveInt,
  modelSelection: ModelSelection,
});
export type WorkflowDirectorStartInput = typeof WorkflowDirectorStartInput.Type;

export const WorkflowDirectorRequestedProfile = Schema.Struct({
  instanceId: TrimmedNonEmptyString,
  model: TrimmedNonEmptyString,
  effort: TrimmedNonEmptyString,
});
export type WorkflowDirectorRequestedProfile = typeof WorkflowDirectorRequestedProfile.Type;

export const WorkflowDirectorObservedProfile = Schema.Struct({
  model: Schema.NullOr(TrimmedNonEmptyString),
  effort: Schema.NullOr(TrimmedNonEmptyString),
  match: Schema.Literals(["match", "mismatch", "unknown"]),
});
export type WorkflowDirectorObservedProfile = typeof WorkflowDirectorObservedProfile.Type;

export const WorkflowDirectorLifecycleStatus = Schema.Literals([
  "preparing-worktree",
  "submitting",
  "active",
  "held",
  "waiting",
  "completed",
]);
export type WorkflowDirectorLifecycleStatus = typeof WorkflowDirectorLifecycleStatus.Type;

export const WorkflowDirectorStatusInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  capabilityNumber: Schema.optional(PositiveInt),
  ticketNumber: Schema.optional(PositiveInt),
});
export type WorkflowDirectorStatusInput = typeof WorkflowDirectorStatusInput.Type;

export const WorkflowActiveWorkCursor = Schema.Struct({
  directorCreatedAt: IsoDateTime,
  directorId: TrimmedNonEmptyString,
  entryOrder: Schema.Number,
  entryCreatedAt: IsoDateTime,
  entryId: TrimmedNonEmptyString,
});
export type WorkflowActiveWorkCursor = typeof WorkflowActiveWorkCursor.Type;

export const WorkflowActiveWorkInput = Schema.Struct({
  cursor: Schema.optional(WorkflowActiveWorkCursor),
});
export type WorkflowActiveWorkInput = typeof WorkflowActiveWorkInput.Type;

export const WorkflowActiveWorkEntry = Schema.Struct({
  entryId: TrimmedNonEmptyString,
  kind: Schema.Literals(["director", "worker", "reviewer", "unassociated"]),
  environmentId: EnvironmentId,
  projectId: ProjectId,
  projectTitle: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  capabilityNumber: PositiveInt,
  issueNumber: PositiveInt,
  directorId: TrimmedNonEmptyString,
  ownerThreadId: ThreadId,
  navigationThreadId: Schema.NullOr(ThreadId),
  title: Schema.NullOr(TrimmedNonEmptyString),
  providerThreadId: Schema.NullOr(TrimmedNonEmptyString),
  activity: Schema.Literals(["running", "waiting", "attention", "unknown", "settled"]),
  unresolved: Schema.Boolean,
  updatedAt: IsoDateTime,
  cursor: WorkflowActiveWorkCursor,
});
export type WorkflowActiveWorkEntry = typeof WorkflowActiveWorkEntry.Type;

export const WorkflowActiveWorkResult = Schema.Struct({
  environmentId: EnvironmentId,
  entries: Schema.Array(WorkflowActiveWorkEntry),
  nextCursor: Schema.NullOr(WorkflowActiveWorkCursor),
  refreshedAt: IsoDateTime,
});
export type WorkflowActiveWorkResult = typeof WorkflowActiveWorkResult.Type;

export const WorkflowWorkerRequestedProfile = Schema.Struct({
  model: TrimmedNonEmptyString,
  effort: TrimmedNonEmptyString,
  skillPath: TrimmedNonEmptyString,
});
export type WorkflowWorkerRequestedProfile = typeof WorkflowWorkerRequestedProfile.Type;

export const WorkflowWorkerObservedProfile = Schema.Struct({
  model: Schema.NullOr(TrimmedNonEmptyString),
  effort: Schema.NullOr(TrimmedNonEmptyString),
  match: Schema.Literals(["match", "mismatch", "unknown"]),
});
export type WorkflowWorkerObservedProfile = typeof WorkflowWorkerObservedProfile.Type;

export const WorkflowWorkerStatus = Schema.Struct({
  dispatchId: Schema.NullOr(TrimmedNonEmptyString),
  admissionId: Schema.NullOr(TrimmedNonEmptyString),
  ticketNumber: Schema.NullOr(PositiveInt),
  providerThreadId: Schema.NullOr(TrimmedNonEmptyString),
  parentProviderThreadId: Schema.NullOr(TrimmedNonEmptyString),
  ownership: Schema.NullOr(TrimmedNonEmptyString),
  writePaths: Schema.Array(TrimmedNonEmptyString),
  writeReservation: Schema.NullOr(Schema.Literals(["held", "released"])),
  settlementEvidence: Schema.NullOr(Schema.Literal("native-closed")),
  association: Schema.Literals(["associated", "unassociated", "unconfirmed"]),
  providerStatus: TrimmedNonEmptyString,
  requestedProfile: Schema.NullOr(WorkflowWorkerRequestedProfile),
  observedProfile: WorkflowWorkerObservedProfile,
  handoff: Schema.NullOr(
    Schema.Struct({
      outcome: Schema.Literals(["succeeded", "failed", "unconfirmed"]),
      summary: TrimmedNonEmptyString,
      commits: Schema.Array(TrimmedNonEmptyString),
      checks: Schema.Array(TrimmedNonEmptyString),
    }),
  ),
  title: Schema.NullOr(TrimmedNonEmptyString),
  role: Schema.NullOr(TrimmedNonEmptyString),
  updatedAt: IsoDateTime,
});
export type WorkflowWorkerStatus = typeof WorkflowWorkerStatus.Type;

export const WorkflowReviewCheckInput = Schema.Struct({
  label: TrimmedNonEmptyString,
  command: TrimmedNonEmptyString,
});
export type WorkflowReviewCheckInput = typeof WorkflowReviewCheckInput.Type;

export const WorkflowReviewCheck = Schema.Struct({
  label: TrimmedNonEmptyString,
  command: TrimmedNonEmptyString,
  toolCallId: Schema.NullOr(TrimmedNonEmptyString),
  exitCode: Schema.NullOr(Schema.Number),
  output: Schema.String,
  startedHead: TrimmedNonEmptyString,
  finishedHead: Schema.NullOr(TrimmedNonEmptyString),
  startedClean: Schema.Boolean,
  finishedClean: Schema.NullOr(Schema.Boolean),
  status: Schema.Literals(["pending", "passed", "failed"]),
  verificationError: Schema.NullOr(TrimmedNonEmptyString),
});
export type WorkflowReviewCheck = typeof WorkflowReviewCheck.Type;

export const WorkflowReviewAxis = Schema.Literals(["standards", "spec"]);
export type WorkflowReviewAxis = typeof WorkflowReviewAxis.Type;

export const WorkflowReviewFindingSeverity = Schema.Literals(["critical", "high", "medium", "low"]);
export type WorkflowReviewFindingSeverity = typeof WorkflowReviewFindingSeverity.Type;

export const WorkflowReviewFindingDisposition = Schema.Struct({
  outcome: Schema.Literals(["fixed", "dismissed", "owner-accepted"]),
  rationale: TrimmedNonEmptyString,
  evidenceSource: Schema.NullOr(TrimmedNonEmptyString),
  evidenceQuote: Schema.NullOr(TrimmedNonEmptyString),
  resultingReviewId: Schema.NullOr(TrimmedNonEmptyString),
});
export type WorkflowReviewFindingDisposition = typeof WorkflowReviewFindingDisposition.Type;

export const WorkflowReviewFinding = Schema.Struct({
  id: TrimmedNonEmptyString,
  axis: WorkflowReviewAxis,
  severity: WorkflowReviewFindingSeverity,
  summary: TrimmedNonEmptyString,
  location: Schema.NullOr(TrimmedNonEmptyString),
  disposition: Schema.NullOr(WorkflowReviewFindingDisposition),
});
export type WorkflowReviewFinding = typeof WorkflowReviewFinding.Type;

export const WorkflowReviewIdentity = Schema.Struct({
  axis: WorkflowReviewAxis,
  providerThreadId: TrimmedNonEmptyString,
  parentProviderThreadId: Schema.NullOr(TrimmedNonEmptyString),
  providerStatus: TrimmedNonEmptyString,
  settlementEvidence: Schema.NullOr(Schema.Literal("native-closed")),
  observedProfile: WorkflowWorkerObservedProfile,
});
export type WorkflowReviewIdentity = typeof WorkflowReviewIdentity.Type;

export const WorkflowTicketReviewStatus = Schema.Struct({
  reviewId: TrimmedNonEmptyString,
  admissionId: TrimmedNonEmptyString,
  ticketNumber: PositiveInt,
  implementationProviderThreadId: TrimmedNonEmptyString,
  fixedBase: TrimmedNonEmptyString,
  implementationHead: TrimmedNonEmptyString,
  status: Schema.Literals([
    "checks-pending",
    "checks-failed",
    "prepared",
    "spawn-issued",
    "associated",
    "reported",
  ]),
  association: Schema.Literals(["associated", "unconfirmed"]),
  providerThreadId: Schema.NullOr(TrimmedNonEmptyString),
  parentProviderThreadId: Schema.NullOr(TrimmedNonEmptyString),
  providerStatus: TrimmedNonEmptyString,
  settlementEvidence: Schema.NullOr(Schema.Literal("native-closed")),
  requestedProfile: WorkflowWorkerRequestedProfile,
  observedProfile: WorkflowWorkerObservedProfile,
  checks: Schema.Array(WorkflowReviewCheck),
  axes: Schema.Array(WorkflowReviewIdentity),
  findings: Schema.Array(WorkflowReviewFinding),
  summary: Schema.NullOr(TrimmedNonEmptyString),
  updatedAt: IsoDateTime,
});
export type WorkflowTicketReviewStatus = typeof WorkflowTicketReviewStatus.Type;

export const WorkflowTicketResolutionStatus = Schema.Struct({
  resolutionId: TrimmedNonEmptyString,
  reviewId: TrimmedNonEmptyString,
  ticketNumber: PositiveInt,
  finalHead: TrimmedNonEmptyString,
  status: Schema.Literals([
    "comment-pending",
    "comment-uncertain",
    "close-pending",
    "close-uncertain",
    "resolved",
  ]),
  commentUrl: Schema.NullOr(TrimmedNonEmptyString),
  lastError: Schema.NullOr(TrimmedNonEmptyString),
  readyIssueIds: Schema.Array(TrimmedNonEmptyString),
  updatedAt: IsoDateTime,
});
export type WorkflowTicketResolutionStatus = typeof WorkflowTicketResolutionStatus.Type;

export const WorkflowInterruptionSubject = Schema.Struct({
  subjectId: TrimmedNonEmptyString,
  kind: Schema.Literals(["director", "worker", "reviewer", "unknown-child"]),
  providerThreadId: Schema.NullOr(TrimmedNonEmptyString),
  parentProviderThreadId: Schema.NullOr(TrimmedNonEmptyString),
  nativeSessionId: Schema.NullOr(TrimmedNonEmptyString),
  nativeTurnId: Schema.NullOr(TrimmedNonEmptyString),
  interruptAttemptId: Schema.NullOr(TrimmedNonEmptyString),
  requestStatus: Schema.Literals(["not-issued", "requested", "acknowledged", "failed", "unknown"]),
  outcome: Schema.Literals(["stopping", "stopped", "failed", "unknown", "closed", "resumed"]),
  detail: Schema.NullOr(TrimmedNonEmptyString),
  updatedAt: IsoDateTime,
});
export type WorkflowInterruptionSubject = typeof WorkflowInterruptionSubject.Type;

export const WorkflowReassessmentStatus = Schema.Struct({
  reassessmentId: TrimmedNonEmptyString,
  triggerKind: Schema.Literals(["scope-change", "prerequisite"]),
  triggerIssueNumber: PositiveInt,
  triggerSource: TrimmedNonEmptyString,
  status: Schema.Literals(["stopping", "held", "clearing", "cleared"]),
  requiredAction: TrimmedNonEmptyString,
  stopRequestStatus: Schema.Literals(["not-issued", "submitted", "failed", "unknown"]),
  trackerStatus: Schema.Literals(["not-written", "pending", "uncertain", "confirmed"]),
  trackerUrl: Schema.NullOr(TrimmedNonEmptyString),
  triggers: Schema.Array(
    Schema.Struct({
      kind: Schema.Literals(["scope-change", "prerequisite"]),
      issueNumber: PositiveInt,
      source: TrimmedNonEmptyString,
      requiredAction: TrimmedNonEmptyString,
      discoveredAt: IsoDateTime,
    }),
  ),
  subjects: Schema.Array(WorkflowInterruptionSubject),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type WorkflowReassessmentStatus = typeof WorkflowReassessmentStatus.Type;

export const WorkflowCapabilityCompletionStatus = Schema.Struct({
  completionId: TrimmedNonEmptyString,
  resultingHead: TrimmedNonEmptyString,
  status: Schema.Literals([
    "checks-pending",
    "checks-failed",
    "comment-uncertain",
    "close-pending",
    "close-uncertain",
    "reopen-uncertain",
    "invalidated",
    "completed",
  ]),
  authority: Schema.Literals(["current", "historical", "unknown"]),
  checks: Schema.Array(WorkflowReviewCheck),
  evidenceUrl: Schema.NullOr(TrimmedNonEmptyString),
  requiredAction: TrimmedNonEmptyString,
  lastError: Schema.NullOr(TrimmedNonEmptyString),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type WorkflowCapabilityCompletionStatus = typeof WorkflowCapabilityCompletionStatus.Type;

export const WorkflowDirectorHandoffPrepareInput = Schema.Struct({
  lessons: Schema.Array(TrimmedNonEmptyString),
  unresolvedContext: Schema.Array(TrimmedNonEmptyString),
  suggestedSkills: Schema.Array(TrimmedNonEmptyString),
  suggestedStaffing: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowDirectorHandoffPrepareInput = typeof WorkflowDirectorHandoffPrepareInput.Type;

export const WorkflowDirectorHandoffReconcileInput = Schema.Struct({
  handoffId: TrimmedNonEmptyString,
  summary: TrimmedNonEmptyString,
});
export type WorkflowDirectorHandoffReconcileInput =
  typeof WorkflowDirectorHandoffReconcileInput.Type;

export const WorkflowDirectorHandoffOwnerReconcileInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  capabilityNumber: PositiveInt,
  expectedDirectorId: TrimmedNonEmptyString,
  expectedObservation: TrimmedNonEmptyString,
  handoffId: TrimmedNonEmptyString,
  expectedTargetObservation: TrimmedNonEmptyString,
  summary: TrimmedNonEmptyString,
});
export type WorkflowDirectorHandoffOwnerReconcileInput =
  typeof WorkflowDirectorHandoffOwnerReconcileInput.Type;

export const WorkflowDirectorHandoffReconciliationActor = Schema.Struct({
  kind: Schema.Literals(["director", "owner-session"]),
  subject: TrimmedNonEmptyString,
});
export type WorkflowDirectorHandoffReconciliationActor =
  typeof WorkflowDirectorHandoffReconciliationActor.Type;

export const WorkflowDirectorHandoffReconciliation = Schema.Struct({
  reconciliationId: TrimmedNonEmptyString,
  sequence: PositiveInt,
  acknowledgedByDirectorId: TrimmedNonEmptyString,
  acknowledgementActor: Schema.optionalKey(WorkflowDirectorHandoffReconciliationActor),
  implementationHead: TrimmedNonEmptyString,
  settlementCount: Schema.Number,
  summary: TrimmedNonEmptyString,
  createdAt: IsoDateTime,
});
export type WorkflowDirectorHandoffReconciliation =
  typeof WorkflowDirectorHandoffReconciliation.Type;

export const WorkflowDirectorHandoffStatus = Schema.Struct({
  handoffId: TrimmedNonEmptyString,
  sourceDirectorId: TrimmedNonEmptyString,
  sourceBatchId: TrimmedNonEmptyString,
  sourceThreadId: ThreadId,
  successorDirectorId: Schema.NullOr(TrimmedNonEmptyString),
  successorThreadId: Schema.NullOr(ThreadId),
  implementationHead: Schema.NullOr(TrimmedNonEmptyString),
  status: Schema.Literals(["waiting-settlement", "submitting", "submitted", "held"]),
  admissionCount: Schema.Number,
  lessons: Schema.Array(TrimmedNonEmptyString),
  unresolvedContext: Schema.Array(TrimmedNonEmptyString),
  latestReconciliation: Schema.NullOr(WorkflowDirectorHandoffReconciliation),
  requiredAction: TrimmedNonEmptyString,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type WorkflowDirectorHandoffStatus = typeof WorkflowDirectorHandoffStatus.Type;

export const WorkflowDirectorHandoffRecoveryTarget = Schema.Struct({
  handoffId: TrimmedNonEmptyString,
  sourceDirectorId: TrimmedNonEmptyString,
  sourceBatchId: TrimmedNonEmptyString,
  sourceBatchNumber: PositiveInt,
  sourceThreadId: ThreadId,
  status: Schema.Literals(["waiting-settlement", "submitting", "submitted", "held"]),
  detail: Schema.NullOr(TrimmedNonEmptyString),
  rootSettlement: Schema.NullOr(
    Schema.Struct({
      status: Schema.Literals(["running", "completed", "failed", "interrupted"]),
      observedAt: IsoDateTime,
    }),
  ),
  childSettlements: Schema.Struct({
    observedCount: Schema.Number,
    settledCount: Schema.Number,
  }),
  targetObservation: TrimmedNonEmptyString,
  latestReconciliation: Schema.NullOr(WorkflowDirectorHandoffReconciliation),
});
export type WorkflowDirectorHandoffRecoveryTarget =
  typeof WorkflowDirectorHandoffRecoveryTarget.Type;

export const WorkflowDirectorStatus = Schema.Struct({
  directorId: TrimmedNonEmptyString,
  batchId: TrimmedNonEmptyString,
  environmentId: EnvironmentId,
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  capabilityNumber: PositiveInt,
  threadId: ThreadId,
  worktreePath: TrimmedNonEmptyString,
  worktreeBranch: TrimmedNonEmptyString,
  status: WorkflowDirectorLifecycleStatus,
  requestedProfile: WorkflowDirectorRequestedProfile,
  observedProfile: WorkflowDirectorObservedProfile,
  admissionCount: Schema.Number,
  admissionLimit: PositiveInt,
  workers: Schema.Array(WorkflowWorkerStatus),
  reviews: Schema.optionalKey(Schema.Array(WorkflowTicketReviewStatus)),
  resolutions: Schema.optionalKey(Schema.Array(WorkflowTicketResolutionStatus)),
  reassessment: Schema.optionalKey(Schema.NullOr(WorkflowReassessmentStatus)),
  completion: Schema.optionalKey(Schema.NullOr(WorkflowCapabilityCompletionStatus)),
  handoff: Schema.optionalKey(Schema.NullOr(WorkflowDirectorHandoffStatus)),
  handoffRecoveryTargets: Schema.optionalKey(Schema.Array(WorkflowDirectorHandoffRecoveryTarget)),
  observation: TrimmedNonEmptyString,
  actions: Schema.Array(Schema.Literals(["open", "resume", "retry", "stop"])),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  message: TrimmedNonEmptyString,
});
export type WorkflowDirectorStatus = typeof WorkflowDirectorStatus.Type;

export const WorkflowDirectorStartResult = Schema.Struct({
  disposition: Schema.Literals(["started", "existing", "held"]),
  director: WorkflowDirectorStatus,
});
export type WorkflowDirectorStartResult = typeof WorkflowDirectorStartResult.Type;

export const WorkflowDirectorResumeInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  capabilityNumber: PositiveInt,
  directorId: TrimmedNonEmptyString,
  observation: TrimmedNonEmptyString,
  modelSelection: ModelSelection,
});
export type WorkflowDirectorResumeInput = typeof WorkflowDirectorResumeInput.Type;

export const WorkflowDirectorReassessmentRetryInput = Schema.Struct({
  projectId: ProjectId,
  repository: WorkflowRepositoryNameWithOwner,
  capabilityNumber: PositiveInt,
  directorId: TrimmedNonEmptyString,
  observation: TrimmedNonEmptyString,
});
export type WorkflowDirectorReassessmentRetryInput =
  typeof WorkflowDirectorReassessmentRetryInput.Type;

export const WorkflowDirectorAdmissionPurpose = Schema.Literals(["implement", "retry", "review"]);
export type WorkflowDirectorAdmissionPurpose = typeof WorkflowDirectorAdmissionPurpose.Type;

export const WorkflowDirectorAdmissionInput = Schema.Struct({
  projectId: ProjectId,
  directorId: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  ticketNumber: PositiveInt,
  parentTicketNumber: Schema.optional(PositiveInt),
  purpose: WorkflowDirectorAdmissionPurpose,
  ownership: TrimmedNonEmptyString,
});
export type WorkflowDirectorAdmissionInput = typeof WorkflowDirectorAdmissionInput.Type;

export const WorkflowDirectorAdmission = Schema.Struct({
  admissionId: TrimmedNonEmptyString,
  directorId: TrimmedNonEmptyString,
  batchId: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  ticketNumber: PositiveInt,
  slotTicketNumber: PositiveInt,
  purpose: WorkflowDirectorAdmissionPurpose,
  ownership: TrimmedNonEmptyString,
  claimLogin: Schema.NullOr(TrimmedNonEmptyString),
  claimStatus: Schema.Literals(["pending", "confirmed", "uncertain", "conflict"]),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type WorkflowDirectorAdmission = typeof WorkflowDirectorAdmission.Type;

export const WorkflowDirectorAdmissionResult = Schema.Struct({
  disposition: Schema.Literals(["admitted", "existing", "limit-reached"]),
  admission: Schema.NullOr(WorkflowDirectorAdmission),
  admissionCount: Schema.Number,
  admissionLimit: PositiveInt,
  directorStatus: WorkflowDirectorLifecycleStatus,
  message: TrimmedNonEmptyString,
});
export type WorkflowDirectorAdmissionResult = typeof WorkflowDirectorAdmissionResult.Type;

export const WorkflowWorkerPrepareInput = Schema.Struct({
  ticketNumber: PositiveInt,
  parentTicketNumber: Schema.optional(PositiveInt),
  ownership: TrimmedNonEmptyString,
  writePaths: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowWorkerPrepareInput = typeof WorkflowWorkerPrepareInput.Type;

export const WorkflowWorkerPrepareResult = Schema.Struct({
  dispatchId: TrimmedNonEmptyString,
  associationToken: TrimmedNonEmptyString,
  admission: WorkflowDirectorAdmission,
  requestedProfile: WorkflowWorkerRequestedProfile,
  taskName: TrimmedNonEmptyString,
  instructions: TrimmedNonEmptyString,
  disposition: Schema.Literals(["prepared", "existing"]),
  worker: WorkflowWorkerStatus,
});
export type WorkflowWorkerPrepareResult = typeof WorkflowWorkerPrepareResult.Type;

export const WorkflowWorkerAssociateInput = Schema.Struct({
  associationToken: TrimmedNonEmptyString,
  providerThreadId: TrimmedNonEmptyString,
});
export type WorkflowWorkerAssociateInput = typeof WorkflowWorkerAssociateInput.Type;

export const WorkflowWorkerHandoffInput = Schema.Struct({
  providerThreadId: TrimmedNonEmptyString,
  outcome: Schema.Literals(["succeeded", "failed", "unconfirmed"]),
  summary: TrimmedNonEmptyString,
  commits: Schema.Array(TrimmedNonEmptyString),
  checks: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowWorkerHandoffInput = typeof WorkflowWorkerHandoffInput.Type;

export const WorkflowTicketReviewPrepareInput = Schema.Struct({
  ticketNumber: PositiveInt,
  implementationProviderThreadId: TrimmedNonEmptyString,
  fixedBase: TrimmedNonEmptyString,
  implementationHead: TrimmedNonEmptyString,
  checks: Schema.Array(WorkflowReviewCheckInput).check(Schema.isMinLength(1)),
});
export type WorkflowTicketReviewPrepareInput = typeof WorkflowTicketReviewPrepareInput.Type;

export const WorkflowTicketReviewPrepareResult = Schema.Struct({
  disposition: Schema.Literals(["prepared", "existing", "held"]),
  associationToken: TrimmedNonEmptyString,
  taskName: TrimmedNonEmptyString,
  instructions: TrimmedNonEmptyString,
  review: WorkflowTicketReviewStatus,
});
export type WorkflowTicketReviewPrepareResult = typeof WorkflowTicketReviewPrepareResult.Type;

export const WorkflowTicketReviewAssociateInput = Schema.Struct({
  associationToken: TrimmedNonEmptyString,
  providerThreadId: TrimmedNonEmptyString,
});
export type WorkflowTicketReviewAssociateInput = typeof WorkflowTicketReviewAssociateInput.Type;

export const WorkflowReviewCheckReceiptInput = Schema.Struct({
  reviewId: TrimmedNonEmptyString,
  receipts: Schema.Array(
    Schema.Struct({
      label: TrimmedNonEmptyString,
      toolCallId: TrimmedNonEmptyString,
    }),
  ).check(Schema.isMinLength(1)),
});
export type WorkflowReviewCheckReceiptInput = typeof WorkflowReviewCheckReceiptInput.Type;

export const WorkflowTicketReviewReportInput = Schema.Struct({
  providerThreadId: TrimmedNonEmptyString,
  standardsReviewerThreadId: TrimmedNonEmptyString,
  specReviewerThreadId: TrimmedNonEmptyString,
  summary: TrimmedNonEmptyString,
  findings: Schema.Array(
    Schema.Struct({
      id: TrimmedNonEmptyString,
      axis: WorkflowReviewAxis,
      severity: WorkflowReviewFindingSeverity,
      summary: TrimmedNonEmptyString,
      location: Schema.optional(TrimmedNonEmptyString),
    }),
  ),
});
export type WorkflowTicketReviewReportInput = typeof WorkflowTicketReviewReportInput.Type;

export const WorkflowReviewDispositionInput = Schema.Struct({
  reviewId: TrimmedNonEmptyString,
  dispositions: Schema.Array(
    Schema.Struct({
      findingId: TrimmedNonEmptyString,
      outcome: Schema.Literals(["fixed", "dismissed", "owner-accepted"]),
      rationale: TrimmedNonEmptyString,
      evidenceSource: Schema.optional(TrimmedNonEmptyString),
      evidenceQuote: Schema.optional(TrimmedNonEmptyString),
      resultingReviewId: Schema.optional(TrimmedNonEmptyString),
    }),
  ),
});
export type WorkflowReviewDispositionInput = typeof WorkflowReviewDispositionInput.Type;

export const WorkflowTicketResolveInput = Schema.Struct({
  reviewId: TrimmedNonEmptyString,
});
export type WorkflowTicketResolveInput = typeof WorkflowTicketResolveInput.Type;

export const WorkflowTicketResolveResult = Schema.Struct({
  disposition: Schema.Literals(["resolved", "pending"]),
  resolution: WorkflowTicketResolutionStatus,
});
export type WorkflowTicketResolveResult = typeof WorkflowTicketResolveResult.Type;

export const WorkflowCapabilityCompleteInput = Schema.Struct({
  resultingHead: TrimmedNonEmptyString,
  checks: Schema.Array(WorkflowReviewCheckInput).check(Schema.isMinLength(1)),
  receipts: Schema.Array(
    Schema.Struct({
      label: TrimmedNonEmptyString,
      toolCallId: TrimmedNonEmptyString,
    }),
  ),
});
export type WorkflowCapabilityCompleteInput = typeof WorkflowCapabilityCompleteInput.Type;

export const WorkflowCapabilityCompleteResult = Schema.Struct({
  disposition: Schema.Literals(["pending", "held", "completed"]),
  completion: WorkflowCapabilityCompletionStatus,
});
export type WorkflowCapabilityCompleteResult = typeof WorkflowCapabilityCompleteResult.Type;

export const WorkflowDirectorFailure = Schema.Literals([
  "not-ready",
  "approval-unavailable",
  "breakdown-incomplete",
  "workspace-unavailable",
  "provider-unavailable",
  "model-unavailable",
  "effort-required",
  "skill-unavailable",
  "worktree-failed",
  "director-active",
  "director-not-found",
  "claim-failed",
  "persistence-failed",
  "dispatch-failed",
  "review-incomplete",
  "stale-review",
  "checks-failed",
  "resolution-pending",
  "completion-pending",
  "completion-unavailable",
]);
export type WorkflowDirectorFailure = typeof WorkflowDirectorFailure.Type;

export class WorkflowDirectorError extends Schema.TaggedErrorClass<WorkflowDirectorError>()(
  "WorkflowDirectorError",
  {
    failure: WorkflowDirectorFailure,
    message: TrimmedNonEmptyString,
    detail: Schema.optional(TrimmedNonEmptyString),
  },
) {}

export const WorkflowStartFailure = Schema.Literals([
  "not-ready",
  "unsupported-issue",
  "workspace-unavailable",
  "provider-unavailable",
  "model-unavailable",
  "effort-required",
  "skill-unavailable",
  "claim-failed",
  "persistence-failed",
  "dispatch-failed",
]);
export type WorkflowStartFailure = typeof WorkflowStartFailure.Type;

export class WorkflowStartError extends Schema.TaggedErrorClass<WorkflowStartError>()(
  "WorkflowStartError",
  {
    failure: WorkflowStartFailure,
    message: TrimmedNonEmptyString,
    detail: Schema.optional(TrimmedNonEmptyString),
  },
) {}

export const WorkflowAdoptionRelationship = Schema.Struct({
  issueNumber: PositiveInt,
  relationship: Schema.Literals(["source", "specification"]),
  source: TrimmedNonEmptyString,
});
export type WorkflowAdoptionRelationship = typeof WorkflowAdoptionRelationship.Type;

export const WorkflowAdoptionItem = Schema.Struct({
  id: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  number: PositiveInt,
  title: TrimmedNonEmptyString,
  url: TrimmedNonEmptyString,
  updatedAt: TrimmedNonEmptyString,
  sourceBody: Schema.String,
  currentKind: Schema.NullOr(WorkflowIssueKind),
  proposedKind: WorkflowIssueKind,
  currentParentNumber: Schema.NullOr(PositiveInt),
  proposedParentNumber: Schema.NullOr(PositiveInt),
  labels: Schema.Array(TrimmedNonEmptyString),
  relationships: Schema.Array(WorkflowAdoptionRelationship),
  changes: Schema.Array(TrimmedNonEmptyString),
  included: Schema.Boolean,
  parentChangeConfirmed: Schema.Boolean,
});
export type WorkflowAdoptionItem = typeof WorkflowAdoptionItem.Type;

export const WorkflowAdoptionPreviewInput = Schema.Struct({
  ...WorkflowRootsInput.fields,
  rootNumber: PositiveInt,
});
export type WorkflowAdoptionPreviewInput = typeof WorkflowAdoptionPreviewInput.Type;

export const WorkflowAdoptionPreview = Schema.Struct({
  previewId: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  items: Schema.Array(WorkflowAdoptionItem),
});
export type WorkflowAdoptionPreview = typeof WorkflowAdoptionPreview.Type;

export const WorkflowAdoptionApplyInput = Schema.Struct({
  projectId: ProjectId,
  previewId: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  items: Schema.Array(WorkflowAdoptionItem),
});
export type WorkflowAdoptionApplyInput = typeof WorkflowAdoptionApplyInput.Type;

export const WorkflowAdoptionOperation = Schema.Struct({
  issueId: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  issueNumber: PositiveInt,
  kind: Schema.Literals(["add-label", "remove-label", "change-parent"]),
  status: Schema.Literals([
    "applied",
    "already-current",
    "uncertain",
    "failed",
    "undo-skipped",
    "undone",
  ]),
  description: TrimmedNonEmptyString,
  owned: Schema.Boolean,
  detail: Schema.optionalKey(TrimmedNonEmptyString),
});
export type WorkflowAdoptionOperation = typeof WorkflowAdoptionOperation.Type;

export const WorkflowAdoptionRecord = Schema.Struct({
  adoptionId: TrimmedNonEmptyString,
  previewId: TrimmedNonEmptyString,
  repository: WorkflowRepositoryNameWithOwner,
  rootNumber: PositiveInt,
  createdAt: TrimmedNonEmptyString,
  status: Schema.Literals(["applied", "partial", "undone", "undo-partial"]),
  operations: Schema.Array(WorkflowAdoptionOperation),
});
export type WorkflowAdoptionRecord = typeof WorkflowAdoptionRecord.Type;

export const WorkflowAdoptionHistoryInput = Schema.Struct({
  ...WorkflowRootsInput.fields,
  rootNumber: PositiveInt,
});
export type WorkflowAdoptionHistoryInput = typeof WorkflowAdoptionHistoryInput.Type;

export const WorkflowAdoptionHistoryResult = Schema.Struct({
  records: Schema.Array(WorkflowAdoptionRecord),
});
export type WorkflowAdoptionHistoryResult = typeof WorkflowAdoptionHistoryResult.Type;

export const WorkflowAdoptionRecoveryInput = Schema.Struct({
  ...WorkflowAdoptionHistoryInput.fields,
  adoptionId: TrimmedNonEmptyString,
});
export type WorkflowAdoptionRecoveryInput = typeof WorkflowAdoptionRecoveryInput.Type;

export const WorkflowAdoptionRecoveryResult = Schema.Struct({
  record: WorkflowAdoptionRecord,
  preview: WorkflowAdoptionPreview,
});
export type WorkflowAdoptionRecoveryResult = typeof WorkflowAdoptionRecoveryResult.Type;

export const WorkflowAdoptionUndoInput = Schema.Struct({
  projectId: ProjectId,
  adoptionId: TrimmedNonEmptyString,
});
export type WorkflowAdoptionUndoInput = typeof WorkflowAdoptionUndoInput.Type;

export const WorkflowAdoptionFailure = Schema.Literals([
  "preview-not-found",
  "changed-source",
  "parent-confirmation-required",
  "invalid-selection",
  "adoption-not-found",
  "persistence-failed",
]);
export type WorkflowAdoptionFailure = typeof WorkflowAdoptionFailure.Type;

export class WorkflowAdoptionError extends Schema.TaggedErrorClass<WorkflowAdoptionError>()(
  "WorkflowAdoptionError",
  {
    failure: WorkflowAdoptionFailure,
    message: TrimmedNonEmptyString,
    detail: Schema.optional(TrimmedNonEmptyString),
  },
) {}

export const WorkflowQueryFailure = Schema.Literals([
  "project-not-found",
  "missing-git-repository",
  "missing-github-repository",
  "github-unavailable",
  "github-unauthenticated",
  "github-forbidden",
  "github-rate-limited",
  "repository-not-found",
  "issue-not-found",
  "invalid-response",
  "request-failed",
]);
export type WorkflowQueryFailure = typeof WorkflowQueryFailure.Type;

export class WorkflowQueryError extends Schema.TaggedErrorClass<WorkflowQueryError>()(
  "WorkflowQueryError",
  {
    failure: WorkflowQueryFailure,
    message: TrimmedNonEmptyString,
    detail: Schema.optional(TrimmedNonEmptyString),
  },
) {}
