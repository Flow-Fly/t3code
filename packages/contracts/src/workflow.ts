import * as Schema from "effect/Schema";

import { PositiveInt, ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";

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
  id: TrimmedNonEmptyString,
  number: PositiveInt,
});
export type WorkflowLocateInput = typeof WorkflowLocateInput.Type;

export const WorkflowLocateResult = WorkflowSearchMatch;
export type WorkflowLocateResult = typeof WorkflowLocateResult.Type;

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
  issueNumber: PositiveInt,
  kind: Schema.Literals(["add-label", "remove-label", "change-parent"]),
  status: Schema.Literals(["applied", "already-current", "failed", "undo-skipped", "undone"]),
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
