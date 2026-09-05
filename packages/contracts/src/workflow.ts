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

export const WorkflowRepository = Schema.Struct({
  nameWithOwner: TrimmedNonEmptyString,
  remoteNames: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowRepository = typeof WorkflowRepository.Type;

export const WorkflowIssueSummary = Schema.Struct({
  number: PositiveInt,
  title: TrimmedNonEmptyString,
  url: TrimmedNonEmptyString,
  kind: WorkflowIssueKind,
  state: WorkflowIssueState,
  stateReason: Schema.NullOr(WorkflowIssueStateReason),
  updatedAt: TrimmedNonEmptyString,
  childCount: Schema.Number,
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
  repository: TrimmedNonEmptyString,
});
export type WorkflowRootsInput = typeof WorkflowRootsInput.Type;

export const WorkflowRootsResult = Schema.Struct({
  repository: TrimmedNonEmptyString,
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
  labels: Schema.Array(TrimmedNonEmptyString),
});
export type WorkflowIssueDetail = typeof WorkflowIssueDetail.Type;

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
