import {
  type ProjectId,
  type WorkflowChildrenInput,
  type WorkflowChildrenResult,
  type WorkflowIssueDetail,
  type WorkflowIssueDetailInput,
  type WorkflowIssueKind,
  type WorkflowIssueStateReason,
  type WorkflowReadiness,
  type WorkflowLocateInput,
  type WorkflowLocateResult,
  WorkflowQueryError,
  type WorkflowRepositoriesInput,
  type WorkflowRepositoriesResult,
  type WorkflowRepository,
  WorkflowRepositoryNameWithOwner,
  type WorkflowRootsInput,
  type WorkflowRootsResult,
  type WorkflowSearchInput,
  type WorkflowSearchResult,
} from "@t3tools/contracts";
import { normalizeGitRemoteUrl } from "@t3tools/shared/git";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import {
  interpretWorkflowEvidence,
  type WorkflowEvidenceComment,
  type WorkflowEvidenceIssue,
  workflowFrontier,
  workflowHasRemainingFog,
} from "./WorkflowEvidence.ts";

const WORKFLOW_LABEL_TO_KIND = {
  "wayfinder:map": "map",
  "wayfinder:research": "decision",
  "wayfinder:prototype": "decision",
  "wayfinder:grilling": "decision",
  "wayfinder:task": "task",
  "workflow:map": "map",
  "workflow:decision": "decision",
  "workflow:capability": "capability",
  "workflow:container": "container",
  "workflow:ticket": "ticket",
  "workflow:task": "task",
} as const satisfies Record<string, WorkflowIssueKind>;

const RawLabel = Schema.Struct({ name: Schema.String });
const RawComment = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  body: Schema.String,
  createdAt: Schema.String,
  author: Schema.NullOr(Schema.Struct({ login: Schema.String })),
  authorAssociation: Schema.String,
});
const RawAssignee = Schema.Struct({ login: Schema.String });
const RawReopenedEvent = Schema.Struct({ createdAt: Schema.String });
const RawPageInfo = Schema.Struct({
  hasNextPage: Schema.Boolean,
  endCursor: Schema.NullOr(Schema.String),
  hasPreviousPage: Schema.optional(Schema.Boolean),
  startCursor: Schema.optional(Schema.NullOr(Schema.String)),
});
const RawIssueReference = Schema.Struct({
  id: Schema.String,
  number: Schema.Number,
  title: Schema.String,
  url: Schema.String,
  state: Schema.String,
  stateReason: Schema.NullOr(Schema.String),
  updatedAt: Schema.String,
  lastEditedAt: Schema.optional(Schema.NullOr(Schema.String)),
  labels: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawLabel) }),
  repository: Schema.Struct({ nameWithOwner: Schema.String }),
  subIssuesSummary: Schema.Struct({ total: Schema.Number }),
});
type RawIssueReference = typeof RawIssueReference.Type;

const RawIssue = Schema.Struct({
  ...RawIssueReference.fields,
  body: Schema.optional(Schema.String),
  assignees: Schema.optional(
    Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawAssignee) }),
  ),
  comments: Schema.optional(
    Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawComment) }),
  ),
  timelineItems: Schema.optional(
    Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawReopenedEvent) }),
  ),
  parent: Schema.optional(Schema.NullOr(RawIssueReference)),
  blockedBy: Schema.optional(
    Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawIssueReference) }),
  ),
});
type RawIssue = typeof RawIssue.Type;

const RawRootsPage = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.NullOr(
      Schema.Struct({
        issues: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawIssue) }),
      }),
    ),
  }),
});

const RawChildrenPage = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.NullOr(
      Schema.Struct({
        issue: Schema.NullOr(
          Schema.Struct({
            id: Schema.optional(Schema.String),
            body: Schema.optional(Schema.String),
            comments: Schema.optional(
              Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawComment) }),
            ),
            subIssues: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawIssue) }),
          }),
        ),
      }),
    ),
  }),
});

const RawDetail = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.NullOr(Schema.Struct({ issue: Schema.NullOr(RawIssue) })),
  }),
});

const RawLabelsPage = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(
      Schema.Struct({
        labels: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawLabel) }),
      }),
    ),
  }),
});

const RawBlockedByPage = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(
      Schema.Struct({
        blockedBy: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawIssueReference) }),
      }),
    ),
  }),
});

const RawCommentsPage = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(
      Schema.Struct({
        comments: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawComment) }),
      }),
    ),
  }),
});

const RawCommentsBackwardPage = RawCommentsPage;

const RawAssigneesPage = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(
      Schema.Struct({
        assignees: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawAssignee) }),
      }),
    ),
  }),
});

const RawReopenedPage = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(
      Schema.Struct({
        timelineItems: Schema.Struct({
          pageInfo: RawPageInfo,
          nodes: Schema.Array(RawReopenedEvent),
        }),
      }),
    ),
  }),
});

const RawEvidenceLookup = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(RawIssue),
  }),
});

const RawSearchPage = Schema.Struct({
  data: Schema.Struct({
    search: Schema.Struct({
      pageInfo: RawPageInfo,
      nodes: Schema.Array(RawIssue),
    }),
  }),
});

const RawIssueLookup = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.NullOr(Schema.Struct({ issue: Schema.NullOr(RawIssue) })),
  }),
});

const decodeRootsPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawRootsPage));
const decodeChildrenPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawChildrenPage));
const decodeDetail = Schema.decodeUnknownSync(Schema.fromJsonString(RawDetail));
const decodeLabelsPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawLabelsPage));
const decodeBlockedByPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawBlockedByPage));
const decodeCommentsPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawCommentsPage));
const decodeCommentsBackwardPage = Schema.decodeUnknownSync(
  Schema.fromJsonString(RawCommentsBackwardPage),
);
const decodeAssigneesPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawAssigneesPage));
const decodeReopenedPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawReopenedPage));
const decodeEvidenceLookup = Schema.decodeUnknownSync(Schema.fromJsonString(RawEvidenceLookup));
const decodeSearchPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawSearchPage));
const decodeIssueLookup = Schema.decodeUnknownSync(Schema.fromJsonString(RawIssueLookup));
const isWorkflowRepositoryNameWithOwner = Schema.is(WorkflowRepositoryNameWithOwner);

const ISSUE_BASE_FIELDS = `id number title url state stateReason updatedAt lastEditedAt repository{nameWithOwner} labels(first:100){pageInfo{hasNextPage endCursor}nodes{name}} subIssuesSummary{total}`;
const ISSUE_FIELDS = `${ISSUE_BASE_FIELDS} parent{${ISSUE_BASE_FIELDS}}`;
const COMMENTS_FIELDS = `comments(first:100){pageInfo{hasNextPage endCursor}nodes{id url body createdAt author{login} authorAssociation}}`;
const RECENT_COMMENTS_FIELDS = `comments(last:20){pageInfo{hasNextPage endCursor hasPreviousPage startCursor}nodes{id url body createdAt author{login} authorAssociation}}`;
const ASSIGNEES_FIELDS = `assignees(first:100){pageInfo{hasNextPage endCursor}nodes{login}}`;
const REOPENED_FIELDS = `timelineItems(first:100,itemTypes:[REOPENED_EVENT]){pageInfo{hasNextPage endCursor}nodes{... on ReopenedEvent{createdAt}}}`;
const BLOCKED_BY_FIELDS = `blockedBy(first:100){pageInfo{hasNextPage endCursor}nodes{${ISSUE_FIELDS}}}`;
const CHILD_BLOCKER_FIELDS = `id number title url state stateReason updatedAt lastEditedAt repository{nameWithOwner} labels(first:20){pageInfo{hasNextPage endCursor}nodes{name}} subIssuesSummary{total}`;
const CHILD_BLOCKED_BY_FIELDS = `blockedBy(first:20){pageInfo{hasNextPage endCursor}nodes{${CHILD_BLOCKER_FIELDS}}}`;
const ISSUE_EVIDENCE_FIELDS = `${ISSUE_FIELDS} body ${COMMENTS_FIELDS} ${ASSIGNEES_FIELDS} ${REOPENED_FIELDS} ${BLOCKED_BY_FIELDS}`;
const CHILD_EVIDENCE_FIELDS = `${ISSUE_FIELDS} body ${RECENT_COMMENTS_FIELDS} ${ASSIGNEES_FIELDS} ${REOPENED_FIELDS} ${CHILD_BLOCKED_BY_FIELDS}`;
const ROOTS_QUERY = `query WorkflowRoots($owner:String!,$name:String!,$after:String){repository(owner:$owner,name:$name){issues(first:100,after:$after,orderBy:{field:UPDATED_AT,direction:DESC}){pageInfo{hasNextPage endCursor}nodes{${ISSUE_FIELDS}}}}}`;
const CHILDREN_QUERY = `query WorkflowChildren($owner:String!,$name:String!,$number:Int!,$after:String){repository(owner:$owner,name:$name){issue(number:$number){id body ${COMMENTS_FIELDS} subIssues(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{${CHILD_EVIDENCE_FIELDS}}}}}}`;
const DETAIL_QUERY = `query WorkflowDetail($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){issue(number:$number){${ISSUE_EVIDENCE_FIELDS}}}}`;
const LABELS_QUERY = `query WorkflowLabels($id:ID!,$after:String){node(id:$id){... on Issue{labels(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{name}}}}}`;
const BLOCKED_BY_QUERY = `query WorkflowBlockedBy($id:ID!,$after:String){node(id:$id){... on Issue{blockedBy(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{${ISSUE_FIELDS}}}}}}`;
const COMMENTS_QUERY = `query WorkflowComments($id:ID!,$after:String){node(id:$id){... on Issue{comments(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{id url body createdAt author{login} authorAssociation}}}}}`;
const COMMENTS_BACKWARD_QUERY = `query WorkflowCommentsBackward($id:ID!,$before:String){node(id:$id){... on Issue{comments(last:100,before:$before){pageInfo{hasNextPage endCursor hasPreviousPage startCursor}nodes{id url body createdAt author{login} authorAssociation}}}}}`;
const ASSIGNEES_QUERY = `query WorkflowAssignees($id:ID!,$after:String){node(id:$id){... on Issue{assignees(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{login}}}}}`;
const REOPENED_QUERY = `query WorkflowReopened($id:ID!,$after:String){node(id:$id){... on Issue{timelineItems(first:100,after:$after,itemTypes:[REOPENED_EVENT]){pageInfo{hasNextPage endCursor}nodes{... on ReopenedEvent{createdAt}}}}}}`;
const EVIDENCE_LOOKUP_QUERY = `query WorkflowEvidenceLookup($id:ID!){node(id:$id){... on Issue{${ISSUE_EVIDENCE_FIELDS}}}}`;
const SEARCH_QUERY = `query WorkflowSearch($searchQuery:String!,$after:String){search(query:$searchQuery,type:ISSUE,first:20,after:$after){pageInfo{hasNextPage endCursor}nodes{... on Issue{${ISSUE_FIELDS}}}}}`;
const ISSUE_LOOKUP_QUERY = `query WorkflowIssueLookup($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){issue(number:$number){${ISSUE_FIELDS}}}}`;

function queryError(failure: WorkflowQueryError["failure"], message: string, detail?: string) {
  return new WorkflowQueryError({ failure, message, ...(detail ? { detail } : {}) });
}

function parseRepository(repository: string): { owner: string; name: string } | null {
  if (!isWorkflowRepositoryNameWithOwner(repository)) return null;
  const [owner, name] = repository.split("/") as [string, string];
  return { owner, name };
}

function issueKind(labels: ReadonlyArray<{ readonly name: string }>): WorkflowIssueKind | null {
  for (const label of labels) {
    const kind = WORKFLOW_LABEL_TO_KIND[label.name as keyof typeof WORKFLOW_LABEL_TO_KIND];
    if (kind) return kind;
  }
  return null;
}

function stateReason(value: string | null): WorkflowIssueStateReason | null {
  if (value === "COMPLETED") return "completed";
  if (value === "NOT_PLANNED") return "not_planned";
  if (value === "REOPENED") return "reopened";
  return null;
}

function issueSummary(issue: RawIssueReference | RawIssue, parentNumber?: number | null) {
  const kind =
    issueKind(issue.labels.nodes) ?? (issue.subIssuesSummary.total > 0 ? "container" : "task");
  return {
    id: issue.id,
    repository: issue.repository.nameWithOwner,
    number: issue.number,
    title: issue.title.trim(),
    url: issue.url.trim(),
    kind,
    state: issue.state === "OPEN" ? ("open" as const) : ("closed" as const),
    stateReason: stateReason(issue.stateReason),
    updatedAt: issue.updatedAt,
    childCount: issue.subIssuesSummary.total,
    parentNumber: parentNumber ?? ("parent" in issue ? issue.parent?.number : null) ?? null,
    labels: issue.labels.nodes.map((label) => label.name),
  };
}

function workflowRepositories(stdout: string): ReadonlyArray<WorkflowRepository> {
  const byRepository = new Map<string, Set<string>>();
  for (const line of stdout.split("\n")) {
    const match = /^(\S+)\s+(\S+)\s+\(fetch\)$/.exec(line.trim());
    if (!match) continue;
    const remoteName = match[1];
    const remoteUrl = match[2];
    if (!remoteName || !remoteUrl) continue;
    const canonical = normalizeGitRemoteUrl(remoteUrl);
    if (!canonical.startsWith("github.com/")) continue;
    const nameWithOwner = canonical.slice("github.com/".length);
    if (!parseRepository(nameWithOwner)) continue;
    const remoteNames = byRepository.get(nameWithOwner) ?? new Set<string>();
    remoteNames.add(remoteName);
    byRepository.set(nameWithOwner, remoteNames);
  }
  return [...byRepository.entries()]
    .map(([nameWithOwner, remoteNames]) => ({
      nameWithOwner,
      remoteNames: [...remoteNames].toSorted(),
    }))
    .toSorted((left, right) => left.nameWithOwner.localeCompare(right.nameWithOwner));
}

function mapGitHubError(error: GitHubCli.GitHubCliError): WorkflowQueryError {
  switch (error._tag) {
    case "GitHubCliUnavailableError":
      return queryError("github-unavailable", "GitHub CLI is not installed in this environment.");
    case "GitHubCliAuthenticationError":
      return queryError(
        "github-unauthenticated",
        "GitHub CLI is not authenticated in this environment.",
      );
    case "GitHubCliRateLimitError":
      return queryError("github-rate-limited", "GitHub API rate limit exceeded.");
    default:
      return queryError("request-failed", "GitHub could not load workflow issues.", error.message);
  }
}

export class WorkflowService extends Context.Service<
  WorkflowService,
  {
    readonly repositories: (
      input: WorkflowRepositoriesInput,
    ) => Effect.Effect<WorkflowRepositoriesResult, WorkflowQueryError>;
    readonly roots: (
      input: WorkflowRootsInput,
    ) => Effect.Effect<WorkflowRootsResult, WorkflowQueryError>;
    readonly children: (
      input: WorkflowChildrenInput,
    ) => Effect.Effect<WorkflowChildrenResult, WorkflowQueryError>;
    readonly issueDetail: (
      input: WorkflowIssueDetailInput,
    ) => Effect.Effect<WorkflowIssueDetail, WorkflowQueryError>;
    readonly search: (
      input: WorkflowSearchInput,
    ) => Effect.Effect<WorkflowSearchResult, WorkflowQueryError>;
    readonly locate: (
      input: WorkflowLocateInput,
    ) => Effect.Effect<WorkflowLocateResult, WorkflowQueryError>;
  }
>()("t3/workflow/WorkflowService") {}

export const make = Effect.gen(function* () {
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const process = yield* ProcessRunner.ProcessRunner;
  const github = yield* GitHubCli.GitHubCli;

  const project = Effect.fn("WorkflowService.project")(function* (projectId: ProjectId) {
    const projectOption = yield* projection
      .getProjectShellById(projectId)
      .pipe(
        Effect.mapError((error) =>
          queryError("request-failed", "The project could not be read.", error.message),
        ),
      );
    return yield* Option.match(projectOption, {
      onNone: () =>
        Effect.fail(queryError("project-not-found", "This project is no longer available.")),
      onSome: Effect.succeed,
    });
  });

  const executeGraphQl = Effect.fn("WorkflowService.executeGraphQl")(function* (input: {
    cwd: string;
    repository: string;
    query: string;
    number?: number;
    id?: string;
    cursor?: string;
    before?: string;
    searchQuery?: string;
  }) {
    const parsed = parseRepository(input.repository);
    if (!parsed) {
      return yield* queryError("repository-not-found", "Choose a valid GitHub repository.");
    }
    const args = [
      "api",
      "graphql",
      "--method",
      "POST",
      "-f",
      `query=${input.query}`,
      "-F",
      `owner=${parsed.owner}`,
      "-F",
      `name=${parsed.name}`,
      ...(input.number === undefined ? [] : ["-F", `number=${input.number}`]),
      ...(input.id === undefined ? [] : ["-F", `id=${input.id}`]),
      ...(input.cursor === undefined ? [] : ["-f", `after=${input.cursor}`]),
      ...(input.before === undefined ? [] : ["-f", `before=${input.before}`]),
      ...(input.searchQuery === undefined ? [] : ["-f", `searchQuery=${input.searchQuery}`]),
    ];
    const result = yield* github
      .execute({ cwd: input.cwd, args, maxOutputBytes: 5_000_000 })
      .pipe(Effect.mapError(mapGitHubError));
    return result.stdout;
  });

  const completeLabels = Effect.fn("WorkflowService.completeLabels")(function* (
    cwd: string,
    repository: string,
    issue: RawIssueReference | RawIssue,
  ) {
    const labels = [...issue.labels.nodes];
    let pageInfo = issue.labels.pageInfo;
    while (pageInfo.hasNextPage) {
      if (!pageInfo.endCursor) {
        return yield* queryError(
          "invalid-response",
          "GitHub returned incomplete workflow label pagination.",
        );
      }
      const raw = yield* executeGraphQl({
        cwd,
        repository,
        query: LABELS_QUERY,
        id: issue.id,
        cursor: pageInfo.endCursor,
      });
      const decoded = yield* Effect.try({
        try: () => decodeLabelsPage(raw),
        catch: (error) =>
          queryError("invalid-response", "GitHub returned invalid workflow labels.", String(error)),
      });
      if (!decoded.data.node) {
        return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
      }
      labels.push(...decoded.data.node.labels.nodes);
      pageInfo = decoded.data.node.labels.pageInfo;
    }
    return { ...issue, labels: { ...issue.labels, nodes: labels } };
  });

  const completeBlockerReferences = Effect.fn("WorkflowService.completeBlockerReferences")(
    function* (cwd: string, repository: string, issue: RawIssue) {
      const blockers = [...(issue.blockedBy?.nodes ?? [])];
      let pageInfo = issue.blockedBy?.pageInfo;
      while (pageInfo?.hasNextPage) {
        if (!pageInfo.endCursor) {
          return yield* queryError(
            "invalid-response",
            "GitHub returned incomplete workflow blocker pagination.",
          );
        }
        const raw = yield* executeGraphQl({
          cwd,
          repository,
          query: BLOCKED_BY_QUERY,
          id: issue.id,
          cursor: pageInfo.endCursor,
        });
        const decoded = yield* Effect.try({
          try: () => decodeBlockedByPage(raw),
          catch: (error) =>
            queryError(
              "invalid-response",
              "GitHub returned invalid workflow blockers.",
              String(error),
            ),
        });
        if (!decoded.data.node) {
          return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
        }
        blockers.push(...decoded.data.node.blockedBy.nodes);
        pageInfo = decoded.data.node.blockedBy.pageInfo;
      }
      return blockers;
    },
  );

  const completeBlockedBy = Effect.fn("WorkflowService.completeBlockedBy")(function* (
    cwd: string,
    repository: string,
    issue: RawIssue,
  ) {
    const blockers = yield* completeBlockerReferences(cwd, repository, issue);
    const summaries = [];
    const seen = new Set<string>();
    for (const blocker of blockers) {
      if (seen.has(blocker.id)) continue;
      seen.add(blocker.id);
      const complete = yield* completeLabels(cwd, blocker.repository.nameWithOwner, blocker);
      summaries.push(issueSummary(complete));
    }
    return summaries;
  });

  const completeComments = Effect.fn("WorkflowService.completeComments")(function* (
    cwd: string,
    repository: string,
    id: string,
    initial:
      | {
          readonly pageInfo: typeof RawPageInfo.Type;
          readonly nodes: ReadonlyArray<typeof RawComment.Type>;
        }
      | undefined,
  ) {
    const comments = [...(initial?.nodes ?? [])];
    let pageInfo = initial?.pageInfo;
    while (pageInfo?.hasNextPage) {
      if (!pageInfo.endCursor) {
        return yield* queryError(
          "invalid-response",
          "GitHub returned incomplete workflow comment pagination.",
        );
      }
      const raw = yield* executeGraphQl({
        cwd,
        repository,
        query: COMMENTS_QUERY,
        id,
        cursor: pageInfo.endCursor,
      });
      const decoded = yield* Effect.try({
        try: () => decodeCommentsPage(raw),
        catch: (error) =>
          queryError(
            "invalid-response",
            "GitHub returned invalid workflow comments.",
            String(error),
          ),
      });
      if (!decoded.data.node)
        return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
      comments.push(...decoded.data.node.comments.nodes);
      pageInfo = decoded.data.node.comments.pageInfo;
    }
    return comments;
  });

  const completeRecentComments = Effect.fn("WorkflowService.completeRecentComments")(function* (
    cwd: string,
    repository: string,
    id: string,
    initial: RawIssue["comments"],
  ) {
    const comments = [...(initial?.nodes ?? [])];
    let pageInfo = initial?.pageInfo;
    while (pageInfo?.hasPreviousPage) {
      if (!pageInfo.startCursor) {
        return yield* queryError(
          "invalid-response",
          "GitHub returned incomplete workflow comment pagination.",
        );
      }
      const raw = yield* executeGraphQl({
        cwd,
        repository,
        query: COMMENTS_BACKWARD_QUERY,
        id,
        before: pageInfo.startCursor,
      });
      const decoded = yield* Effect.try({
        try: () => decodeCommentsBackwardPage(raw),
        catch: (error) =>
          queryError(
            "invalid-response",
            "GitHub returned invalid workflow comments.",
            String(error),
          ),
      });
      if (!decoded.data.node)
        return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
      comments.unshift(...decoded.data.node.comments.nodes);
      pageInfo = decoded.data.node.comments.pageInfo;
    }
    return comments;
  });

  const completeAssignees = Effect.fn("WorkflowService.completeAssignees")(function* (
    cwd: string,
    repository: string,
    id: string,
    initial: RawIssue["assignees"],
  ) {
    const assignees = [...(initial?.nodes ?? [])];
    let pageInfo = initial?.pageInfo;
    while (pageInfo?.hasNextPage) {
      if (!pageInfo.endCursor) {
        return yield* queryError(
          "invalid-response",
          "GitHub returned incomplete workflow assignee pagination.",
        );
      }
      const raw = yield* executeGraphQl({
        cwd,
        repository,
        query: ASSIGNEES_QUERY,
        id,
        cursor: pageInfo.endCursor,
      });
      const decoded = yield* Effect.try({
        try: () => decodeAssigneesPage(raw),
        catch: (error) =>
          queryError(
            "invalid-response",
            "GitHub returned invalid workflow assignees.",
            String(error),
          ),
      });
      if (!decoded.data.node)
        return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
      assignees.push(...decoded.data.node.assignees.nodes);
      pageInfo = decoded.data.node.assignees.pageInfo;
    }
    return assignees;
  });

  const completeReopened = Effect.fn("WorkflowService.completeReopened")(function* (
    cwd: string,
    repository: string,
    id: string,
    initial: RawIssue["timelineItems"],
  ) {
    const events = [...(initial?.nodes ?? [])];
    let pageInfo = initial?.pageInfo;
    while (pageInfo?.hasNextPage) {
      if (!pageInfo.endCursor) {
        return yield* queryError(
          "invalid-response",
          "GitHub returned incomplete workflow timeline pagination.",
        );
      }
      const raw = yield* executeGraphQl({
        cwd,
        repository,
        query: REOPENED_QUERY,
        id,
        cursor: pageInfo.endCursor,
      });
      const decoded = yield* Effect.try({
        try: () => decodeReopenedPage(raw),
        catch: (error) =>
          queryError(
            "invalid-response",
            "GitHub returned invalid workflow history.",
            String(error),
          ),
      });
      if (!decoded.data.node)
        return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
      events.push(...decoded.data.node.timelineItems.nodes);
      pageInfo = decoded.data.node.timelineItems.pageInfo;
    }
    return events;
  });

  const evidenceComment = (comment: typeof RawComment.Type): WorkflowEvidenceComment => ({
    id: comment.id,
    url: comment.url,
    body: comment.body,
    createdAt: comment.createdAt,
    author: comment.author?.login ?? null,
    authorAssociation: comment.authorAssociation,
  });

  const completeEvidenceIssue = Effect.fn("WorkflowService.completeEvidenceIssue")(function* (
    cwd: string,
    rawIssue: RawIssue,
    mode: "child" | "detail" = "detail",
  ) {
    const repository = rawIssue.repository.nameWithOwner;
    const issue = yield* completeLabels(cwd, repository, rawIssue);
    const compactClosed = mode === "child" && issue.state === "CLOSED";
    const comments = compactClosed
      ? [...(issue.comments?.nodes ?? [])]
      : mode === "child"
        ? yield* completeRecentComments(cwd, repository, issue.id, issue.comments)
        : yield* completeComments(cwd, repository, issue.id, issue.comments);
    const assignees = compactClosed
      ? [...(issue.assignees?.nodes ?? [])]
      : yield* completeAssignees(cwd, repository, issue.id, issue.assignees);
    const reopened = compactClosed
      ? [...(issue.timelineItems?.nodes ?? [])]
      : yield* completeReopened(cwd, repository, issue.id, issue.timelineItems);
    const blockers = compactClosed
      ? [...(issue.blockedBy?.nodes ?? [])]
      : yield* completeBlockerReferences(cwd, repository, issue);
    const historyComplete =
      !compactClosed ||
      (issue.comments?.pageInfo.hasPreviousPage !== true &&
        issue.timelineItems?.pageInfo.hasNextPage !== true);
    const summary = issueSummary(issue);
    return {
      raw: {
        ...issue,
        blockedBy: issue.blockedBy
          ? {
              ...issue.blockedBy,
              pageInfo: { hasNextPage: false, endCursor: null },
              nodes: blockers,
            }
          : undefined,
      },
      issue: {
        id: summary.id,
        url: summary.url,
        number: summary.number,
        title: summary.title,
        kind: summary.kind,
        state: summary.state,
        stateReason: summary.stateReason,
        labels: summary.labels,
        assignees: assignees.map((assignee) => assignee.login),
        body: issue.body ?? "",
        comments: comments.map(evidenceComment),
        reopenedAt: reopened.map((event) => event.createdAt),
        lastEditedAt: issue.lastEditedAt ?? null,
      } satisfies WorkflowEvidenceIssue,
      historyComplete,
    };
  });

  const loadEvidenceIssue = Effect.fn("WorkflowService.loadEvidenceIssue")(function* (
    cwd: string,
    reference: RawIssueReference,
  ) {
    const raw = yield* executeGraphQl({
      cwd,
      repository: reference.repository.nameWithOwner,
      query: EVIDENCE_LOOKUP_QUERY,
      id: reference.id,
    });
    const decoded = yield* Effect.try({
      try: () => decodeEvidenceLookup(raw),
      catch: (error) =>
        queryError("invalid-response", "GitHub returned invalid workflow evidence.", String(error)),
    });
    if (!decoded.data.node)
      return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
    return yield* completeEvidenceIssue(cwd, decoded.data.node);
  });

  const assessEvidenceIssue = Effect.fn("WorkflowService.assessEvidenceIssue")(function* (input: {
    cwd: string;
    issue: {
      readonly raw: RawIssue;
      readonly issue: WorkflowEvidenceIssue;
      readonly historyComplete: boolean;
    };
    approvalComments: ReadonlyArray<WorkflowEvidenceComment>;
    cache: Map<string, WorkflowReadiness>;
  }) {
    const blockers = [];
    for (const blocker of input.issue.raw.blockedBy?.nodes ?? []) {
      let readiness = input.cache.get(blocker.id);
      if (!readiness) {
        const completeBlocker = yield* completeLabels(
          input.cwd,
          blocker.repository.nameWithOwner,
          blocker,
        );
        const blockerSummary = issueSummary(completeBlocker);
        if (blockerSummary.state === "open" || blockerSummary.stateReason === "not_planned") {
          readiness = interpretWorkflowEvidence({
            issue: {
              id: blockerSummary.id,
              url: blockerSummary.url,
              number: blockerSummary.number,
              title: blockerSummary.title,
              kind: blockerSummary.kind,
              state: blockerSummary.state,
              stateReason: blockerSummary.stateReason,
              labels: blockerSummary.labels,
              assignees: [],
              body: "",
              comments: [],
              reopenedAt: [],
            },
          }).readiness;
        } else {
          const loaded = yield* loadEvidenceIssue(input.cwd, completeBlocker);
          readiness = interpretWorkflowEvidence({ issue: loaded.issue }).readiness;
        }
        input.cache.set(blocker.id, readiness);
      }
      blockers.push({
        id: blocker.id,
        number: blocker.number,
        title: blocker.title,
        url: blocker.url,
        readiness,
      });
    }
    return interpretWorkflowEvidence({
      issue: input.issue.issue,
      approvalComments: input.approvalComments,
      blockers,
      historyComplete: input.issue.historyComplete,
    });
  });

  const repositories: WorkflowService["Service"]["repositories"] = Effect.fn(
    "WorkflowService.repositories",
  )(function* (input) {
    const selectedProject = yield* project(input.projectId);
    const result = yield* process
      .run({
        command: "git",
        args: ["-C", selectedProject.workspaceRoot, "remote", "-v"],
        timeoutBehavior: "timedOutResult",
      })
      .pipe(
        Effect.mapError(() =>
          queryError("missing-git-repository", "This project is not a Git repository."),
        ),
      );
    if (result.code !== 0) {
      return yield* queryError("missing-git-repository", "This project is not a Git repository.");
    }
    return {
      projectId: input.projectId,
      projectTitle: selectedProject.title,
      repositories: workflowRepositories(result.stdout),
    };
  });

  const roots: WorkflowService["Service"]["roots"] = Effect.fn("WorkflowService.roots")(
    function* (input) {
      const selectedProject = yield* project(input.projectId);
      const roots = [];
      let cursor: string | undefined;
      do {
        const raw = yield* executeGraphQl({
          cwd: selectedProject.workspaceRoot,
          repository: input.repository,
          query: ROOTS_QUERY,
          ...(cursor ? { cursor } : {}),
        });
        const decoded = yield* Effect.try({
          try: () => decodeRootsPage(raw),
          catch: (error) =>
            queryError("invalid-response", "GitHub returned invalid workflow data.", String(error)),
        });
        const repository = decoded.data.repository;
        if (!repository)
          return yield* queryError(
            "repository-not-found",
            "The selected GitHub repository was not found.",
          );
        for (const issue of repository.issues.nodes) {
          if (issue.parent !== null && issue.parent !== undefined) continue;
          const completeIssue = yield* completeLabels(
            selectedProject.workspaceRoot,
            input.repository,
            issue,
          );
          if (
            completeIssue.subIssuesSummary.total === 0 &&
            issueKind(completeIssue.labels.nodes) === null
          ) {
            continue;
          }
          roots.push(issueSummary(completeIssue));
        }
        if (repository.issues.pageInfo.hasNextPage && !repository.issues.pageInfo.endCursor) {
          return yield* queryError(
            "invalid-response",
            "GitHub returned incomplete workflow root pagination.",
          );
        }
        cursor = repository.issues.pageInfo.hasNextPage
          ? (repository.issues.pageInfo.endCursor ?? undefined)
          : undefined;
      } while (cursor);
      return { repository: input.repository, roots };
    },
  );

  const children: WorkflowService["Service"]["children"] = Effect.fn("WorkflowService.children")(
    function* (input) {
      const selectedProject = yield* project(input.projectId);
      const children = [];
      let approvalComments: ReadonlyArray<WorkflowEvidenceComment> = [];
      let approvalCommentsLoaded = false;
      let parentBody = "";
      let cursor: string | undefined;
      do {
        const raw = yield* executeGraphQl({
          cwd: selectedProject.workspaceRoot,
          repository: input.repository,
          query: CHILDREN_QUERY,
          number: input.parentNumber,
          ...(cursor ? { cursor } : {}),
        });
        const decoded = yield* Effect.try({
          try: () => decodeChildrenPage(raw),
          catch: (error) =>
            queryError("invalid-response", "GitHub returned invalid workflow data.", String(error)),
        });
        const repository = decoded.data.repository;
        if (!repository)
          return yield* queryError(
            "repository-not-found",
            "The selected GitHub repository was not found.",
          );
        if (!repository.issue)
          return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
        parentBody = repository.issue.body ?? parentBody;
        if (!approvalCommentsLoaded && repository.issue.id && repository.issue.comments) {
          approvalComments = (yield* completeComments(
            selectedProject.workspaceRoot,
            input.repository,
            repository.issue.id,
            repository.issue.comments,
          )).map(evidenceComment);
          approvalCommentsLoaded = true;
        }
        for (const issue of repository.issue.subIssues.nodes) {
          children.push(
            yield* completeEvidenceIssue(selectedProject.workspaceRoot, issue, "child"),
          );
        }
        if (
          repository.issue.subIssues.pageInfo.hasNextPage &&
          !repository.issue.subIssues.pageInfo.endCursor
        ) {
          return yield* queryError(
            "invalid-response",
            "GitHub returned incomplete workflow child pagination.",
          );
        }
        cursor = repository.issue.subIssues.pageInfo.hasNextPage
          ? (repository.issue.subIssues.pageInfo.endCursor ?? undefined)
          : undefined;
      } while (cursor);
      const blockerCache = new Map<string, WorkflowReadiness>();
      const assessed = [];
      for (const child of children) {
        const interpretation = yield* assessEvidenceIssue({
          cwd: selectedProject.workspaceRoot,
          issue: child,
          approvalComments,
          cache: blockerCache,
        });
        assessed.push({
          ...issueSummary(child.raw, input.parentNumber),
          readiness: interpretation.readiness,
        });
      }
      return {
        parentNumber: input.parentNumber,
        children: assessed,
        frontier: workflowFrontier(
          assessed.map((child) => ({ id: child.id, readiness: child.readiness })),
          { hasRemainingFog: workflowHasRemainingFog(parentBody) },
        ),
      };
    },
  );

  const issueDetail: WorkflowService["Service"]["issueDetail"] = Effect.fn(
    "WorkflowService.issueDetail",
  )(function* (input) {
    const selectedProject = yield* project(input.projectId);
    const raw = yield* executeGraphQl({
      cwd: selectedProject.workspaceRoot,
      repository: input.repository,
      query: DETAIL_QUERY,
      number: input.number,
    });
    const decoded = yield* Effect.try({
      try: () => decodeDetail(raw),
      catch: (error) =>
        queryError("invalid-response", "GitHub returned invalid workflow detail.", String(error)),
    });
    const repository = decoded.data.repository;
    if (!repository)
      return yield* queryError(
        "repository-not-found",
        "The selected GitHub repository was not found.",
      );
    if (!repository.issue)
      return yield* queryError("issue-not-found", "The selected workflow issue was not found.");
    const completeIssue = yield* completeEvidenceIssue(
      selectedProject.workspaceRoot,
      repository.issue,
    );
    let approvalComments: ReadonlyArray<WorkflowEvidenceComment> = [];
    if (
      completeIssue.issue.kind === "ticket" &&
      /Approved slice:\s*\*\*T\d+\*\*/iu.test(completeIssue.issue.body) &&
      completeIssue.raw.parent
    ) {
      const approvalIssue = yield* loadEvidenceIssue(
        selectedProject.workspaceRoot,
        completeIssue.raw.parent,
      );
      approvalComments = approvalIssue.issue.comments;
    }
    const interpretation = yield* assessEvidenceIssue({
      cwd: selectedProject.workspaceRoot,
      issue: completeIssue,
      approvalComments,
      cache: new Map(),
    });
    const summary = issueSummary(completeIssue.raw);
    const blockedBy = yield* completeBlockedBy(
      selectedProject.workspaceRoot,
      input.repository,
      completeIssue.raw,
    );
    return {
      ...summary,
      readiness: interpretation.readiness,
      body: completeIssue.issue.body,
      blockedBy,
      evidence: interpretation.evidence,
    };
  });

  const search: WorkflowService["Service"]["search"] = Effect.fn("WorkflowService.search")(
    function* (input) {
      const selectedProject = yield* project(input.projectId);
      const matches: Array<WorkflowSearchResult["matches"][number]> = [];
      const lookupCache = new Map<string, RawIssue | null>();
      const summaryCache = new Map<string, ReturnType<typeof issueSummary>>();

      const lookupIssue = Effect.fn("WorkflowService.search.lookupIssue")(function* (
        reference: RawIssueReference,
      ) {
        const key = reference.id;
        if (lookupCache.has(key)) return lookupCache.get(key) ?? null;
        const parentRaw = yield* executeGraphQl({
          cwd: selectedProject.workspaceRoot,
          repository: reference.repository.nameWithOwner,
          query: ISSUE_LOOKUP_QUERY,
          number: reference.number,
        });
        const parentDecoded = yield* Effect.try({
          try: () => decodeIssueLookup(parentRaw),
          catch: (error) =>
            queryError(
              "invalid-response",
              "GitHub returned invalid workflow ancestry.",
              String(error),
            ),
        });
        const issue = parentDecoded.data.repository?.issue ?? null;
        lookupCache.set(key, issue);
        return issue;
      });

      const ancestryFor = Effect.fn("WorkflowService.search.ancestryFor")(function* (
        firstParent: RawIssueReference | null | undefined,
      ) {
        const ancestry = [];
        let parent = firstParent;
        for (let depth = 0; parent && depth < 8; depth += 1) {
          let summary = summaryCache.get(parent.id);
          if (!summary) {
            const completeParent = yield* completeLabels(
              selectedProject.workspaceRoot,
              parent.repository.nameWithOwner,
              parent,
            );
            summary = issueSummary(completeParent);
            summaryCache.set(parent.id, summary);
          }
          ancestry.unshift(summary);
          const parentIssue = yield* lookupIssue(parent);
          parent = parentIssue?.parent ?? null;
        }
        return { ancestry, ancestryComplete: parent === null };
      });

      let cursor: string | undefined;
      let hasMore = false;
      do {
        const raw = yield* executeGraphQl({
          cwd: selectedProject.workspaceRoot,
          repository: input.repository,
          query: SEARCH_QUERY,
          searchQuery: `repo:${input.repository} is:issue ${input.query}`,
          ...(cursor ? { cursor } : {}),
        });
        const decoded = yield* Effect.try({
          try: () => decodeSearchPage(raw),
          catch: (error) =>
            queryError(
              "invalid-response",
              "GitHub returned invalid workflow search data.",
              String(error),
            ),
        });
        let consumed = 0;
        for (const rawIssue of decoded.data.search.nodes) {
          const complete = yield* completeLabels(
            selectedProject.workspaceRoot,
            rawIssue.repository.nameWithOwner,
            rawIssue,
          );
          const ancestryResult = yield* ancestryFor(complete.parent);
          matches.push({
            issue: issueSummary(complete),
            ...ancestryResult,
          });
          consumed += 1;
          if (matches.length === 50) break;
        }
        hasMore =
          consumed < decoded.data.search.nodes.length || decoded.data.search.pageInfo.hasNextPage;
        if (hasMore && !decoded.data.search.pageInfo.endCursor) {
          return yield* queryError(
            "invalid-response",
            "GitHub returned incomplete workflow search pagination.",
          );
        }
        cursor = hasMore ? (decoded.data.search.pageInfo.endCursor ?? undefined) : undefined;
      } while (cursor && matches.length < 50);
      return { matches, hasMore };
    },
  );

  const locate: WorkflowService["Service"]["locate"] = Effect.fn("WorkflowService.locate")(
    function* (input) {
      const selectedProject = yield* project(input.projectId);
      const raw = yield* executeGraphQl({
        cwd: selectedProject.workspaceRoot,
        repository: input.repository,
        query: ISSUE_LOOKUP_QUERY,
        number: input.number,
      });
      const decoded = yield* Effect.try({
        try: () => decodeIssueLookup(raw),
        catch: (error) =>
          queryError(
            "invalid-response",
            "GitHub returned invalid workflow ancestry.",
            String(error),
          ),
      });
      const issue = decoded.data.repository?.issue;
      if (!issue || issue.id !== input.id) {
        return yield* queryError(
          "issue-not-found",
          "The selected workflow issue is no longer available at this location.",
        );
      }
      const complete = yield* completeLabels(
        selectedProject.workspaceRoot,
        issue.repository.nameWithOwner,
        issue,
      );
      const ancestry = [];
      let parent = complete.parent;
      for (let depth = 0; parent && depth < 8; depth += 1) {
        const completeParent = yield* completeLabels(
          selectedProject.workspaceRoot,
          parent.repository.nameWithOwner,
          parent,
        );
        ancestry.unshift(issueSummary(completeParent));
        const parentRaw = yield* executeGraphQl({
          cwd: selectedProject.workspaceRoot,
          repository: parent.repository.nameWithOwner,
          query: ISSUE_LOOKUP_QUERY,
          number: parent.number,
        });
        const parentDecoded = yield* Effect.try({
          try: () => decodeIssueLookup(parentRaw),
          catch: (error) =>
            queryError(
              "invalid-response",
              "GitHub returned invalid workflow ancestry.",
              String(error),
            ),
        });
        parent = parentDecoded.data.repository?.issue?.parent ?? null;
      }
      return {
        issue: issueSummary(complete),
        ancestry,
        ancestryComplete: parent === null,
      };
    },
  );

  return WorkflowService.of({ repositories, roots, children, issueDetail, search, locate });
});

export const layer = Layer.effect(WorkflowService, make).pipe(Layer.provide(GitHubCli.layer));
