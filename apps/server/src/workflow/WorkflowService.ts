import {
  type ProjectId,
  type WorkflowChildrenInput,
  type WorkflowChildrenResult,
  type WorkflowIssueDetail,
  type WorkflowIssueDetailInput,
  type WorkflowIssueKind,
  type WorkflowIssueStateReason,
  WorkflowQueryError,
  type WorkflowRepositoriesInput,
  type WorkflowRepositoriesResult,
  type WorkflowRepository,
  WorkflowRepositoryNameWithOwner,
  type WorkflowRootsInput,
  type WorkflowRootsResult,
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
const RawPageInfo = Schema.Struct({
  hasNextPage: Schema.Boolean,
  endCursor: Schema.NullOr(Schema.String),
});
const RawIssue = Schema.Struct({
  id: Schema.String,
  number: Schema.Number,
  title: Schema.String,
  url: Schema.String,
  state: Schema.String,
  stateReason: Schema.NullOr(Schema.String),
  updatedAt: Schema.String,
  body: Schema.optional(Schema.String),
  labels: Schema.Struct({ pageInfo: RawPageInfo, nodes: Schema.Array(RawLabel) }),
  parent: Schema.optional(Schema.NullOr(Schema.Struct({ number: Schema.Number }))),
  subIssuesSummary: Schema.Struct({ total: Schema.Number }),
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

const decodeRootsPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawRootsPage));
const decodeChildrenPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawChildrenPage));
const decodeDetail = Schema.decodeUnknownSync(Schema.fromJsonString(RawDetail));
const decodeLabelsPage = Schema.decodeUnknownSync(Schema.fromJsonString(RawLabelsPage));
const isWorkflowRepositoryNameWithOwner = Schema.is(WorkflowRepositoryNameWithOwner);

const ISSUE_FIELDS = `id number title url state stateReason updatedAt labels(first:100){pageInfo{hasNextPage endCursor}nodes{name}} parent{number} subIssuesSummary{total}`;
const ROOTS_QUERY = `query WorkflowRoots($owner:String!,$name:String!,$after:String){repository(owner:$owner,name:$name){issues(first:100,after:$after,orderBy:{field:UPDATED_AT,direction:DESC}){pageInfo{hasNextPage endCursor}nodes{${ISSUE_FIELDS}}}}}`;
const CHILDREN_QUERY = `query WorkflowChildren($owner:String!,$name:String!,$number:Int!,$after:String){repository(owner:$owner,name:$name){issue(number:$number){subIssues(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{${ISSUE_FIELDS}}}}}}`;
const DETAIL_QUERY = `query WorkflowDetail($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){issue(number:$number){${ISSUE_FIELDS} body}}}`;
const LABELS_QUERY = `query WorkflowLabels($id:ID!,$after:String){node(id:$id){... on Issue{labels(first:100,after:$after){pageInfo{hasNextPage endCursor}nodes{name}}}}}`;

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

function issueSummary(issue: RawIssue) {
  const kind =
    issueKind(issue.labels.nodes) ?? (issue.subIssuesSummary.total > 0 ? "container" : "task");
  return {
    number: issue.number,
    title: issue.title.trim(),
    url: issue.url.trim(),
    kind,
    state: issue.state === "OPEN" ? ("open" as const) : ("closed" as const),
    stateReason: stateReason(issue.stateReason),
    updatedAt: issue.updatedAt,
    childCount: issue.subIssuesSummary.total,
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
    ];
    const result = yield* github
      .execute({ cwd: input.cwd, args, maxOutputBytes: 5_000_000 })
      .pipe(Effect.mapError(mapGitHubError));
    return result.stdout;
  });

  const completeLabels = Effect.fn("WorkflowService.completeLabels")(function* (
    cwd: string,
    repository: string,
    issue: RawIssue,
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
        for (const issue of repository.issue.subIssues.nodes) {
          const completeIssue = yield* completeLabels(
            selectedProject.workspaceRoot,
            input.repository,
            issue,
          );
          children.push(issueSummary(completeIssue));
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
      return { parentNumber: input.parentNumber, children };
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
    const completeIssue = yield* completeLabels(
      selectedProject.workspaceRoot,
      input.repository,
      repository.issue,
    );
    const summary = issueSummary(completeIssue);
    return {
      ...summary,
      body: repository.issue.body ?? "",
      labels: completeIssue.labels.nodes.map((label) => label.name),
    };
  });

  return WorkflowService.of({ repositories, roots, children, issueDetail });
});

export const layer = Layer.effect(WorkflowService, make).pipe(Layer.provide(GitHubCli.layer));
