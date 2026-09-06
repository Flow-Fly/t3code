import {
  WorkflowAdoptionError,
  WorkflowQueryError,
  type ProjectId,
  type WorkflowAdoptionApplyInput,
  type WorkflowAdoptionHistoryInput,
  type WorkflowAdoptionHistoryResult,
  WorkflowAdoptionItem,
  WorkflowAdoptionPreview,
  type WorkflowAdoptionPreviewInput,
  type WorkflowAdoptionRecord,
  type WorkflowAdoptionRecoveryInput,
  type WorkflowAdoptionRecoveryResult,
  type WorkflowAdoptionUndoInput,
  type WorkflowIssueKind,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";

const KIND_BY_LABEL = {
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

const RawLabel = Schema.Union([Schema.String, Schema.Struct({ name: Schema.String })]);
const RawIssue = Schema.Struct({
  id: Schema.Number,
  node_id: Schema.String,
  number: Schema.Number,
  title: Schema.String,
  html_url: Schema.String,
  repository_url: Schema.String,
  body: Schema.NullOr(Schema.String),
  updated_at: Schema.String,
  labels: Schema.Array(RawLabel),
  parent_issue_url: Schema.optionalKey(Schema.NullOr(Schema.String)),
  sub_issues_summary: Schema.optionalKey(
    Schema.Struct({ total: Schema.Number, completed: Schema.optionalKey(Schema.Number) }),
  ),
});
type RawIssue = typeof RawIssue.Type;

const InternalOperation = Schema.Struct({
  issueId: Schema.String,
  issueNumber: Schema.Number,
  issueDatabaseId: Schema.Number,
  repository: Schema.String,
  kind: Schema.Literals(["add-label", "remove-label", "change-parent"]),
  status: Schema.Literals([
    "pending",
    "applied",
    "already-current",
    "uncertain",
    "failed",
    "undo-skipped",
    "undone",
  ]),
  description: Schema.String,
  owned: Schema.Boolean,
  beforeLabelPresent: Schema.optionalKey(Schema.Boolean),
  label: Schema.optionalKey(Schema.String),
  beforeParentNumber: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  afterParentNumber: Schema.optionalKey(Schema.NullOr(Schema.Number)),
  failure: Schema.optionalKey(Schema.String),
});
type InternalOperation = typeof InternalOperation.Type;

const InternalRecord = Schema.Struct({
  adoptionId: Schema.String,
  previewId: Schema.String,
  repository: Schema.String,
  rootNumber: Schema.Number,
  projectId: Schema.String,
  createdAt: Schema.String,
  status: Schema.Literals(["applying", "applied", "partial", "undone", "undo-partial"]),
  planJson: Schema.String,
  operations: Schema.Array(InternalOperation),
});
type InternalRecord = typeof InternalRecord.Type;

const PreviewRow = Schema.Struct({ previewJson: Schema.String });
const RecordRow = Schema.Struct({ recordJson: Schema.String });
const decodeRawIssue = Schema.decodeEffect(Schema.fromJsonString(RawIssue));
const decodeRawIssues = Schema.decodeEffect(Schema.fromJsonString(Schema.Array(RawIssue)));
const decodePreview = Schema.decodeEffect(Schema.fromJsonString(WorkflowAdoptionPreview));
const decodeInternalRecord = Schema.decodeEffect(Schema.fromJsonString(InternalRecord));
const encodePreview = Schema.encodeEffect(Schema.fromJsonString(WorkflowAdoptionPreview));
const encodeInternalRecord = Schema.encodeEffect(Schema.fromJsonString(InternalRecord));
const encodeSelection = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(WorkflowAdoptionItem)),
);
const decodeSelection = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Array(WorkflowAdoptionItem)),
);
const encodeLabelPayload = Schema.encodeSync(
  Schema.fromJsonString(Schema.Struct({ labels: Schema.Array(Schema.String) })),
);

function adoptionError(
  failure: WorkflowAdoptionError["failure"],
  message: string,
  detail?: string,
) {
  return new WorkflowAdoptionError({ failure, message, ...(detail ? { detail } : {}) });
}

function queryError(message: string, detail?: string) {
  return new WorkflowQueryError({
    failure: "request-failed",
    message,
    ...(detail ? { detail } : {}),
  });
}

function mapGitHubError(error: GitHubCli.GitHubCliError) {
  if (error._tag === "GitHubCliUnavailableError")
    return new WorkflowQueryError({
      failure: "github-unavailable",
      message: "GitHub CLI is not installed in this environment.",
    });
  if (error._tag === "GitHubCliAuthenticationError")
    return new WorkflowQueryError({
      failure: "github-unauthenticated",
      message: "GitHub CLI is not authenticated in this environment.",
    });
  if (error._tag === "GitHubCliRateLimitError")
    return new WorkflowQueryError({
      failure: "github-rate-limited",
      message: "GitHub API rate limit exceeded.",
    });
  return queryError("GitHub could not update this workflow.", error.message);
}

function repositoryParts(repository: string) {
  const [owner, name, extra] = repository.split("/");
  return owner && name && !extra ? { owner, name } : null;
}

function labelsOf(issue: RawIssue) {
  return issue.labels.map((label) => (typeof label === "string" ? label : label.name));
}

function kindOf(labels: ReadonlyArray<string>): WorkflowIssueKind | null {
  for (const label of labels) {
    const kind = KIND_BY_LABEL[label as keyof typeof KIND_BY_LABEL];
    if (kind) return kind;
  }
  return null;
}

function proposedKind(issue: RawIssue, rootNumber: number): WorkflowIssueKind {
  const current = kindOf(labelsOf(issue));
  if (current) return current;
  const body = issue.body ?? "";
  if (/^## Source map/im.test(body) || /^## Specification/im.test(body)) return "capability";
  if (/^## What to build/im.test(body)) return "ticket";
  if (issue.sub_issues_summary?.total)
    return rootNumber === issue.number ? "container" : "container";
  return "task";
}

function issueRepository(issue: RawIssue) {
  const match = /\/repos\/([^/]+\/[^/]+)$/u.exec(issue.repository_url);
  return match?.[1] ?? "";
}

function parentNumber(issue: RawIssue): number | null {
  const match = /\/issues\/(\d+)(?:$|[?#])/u.exec(issue.parent_issue_url ?? "");
  return match?.[1] ? Number(match[1]) : null;
}

function relationships(issue: RawIssue) {
  const found = new Map<
    string,
    { issueNumber: number; relationship: "source" | "specification"; source: string }
  >();
  let section = "";
  for (const line of (issue.body ?? "").split("\n")) {
    const heading = /^#{1,6}\s+(.+)$/u.exec(line);
    if (heading?.[1]) section = heading[1].trim().toLowerCase();
    const relationship = /specification|approved spec/u.test(section)
      ? "specification"
      : /source/u.test(section)
        ? "source"
        : null;
    if (!relationship) continue;
    for (const match of line.matchAll(/https:\/\/github\.com\/[^/\s]+\/[^/\s]+\/issues\/(\d+)/gu)) {
      const number = match[1] ? Number(match[1]) : 0;
      if (number > 0)
        found.set(`${relationship}:${number}`, {
          issueNumber: number,
          relationship,
          source: match[0],
        });
    }
  }
  return [...found.values()];
}

function canonicalLabel(kind: WorkflowIssueKind) {
  return `workflow:${kind}`;
}

function classificationChanges(labels: ReadonlyArray<string>, kind: WorkflowIssueKind) {
  const conflicting = labels.filter((label) => {
    const current = KIND_BY_LABEL[label as keyof typeof KIND_BY_LABEL];
    return current !== undefined && current !== kind;
  });
  const hasMatching = labels.some(
    (label) => KIND_BY_LABEL[label as keyof typeof KIND_BY_LABEL] === kind,
  );
  return {
    conflicting,
    added: hasMatching ? null : canonicalLabel(kind),
  };
}

function itemFromIssue(issue: RawIssue, rootNumber: number): WorkflowAdoptionItem {
  const labels = labelsOf(issue);
  const currentKind = kindOf(labels);
  const nextKind = proposedKind(issue, rootNumber);
  const currentParentNumber = parentNumber(issue);
  const classification = classificationChanges(labels, nextKind);
  const changes = [
    ...(classification.added ? [`Add ${classification.added} label`] : []),
    ...classification.conflicting.map((label) => `Remove conflicting ${label} label`),
  ];
  return {
    id: issue.node_id,
    repository: issueRepository(issue) as WorkflowAdoptionItem["repository"],
    number: issue.number,
    title: issue.title.trim(),
    url: issue.html_url,
    updatedAt: issue.updated_at,
    sourceBody: issue.body ?? "",
    currentKind,
    proposedKind: nextKind,
    currentParentNumber,
    proposedParentNumber: currentParentNumber,
    labels,
    relationships: relationships(issue),
    changes,
    included: true,
    parentChangeConfirmed: true,
  };
}

function publicRecord(record: InternalRecord): WorkflowAdoptionRecord {
  return {
    adoptionId: record.adoptionId,
    previewId: record.previewId,
    repository: record.repository as WorkflowAdoptionRecord["repository"],
    rootNumber: record.rootNumber,
    createdAt: record.createdAt,
    status: record.status === "applying" ? "partial" : record.status,
    operations: record.operations.map((operation) => ({
      issueId: operation.issueId,
      repository:
        operation.repository as WorkflowAdoptionRecord["operations"][number]["repository"],
      issueNumber: operation.issueNumber,
      kind: operation.kind,
      status: operation.status === "pending" ? "failed" : operation.status,
      description: operation.description,
      owned: operation.owned,
      ...(operation.failure ? { detail: operation.failure } : {}),
    })),
  };
}

function selectionJson(input: WorkflowAdoptionApplyInput) {
  return encodeSelection(input.items);
}

function sameStrings(left: ReadonlyArray<string>, right: ReadonlyArray<string>) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function clearFailure(operation: InternalOperation): InternalOperation {
  const { failure: _, ...remaining } = operation;
  return remaining;
}

function operationIdentity(operation: InternalOperation) {
  return `${operation.repository}:${operation.issueId}`;
}

function classificationLabels(labels: ReadonlyArray<string>) {
  return labels
    .filter((label) => KIND_BY_LABEL[label as keyof typeof KIND_BY_LABEL] !== undefined)
    .toSorted();
}

function updateClassification(
  labels: ReadonlyArray<string>,
  operation: InternalOperation,
): ReadonlyArray<string> {
  if (!operation.label || operation.kind === "change-parent") return labels;
  if (operation.kind === "add-label") return [...new Set([...labels, operation.label])].toSorted();
  return labels.filter((label) => label !== operation.label);
}

function expectedClassificationStates(
  item: WorkflowAdoptionItem,
  operations: ReadonlyArray<InternalOperation>,
) {
  let states: ReadonlyArray<ReadonlyArray<string>> = [classificationLabels(item.labels)];
  for (const operation of operations) {
    if (operation.kind === "change-parent") continue;
    if (
      operation.status === "undone" ||
      operation.status === "pending" ||
      operation.status === "failed"
    )
      continue;
    if (operation.status === "uncertain") {
      const possibleStates = Array.from(states);
      for (const labels of states) possibleStates.push(updateClassification(labels, operation));
      states = possibleStates;
    } else {
      states = states.map((labels) => updateClassification(labels, operation));
    }
  }
  return new Set(states.map((labels) => labels.join("\n")));
}

function expectedParentNumbers(
  item: WorkflowAdoptionItem,
  operations: ReadonlyArray<InternalOperation>,
) {
  const expected = new Set<number | null>([item.currentParentNumber]);
  const operation = operations.find((candidate) => candidate.kind === "change-parent");
  if (!operation) return expected;
  if (operation.status === "uncertain") expected.add(operation.afterParentNumber ?? null);
  else if (
    operation.status === "applied" ||
    operation.status === "already-current" ||
    operation.status === "undo-skipped"
  )
    return new Set([operation.afterParentNumber ?? null]);
  return expected;
}

function reviewedStateFailure(
  item: WorkflowAdoptionItem,
  operations: ReadonlyArray<InternalOperation>,
  live: RawIssue,
) {
  if (
    live.node_id !== item.id ||
    issueRepository(live) !== item.repository ||
    live.number !== item.number ||
    live.title.trim() !== item.title ||
    (live.body ?? "") !== item.sourceBody
  )
    return "The reviewed source changed before this write.";
  if (!expectedParentNumbers(item, operations).has(parentNumber(live)))
    return "The reviewed branch changed before this write.";
  const liveClassification = classificationLabels(labelsOf(live)).join("\n");
  if (!expectedClassificationStates(item, operations).has(liveClassification))
    return "The reviewed classification changed before this write.";
  return null;
}

export class WorkflowAdoptionService extends Context.Service<
  WorkflowAdoptionService,
  {
    readonly preview: (
      input: WorkflowAdoptionPreviewInput,
    ) => Effect.Effect<WorkflowAdoptionPreview, WorkflowQueryError | WorkflowAdoptionError>;
    readonly apply: (
      input: WorkflowAdoptionApplyInput,
    ) => Effect.Effect<WorkflowAdoptionRecord, WorkflowQueryError | WorkflowAdoptionError>;
    readonly history: (
      input: WorkflowAdoptionHistoryInput,
    ) => Effect.Effect<WorkflowAdoptionHistoryResult, WorkflowAdoptionError>;
    readonly recover: (
      input: WorkflowAdoptionRecoveryInput,
    ) => Effect.Effect<WorkflowAdoptionRecoveryResult, WorkflowAdoptionError>;
    readonly undo: (
      input: WorkflowAdoptionUndoInput,
    ) => Effect.Effect<WorkflowAdoptionRecord, WorkflowQueryError | WorkflowAdoptionError>;
  }
>()("t3/workflow/WorkflowAdoptionService") {}

export const make = Effect.gen(function* () {
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const github = yield* GitHubCli.GitHubCli;
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const mutationLock = yield* Semaphore.make(1);

  const selectedProject = Effect.fn("WorkflowAdoptionService.selectedProject")(function* (
    projectId: ProjectId,
  ) {
    const result = yield* projection
      .getProjectShellById(projectId)
      .pipe(
        Effect.mapError((error) => queryError("The project could not be read.", error.message)),
      );
    return yield* Option.match(result, {
      onNone: () =>
        Effect.fail(
          new WorkflowQueryError({
            failure: "project-not-found",
            message: "This project is no longer available.",
          }),
        ),
      onSome: Effect.succeed,
    });
  });

  const execute = Effect.fn("WorkflowAdoptionService.execute")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
    stdin?: string,
  ) {
    return yield* github
      .execute({ cwd, args, ...(stdin === undefined ? {} : { stdin }), maxOutputBytes: 5_000_000 })
      .pipe(Effect.mapError(mapGitHubError));
  });

  const readIssue = Effect.fn("WorkflowAdoptionService.readIssue")(function* (
    cwd: string,
    repository: string,
    number: number,
  ) {
    const parts = repositoryParts(repository);
    if (!parts)
      return yield* new WorkflowQueryError({
        failure: "repository-not-found",
        message: "Choose a valid GitHub repository.",
      });
    const result = yield* execute(cwd, [
      "api",
      `repos/${parts.owner}/${parts.name}/issues/${number}`,
    ]);
    return yield* decodeRawIssue(result.stdout).pipe(
      Effect.mapError((error) =>
        queryError("GitHub returned invalid workflow adoption data.", String(error)),
      ),
    );
  });

  const readChildren = Effect.fn("WorkflowAdoptionService.readChildren")(function* (
    cwd: string,
    repository: string,
    number: number,
  ) {
    const parts = repositoryParts(repository);
    if (!parts) return [];
    const children = new Array<RawIssue>();
    for (let page = 1; ; page += 1) {
      const result = yield* execute(cwd, [
        "api",
        `repos/${parts.owner}/${parts.name}/issues/${number}/sub_issues?per_page=100&page=${page}`,
      ]);
      const pageItems = yield* decodeRawIssues(result.stdout).pipe(
        Effect.mapError((error) =>
          queryError("GitHub returned invalid workflow children.", String(error)),
        ),
      );
      children.push(...pageItems);
      if (pageItems.length < 100) break;
    }
    return children;
  });

  const savePreview = Effect.fn("WorkflowAdoptionService.savePreview")(function* (
    preview: WorkflowAdoptionPreview,
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    const previewJson = yield* encodePreview(preview).pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption preview could not be encoded.",
          String(error),
        ),
      ),
    );
    yield* sql`
      INSERT INTO workflow_adoption_previews (preview_id, repository, root_number, preview_json, created_at)
      VALUES (${preview.previewId}, ${preview.repository}, ${preview.rootNumber}, ${previewJson}, ${now})
    `.pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption preview could not be saved.",
          String(error),
        ),
      ),
    );
  });

  const loadPreview = Effect.fn("WorkflowAdoptionService.loadPreview")(function* (
    previewId: string,
  ) {
    const rows = yield* sql<typeof PreviewRow.Type>`
      SELECT preview_json AS "previewJson" FROM workflow_adoption_previews WHERE preview_id = ${previewId}
    `.pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption preview could not be read.",
          String(error),
        ),
      ),
    );
    const row = rows[0];
    if (!row)
      return yield* adoptionError(
        "preview-not-found",
        "This adoption preview is no longer available. Preview the branch again.",
      );
    return yield* decodePreview(row.previewJson).pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The saved adoption preview is invalid.",
          String(error),
        ),
      ),
    );
  });

  const saveRecord = Effect.fn("WorkflowAdoptionService.saveRecord")(function* (
    record: InternalRecord,
  ) {
    const recordJson = yield* encodeInternalRecord(record).pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption history could not be encoded.",
          String(error),
        ),
      ),
    );
    yield* sql`
      INSERT INTO workflow_adoptions (adoption_id, preview_id, project_id, repository, root_number, record_json, created_at)
      VALUES (${record.adoptionId}, ${record.previewId}, ${record.projectId}, ${record.repository}, ${record.rootNumber}, ${recordJson}, ${record.createdAt})
      ON CONFLICT (adoption_id) DO UPDATE SET record_json = excluded.record_json
    `.pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption history could not be saved.",
          String(error),
        ),
      ),
    );
  });

  const decodeRecord = Effect.fn("WorkflowAdoptionService.decodeRecord")(function* (json: string) {
    return yield* decodeInternalRecord(json).pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The saved adoption history is invalid.",
          String(error),
        ),
      ),
    );
  });

  const loadRecordByPreview = Effect.fn("WorkflowAdoptionService.loadRecordByPreview")(function* (
    previewId: string,
  ) {
    const rows = yield* sql<typeof RecordRow.Type>`
      SELECT record_json AS "recordJson" FROM workflow_adoptions WHERE preview_id = ${previewId}
    `.pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption history could not be read.",
          String(error),
        ),
      ),
    );
    return rows[0]
      ? Option.some(yield* decodeRecord(rows[0].recordJson))
      : Option.none<InternalRecord>();
  });

  const loadRecord = Effect.fn("WorkflowAdoptionService.loadRecord")(function* (
    adoptionId: string,
  ) {
    const rows = yield* sql<typeof RecordRow.Type>`
      SELECT record_json AS "recordJson" FROM workflow_adoptions WHERE adoption_id = ${adoptionId}
    `.pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption history could not be read.",
          String(error),
        ),
      ),
    );
    if (!rows[0])
      return yield* adoptionError("adoption-not-found", "This adoption record was not found.");
    return yield* decodeRecord(rows[0].recordJson);
  });

  const addLabel = (cwd: string, repository: string, operation: InternalOperation) =>
    execute(
      cwd,
      [
        "api",
        "--method",
        "POST",
        `repos/${repository}/issues/${operation.issueNumber}/labels`,
        "--input",
        "-",
      ],
      encodeLabelPayload({ labels: operation.label ? [operation.label] : [] }),
    );

  const removeLabel = (cwd: string, repository: string, operation: InternalOperation) =>
    execute(cwd, [
      "api",
      "--method",
      "DELETE",
      `repos/${repository}/issues/${operation.issueNumber}/labels/${encodeURIComponent(operation.label ?? "")}`,
    ]);

  const changeParent = Effect.fn("WorkflowAdoptionService.changeParent")(function* (
    cwd: string,
    repository: string,
    operation: InternalOperation,
    fromParentNumber: number | null,
    toParentNumber: number | null,
  ) {
    if (toParentNumber === null) {
      if (fromParentNumber === null) return;
      yield* execute(cwd, [
        "api",
        "--method",
        "DELETE",
        `repos/${repository}/issues/${fromParentNumber}/sub_issue`,
        "-F",
        `sub_issue_id=${operation.issueDatabaseId}`,
      ]);
      return;
    }
    yield* execute(cwd, [
      "api",
      "--method",
      "POST",
      `repos/${repository}/issues/${toParentNumber}/sub_issues`,
      "-F",
      `sub_issue_id=${operation.issueDatabaseId}`,
      "-F",
      "replace_parent=true",
    ]);
  });

  const preview: WorkflowAdoptionService["Service"]["preview"] = Effect.fn(
    "WorkflowAdoptionService.preview",
  )(function* (input) {
    const project = yield* selectedProject(input.projectId);
    const root = yield* readIssue(project.workspaceRoot, input.repository, input.rootNumber);
    const queue = [root];
    const seen = new Set<string>();
    const items = new Array<WorkflowAdoptionItem>();
    while (queue.length > 0) {
      const issue = queue.shift();
      if (!issue) continue;
      const repository = issueRepository(issue);
      const identity = `${repository}#${issue.number}`;
      if (seen.has(identity)) continue;
      seen.add(identity);
      items.push(itemFromIssue(issue, input.rootNumber));
      queue.push(...(yield* readChildren(project.workspaceRoot, repository, issue.number)));
    }
    const result: WorkflowAdoptionPreview = {
      previewId: yield* crypto.randomUUIDv4.pipe(Effect.orDie),
      repository: input.repository,
      rootNumber: input.rootNumber,
      items,
    };
    yield* savePreview(result);
    return result;
  });

  const validateSelection = Effect.fn("WorkflowAdoptionService.validateSelection")(function* (
    preview: WorkflowAdoptionPreview,
    input: WorkflowAdoptionApplyInput,
  ) {
    if (preview.repository !== input.repository || preview.rootNumber !== input.rootNumber)
      return yield* adoptionError(
        "invalid-selection",
        "The reviewed selection does not match this preview.",
      );
    const originals = new Map(
      preview.items.map((item) => [`${item.repository}#${item.number}`, item]),
    );
    if (input.items.length !== preview.items.length)
      return yield* adoptionError(
        "invalid-selection",
        "The reviewed selection changed the previewed branch.",
      );
    for (const item of input.items) {
      const original = originals.get(`${item.repository}#${item.number}`);
      if (
        !original ||
        item.id !== original.id ||
        item.updatedAt !== original.updatedAt ||
        item.sourceBody !== original.sourceBody ||
        item.title !== original.title ||
        !sameStrings(item.labels, original.labels) ||
        item.currentParentNumber !== original.currentParentNumber
      )
        return yield* adoptionError(
          "invalid-selection",
          `Issue #${item.number} no longer matches the previewed selection.`,
        );
      if (
        item.included &&
        item.repository !== input.repository &&
        item.proposedParentNumber !== item.currentParentNumber
      )
        return yield* adoptionError(
          "invalid-selection",
          `Cross-repository parent correction for ${item.repository}#${item.number} needs a repository-qualified parent and is held for review.`,
        );
      if (
        item.included &&
        item.proposedParentNumber !== item.currentParentNumber &&
        !item.parentChangeConfirmed
      )
        return yield* adoptionError(
          "parent-confirmation-required",
          `Confirm the native parent change for issue #${item.number}.`,
        );
    }
  });

  const applyUnlocked: WorkflowAdoptionService["Service"]["apply"] = Effect.fn(
    "WorkflowAdoptionService.apply",
  )(function* (input) {
    const preview = yield* loadPreview(input.previewId);
    yield* validateSelection(preview, input);
    const project = yield* selectedProject(input.projectId);
    const existing = yield* loadRecordByPreview(input.previewId);
    let record: InternalRecord;
    if (Option.isSome(existing)) {
      if (existing.value.planJson !== selectionJson(input))
        return yield* adoptionError("invalid-selection", "Retry with the same reviewed selection.");
      record = existing.value;
    } else {
      const liveIssues = new Map<string, RawIssue>();
      for (const item of input.items.filter((item) => item.included)) {
        const live = yield* readIssue(project.workspaceRoot, item.repository, item.number);
        liveIssues.set(`${item.repository}#${item.number}`, live);
        if (
          live.node_id !== item.id ||
          live.updated_at !== item.updatedAt ||
          (live.body ?? "") !== item.sourceBody ||
          parentNumber(live) !== item.currentParentNumber ||
          !sameStrings(labelsOf(live), item.labels)
        )
          return yield* adoptionError(
            "changed-source",
            `Issue #${item.number} changed after preview. Review the branch again.`,
          );
      }
      const operations = new Array<InternalOperation>();
      for (const item of input.items.filter((item) => item.included)) {
        const live = liveIssues.get(`${item.repository}#${item.number}`)!;
        const classification = classificationChanges(labelsOf(live), item.proposedKind);
        if (classification.added)
          operations.push({
            issueId: item.id,
            issueNumber: item.number,
            issueDatabaseId: live.id,
            repository: item.repository,
            kind: "add-label",
            status: "pending",
            description: `Added ${classification.added}`,
            owned: false,
            beforeLabelPresent: false,
            label: classification.added,
          });
        for (const label of classification.conflicting)
          operations.push({
            issueId: item.id,
            issueNumber: item.number,
            issueDatabaseId: live.id,
            repository: item.repository,
            kind: "remove-label",
            status: "pending",
            description: `Removed conflicting ${label}`,
            owned: false,
            beforeLabelPresent: true,
            label,
          });
        if (item.proposedParentNumber !== item.currentParentNumber)
          operations.push({
            issueId: item.id,
            issueNumber: item.number,
            issueDatabaseId: live.id,
            repository: item.repository,
            kind: "change-parent",
            status: "pending",
            description: `Changed native parent from ${item.currentParentNumber === null ? "none" : `#${item.currentParentNumber}`} to ${item.proposedParentNumber === null ? "none" : `#${item.proposedParentNumber}`}`,
            owned: false,
            beforeParentNumber: item.currentParentNumber,
            afterParentNumber: item.proposedParentNumber,
          });
      }
      record = {
        adoptionId: yield* crypto.randomUUIDv4.pipe(Effect.orDie),
        previewId: input.previewId,
        repository: input.repository,
        rootNumber: input.rootNumber,
        projectId: input.projectId,
        createdAt: DateTime.formatIso(yield* DateTime.now),
        status: "applying",
        planJson: selectionJson(input),
        operations,
      };
      yield* saveRecord(record);
    }

    const reviewedItems = yield* decodeSelection(record.planJson).pipe(
      Effect.mapError((error) =>
        adoptionError("persistence-failed", "The saved adoption plan is invalid.", String(error)),
      ),
    );
    const reviewedItemsByIdentity = new Map(
      reviewedItems.map((item) => [`${item.repository}:${item.id}`, item]),
    );
    let planFailure: string | null = null;
    if (Option.isSome(existing)) {
      for (const reviewedItem of reviewedItems.filter((item) => item.included)) {
        const reviewedIdentity = `${reviewedItem.repository}:${reviewedItem.id}`;
        const live = yield* readIssue(
          project.workspaceRoot,
          reviewedItem.repository,
          reviewedItem.number,
        );
        const itemOperations = record.operations.filter(
          (candidate) => operationIdentity(candidate) === reviewedIdentity,
        );
        const failure = reviewedStateFailure(reviewedItem, itemOperations, live);
        if (failure) {
          planFailure = `${reviewedItem.repository}#${reviewedItem.number}: ${failure}`;
          break;
        }
      }
    }
    if (planFailure) {
      const failure = planFailure;
      record = {
        ...record,
        operations: record.operations.map((operation) =>
          operation.status === "applied" ||
          operation.status === "already-current" ||
          operation.status === "undone"
            ? operation
            : {
                ...operation,
                status: operation.status === "uncertain" ? "uncertain" : "failed",
                owned: false,
                failure,
              },
        ),
      };
      yield* saveRecord(record);
    }
    for (let index = 0; index < record.operations.length; index += 1) {
      if (planFailure) break;
      const operation = record.operations[index];
      if (
        !operation ||
        operation.status === "applied" ||
        operation.status === "already-current" ||
        operation.status === "undone"
      )
        continue;
      const identity = operationIdentity(operation);
      const reviewedItem = reviewedItemsByIdentity.get(identity);
      const live = yield* readIssue(
        project.workspaceRoot,
        operation.repository,
        operation.issueNumber,
      );
      const itemOperations = record.operations.filter(
        (candidate) => operationIdentity(candidate) === identity,
      );
      const stateFailure = reviewedItem
        ? reviewedStateFailure(reviewedItem, itemOperations, live)
        : "The reviewed issue identity is missing from the saved plan.";
      if (stateFailure) {
        record = {
          ...record,
          operations: record.operations.map((item) =>
            item.status === "applied" ||
            item.status === "already-current" ||
            item.status === "undone"
              ? item
              : {
                  ...item,
                  status: item.status === "uncertain" ? "uncertain" : "failed",
                  owned: false,
                  failure: `${operation.repository}#${operation.issueNumber}: ${stateFailure}`,
                },
          ),
        };
        yield* saveRecord(record);
        break;
      }
      const alreadyCurrent =
        operation.kind === "add-label"
          ? labelsOf(live).includes(operation.label ?? "")
          : operation.kind === "remove-label"
            ? !labelsOf(live).includes(operation.label ?? "")
            : parentNumber(live) === operation.afterParentNumber;
      if (alreadyCurrent) {
        record = {
          ...record,
          operations: record.operations.map((item, itemIndex) =>
            itemIndex === index
              ? operation.status === "uncertain"
                ? {
                    ...item,
                    status: "uncertain",
                    owned: false,
                    failure:
                      "The attempted write is present, but it could not be attributed to this adoption.",
                  }
                : { ...clearFailure(item), status: "already-current", owned: false }
              : item,
          ),
        };
        yield* saveRecord(record);
        continue;
      }
      record = {
        ...record,
        operations: record.operations.map((item, itemIndex) =>
          itemIndex === index ? { ...clearFailure(item), status: "pending" } : item,
        ),
      };
      yield* saveRecord(record);
      const attempt =
        operation.kind === "add-label"
          ? addLabel(project.workspaceRoot, operation.repository, operation)
          : operation.kind === "remove-label"
            ? removeLabel(project.workspaceRoot, operation.repository, operation)
            : changeParent(
                project.workspaceRoot,
                operation.repository,
                operation,
                operation.beforeParentNumber ?? null,
                operation.afterParentNumber ?? null,
              );
      const outcome = yield* Effect.result(attempt);
      let completedOperation: InternalOperation;
      if (outcome._tag === "Failure") {
        completedOperation = {
          ...operation,
          status: "uncertain",
          owned: false,
          failure: `The write was attempted, but its result could not be confirmed: ${String(outcome.failure)}`,
        };
      } else if (operation.kind === "change-parent") {
        const readback = yield* Effect.result(
          readIssue(project.workspaceRoot, operation.repository, operation.issueNumber),
        );
        completedOperation =
          readback._tag === "Failure"
            ? {
                ...operation,
                status: "uncertain",
                owned: false,
                failure: `The parent write was accepted, but readback failed: ${String(readback.failure)}`,
              }
            : parentNumber(readback.success) !== operation.afterParentNumber
              ? {
                  ...operation,
                  status: "failed",
                  owned: false,
                  failure: "GitHub did not retain the reviewed parent change.",
                }
              : { ...clearFailure(operation), status: "applied", owned: true };
      } else {
        completedOperation = { ...clearFailure(operation), status: "applied", owned: true };
      }
      record = {
        ...record,
        operations: record.operations.map((item, itemIndex) =>
          itemIndex === index ? completedOperation : item,
        ),
      };
      yield* saveRecord(record);
    }
    record = {
      ...record,
      status: record.operations.some(
        (operation) =>
          operation.status === "failed" ||
          operation.status === "pending" ||
          operation.status === "uncertain",
      )
        ? "partial"
        : "applied",
    };
    yield* saveRecord(record);
    return publicRecord(record);
  });
  const apply: WorkflowAdoptionService["Service"]["apply"] = (input) =>
    mutationLock.withPermit(applyUnlocked(input));

  const history: WorkflowAdoptionService["Service"]["history"] = Effect.fn(
    "WorkflowAdoptionService.history",
  )(function* (input) {
    const rows = yield* sql<typeof RecordRow.Type>`
      SELECT record_json AS "recordJson" FROM workflow_adoptions
      WHERE project_id = ${input.projectId} AND repository = ${input.repository} AND root_number = ${input.rootNumber}
      ORDER BY created_at DESC
    `.pipe(
      Effect.mapError((error) =>
        adoptionError(
          "persistence-failed",
          "The adoption history could not be read.",
          String(error),
        ),
      ),
    );
    const records = new Array<WorkflowAdoptionRecord>();
    for (const row of rows) records.push(publicRecord(yield* decodeRecord(row.recordJson)));
    return { records };
  });

  const recover: WorkflowAdoptionService["Service"]["recover"] = Effect.fn(
    "WorkflowAdoptionService.recover",
  )(function* (input) {
    const record = yield* loadRecord(input.adoptionId);
    if (
      record.projectId !== input.projectId ||
      record.repository !== input.repository ||
      record.rootNumber !== input.rootNumber
    )
      return yield* adoptionError(
        "adoption-not-found",
        "This adoption record does not belong to the selected workflow branch.",
      );
    const items = yield* decodeSelection(record.planJson).pipe(
      Effect.mapError((error) =>
        adoptionError("persistence-failed", "The saved adoption plan is invalid.", String(error)),
      ),
    );
    return {
      record: publicRecord(record),
      preview: {
        previewId: record.previewId,
        repository: record.repository as WorkflowAdoptionPreview["repository"],
        rootNumber: record.rootNumber,
        items,
      },
    };
  });

  const undoUnlocked: WorkflowAdoptionService["Service"]["undo"] = Effect.fn(
    "WorkflowAdoptionService.undo",
  )(function* (input) {
    let record = yield* loadRecord(input.adoptionId);
    if (record.projectId !== input.projectId)
      return yield* adoptionError(
        "adoption-not-found",
        "This adoption record does not belong to the selected project.",
      );
    const project = yield* selectedProject(input.projectId);
    const reviewedItems = yield* decodeSelection(record.planJson).pipe(
      Effect.mapError((error) =>
        adoptionError("persistence-failed", "The saved adoption plan is invalid.", String(error)),
      ),
    );
    const reviewedItemsByIdentity = new Map(
      reviewedItems.map((item) => [`${item.repository}:${item.id}`, item]),
    );
    const checkedClassifications = new Set<string>();
    for (let index = record.operations.length - 1; index >= 0; index -= 1) {
      const operation = record.operations[index];
      if (!operation || !operation.owned || operation.status === "undone") continue;
      const identity = operationIdentity(operation);
      if (operation.kind !== "change-parent") {
        if (checkedClassifications.has(identity)) continue;
        checkedClassifications.add(identity);
        const relatedIndexes = record.operations.flatMap((candidate, candidateIndex) =>
          operationIdentity(candidate) === identity && candidate.kind !== "change-parent"
            ? [candidateIndex]
            : [],
        );
        const ownedIndexes = relatedIndexes.filter((candidateIndex) => {
          const candidate = record.operations[candidateIndex];
          return candidate?.owned && candidate.status !== "undone";
        });
        if (ownedIndexes.length === 0) continue;
        const live = yield* readIssue(
          project.workspaceRoot,
          operation.repository,
          operation.issueNumber,
        );
        const item = reviewedItemsByIdentity.get(identity);
        const related = relatedIndexes.flatMap((candidateIndex) => {
          const candidate = record.operations[candidateIndex];
          return candidate ? [candidate] : [];
        });
        const hasUnattributedChange = related.some(
          (candidate) =>
            !candidate.owned &&
            (candidate.status === "uncertain" || candidate.status === "already-current"),
        );
        const expected = item ? expectedClassificationStates(item, related) : new Set<string>();
        const safe =
          !hasUnattributedChange &&
          expected.size === 1 &&
          expected.has(classificationLabels(labelsOf(live)).join("\n"));
        if (!safe) {
          const operations = [...record.operations];
          for (const candidateIndex of ownedIndexes) {
            const candidate = operations[candidateIndex];
            if (candidate)
              operations[candidateIndex] = {
                ...candidate,
                status: "undo-skipped",
                failure: "A later or unattributed classification replaced this adoption change.",
              };
          }
          record = Object.assign({}, record, { operations });
          yield* saveRecord(record);
          continue;
        }
        for (const candidateIndex of ownedIndexes.toReversed()) {
          const candidate = record.operations[candidateIndex];
          if (!candidate) continue;
          const attempt =
            candidate.kind === "add-label"
              ? removeLabel(project.workspaceRoot, candidate.repository, candidate)
              : addLabel(project.workspaceRoot, candidate.repository, candidate);
          const outcome = yield* Effect.result(attempt);
          const operations = [...record.operations];
          operations[candidateIndex] =
            outcome._tag === "Success"
              ? { ...clearFailure(candidate), status: "undone" }
              : {
                  ...candidate,
                  status: "undo-skipped",
                  failure: `The classification restoration failed: ${String(outcome.failure)}`,
                };
          record = Object.assign({}, record, { operations });
          yield* saveRecord(record);
        }
        continue;
      }
      const live = yield* readIssue(
        project.workspaceRoot,
        operation.repository,
        operation.issueNumber,
      );
      const safe = parentNumber(live) === operation.afterParentNumber;
      if (!safe) {
        const operations = [...record.operations];
        operations[index] = {
          ...operation,
          status: "undo-skipped",
          owned: true,
          failure: "The adoption-owned value changed later.",
        };
        record = Object.assign({}, record, { operations });
        yield* saveRecord(record);
        continue;
      }
      const attempt = changeParent(
        project.workspaceRoot,
        operation.repository,
        operation,
        operation.afterParentNumber ?? null,
        operation.beforeParentNumber ?? null,
      );
      const outcome = yield* Effect.result(attempt);
      const readback = yield* Effect.result(
        readIssue(project.workspaceRoot, operation.repository, operation.issueNumber),
      );
      const operations = [...record.operations];
      operations[index] =
        readback._tag === "Success" &&
        parentNumber(readback.success) === operation.beforeParentNumber
          ? outcome._tag === "Success"
            ? { ...clearFailure(operation), status: "undone" }
            : {
                ...operation,
                status: "uncertain",
                failure:
                  "The parent was restored, but the restoration could not be attributed to this undo.",
              }
          : readback._tag === "Success"
            ? {
                ...operation,
                status: "undo-skipped",
                failure: "GitHub did not retain the parent restoration.",
              }
            : {
                ...operation,
                status: "uncertain",
                failure: `The parent restoration could not be verified: ${String(readback.failure)}`,
              };
      record = Object.assign({}, record, { operations });
      yield* saveRecord(record);
    }
    record = {
      ...record,
      status: record.operations.some(
        (operation) =>
          operation.status === "uncertain" || (operation.owned && operation.status !== "undone"),
      )
        ? "undo-partial"
        : "undone",
    };
    yield* saveRecord(record);
    return publicRecord(record);
  });
  const undo: WorkflowAdoptionService["Service"]["undo"] = (input) =>
    mutationLock.withPermit(undoUnlocked(input));

  return WorkflowAdoptionService.of({ preview, apply, history, recover, undo });
});

export const layer = Layer.effect(WorkflowAdoptionService, make).pipe(
  Layer.provide(GitHubCli.layer),
);
