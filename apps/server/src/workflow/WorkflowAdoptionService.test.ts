import { ProjectId, WorkflowAdoptionError, type WorkflowAdoptionItem } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowAdoptionService from "./WorkflowAdoptionService.ts";

const projectId = ProjectId.make("project-1");
const repository = "Flow-Fly/t3code" as const;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeLabelPayload = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ labels: Schema.optionalKey(Schema.Array(Schema.String)) })),
);

interface MutableIssue {
  id: number;
  nodeId: string;
  repository: string;
  number: number;
  title: string;
  body: string;
  updatedAt: string;
  labels: string[];
  parent: number | null;
  children: number[];
}

function output(stdout: string) {
  return {
    stdout,
    stderr: "",
    exitCode: ChildProcessSpawner.ExitCode(0),
    timedOut: false,
    stdoutTruncated: false,
    stderrTruncated: false,
    stdoutInvalidUtf8: false,
    stderrInvalidUtf8: false,
  };
}

function serialized(issue: MutableIssue) {
  return {
    id: issue.id,
    node_id: issue.nodeId,
    number: issue.number,
    title: issue.title,
    html_url: `https://github.com/${issue.repository}/issues/${issue.number}`,
    repository_url: `https://api.github.com/repos/${issue.repository}`,
    body: issue.body,
    updated_at: issue.updatedAt,
    labels: issue.labels.map((name) => ({ name })),
    parent_issue_url:
      issue.parent === null
        ? null
        : `https://api.github.com/repos/${issue.repository}/issues/${issue.parent}`,
    sub_issues_summary: { total: issue.children.length, completed: 0 },
  };
}

function harness() {
  const issues = new Map<number, MutableIssue>([
    [
      10,
      {
        id: 1010,
        nodeId: "issue-10",
        repository,
        number: 10,
        title: "Existing initiative",
        body: "## Source map\n\nhttps://github.com/Flow-Fly/t3code/issues/3",
        updatedAt: "2026-09-06T10:00:00Z",
        labels: ["existing-label"],
        parent: null,
        children: [11],
      },
    ],
    [
      11,
      {
        id: 1011,
        nodeId: "issue-11",
        repository,
        number: 11,
        title: "Existing slice",
        body: "## What to build\n\nShip it.",
        updatedAt: "2026-09-06T10:00:00Z",
        labels: ["workflow:task", "keep-me"],
        parent: 10,
        children: [],
      },
    ],
  ]);
  const writes = new Array<ReadonlyArray<string>>();
  const issueReads = new Array<string>();
  let failNextWrite = false;
  let ignoreParentWrite = false;
  let loseNextWriteResponse = false;

  const execute: GitHubCli.GitHubCli["Service"]["execute"] = ({ args, stdin }) =>
    Effect.suspend(() => {
      const endpoint = args.find((argument) => argument.startsWith("repos/"));
      if (!endpoint) return Effect.die("missing endpoint");
      const subIssues = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/sub_issues\?/u.exec(endpoint);
      if (subIssues?.[1]) {
        const parent = issues.get(Number(subIssues[1]));
        return Effect.succeed(
          output(
            encodeJson(parent?.children.map((number) => serialized(issues.get(number)!)) ?? []),
          ),
        );
      }
      const issueRead = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)$/u.exec(endpoint);
      if (issueRead?.[1] && !args.includes("--method")) {
        const issue = issues.get(Number(issueRead[1]));
        issueReads.push(`${issue?.repository}#${issueRead[1]}`);
        return issue
          ? Effect.succeed(output(encodeJson(serialized(issue))))
          : Effect.die("missing issue");
      }
      writes.push(args);
      if (failNextWrite) {
        failNextWrite = false;
        return Effect.fail(
          new GitHubCli.GitHubCliCommandError({
            command: "gh",
            cwd: "/workspace",
            cause: new Error("controlled failure"),
          }),
        ) as Effect.Effect<never, GitHubCli.GitHubCliError>;
      }
      const labelAdd = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/labels$/u.exec(endpoint);
      if (labelAdd?.[1] && args.includes("POST")) {
        const issue = issues.get(Number(labelAdd[1]))!;
        const payload = decodeLabelPayload(stdin ?? "{}");
        issue.labels.push(
          ...(payload.labels ?? []).filter((label) => !issue.labels.includes(label)),
        );
      }
      const labelRemove = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/labels\/(.+)$/u.exec(endpoint);
      if (labelRemove?.[1] && labelRemove[2]) {
        const issue = issues.get(Number(labelRemove[1]))!;
        issue.labels = issue.labels.filter(
          (label) => label !== decodeURIComponent(labelRemove[2]!),
        );
      }
      const parentAdd = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/sub_issues$/u.exec(endpoint);
      if (parentAdd?.[1]) {
        const idArg = args.find((argument) => argument.startsWith("sub_issue_id="));
        const child = [...issues.values()].find(
          (issue) => issue.id === Number(idArg?.split("=")[1]),
        )!;
        if (!ignoreParentWrite) child.parent = Number(parentAdd[1]);
      }
      const parentRemove = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/sub_issue$/u.exec(endpoint);
      if (parentRemove?.[1]) {
        const idArg = args.find((argument) => argument.startsWith("sub_issue_id="));
        const child = [...issues.values()].find(
          (issue) => issue.id === Number(idArg?.split("=")[1]),
        )!;
        if (!ignoreParentWrite) child.parent = null;
      }
      if (loseNextWriteResponse) {
        loseNextWriteResponse = false;
        return Effect.fail(
          new GitHubCli.GitHubCliCommandError({
            command: "gh",
            cwd: "/workspace",
            cause: new Error("response lost"),
          }),
        ) as Effect.Effect<never, GitHubCli.GitHubCliError>;
      }
      return Effect.succeed(output("{}"));
    });

  const serviceLayer = Layer.effect(
    WorkflowAdoptionService.WorkflowAdoptionService,
    WorkflowAdoptionService.make,
  ).pipe(
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getProjectShellById: () =>
          Effect.succeed(
            Option.some({
              id: projectId,
              title: "T3 Code",
              workspaceRoot: "/workspace",
            } as never),
          ),
      }),
    ),
    Layer.provide(Layer.mock(GitHubCli.GitHubCli)({ execute })),
    Layer.provide(SqlitePersistenceMemory),
    Layer.provide(NodeServices.layer),
  );

  return {
    issues,
    issueReads,
    writes,
    layer: serviceLayer,
    failNextWrite: () => {
      failNextWrite = true;
    },
    ignoreParentWrite: () => {
      ignoreParentWrite = true;
    },
    loseNextWriteResponse: () => {
      loseNextWriteResponse = true;
    },
  };
}

function corrected(
  item: WorkflowAdoptionItem,
  patch: Partial<WorkflowAdoptionItem>,
): WorkflowAdoptionItem {
  return { ...item, ...patch };
}

describe("WorkflowAdoptionService", () => {
  it.effect(
    "previews sources and requires explicit reparent confirmation after corrections",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
        const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
        expect(preview.items.map((item) => [item.number, item.proposedKind])).toEqual([
          [10, "capability"],
          [11, "task"],
        ]);
        expect(preview.items[0]?.relationships[0]).toMatchObject({
          relationship: "source",
          issueNumber: 3,
        });
        const items = preview.items.map((item) =>
          item.number === 11
            ? corrected(item, {
                proposedKind: "ticket",
                proposedParentNumber: null,
                parentChangeConfirmed: false,
              })
            : item,
        );
        const error = yield* service
          .apply({ projectId, previewId: preview.previewId, repository, rootNumber: 10, items })
          .pipe(Effect.flip);
        expect(error).toBeInstanceOf(WorkflowAdoptionError);
        expect(error).toMatchObject({ failure: "parent-confirmation-required" });
        expect(test.writes).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("rereads issues linearly while applying a multi-issue branch", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const readsBeforeApply = test.issueReads.length;
      const items = preview.items.map((item) =>
        item.number === 11 ? corrected(item, { proposedKind: "ticket" }) : item,
      );
      const applied = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(applied.status).toBe("applied");
      expect(applied.operations).toHaveLength(3);
      expect(test.issueReads.length - readsBeforeApply).toBe(5);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("refuses changed source body before the first write", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      test.issues.get(10)!.body = "## Source map\n\nChanged without a timestamp update.";
      const error = yield* service
        .apply({
          projectId,
          previewId: preview.previewId,
          repository,
          rootNumber: 10,
          items: preview.items,
        })
        .pipe(Effect.flip);
      expect(error).toMatchObject({ failure: "changed-source" });
      expect(test.writes).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("preserves an uncertain write without repeating it or claiming ownership", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedKind: "ticket" })
          : corrected(item, { included: false }),
      );
      test.loseNextWriteResponse();
      const partial = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(partial.status).toBe("partial");
      const retried = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(retried.status).toBe("partial");
      expect(retried.operations[0]).toMatchObject({
        repository,
        issueId: "issue-11",
        status: "uncertain",
        owned: false,
      });
      expect(retried.operations[0]?.detail).toContain("could not be attributed");
      expect(test.writes.filter((args) => args.includes("POST"))).toHaveLength(1);
      expect(test.issues.get(11)!.labels).toContain("workflow:ticket");
      const undone = yield* service.undo({ projectId, adoptionId: retried.adoptionId });
      expect(undone.status).toBe("undo-partial");
      expect(undone.operations[0]).toMatchObject({ status: "uncertain", owned: false });
      expect(test.issues.get(11)!.labels).toContain("workflow:ticket");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("refuses pending retry writes after the reviewed body changes", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedKind: "ticket" })
          : corrected(item, { included: false }),
      );
      test.failNextWrite();
      const partial = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(partial.status).toBe("partial");
      test.issues.get(11)!.body = "## What to build\n\nA different reviewed scope.";
      const writesBeforeRetry = test.writes.length;
      const retried = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(retried.status).toBe("partial");
      expect(retried.operations[0]?.detail).toContain("reviewed source changed");
      expect(test.writes).toHaveLength(writesBeforeRetry);
      expect(test.issues.get(11)!.labels).not.toContain("workflow:ticket");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("refuses pending retry writes after the issue leaves the reviewed branch", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedKind: "ticket" })
          : corrected(item, { included: false }),
      );
      test.failNextWrite();
      yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      test.issues.get(11)!.parent = 99;
      const writesBeforeRetry = test.writes.length;
      const retried = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(retried.status).toBe("partial");
      expect(retried.operations[0]?.detail).toContain("reviewed branch changed");
      expect(test.writes).toHaveLength(writesBeforeRetry);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("refuses pending retry writes after a later classification replaces the review", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedKind: "ticket" })
          : corrected(item, { included: false }),
      );
      test.failNextWrite();
      yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      test.issues.get(11)!.labels = ["workflow:container", "keep-me"];
      const writesBeforeRetry = test.writes.length;
      const retried = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(retried.status).toBe("partial");
      expect(retried.operations[0]?.detail).toContain("reviewed classification changed");
      expect(test.writes).toHaveLength(writesBeforeRetry);
      expect(test.issues.get(11)!.labels).toEqual(["workflow:container", "keep-me"]);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "refuses a retry when a pending reparent no longer matches the reviewed baseline",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
        const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
        const items = preview.items.map((item) =>
          item.number === 11
            ? corrected(item, { proposedParentNumber: null, parentChangeConfirmed: true })
            : corrected(item, { included: false }),
        );
        test.failNextWrite();
        const partial = yield* service.apply({
          projectId,
          previewId: preview.previewId,
          repository,
          rootNumber: 10,
          items,
        });
        expect(partial.status).toBe("partial");
        test.issues.get(11)!.parent = 99;
        const retried = yield* service.apply({
          projectId,
          previewId: preview.previewId,
          repository,
          rootNumber: 10,
          items,
        });
        expect(retried.status).toBe("partial");
        expect(
          retried.operations.find((operation) => operation.kind === "change-parent")?.detail,
        ).toContain("reviewed branch changed");
        expect(test.issues.get(11)!.parent).toBe(99);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "undoes only owned classification and parent changes while preserving later edits",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
        const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
        const items = preview.items.map((item) =>
          item.number === 11
            ? corrected(item, {
                proposedKind: "ticket",
                proposedParentNumber: null,
                parentChangeConfirmed: true,
              })
            : corrected(item, { included: false }),
        );
        const applied = yield* service.apply({
          projectId,
          previewId: preview.previewId,
          repository,
          rootNumber: 10,
          items,
        });
        expect(
          applied.operations.some(
            (operation) => operation.kind === "remove-label" && operation.owned,
          ),
        ).toBe(true);
        test.issues.get(11)!.labels.push("later-edit");
        const undone = yield* service.undo({ projectId, adoptionId: applied.adoptionId });
        expect(undone.status).toBe("undone");
        expect(test.issues.get(11)!.labels).toEqual(
          expect.arrayContaining(["workflow:task", "keep-me", "later-edit"]),
        );
        expect(test.issues.get(11)!.labels).not.toContain("workflow:ticket");
        expect(test.issues.get(11)!.parent).toBe(10);
        const history = yield* service.history({ projectId, repository, rootNumber: 10 });
        expect(history.records[0]?.status).toBe("undone");
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("keeps a later classification intact when undoing a replaced classification", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedKind: "ticket" })
          : corrected(item, { included: false }),
      );
      const applied = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      test.issues.get(11)!.labels = ["workflow:container", "keep-me"];
      const undone = yield* service.undo({ projectId, adoptionId: applied.adoptionId });
      expect(undone.status).toBe("undo-partial");
      expect(test.issues.get(11)!.labels).toEqual(["workflow:container", "keep-me"]);
      expect(
        undone.operations.filter(
          (operation) => operation.issueId === "issue-11" && operation.kind !== "change-parent",
        ),
      ).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ status: "undo-skipped" }),
          expect.objectContaining({ status: "undo-skipped" }),
        ]),
      );
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("records a successful parent command as failed when readback does not match", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedParentNumber: null, parentChangeConfirmed: true })
          : corrected(item, { included: false }),
      );
      test.ignoreParentWrite();
      const result = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      expect(result.status).toBe("partial");
      expect(
        result.operations.find((operation) => operation.kind === "change-parent"),
      ).toMatchObject({
        status: "failed",
        owned: false,
        detail: "GitHub did not retain the reviewed parent change.",
      });
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("recovers the persisted reviewed plan for the original partial adoption", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedKind: "ticket" })
          : corrected(item, { included: false }),
      );
      test.failNextWrite();
      const partial = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      const history = yield* service.history({ projectId, repository, rootNumber: 10 });
      const recovered = yield* service.recover({
        projectId,
        repository,
        rootNumber: 10,
        adoptionId: history.records[0]!.adoptionId,
      });
      expect(recovered.record.adoptionId).toBe(partial.adoptionId);
      expect(recovered.preview.previewId).toBe(preview.previewId);
      expect(recovered.preview.items).toEqual(items);
      const retried = yield* service.apply({
        projectId,
        previewId: recovered.preview.previewId,
        repository: recovered.preview.repository,
        rootNumber: recovered.preview.rootNumber,
        items: recovered.preview.items,
      });
      expect(retried.adoptionId).toBe(partial.adoptionId);
      expect(retried.status).toBe("applied");
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("skips an adoption-owned parent change that was edited again later", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
      const preview = yield* service.preview({ projectId, repository, rootNumber: 10 });
      const items = preview.items.map((item) =>
        item.number === 11
          ? corrected(item, { proposedParentNumber: null, parentChangeConfirmed: true })
          : corrected(item, { included: false }),
      );
      const applied = yield* service.apply({
        projectId,
        previewId: preview.previewId,
        repository,
        rootNumber: 10,
        items,
      });
      test.issues.get(11)!.parent = 99;
      const undone = yield* service.undo({ projectId, adoptionId: applied.adoptionId });
      expect(undone.status).toBe("undo-partial");
      expect(
        undone.operations.find((operation) => operation.kind === "change-parent"),
      ).toMatchObject({
        status: "undo-skipped",
        detail: "The adoption-owned value changed later.",
      });
      expect(test.issues.get(11)!.parent).toBe(99);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "does not record parent undo when relationship readback still shows the adopted parent",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowAdoptionService.WorkflowAdoptionService;
        test.issues.get(11)!.parent = null;
        const refreshed = yield* service.preview({ projectId, repository, rootNumber: 10 });
        const items = refreshed.items.map((item) =>
          item.number === 11
            ? corrected(item, { proposedParentNumber: 10, parentChangeConfirmed: true })
            : corrected(item, { included: false }),
        );
        const applied = yield* service.apply({
          projectId,
          previewId: refreshed.previewId,
          repository,
          rootNumber: 10,
          items,
        });
        expect(applied.status).toBe("applied");
        test.ignoreParentWrite();
        const undone = yield* service.undo({ projectId, adoptionId: applied.adoptionId });
        expect(undone.status).toBe("undo-partial");
        expect(
          undone.operations.find((operation) => operation.kind === "change-parent"),
        ).toMatchObject({
          status: "undo-skipped",
          detail: "GitHub did not retain the parent restoration.",
        });
        expect(test.issues.get(11)!.parent).toBe(10);
      }).pipe(Effect.provide(test.layer));
    },
  );
});
