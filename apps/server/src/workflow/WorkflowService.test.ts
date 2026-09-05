import { describe, expect, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowService from "./WorkflowService.ts";

const project = {
  id: "project-1",
  title: "T3 Code",
  workspaceRoot: "/workspace/t3code",
};

const processOutput = (stdout: string) => ({
  stdout,
  stderr: "",
  code: ChildProcessSpawner.ExitCode(0),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

function issue(number: number, parent: number | null = null) {
  return {
    id: `issue-${number}`,
    number,
    title: `Issue ${number}`,
    url: `https://github.com/Flow-Fly/t3code/issues/${number}`,
    state: number === 2 ? "CLOSED" : "OPEN",
    stateReason: number === 2 ? "COMPLETED" : null,
    updatedAt: "2026-09-05T19:30:00Z",
    labels: {
      pageInfo: { hasNextPage: false, endCursor: null },
      nodes: [{ name: number === 1 ? "workflow:capability" : "workflow:ticket" }],
    },
    parent: parent === null ? null : { number: parent },
    subIssuesSummary: { total: number === 1 ? 1 : 0 },
  };
}

function layer(githubExecute: GitHubCli.GitHubCli["Service"]["execute"], gitStdout = "") {
  return Layer.effect(WorkflowService.WorkflowService, WorkflowService.make).pipe(
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getProjectShellById: () => Effect.succeed(Option.some(project as never)),
      }),
    ),
    Layer.provide(
      Layer.mock(ProcessRunner.ProcessRunner)({
        run: () => Effect.succeed(processOutput(gitStdout)),
      }),
    ),
    Layer.provide(Layer.mock(GitHubCli.GitHubCli)({ execute: githubExecute })),
  );
}

describe("WorkflowService", () => {
  it.effect("returns every GitHub remote so fork/upstream selection stays explicit", () =>
    Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.repositories({ projectId: "project-1" as never });

      expect(result.repositories).toEqual([
        { nameWithOwner: "flow-fly/t3code", remoteNames: ["origin", "publish"] },
        { nameWithOwner: "pingdotgg/t3code", remoteNames: ["upstream"] },
      ]);
    }).pipe(
      Effect.provide(
        layer(
          () => Effect.die("not called"),
          [
            "origin git@github.com:Flow-Fly/t3code.git (fetch)",
            "origin git@github.com:Flow-Fly/t3code.git (push)",
            "publish https://github.com/Flow-Fly/t3code (fetch)",
            "upstream https://github.com/pingdotgg/t3code.git (fetch)",
          ].join("\n"),
        ),
      ),
    ),
  );

  it.effect("follows GitHub pagination and returns only top-level workflow roots", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issues: {
                    pageInfo: { hasNextPage: true, endCursor: "page-2" },
                    nodes: [issue(1), issue(2, 1)],
                  },
                },
              },
            }),
          ) as never,
        ),
      )
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issues: {
                    pageInfo: { hasNextPage: false, endCursor: null },
                    nodes: [issue(3)],
                  },
                },
              },
            }),
          ) as never,
        ),
      );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.roots({
        projectId: "project-1" as never,
        repository: "Flow-Fly/t3code",
      });

      expect(result.roots.map((root) => root.number)).toEqual([1, 3]);
      expect(execute).toHaveBeenCalledTimes(2);
      expect(execute.mock.calls[0]?.[0].cwd).toBe("/workspace/t3code");
      expect(execute.mock.calls[1]?.[0].args).toContain("after=page-2");
      expect(result.roots[0]).not.toHaveProperty("body");
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("keeps unlabeled native hierarchy visible and follows nested issue pagination", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issue: {
                    subIssues: {
                      pageInfo: { hasNextPage: true, endCursor: "page-2" },
                      nodes: [
                        {
                          ...issue(12, 10),
                          labels: {
                            pageInfo: { hasNextPage: false, endCursor: null },
                            nodes: [],
                          },
                          subIssuesSummary: { total: 2 },
                        },
                      ],
                    },
                  },
                },
              },
            }),
          ) as never,
        ),
      )
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issue: {
                    subIssues: {
                      pageInfo: { hasNextPage: false, endCursor: null },
                      nodes: [
                        {
                          ...issue(13, 10),
                          labels: {
                            pageInfo: { hasNextPage: false, endCursor: null },
                            nodes: [],
                          },
                        },
                      ],
                    },
                  },
                },
              },
            }),
          ) as never,
        ),
      );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.children({
        projectId: "project-1" as never,
        repository: "Flow-Fly/t3code",
        parentNumber: 10,
      });

      expect(result.children.map(({ number, kind }) => ({ number, kind }))).toEqual([
        { number: 12, kind: "container" },
        { number: 13, kind: "task" },
      ]);
      expect(execute.mock.calls[1]?.[0].args).toContain("after=page-2");
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("uses later label pages to classify Wayfinder work", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issues: {
                    pageInfo: { hasNextPage: false, endCursor: null },
                    nodes: [
                      {
                        ...issue(7),
                        labels: {
                          pageInfo: { hasNextPage: true, endCursor: "labels-2" },
                          nodes: Array.from({ length: 100 }, (_, index) => ({
                            name: `label-${index}`,
                          })),
                        },
                      },
                    ],
                  },
                },
              },
            }),
          ) as never,
        ),
      )
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                node: {
                  labels: {
                    pageInfo: { hasNextPage: false, endCursor: null },
                    nodes: [{ name: "wayfinder:map" }],
                  },
                },
              },
            }),
          ) as never,
        ),
      );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.roots({
        projectId: "project-1" as never,
        repository: "Flow-Fly/t3code",
      });

      expect(result.roots[0]?.kind).toBe("map");
      expect(execute.mock.calls[1]?.[0].args).toContain("id=issue-7");
      expect(execute.mock.calls[1]?.[0].args).toContain("after=labels-2");
    }).pipe(Effect.provide(layer(execute)));
  });
});
