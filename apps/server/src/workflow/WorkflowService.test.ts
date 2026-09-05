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
  const summary = (issueNumber: number) => ({
    id: `issue-${issueNumber}`,
    number: issueNumber,
    title: `Issue ${issueNumber}`,
    url: `https://github.com/Flow-Fly/t3code/issues/${issueNumber}`,
    state: issueNumber === 2 ? "CLOSED" : "OPEN",
    stateReason: issueNumber === 2 ? "COMPLETED" : null,
    updatedAt: "2026-09-05T19:30:00Z",
    repository: { nameWithOwner: "Flow-Fly/t3code" },
    labels: {
      pageInfo: { hasNextPage: false, endCursor: `labels-${issueNumber}` },
      nodes: [{ name: issueNumber === 1 ? "workflow:capability" : "workflow:ticket" }],
    },
    subIssuesSummary: { total: issueNumber === 1 ? 1 : 0 },
  });
  return {
    ...summary(number),
    parent: parent === null ? null : summary(parent),
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
                    pageInfo: { hasNextPage: false, endCursor: "roots-terminal" },
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
                            pageInfo: { hasNextPage: false, endCursor: "labels-12" },
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
                      pageInfo: { hasNextPage: false, endCursor: "children-terminal" },
                      nodes: [
                        {
                          ...issue(13, 10),
                          id: "another-repository-13",
                          repository: { nameWithOwner: "another/repository" },
                          url: "https://github.com/another/repository/issues/13",
                          labels: {
                            pageInfo: { hasNextPage: false, endCursor: "labels-13" },
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
      expect(result.children[1]).toMatchObject({
        id: "another-repository-13",
        repository: "another/repository",
        parentNumber: 10,
      });
      expect(execute).toHaveBeenCalledTimes(2);
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
                    pageInfo: { hasNextPage: false, endCursor: "roots-terminal" },
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
                    pageInfo: { hasNextPage: false, endCursor: "labels-terminal" },
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

  it.effect("keeps a native cross-repository blocker as one repository-qualified identity", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    execute.mockReturnValueOnce(
      Effect.succeed(
        processOutput(
          JSON.stringify({
            data: {
              repository: {
                issue: {
                  ...issue(11, 10),
                  body: "## Source map\n\n[Map](https://github.com/Flow-Fly/t3code/issues/1)",
                  blockedBy: {
                    pageInfo: { hasNextPage: true, endCursor: "blockers-2" },
                    nodes: [
                      {
                        ...issue(7),
                        id: "other-repo-issue-7",
                        repository: { nameWithOwner: "another/repository" },
                        url: "https://github.com/another/repository/issues/7",
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
    execute.mockReturnValueOnce(
      Effect.succeed(
        processOutput(
          JSON.stringify({
            data: {
              node: {
                blockedBy: {
                  pageInfo: { hasNextPage: false, endCursor: "blockers-terminal" },
                  nodes: [
                    {
                      ...issue(7),
                      id: "other-repo-issue-7",
                      repository: { nameWithOwner: "another/repository" },
                      url: "https://github.com/another/repository/issues/7",
                    },
                  ],
                },
              },
            },
          }),
        ) as never,
      ),
    );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.issueDetail({
        projectId: "project-1" as never,
        repository: "Flow-Fly/t3code",
        number: 11,
      });

      expect(result.blockedBy).toHaveLength(1);
      expect(result.blockedBy[0]).toMatchObject({
        id: "other-repo-issue-7",
        repository: "another/repository",
        number: 7,
      });
      expect(execute.mock.calls[1]?.[0].args).toContain("after=blockers-2");
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect(
    "reports truncation when the fifty-result search limit lands inside a terminal page",
    () => {
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      const page = (start: number, count: number, hasNextPage: boolean, endCursor: string) =>
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                search: {
                  pageInfo: { hasNextPage, endCursor },
                  nodes: Array.from({ length: count }, (_, index) => issue(start + index)),
                },
              },
            }),
          ) as never,
        );
      execute
        .mockReturnValueOnce(page(100, 20, true, "search-2"))
        .mockReturnValueOnce(page(120, 20, true, "search-3"))
        .mockReturnValueOnce(page(140, 15, false, "search-terminal"));

      return Effect.gen(function* () {
        const service = yield* WorkflowService.WorkflowService;
        const result = yield* service.search({
          projectId: "project-1" as never,
          repository: "Flow-Fly/t3code",
          query: "delivery",
        });

        expect(result.matches).toHaveLength(50);
        expect(result.hasMore).toBe(true);
        expect(execute).toHaveBeenCalledTimes(3);
        expect(execute.mock.calls[1]?.[0].args).toContain("after=search-2");
        expect(result.matches[0]?.ancestryComplete).toBe(true);
      }).pipe(Effect.provide(layer(execute)));
    },
  );
});
