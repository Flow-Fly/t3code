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

const terminalPage = { hasNextPage: false, endCursor: "terminal" };

function workflowComment(input: {
  id: string;
  body: string;
  createdAt: string;
  author?: string;
  authorAssociation?: string;
}) {
  return {
    id: input.id,
    url: `https://github.com/Flow-Fly/t3code/issues/10#issuecomment-${input.id}`,
    body: input.body,
    createdAt: input.createdAt,
    author: input.author === undefined ? { login: "Flow-Fly" } : { login: input.author },
    authorAssociation: input.authorAssociation ?? "OWNER",
  };
}

function evidenceIssue(input: {
  number: number;
  parent?: number | null;
  title?: string;
  body?: string;
  state?: "OPEN" | "CLOSED";
  stateReason?: "COMPLETED" | "NOT_PLANNED" | "REOPENED" | null;
  labels?: string[];
  assignees?: string[];
  comments?: ReturnType<typeof workflowComment>[];
  reopenedAt?: string[];
  blockedBy?: ReturnType<typeof issue>[];
}) {
  const base = issue(input.number, input.parent ?? null);
  return {
    ...base,
    title: input.title ?? base.title,
    body: input.body ?? "",
    state: input.state ?? base.state,
    stateReason: input.stateReason === undefined ? base.stateReason : input.stateReason,
    labels: {
      pageInfo: terminalPage,
      nodes: (input.labels ?? base.labels.nodes.map(({ name }) => name)).map((name) => ({ name })),
    },
    assignees: {
      pageInfo: terminalPage,
      nodes: (input.assignees ?? []).map((login) => ({ login })),
    },
    comments: { pageInfo: terminalPage, nodes: input.comments ?? [] },
    timelineItems: {
      pageInfo: terminalPage,
      nodes: (input.reopenedAt ?? []).map((createdAt) => ({ createdAt })),
    },
    blockedBy: { pageInfo: terminalPage, nodes: input.blockedBy ?? [] },
  };
}

function ticketScopeBody(input: {
  id: string;
  title: string;
  what: string;
  acceptance?: string;
  blockedBy?: string;
  extra?: string;
}) {
  return [
    "## Parent",
    "",
    "[Capability](https://github.com/Flow-Fly/t3code/issues/10)",
    "",
    `Approved slice: **${input.id}** ([ticket-breakdown approval](https://github.com/Flow-Fly/t3code/issues/10#issuecomment-breakdown))`,
    "",
    "## What to build",
    "",
    input.what,
    "",
    "## Acceptance criteria",
    "",
    input.acceptance ?? "- [ ] Works as approved.",
    ...(input.extra ? ["", input.extra] : []),
    "",
    "## Blocked by",
    "",
    input.blockedBy ?? "None",
  ].join("\n");
}

function breakdownApproval(
  slices: ReadonlyArray<{ id: string; title: string; body: string }>,
  input?: {
    author?: string;
    authorAssociation?: string;
  },
) {
  return workflowComment({
    id: "breakdown",
    createdAt: "2026-09-05T19:32:15Z",
    ...(input?.author ? { author: input.author } : {}),
    ...(input?.authorAssociation ? { authorAssociation: input.authorAssociation } : {}),
    body: [
      "## Approval",
      "<!-- t3-workflow:v1 approval -->",
      "Kind: ticket-breakdown",
      "Approved by: Flow-Fly (owner)",
      "Source: T3 thread `thread-1`, message `message-1`.",
      "### Approved content",
      ...slices.flatMap((slice) => [
        "<details>",
        `<summary>${slice.id} — ${slice.title}</summary>`,
        slice.body,
        "</details>",
      ]),
    ].join("\n"),
  });
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

  it.effect("reuses a shared ancestor lookup across a bounded search request", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                search: {
                  pageInfo: { hasNextPage: false, endCursor: "search-terminal" },
                  nodes: Array.from({ length: 50 }, (_, index) => issue(100 + index, 1)),
                },
              },
            }),
          ) as never,
        ),
      )
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(JSON.stringify({ data: { repository: { issue: issue(1) } } })) as never,
        ),
      );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.search({
        projectId: "project-1" as never,
        repository: "Flow-Fly/t3code",
        query: "shared parent",
      });

      expect(result.matches).toHaveLength(50);
      expect(result.matches.every((match) => match.ancestry[0]?.number === 1)).toBe(true);
      expect(execute).toHaveBeenCalledTimes(2);
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("locates a stable issue identity in its current ancestry", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({ data: { repository: { issue: issue(12, 20) } } }),
          ) as never,
        ),
      )
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(JSON.stringify({ data: { repository: { issue: issue(20) } } })) as never,
        ),
      );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const result = yield* service.locate({
        projectId: "project-1" as never,
        repository: "Flow-Fly/t3code",
        id: "issue-12",
        number: 12,
      });

      expect(result.issue.id).toBe("issue-12");
      expect(result.ancestry.map((ancestor) => ancestor.number)).toEqual([20]);
      expect(result.ancestryComplete).toBe(true);
      expect(execute).toHaveBeenCalledTimes(2);
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("derives the frontier from canonical approval and current blocker evidence", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    const ticketBody = [
      "## Parent",
      "",
      "[Capability](https://github.com/Flow-Fly/t3code/issues/10)",
      "",
      "Approved slice: **T03** ([ticket-breakdown approval](https://github.com/Flow-Fly/t3code/issues/10#issuecomment-approval))",
      "",
      "## What to build",
      "",
      "Explain the frontier.",
      "",
      "## Acceptance criteria",
      "",
      "- [x] Show current evidence.",
      "",
      "## Blocked by",
      "",
      "- [T01 — Browse](https://github.com/Flow-Fly/t3code/issues/11)",
    ].join("\n");
    const approval = workflowComment({
      id: "approval",
      createdAt: "2026-09-05T19:32:15Z",
      body: [
        "## Approval",
        "<!-- t3-workflow:v1 approval -->",
        "Kind: ticket-breakdown",
        "Approved by: Flow-Fly (owner)",
        "Source: T3 thread `thread-1`, message `message-1`.",
        "### Approved content",
        "<details>",
        "<summary>T03 — Explain the frontier</summary>",
        "## What to build",
        "",
        "Explain the frontier.",
        "",
        "## Acceptance criteria",
        "",
        "- [ ] Show current evidence.",
        "",
        "## Blocked by",
        "",
        "T01",
        "</details>",
      ].join("\n"),
    });
    const blocker = {
      ...issue(11),
      state: "CLOSED",
      stateReason: "COMPLETED",
      labels: {
        pageInfo: terminalPage,
        nodes: [{ name: "workflow:ticket" }],
      },
    };
    const child = evidenceIssue({
      number: 13,
      parent: 10,
      title: "Explain the frontier",
      body: ticketBody,
      labels: ["ready-for-agent", "workflow:ticket"],
      blockedBy: [blocker],
    });
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issue: {
                    id: "issue-10",
                    body: "Capability",
                    comments: { pageInfo: terminalPage, nodes: [approval] },
                    subIssues: { pageInfo: terminalPage, nodes: [child] },
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
                node: evidenceIssue({
                  number: 11,
                  state: "CLOSED",
                  stateReason: "COMPLETED",
                  comments: [
                    workflowComment({
                      id: "resolution",
                      createdAt: "2026-09-05T20:00:00Z",
                      body: [
                        "## Resolution",
                        "<!-- t3-workflow:v1 resolution -->",
                        "Outcome: resolved",
                        "### Summary",
                        "Browsing shipped.",
                        "### Evidence",
                        "[Commit](https://github.com/Flow-Fly/t3code/commit/abc)",
                      ].join("\n"),
                    }),
                  ],
                }),
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

      expect(result.children[0]?.readiness).toEqual(expect.objectContaining({ status: "ready" }));
      expect(result.children[0]?.readiness?.reasons).toEqual(
        expect.arrayContaining([expect.objectContaining({ kind: "approved-scope" })]),
      );
      expect(result.frontier).toEqual({
        status: "available",
        message: "1 item can proceed.",
        readyIssueIds: ["issue-13"],
      });
      expect(execute).toHaveBeenCalledTimes(2);
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("keeps specification and breakdown approval authority and provenance separate", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    const body = ticketScopeBody({
      id: "T03",
      title: "Explain evidence",
      what: "Explain evidence.",
    });
    const specification = workflowComment({
      id: "specification",
      createdAt: "2026-09-05T19:04:53Z",
      body: [
        "## Approval",
        "<!-- t3-workflow:v1 approval -->",
        "Kind: specification",
        "Approved by: Flow-Fly",
        "Source: T3 thread `thread-1`, message `message-0`.",
        "### Approved content",
        "## Summary",
        "Capability scope.",
      ].join("\n"),
    });
    const assertedBreakdown = breakdownApproval([{ id: "T03", title: "Explain evidence", body }], {
      author: "automation-bot",
      authorAssociation: "CONTRIBUTOR",
    });
    execute
      .mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issue: evidenceIssue({
                    number: 13,
                    parent: 10,
                    title: "Explain evidence",
                    body,
                  }),
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
                node: evidenceIssue({
                  number: 10,
                  title: "Capability",
                  body: "## Summary\n\nCapability scope.",
                  labels: ["workflow:capability"],
                  comments: [specification, assertedBreakdown],
                }),
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
        number: 13,
      });

      expect(result.readiness).toMatchObject({ status: "needs-review" });
      expect(result.readiness?.reasons).toContainEqual(
        expect.objectContaining({
          kind: "reassessment",
          message: expect.stringContaining("owner authority is not verified"),
        }),
      );
      expect(result.evidence?.records).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            approvalKind: "specification",
            authority: "verified",
            sourceAccess: "reported",
            scope: "not-applicable",
          }),
          expect.objectContaining({
            approvalKind: "ticket-breakdown",
            authority: "reported",
            sourceAccess: "reported",
            scope: "current",
          }),
        ]),
      );
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("distinguishes partial approval, later scope edits, and unavailable provenance", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    const partialBody = ticketScopeBody({
      id: "T05",
      title: "Partial approval",
      what: "Partially approved work.",
    });
    const approvedChangedBody = ticketScopeBody({
      id: "T06",
      title: "Edited scope",
      what: "Original scope.",
    });
    const currentChangedBody = ticketScopeBody({
      id: "T06",
      title: "Edited scope",
      what: "Original scope plus an unapproved export.",
    });
    const unavailableBody = ticketScopeBody({
      id: "T07",
      title: "Archived provenance",
      what: "Approved archived work.",
    });
    const specification = workflowComment({
      id: "spec-only",
      createdAt: "2026-09-05T19:00:00Z",
      body: [
        "## Approval",
        "<!-- t3-workflow:v1 approval -->",
        "Kind: specification",
        "Approved by: Flow-Fly",
        "Source: T3 thread `thread-1`.",
        "### Approved content",
        "Capability scope.",
      ].join("\n"),
    });
    const changedApproval = breakdownApproval([
      { id: "T06", title: "Edited scope", body: approvedChangedBody },
    ]);
    const unavailableApproval = workflowComment({
      id: "unavailable-breakdown",
      createdAt: "2026-09-05T20:00:00Z",
      body: [
        "## Approval",
        "<!-- t3-workflow:v1 approval -->",
        "Kind: ticket-breakdown",
        "Approved by: Flow-Fly",
        "Source: unavailable: archived T3 thread reference",
        "### Approved content",
        "<details>",
        "<summary>T07 — Archived provenance</summary>",
        unavailableBody,
        "</details>",
      ].join("\n"),
    });
    const detail = (number: number, title: string, body: string) =>
      Effect.succeed(
        processOutput(
          JSON.stringify({
            data: {
              repository: {
                issue: evidenceIssue({ number, parent: 10, title, body }),
              },
            },
          }),
        ) as never,
      );
    const parent = (comments: ReturnType<typeof workflowComment>[]) =>
      Effect.succeed(
        processOutput(
          JSON.stringify({
            data: {
              node: evidenceIssue({
                number: 10,
                title: "Capability",
                labels: ["workflow:capability"],
                comments,
              }),
            },
          }),
        ) as never,
      );
    execute
      .mockReturnValueOnce(detail(15, "Partial approval", partialBody))
      .mockReturnValueOnce(parent([specification]))
      .mockReturnValueOnce(detail(16, "Edited scope", currentChangedBody))
      .mockReturnValueOnce(parent([changedApproval]))
      .mockReturnValueOnce(detail(17, "Archived provenance", unavailableBody))
      .mockReturnValueOnce(parent([unavailableApproval]));

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const query = (number: number) =>
        service.issueDetail({
          projectId: "project-1" as never,
          repository: "Flow-Fly/t3code",
          number,
        });
      const partial = yield* query(15);
      const changed = yield* query(16);
      const unavailable = yield* query(17);

      expect(partial.readiness).toMatchObject({ status: "unapproved" });
      expect(partial.readiness?.reasons).toContainEqual(
        expect.objectContaining({ kind: "missing-approval" }),
      );
      expect(changed.readiness).toMatchObject({ status: "needs-review" });
      expect(changed.readiness?.reasons).toContainEqual(
        expect.objectContaining({ kind: "scope-changed" }),
      );
      expect(unavailable.readiness).toMatchObject({ status: "ready" });
      expect(unavailable.evidence?.records[0]).toMatchObject({
        authority: "verified",
        sourceAccess: "unavailable",
        scope: "current",
      });
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect("does not turn an old resolution or a stale checklist into current completion", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    const oldResolution = workflowComment({
      id: "old-resolution",
      createdAt: "2026-09-05T20:00:00Z",
      body: [
        "## Resolution",
        "<!-- t3-workflow:v1 resolution -->",
        "Outcome: resolved",
        "### Summary",
        "The prototype answered its question by rejecting the approach.",
        "### Evidence",
        "[Decision](https://github.com/Flow-Fly/t3code/issues/7#issuecomment-answer)",
      ].join("\n"),
    });
    execute.mockReturnValueOnce(
      Effect.succeed(
        processOutput(
          JSON.stringify({
            data: {
              repository: {
                issue: evidenceIssue({
                  number: 7,
                  title: "Prototype a rejected approach",
                  body: "## Checklist\n\n- [x] Prototype complete",
                  state: "CLOSED",
                  stateReason: "COMPLETED",
                  labels: ["wayfinder:prototype"],
                  comments: [oldResolution],
                  reopenedAt: ["2026-09-05T21:00:00Z"],
                }),
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
        number: 7,
      });

      expect(result.readiness).toMatchObject({ status: "closed-unverified" });
      expect(result.readiness?.reasons[0]?.message).toContain("after reopening");
      expect(result.evidence?.records[0]).toMatchObject({
        outcome: "resolved",
        scope: "changed",
      });
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect(
    "paginates evidence and lets a rejected prototype resolve after its latest reopening",
    () => {
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      const oldResolution = workflowComment({
        id: "prototype-old",
        createdAt: "2026-09-05T20:00:00Z",
        body: [
          "## Resolution",
          "<!-- t3-workflow:v1 resolution -->",
          "Outcome: resolved",
          "### Summary",
          "The first prototype answered the question.",
          "### Evidence",
          "[Prototype](https://github.com/Flow-Fly/t3code/issues/7#issuecomment-prototype)",
        ].join("\n"),
      });
      const currentResolution = workflowComment({
        id: "prototype-current",
        createdAt: "2026-09-05T23:00:00Z",
        body: [
          "## Resolution",
          "<!-- t3-workflow:v1 resolution -->",
          "Outcome: resolved",
          "This record supersedes https://github.com/Flow-Fly/t3code/issues/10#issuecomment-prototype-old",
          "### Summary",
          "The rejected prototype still resolved the decision by answering the question.",
          "### Evidence",
          "[Recorded answer](https://github.com/Flow-Fly/t3code/issues/7#issuecomment-answer)",
        ].join("\n"),
      });
      const detail = evidenceIssue({
        number: 7,
        title: "Prototype a rejected approach",
        state: "CLOSED",
        stateReason: "COMPLETED",
        labels: ["wayfinder:prototype"],
        comments: [oldResolution],
        assignees: ["worker-one"],
        reopenedAt: ["2026-09-05T21:00:00Z"],
      });
      detail.comments.pageInfo = { hasNextPage: true, endCursor: "comments-2" };
      detail.assignees.pageInfo = { hasNextPage: true, endCursor: "assignees-2" };
      detail.timelineItems.pageInfo = { hasNextPage: true, endCursor: "timeline-2" };
      execute
        .mockReturnValueOnce(
          Effect.succeed(
            processOutput(JSON.stringify({ data: { repository: { issue: detail } } })) as never,
          ),
        )
        .mockReturnValueOnce(
          Effect.succeed(
            processOutput(
              JSON.stringify({
                data: {
                  node: {
                    comments: { pageInfo: terminalPage, nodes: [currentResolution] },
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
                    assignees: {
                      pageInfo: terminalPage,
                      nodes: [{ login: "worker-two" }],
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
                    timelineItems: {
                      pageInfo: terminalPage,
                      nodes: [{ createdAt: "2026-09-05T22:00:00Z" }],
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
          number: 7,
        });

        expect(result.readiness).toMatchObject({ status: "resolved" });
        expect(result.evidence?.records).toHaveLength(2);
        expect(result.evidence?.records.map((record) => record.state)).toEqual([
          "superseded",
          "current",
        ]);
        expect(execute).toHaveBeenCalledTimes(4);
        expect(execute.mock.calls[1]?.[0].args).toContain("after=comments-2");
        expect(execute.mock.calls[2]?.[0].args).toContain("after=assignees-2");
        expect(execute.mock.calls[3]?.[0].args).toContain("after=timeline-2");
      }).pipe(Effect.provide(layer(execute)));
    },
  );

  it.effect(
    "holds source-linked manual alternatives until a specific reassessment clears them",
    () => {
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      const condition =
        "Provide a staging account or an approved fixture ([preparation](https://github.com/acme/runbook/issues/4)).";
      const otherCondition = "Requires: Confirm the release operator.";
      const otherConditionDescription = "Confirm the release operator.";
      const extra = `## Outside prerequisites\n\n- ${condition}\n- ${otherCondition}`;
      const body = ticketScopeBody({
        id: "T04",
        title: "Manual preparation",
        what: "Prepare the integration.",
        extra,
      });
      const reassessment = workflowComment({
        id: "condition-cleared",
        createdAt: "2026-09-05T22:00:00Z",
        body: [
          "## Reassessment",
          "<!-- t3-workflow:v1 reassessment -->",
          "Trigger: https://github.com/acme/runbook/issues/4",
          "Outcome: cleared",
          "### Changes",
          condition,
          "### Evidence",
          "[Prepared fixture](https://github.com/acme/fixtures/issues/9)",
        ].join("\n"),
      });
      execute
        .mockReturnValueOnce(
          Effect.succeed(
            processOutput(
              JSON.stringify({
                data: {
                  repository: {
                    issue: evidenceIssue({
                      number: 14,
                      parent: 10,
                      title: "Manual preparation",
                      body,
                      comments: [reassessment],
                    }),
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
                  node: evidenceIssue({
                    number: 10,
                    title: "Capability",
                    labels: ["workflow:capability"],
                    comments: [
                      breakdownApproval([{ id: "T04", title: "Manual preparation", body }]),
                    ],
                  }),
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
          number: 14,
        });

        expect(result.evidence?.manualConditions).toEqual([
          expect.objectContaining({
            description: condition,
            source: "https://github.com/acme/runbook/issues/4",
            status: "satisfied",
          }),
          expect.objectContaining({
            description: otherConditionDescription,
            status: "review-required",
          }),
        ]);
        expect(result.readiness).toMatchObject({ status: "needs-review" });
        expect(result.readiness?.reasons).toContainEqual(
          expect.objectContaining({
            kind: "manual-condition",
            message: expect.stringContaining(otherConditionDescription),
          }),
        );
      }).pipe(Effect.provide(layer(execute)));
    },
  );

  it.effect("keeps cancelled and superseded prerequisites out of the frontier", () => {
    const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
    const cancelled = {
      ...issue(40),
      state: "CLOSED",
      stateReason: "NOT_PLANNED",
      labels: { pageInfo: terminalPage, nodes: [{ name: "workflow:ticket" }] },
    };
    const superseded = {
      ...issue(41),
      state: "CLOSED",
      stateReason: "NOT_PLANNED",
      labels: {
        pageInfo: terminalPage,
        nodes: [{ name: "workflow:ticket" }, { name: "workflow:superseded" }],
      },
    };
    execute.mockReturnValueOnce(
      Effect.succeed(
        processOutput(
          JSON.stringify({
            data: {
              repository: {
                issue: {
                  id: "issue-30",
                  body: "Container",
                  comments: { pageInfo: terminalPage, nodes: [] },
                  subIssues: {
                    pageInfo: terminalPage,
                    nodes: [
                      evidenceIssue({
                        number: 31,
                        parent: 30,
                        labels: ["workflow:task"],
                        blockedBy: [cancelled, superseded],
                      }),
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
        parentNumber: 30,
      });

      expect(result.children[0]?.readiness).toMatchObject({ status: "blocked" });
      expect(result.children[0]?.readiness?.reasons.map((item) => item.kind)).toEqual([
        "cancelled-blocker",
        "superseded-blocker",
      ]);
      expect(result.frontier?.status).toBe("empty-blocked");
      expect(execute).toHaveBeenCalledTimes(1);
    }).pipe(Effect.provide(layer(execute)));
  });

  it.effect(
    "bounds a fifty-sibling frontier to one GitHub query when evidence is on the page",
    () => {
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      execute.mockReturnValueOnce(
        Effect.succeed(
          processOutput(
            JSON.stringify({
              data: {
                repository: {
                  issue: {
                    id: "issue-50",
                    body: "Container",
                    comments: { pageInfo: terminalPage, nodes: [] },
                    subIssues: {
                      pageInfo: terminalPage,
                      nodes: Array.from({ length: 50 }, (_, index) =>
                        evidenceIssue({
                          number: 100 + index,
                          parent: 50,
                          labels: ["workflow:task"],
                          assignees: ["worker"],
                        }),
                      ),
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
          parentNumber: 50,
        });

        expect(result.children).toHaveLength(50);
        expect(result.frontier).toMatchObject({ status: "empty-claimed", readyIssueIds: [] });
        expect(execute).toHaveBeenCalledTimes(1);
      }).pipe(Effect.provide(layer(execute)));
    },
  );
});
