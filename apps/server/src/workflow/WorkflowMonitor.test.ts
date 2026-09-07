import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type WorkflowIssueDetail,
  WorkflowQueryError,
  type WorkflowDirectorStatus,
  type WorkflowRootsResult,
  type WorkflowSyncState,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { workflowDirectorHandlers } from "../mcp/toolkits/workflow/handlers.ts";
import * as WorkflowDirectorService from "./WorkflowDirectorService.ts";
import * as WorkflowMonitor from "./WorkflowMonitor.ts";
import * as WorkflowService from "./WorkflowService.ts";

const environmentId = EnvironmentId.make("environment-1");
const projectOne = ProjectId.make("project-1");
const projectTwo = ProjectId.make("project-2");
const repository = "Flow-Fly/t3code" as const;

function roots(title: string): WorkflowRootsResult {
  return {
    repository,
    roots: [
      {
        id: `issue-${title}`,
        repository,
        number: 10,
        title,
        url: `https://github.com/${repository}/issues/10`,
        kind: "capability",
        state: "open",
        stateReason: null,
        updatedAt: "2026-09-07T10:00:00.000Z",
        childCount: 0,
        parentNumber: null,
        labels: ["workflow:capability"],
      },
    ],
  };
}

type ReviewStatus = NonNullable<WorkflowDirectorStatus["reviews"]>[number];

function reviewStatus(input: {
  readonly status: ReviewStatus["status"];
  readonly association?: ReviewStatus["association"];
  readonly providerThreadId?: string | null;
  readonly settlementEvidence?: ReviewStatus["settlementEvidence"];
  readonly axes?: ReviewStatus["axes"];
}): ReviewStatus {
  return {
    reviewId: "review-1",
    admissionId: "admission-review-1",
    ticketNumber: 21,
    implementationProviderThreadId: "implementation-thread",
    fixedBase: "base",
    implementationHead: "head",
    status: input.status,
    association: input.association ?? "unconfirmed",
    providerThreadId: input.providerThreadId ?? null,
    parentProviderThreadId: "implementation-thread",
    providerStatus: "idle",
    settlementEvidence: input.settlementEvidence ?? null,
    requestedProfile: { model: "gpt-6-astra", effort: "high", skillPath: "/implement" },
    observedProfile: { model: "gpt-6-astra", effort: "high", match: "match" },
    checks: [],
    axes: input.axes ?? [],
    findings: [],
    summary: null,
    updatedAt: "2026-09-07T10:00:00.000Z",
  };
}

function reviewAxis(
  axis: "standards" | "spec",
  settlementEvidence: "native-closed" | null,
): ReviewStatus["axes"][number] {
  return {
    axis,
    providerThreadId: `${axis}-thread`,
    parentProviderThreadId: "review-thread",
    providerStatus: settlementEvidence === "native-closed" ? "closed" : "idle",
    settlementEvidence,
    observedProfile: { model: "gpt-6-astra", effort: "high", match: "match" },
  };
}

function directorStatus(
  workers: WorkflowDirectorStatus["workers"],
  reviews: WorkflowDirectorStatus["reviews"] = [],
): WorkflowDirectorStatus {
  return {
    directorId: "director-1",
    batchId: "batch-1",
    environmentId,
    projectId: projectOne,
    repository,
    rootNumber: 1,
    capabilityNumber: 10,
    threadId: ThreadId.make("director-thread"),
    worktreePath: "/tmp/worktree",
    worktreeBranch: "capability/workflow-10",
    status: "active",
    requestedProfile: {
      instanceId: "codex-workflow",
      model: "gpt-6-astra",
      effort: "high",
    },
    observedProfile: { model: "gpt-6-astra", effort: "high", match: "match" },
    admissionCount: 1,
    admissionLimit: 10,
    workers,
    reviews,
    observation: "Director is active.",
    actions: ["open"],
    createdAt: "2026-09-07T10:00:00.000Z",
    updatedAt: "2026-09-07T10:00:00.000Z",
    message: "Director is active.",
  };
}

function monitorLayer(input: {
  readonly loadRoots: WorkflowService.WorkflowService["Service"]["roots"];
  readonly onRead?:
    | ((
        operation: "children" | "detail",
        repository: string,
        projectId: ProjectId,
      ) => Effect.Effect<void>)
    | undefined;
  readonly validProjects?: ReadonlySet<ProjectId> | undefined;
  readonly status?:
    | WorkflowDirectorService.WorkflowDirectorService["Service"]["status"]
    | undefined;
}) {
  return WorkflowMonitor.layer.pipe(
    Layer.provideMerge(
      Layer.mock(WorkflowService.WorkflowService)({
        validateProject: (projectId) =>
          (input.validProjects ?? new Set([projectOne, projectTwo])).has(projectId)
            ? Effect.void
            : Effect.fail(
                new WorkflowQueryError({
                  failure: "project-not-found",
                  message: "This project is no longer available.",
                }),
              ),
        roots: input.loadRoots,
        children: ({ projectId, repository, parentNumber }) =>
          Effect.gen(function* () {
            const result = {
              parentNumber,
              children: [],
              frontier: {
                status: "empty" as const,
                message: "No immediate work is visible in this branch.",
                readyIssueIds: [],
              },
            };
            if (input.onRead) yield* input.onRead("children", repository, projectId);
            return result;
          }),
        issueDetail: ({ projectId, repository, number }) =>
          Effect.gen(function* () {
            const detail: WorkflowIssueDetail = {
              ...roots("Capability").roots[0]!,
              repository,
              number,
              body: "",
              blockedBy: [],
            };
            if (input.onRead) yield* input.onRead("detail", repository, projectId);
            return detail;
          }),
      }),
    ),
    Layer.provideMerge(
      Layer.mock(WorkflowDirectorService.WorkflowDirectorService)({
        status: input.status ?? (() => Effect.succeed(directorStatus([]))),
      }),
    ),
    Layer.provideMerge(SqlitePersistenceMemory),
  );
}

const nextStatus = Effect.fn("WorkflowMonitorTest.nextStatus")(function* (
  queue: Queue.Queue<WorkflowSyncState>,
  predicate: (state: WorkflowSyncState) => boolean,
) {
  while (true) {
    const state = yield* Queue.take(queue);
    if (predicate(state)) return state;
  }
});

describe("WorkflowMonitor", () => {
  it.effect("publishes invalidation after a failed director mutation", () =>
    Effect.gen(function* () {
      let notifications = 0;
      const result = yield* workflowDirectorHandlers
        .workflow_prepare_worker({
          ticketNumber: 21,
          ownership: "monitoring lifecycle",
          writePaths: ["apps/server/src/workflow"],
        })
        .pipe(
          Effect.provide(
            Layer.mock(WorkflowDirectorService.WorkflowDirectorService)({
              prepareWorker: () => Effect.die("simulated failed mutation"),
            }),
          ),
          Effect.provideService(McpInvocationContext.McpInvocationContext, {
            environmentId,
            threadId: ThreadId.make("director-thread"),
            providerSessionId: "provider-session",
            providerInstanceId: ProviderInstanceId.make("codex-workflow"),
            capabilities: new Set<"preview">(),
            issuedAt: 1,
          }),
          Effect.provideService(WorkflowMonitor.WorkflowRefreshNotifier, {
            directorChanged: () =>
              Effect.sync(() => {
                notifications += 1;
              }),
          }),
          Effect.exit,
        );
      expect(result._tag).toBe("Failure");
      expect(notifications).toBe(1);
    }),
  );

  it.effect("shares one in-flight refresh across two projects watching the same repository", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let sharedCalls = 0;
      let controlCalls = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const firstEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const secondEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const first = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(firstEvents, state)),
          Effect.forkScoped,
        );
        yield* Deferred.await(started);
        const second = yield* monitor
          .watch({ projectId: projectTwo, repository: repository.toLowerCase() })
          .pipe(
            Stream.runForEach((state) => Queue.offer(secondEvents, state)),
            Effect.forkScoped,
          );
        yield* Effect.yieldNow;
        expect(sharedCalls).toBe(1);
        yield* Deferred.succeed(release, undefined);
        yield* nextStatus(firstEvents, (state) => state.status === "fresh");
        yield* nextStatus(secondEvents, (state) => state.status === "fresh");
        expect((yield* monitor.roots({ projectId: projectTwo, repository })).roots[0]?.title).toBe(
          "shared",
        );
        expect(sharedCalls).toBe(1);
        yield* Fiber.interrupt(second);
        const reconnectEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const reconnected = yield* monitor.watch({ projectId: projectTwo, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(reconnectEvents, state)),
          Effect.forkScoped,
        );
        yield* nextStatus(reconnectEvents, (state) => state.status === "fresh");
        expect(sharedCalls).toBe(2);
        yield* Fiber.interrupt(first);
        yield* Fiber.interrupt(reconnected);
        const controlEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const control = yield* monitor
          .watch({ projectId: projectOne, repository: "Flow-Fly/control" })
          .pipe(
            Stream.runForEach((state) => Queue.offer(controlEvents, state)),
            Effect.forkScoped,
          );
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        yield* TestClock.adjust("30 seconds");
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        expect(controlCalls).toBe(2);
        expect(sharedCalls).toBe(2);
        yield* Fiber.interrupt(control);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: ({ repository: requestedRepository }) =>
              Effect.gen(function* () {
                if (requestedRepository.toLowerCase() === "flow-fly/control") {
                  controlCalls += 1;
                  return roots("control");
                }
                sharedCalls += 1;
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(release);
                return roots("shared");
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("requires a follow-up read when invalidated during an older refresh", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const releaseSecond = yield* Deferred.make<void>();
      let calls = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const events = yield* Queue.unbounded<WorkflowSyncState>();
        const watcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(events, state)),
          Effect.forkScoped,
        );
        yield* Deferred.await(firstStarted);
        yield* monitor.invalidate({ projectId: projectOne, repository });
        yield* Deferred.succeed(releaseFirst, undefined);
        yield* Deferred.await(secondStarted);
        const stale = yield* nextStatus(events, (state) => state.status === "stale");
        expect(stale.message).toContain("newer read");
        expect((yield* monitor.roots({ projectId: projectOne, repository })).roots[0]?.title).toBe(
          "older",
        );
        yield* Deferred.succeed(releaseSecond, undefined);
        yield* nextStatus(events, (state) => state.status === "fresh");
        expect((yield* monitor.roots({ projectId: projectOne, repository })).roots[0]?.title).toBe(
          "newer",
        );
        expect(calls).toBe(2);
        yield* Fiber.interrupt(watcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () =>
              Effect.gen(function* () {
                calls += 1;
                if (calls === 1) {
                  yield* Deferred.succeed(firstStarted, undefined);
                  yield* Deferred.await(releaseFirst);
                  return roots("older");
                }
                yield* Deferred.succeed(secondStarted, undefined);
                yield* Deferred.await(releaseSecond);
                return roots("newer");
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("single-flights the same uncached path and follows up for a path added in flight", () =>
    Effect.gen(function* () {
      const rootStarted = yield* Deferred.make<void>();
      const releaseRoot = yield* Deferred.make<void>();
      let rootReads = 0;
      let detailReads = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const first = yield* monitor
          .roots({ projectId: projectOne, repository })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(rootStarted);
        const second = yield* monitor
          .roots({ projectId: projectTwo, repository })
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(rootReads).toBe(1);

        const laterPath = yield* monitor
          .issueDetail({ projectId: projectOne, repository, number: 11 })
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(detailReads).toBe(0);
        yield* Deferred.succeed(releaseRoot, undefined);

        expect((yield* Fiber.join(first)).roots[0]?.title).toBe("shared root");
        expect((yield* Fiber.join(second)).roots[0]?.title).toBe("shared root");
        expect((yield* Fiber.join(laterPath)).number).toBe(11);
        expect(detailReads).toBe(1);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () =>
              Effect.gen(function* () {
                rootReads += 1;
                if (rootReads === 1) {
                  yield* Deferred.succeed(rootStarted, undefined);
                  yield* Deferred.await(releaseRoot);
                }
                return roots("shared root");
              }),
            onRead: (operation) =>
              Effect.sync(() => {
                if (operation === "detail") detailReads += 1;
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("backs off after rate limits and recovers without discarding cached data", () =>
    Effect.gen(function* () {
      let calls = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const events = yield* Queue.unbounded<WorkflowSyncState>();
        const watcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(events, state)),
          Effect.forkScoped,
        );
        yield* nextStatus(events, (state) => state.status === "fresh");
        const limited = yield* monitor.refresh({ projectId: projectOne, repository });
        expect(limited.status).toBe("rate-limited");
        expect(limited.lastSuccessfulAt).not.toBeNull();
        expect(limited.retryAt).not.toBeNull();
        expect((yield* monitor.roots({ projectId: projectOne, repository })).roots[0]?.title).toBe(
          "cached",
        );
        yield* TestClock.adjust("29 seconds");
        expect(calls).toBe(2);
        yield* TestClock.adjust("1 second");
        yield* nextStatus(events, (state) => state.status === "fresh");
        expect(calls).toBe(3);
        expect((yield* monitor.roots({ projectId: projectOne, repository })).roots).toHaveLength(1);
        yield* Fiber.interrupt(watcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () => {
              calls += 1;
              return calls === 2
                ? Effect.fail(
                    new WorkflowQueryError({
                      failure: "github-rate-limited",
                      message: "GitHub API rate limit exceeded.",
                    }),
                  )
                : Effect.succeed(roots(calls === 1 ? "cached" : "recovered"));
            },
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect(
    "backs off first-load failures across status-driven reads and recovers on cadence",
    () =>
      Effect.gen(function* () {
        let calls = 0;
        let recovered = false;
        const program = Effect.gen(function* () {
          const monitor = yield* WorkflowMonitor.WorkflowMonitor;
          const events = yield* Queue.unbounded<WorkflowSyncState>();
          const watcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
            Stream.runForEach((state) => Queue.offer(events, state)),
            Effect.forkScoped,
          );

          const limited = yield* nextStatus(events, (state) => state.status === "rate-limited");
          expect(limited.lastSuccessfulAt).toBeNull();
          expect(limited.retryAt).not.toBeNull();
          for (let index = 0; index < 5; index += 1) {
            const result = yield* monitor
              .roots({ projectId: projectOne, repository })
              .pipe(Effect.exit);
            expect(result._tag).toBe("Failure");
          }
          expect(calls).toBe(1);

          yield* TestClock.adjust("29 seconds");
          expect(calls).toBe(1);
          recovered = true;
          yield* TestClock.adjust("1 second");
          yield* nextStatus(events, (state) => state.status === "fresh");
          expect(calls).toBe(2);
          expect(
            (yield* monitor.roots({ projectId: projectOne, repository })).roots[0]?.title,
          ).toBe("recovered");
          yield* Fiber.interrupt(watcher);
        });
        yield* program.pipe(
          Effect.provide(
            monitorLayer({
              loadRoots: () => {
                calls += 1;
                return recovered
                  ? Effect.succeed(roots("recovered"))
                  : Effect.fail(
                      new WorkflowQueryError({
                        failure: "github-rate-limited",
                        message: "GitHub API rate limit exceeded.",
                      }),
                    );
              },
            }),
          ),
          Effect.scoped,
        );
      }),
  );

  it.effect("rejects a deleted project before a cache hit can replace shared routing", () =>
    Effect.gen(function* () {
      const requestedProjects = new Array<ProjectId>();
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const sql = yield* SqlClient.SqlClient;
        const events = yield* Queue.unbounded<WorkflowSyncState>();
        const watcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(events, state)),
          Effect.forkScoped,
        );
        yield* nextStatus(events, (state) => state.status === "fresh");
        const now = "2026-09-07T10:00:00.000Z";
        yield* sql`
          INSERT INTO projection_threads
            (thread_id, project_id, title, created_at, updated_at, archived_at, deleted_at)
          VALUES ('thread-invalid-route', ${projectTwo}, 'Invalid route', ${now}, ${now}, NULL, NULL)
        `;
        yield* sql`
          INSERT INTO workflow_directors
            (director_id, batch_id, environment_id, project_id, repository, root_number,
             capability_number, thread_id, command_id, message_id, worktree_path,
             worktree_branch, status, requested_model, requested_instance_id,
             requested_effort, observed_model, observed_effort, observed_match, sequence,
             initial_turn_disposition, is_current, created_at, updated_at)
          VALUES ('director-invalid-route', 'batch', ${environmentId}, ${projectTwo}, ${repository}, 1,
            10, 'thread-invalid-route', 'command-invalid-route', 'message-invalid-route',
            '/tmp/worktree', 'branch', 'active', 'gpt-6-astra', 'codex-workflow', 'high',
            'gpt-6-astra', 'high', 'match', 1, 'accepted', 1, ${now}, ${now})
        `;

        const rejected = yield* monitor
          .roots({ projectId: projectTwo, repository })
          .pipe(Effect.flip);
        expect(rejected.failure).toBe("project-not-found");

        yield* TestClock.adjust("30 seconds");
        yield* nextStatus(events, (state) => state.status === "fresh" && state.revision > 2);
        expect(requestedProjects).toEqual([projectOne, projectOne]);
        yield* Fiber.interrupt(watcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            validProjects: new Set([projectOne]),
            loadRoots: ({ projectId }) => {
              requestedProjects.push(projectId);
              return projectId === projectOne
                ? Effect.succeed(roots("valid"))
                : Effect.fail(
                    new WorkflowQueryError({
                      failure: "project-not-found",
                      message: "This project is no longer available.",
                    }),
                  );
            },
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("continues invalidating healthy repositories after a retained project is deleted", () =>
    Effect.gen(function* () {
      const validProjects = new Set([projectOne, projectTwo]);
      const reads = new Map<string, number>();
      const otherRepository = "Flow-Fly/other" as const;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        yield* monitor.roots({ projectId: projectOne, repository });
        yield* monitor.roots({ projectId: projectTwo, repository: otherRepository });
        const events = yield* Queue.unbounded<WorkflowSyncState>();
        const watcher = yield* monitor
          .watch({
            projectId: projectTwo,
            repository: otherRepository,
          })
          .pipe(
            Stream.runForEach((state) => Queue.offer(events, state)),
            Effect.forkScoped,
          );
        yield* nextStatus(events, (state) => state.status === "fresh");
        expect(reads.get(otherRepository)).toBe(2);

        validProjects.delete(projectOne);
        yield* monitor.invalidateAll;
        yield* nextStatus(events, (state) => state.status === "fresh");

        expect(reads.get(repository)).toBe(1);
        expect(reads.get(otherRepository)).toBe(3);
        yield* Fiber.interrupt(watcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            validProjects,
            loadRoots: ({ repository: requestedRepository }) =>
              Effect.sync(() => {
                reads.set(requestedRepository, (reads.get(requestedRepository) ?? 0) + 1);
                return {
                  ...roots(requestedRepository),
                  repository: requestedRepository,
                  roots: [],
                };
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("isolates a deleted watched route while healthy repositories continue on cadence", () =>
    Effect.gen(function* () {
      const validProjects = new Set([projectOne, projectTwo]);
      const reads = new Map<string, number>();
      const otherRepository = "Flow-Fly/other" as const;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const deletedEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const healthyEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const deletedWatcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(deletedEvents, state)),
          Effect.forkScoped,
        );
        const healthyWatcher = yield* monitor
          .watch({ projectId: projectTwo, repository: otherRepository })
          .pipe(
            Stream.runForEach((state) => Queue.offer(healthyEvents, state)),
            Effect.forkScoped,
          );
        yield* nextStatus(deletedEvents, (state) => state.status === "fresh");
        yield* nextStatus(healthyEvents, (state) => state.status === "fresh");

        validProjects.delete(projectOne);
        yield* TestClock.adjust("30 seconds");
        const deleted = yield* nextStatus(deletedEvents, (state) => state.status === "unavailable");
        yield* nextStatus(healthyEvents, (state) => state.status === "fresh");

        expect(deleted.message).toContain("no longer available");
        expect(deleted.retryAt).not.toBeNull();
        expect(reads.get(repository)).toBe(1);
        expect(reads.get(otherRepository)).toBe(2);
        yield* Fiber.interrupt(deletedWatcher);
        yield* Fiber.interrupt(healthyWatcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            validProjects,
            loadRoots: ({ repository: requestedRepository }) =>
              Effect.sync(() => {
                reads.set(requestedRepository, (reads.get(requestedRepository) ?? 0) + 1);
                return {
                  ...roots(requestedRepository),
                  repository: requestedRepository,
                  roots: [],
                };
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect(
    "renews active paths through a retained valid route after the director project expires",
    () =>
      Effect.gen(function* () {
        const pathReads = new Array<{ operation: string; projectId: ProjectId }>();
        const cadenceCompleted = yield* Deferred.make<void>();
        const program = Effect.gen(function* () {
          const monitor = yield* WorkflowMonitor.WorkflowMonitor;
          const sql = yield* SqlClient.SqlClient;
          yield* monitor.roots({ projectId: projectOne, repository });
          const now = "2026-09-07T10:00:00.000Z";
          yield* sql`
          INSERT INTO projection_threads
            (thread_id, project_id, title, created_at, updated_at, archived_at, deleted_at)
          VALUES ('thread-expired-director', ${projectTwo}, 'Expired director', ${now}, ${now}, NULL, NULL)
        `;
          yield* sql`
          INSERT INTO workflow_directors
            (director_id, batch_id, environment_id, project_id, repository, root_number,
             capability_number, thread_id, command_id, message_id, worktree_path,
             worktree_branch, status, requested_model, requested_instance_id,
             requested_effort, observed_model, observed_effort, observed_match, sequence,
             initial_turn_disposition, is_current, created_at, updated_at)
          VALUES ('director-expired-project', 'batch', ${environmentId}, ${projectTwo}, ${repository}, 1,
            10, 'thread-expired-director', 'command-expired-project', 'message-expired-project',
            '/tmp/worktree', 'branch', 'active', 'gpt-6-astra', 'codex-workflow', 'high',
            'gpt-6-astra', 'high', 'match', 1, 'accepted', 1, ${now}, ${now})
        `;

          yield* TestClock.adjust("90 seconds");
          yield* Deferred.await(cadenceCompleted);
          expect(pathReads.filter(({ operation }) => operation === "detail")).toHaveLength(3);
          expect(pathReads.filter(({ operation }) => operation === "children")).toHaveLength(3);
          expect(pathReads.every(({ projectId }) => projectId === projectOne)).toBe(true);
        });
        yield* program.pipe(
          Effect.provide(
            monitorLayer({
              validProjects: new Set([projectOne]),
              loadRoots: () => Effect.succeed(roots("valid route")),
              onRead: (operation, _repository, projectId) =>
                Effect.gen(function* () {
                  pathReads.push({ operation, projectId });
                  if (pathReads.length === 6) yield* Deferred.succeed(cadenceCompleted, undefined);
                }),
            }),
          ),
          Effect.scoped,
        );
      }),
  );

  it.effect("does not requeue a captured path waiting for a refresh slot", () =>
    Effect.gen(function* () {
      const fourRefreshesStarted = yield* Deferred.make<void>();
      const releaseRefreshes = yield* Deferred.make<void>();
      let mode: "warm" | "outage" | "recovery" = "warm";
      let refreshReads = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        for (const number of [11, 12, 13, 14, 15]) {
          yield* monitor.issueDetail({ projectId: projectOne, repository, number });
        }
        yield* TestClock.adjust("61 seconds");

        mode = "outage";
        const failed = yield* monitor.refresh({ projectId: projectOne, repository });
        expect(failed.status).toBe("unavailable");
        for (const number of [11, 12, 13, 14, 15]) {
          yield* monitor.issueDetail({ projectId: projectOne, repository, number });
        }
        yield* TestClock.adjust("30 seconds");

        mode = "recovery";
        const refresh = yield* monitor
          .refresh({ projectId: projectOne, repository })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(fourRefreshesStarted);
        expect(refreshReads).toBe(4);

        expect(
          (yield* monitor.issueDetail({ projectId: projectOne, repository, number: 15 })).number,
        ).toBe(15);
        yield* Deferred.succeed(releaseRefreshes, undefined);
        const completed = yield* Fiber.join(refresh);

        expect(completed.status).toBe("fresh");
        expect(refreshReads).toBe(5);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () =>
              mode === "outage"
                ? Effect.fail(
                    new WorkflowQueryError({
                      failure: "request-failed",
                      message: "GitHub is unavailable.",
                    }),
                  )
                : Effect.succeed(roots("current")),
            onRead: (operation) =>
              Effect.gen(function* () {
                if (mode !== "recovery" || operation !== "detail") return;
                refreshReads += 1;
                if (refreshReads === 4) {
                  yield* Deferred.succeed(fourRefreshesStarted, undefined);
                }
                yield* Deferred.await(releaseRefreshes);
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("does not retry captured paths before outage backoff", () =>
    Effect.gen(function* () {
      const fourRefreshesStarted = yield* Deferred.make<void>();
      const releaseRefreshes = yield* Deferred.make<void>();
      let mode: "warm" | "outage" | "recovery" = "warm";
      let refreshReads = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        for (const number of [11, 12, 13, 14, 15]) {
          yield* monitor.issueDetail({ projectId: projectOne, repository, number });
        }
        yield* TestClock.adjust("61 seconds");

        mode = "outage";
        const failed = yield* monitor.refresh({ projectId: projectOne, repository });
        expect(failed.status).toBe("unavailable");
        for (const number of [11, 12, 13, 14, 15]) {
          yield* monitor.issueDetail({ projectId: projectOne, repository, number });
        }
        yield* TestClock.adjust("30 seconds");

        mode = "recovery";
        const refresh = yield* monitor
          .refresh({ projectId: projectOne, repository })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(fourRefreshesStarted);
        expect(refreshReads).toBe(4);

        expect(
          (yield* monitor.issueDetail({ projectId: projectOne, repository, number: 15 })).number,
        ).toBe(15);
        yield* Deferred.succeed(releaseRefreshes, undefined);
        const completed = yield* Fiber.join(refresh);

        expect(completed.status).toBe("unavailable");
        expect(completed.retryAt).not.toBeNull();
        expect(refreshReads).toBe(5);
        for (const number of [11, 12, 13, 14, 15]) {
          yield* monitor.issueDetail({ projectId: projectOne, repository, number });
        }
        expect(refreshReads).toBe(5);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () =>
              mode === "warm"
                ? Effect.succeed(roots("current"))
                : Effect.fail(
                    new WorkflowQueryError({
                      failure: "request-failed",
                      message: "GitHub is unavailable.",
                    }),
                  ),
            onRead: (operation) =>
              Effect.gen(function* () {
                if (mode !== "recovery" || operation !== "detail") return;
                refreshReads += 1;
                if (refreshReads === 4) {
                  yield* Deferred.succeed(fourRefreshesStarted, undefined);
                }
                yield* Deferred.await(releaseRefreshes);
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("refreshes a cached path that returns after an in-flight snapshot", () =>
    Effect.gen(function* () {
      const rootStarted = yield* Deferred.make<void>();
      const releaseRoot = yield* Deferred.make<void>();
      let rootReads = 0;
      let detailReads = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 });
        yield* TestClock.adjust("61 seconds");

        const refresh = yield* monitor
          .refresh({ projectId: projectOne, repository })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(rootStarted);
        expect(
          (yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 })).number,
        ).toBe(11);
        yield* Deferred.succeed(releaseRoot, undefined);
        yield* Fiber.join(refresh);
        expect(rootReads).toBe(2);
        expect(detailReads).toBe(2);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () =>
              Effect.gen(function* () {
                rootReads += 1;
                yield* Deferred.succeed(rootStarted, undefined);
                yield* Deferred.await(releaseRoot);
                return roots("current");
              }),
            onRead: (operation) =>
              Effect.sync(() => {
                if (operation === "detail") detailReads += 1;
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("refreshes a cached path after each retirement in the same epoch", () =>
    Effect.gen(function* () {
      const firstCadenceRootStarted = yield* Deferred.make<void>();
      const releaseFirstCadenceRoot = yield* Deferred.make<void>();
      const secondCadenceRootStarted = yield* Deferred.make<void>();
      const releaseSecondCadenceRoot = yield* Deferred.make<void>();
      const firstReactivationCompleted = yield* Deferred.make<void>();
      const secondReactivationCompleted = yield* Deferred.make<void>();
      let rootReads = 0;
      let detailReads = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        const events = yield* Queue.unbounded<WorkflowSyncState>();
        const watcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(events, state)),
          Effect.forkScoped,
        );
        yield* nextStatus(events, (state) => state.status === "fresh");
        yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 });

        yield* TestClock.adjust("121 seconds");
        yield* Deferred.await(firstCadenceRootStarted);
        yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 });
        yield* Deferred.succeed(releaseFirstCadenceRoot, undefined);
        yield* Deferred.await(firstReactivationCompleted);
        expect(detailReads).toBe(4);

        yield* TestClock.adjust("120 seconds");
        yield* Deferred.await(secondCadenceRootStarted);
        yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 });
        const joinedRefresh = yield* monitor
          .refresh({ projectId: projectOne, repository })
          .pipe(Effect.forkScoped);
        yield* Deferred.succeed(releaseSecondCadenceRoot, undefined);
        yield* Fiber.join(joinedRefresh);
        yield* Deferred.await(secondReactivationCompleted);
        expect(detailReads).toBe(7);
        yield* Fiber.interrupt(watcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () =>
              Effect.gen(function* () {
                rootReads += 1;
                if (rootReads === 6) {
                  yield* Deferred.succeed(firstCadenceRootStarted, undefined);
                  yield* Deferred.await(releaseFirstCadenceRoot);
                }
                if (rootReads === 11) {
                  yield* Deferred.succeed(secondCadenceRootStarted, undefined);
                  yield* Deferred.await(releaseSecondCadenceRoot);
                }
                return roots("current");
              }),
            onRead: (operation) =>
              Effect.gen(function* () {
                if (operation !== "detail") return;
                detailReads += 1;
                if (detailReads === 4) {
                  yield* Deferred.succeed(firstReactivationCompleted, undefined);
                }
                if (detailReads === 7) {
                  yield* Deferred.succeed(secondReactivationCompleted, undefined);
                }
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("keeps archived directors only while durable child execution is unsettled", () =>
    Effect.gen(function* () {
      const reads = new Array<{ operation: string; repository: string }>();
      const expected = [
        "archived-held",
        "missing-held",
        "deleted-held",
        "archived-review-issued",
        "archived-review-descendant",
        "archived-review-reported",
        "open",
      ] as const;
      const cadenceReadsCompleted = yield* Deferred.make<void>();
      const program = Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const now = "2026-09-07T10:00:00.000Z";
        const cases = [
          {
            suffix: "archived-held",
            capability: 10,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-closed",
            capability: 20,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "missing-held",
            capability: 30,
            archived: null,
            deleted: null,
            status: "active",
            disposition: "accepted",
            missing: true,
          },
          {
            suffix: "deleted-held",
            capability: 40,
            archived: null,
            deleted: now,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "deleted-closed",
            capability: 50,
            archived: null,
            deleted: now,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-worker-native-closed",
            capability: 80,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-review-issued",
            capability: 90,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-review-native-closed",
            capability: 100,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-review-descendant",
            capability: 110,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-review-never-issued",
            capability: 120,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "archived-review-reported",
            capability: 130,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "invalid-project",
            capability: 140,
            archived: now,
            deleted: null,
            status: "active",
            disposition: "accepted",
            projectId: projectTwo,
          },
          {
            suffix: "open",
            capability: 60,
            archived: null,
            deleted: null,
            status: "active",
            disposition: "accepted",
          },
          {
            suffix: "failed-setup",
            capability: 70,
            archived: null,
            deleted: null,
            status: "preparing-worktree",
            disposition: "not-attempted",
          },
        ] as const;
        for (const entry of cases) {
          const repo = `Flow-Fly/${entry.suffix}`;
          if (!("missing" in entry)) {
            yield* sql`
              INSERT INTO projection_threads
                (thread_id, project_id, title, created_at, updated_at, archived_at, deleted_at)
              VALUES (${`thread-${entry.suffix}`}, ${"projectId" in entry ? entry.projectId : projectOne}, ${entry.suffix}, ${now}, ${now}, ${entry.archived}, ${entry.deleted})
            `;
          }
          yield* sql`
            INSERT INTO workflow_directors
              (director_id, batch_id, environment_id, project_id, repository, root_number,
               capability_number, thread_id, command_id, message_id, worktree_path,
               worktree_branch, status, requested_model, requested_instance_id,
               requested_effort, observed_model, observed_effort, observed_match, sequence,
               initial_turn_disposition, is_current, created_at, updated_at)
            VALUES (${`director-${entry.suffix}`}, 'batch', ${environmentId}, ${"projectId" in entry ? entry.projectId : projectOne}, ${repo}, 1,
              ${entry.capability}, ${`thread-${entry.suffix}`},
              ${`command-${entry.suffix}`}, ${`message-${entry.suffix}`}, '/tmp/worktree', 'branch', ${entry.status},
              'gpt-6-astra', 'codex-workflow', 'high', 'gpt-6-astra', 'high', 'match', 1,
              ${entry.disposition}, 1, ${now}, ${now})
          `;
        }
        yield* TestClock.adjust("30 seconds");
        yield* Deferred.await(cadenceReadsCompleted);

        for (const suffix of expected) {
          expect(reads.filter((read) => read.repository === `Flow-Fly/${suffix}`)).toHaveLength(2);
        }
        for (const suffix of [
          "archived-closed",
          "deleted-closed",
          "archived-worker-native-closed",
          "archived-review-native-closed",
          "archived-review-never-issued",
          "invalid-project",
          "failed-setup",
        ]) {
          expect(reads.some((read) => read.repository === `Flow-Fly/${suffix}`)).toBe(false);
        }
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            validProjects: new Set([projectOne]),
            loadRoots: () => Effect.succeed(roots("unused")),
            onRead: (operation, repo) =>
              Effect.gen(function* () {
                reads.push({ operation, repository: repo });
                if (
                  expected.every(
                    (suffix) =>
                      reads.filter((read) => read.repository === `Flow-Fly/${suffix}`).length === 2,
                  )
                ) {
                  yield* Deferred.succeed(cadenceReadsCompleted, undefined);
                }
              }),
            status: ({ capabilityNumber }) =>
              Effect.sync(() => {
                const heldWorker = {
                  dispatchId: "dispatch-1",
                  admissionId: "admission-1",
                  ticketNumber: 21,
                  providerThreadId: null,
                  parentProviderThreadId: null,
                  ownership: "server",
                  writePaths: ["apps/server"],
                  writeReservation: "held" as const,
                  settlementEvidence: null,
                  association: "unconfirmed" as const,
                  providerStatus: "prepared",
                  requestedProfile: null,
                  observedProfile: { model: null, effort: null, match: "unknown" as const },
                  handoff: null,
                  title: "Ticket 21",
                  role: "implement",
                  updatedAt: "2026-09-07T10:00:00.000Z",
                };
                if (
                  capabilityNumber !== undefined &&
                  [10, 30, 40, 140].includes(capabilityNumber)
                ) {
                  return directorStatus([heldWorker]);
                }
                if (capabilityNumber === 80) {
                  return directorStatus([
                    {
                      ...heldWorker,
                      providerThreadId: "closed-worker",
                      writeReservation: "released",
                      settlementEvidence: "native-closed",
                      association: "associated",
                      providerStatus: "closed",
                    },
                  ]);
                }
                if (capabilityNumber === 90) {
                  return directorStatus([], [reviewStatus({ status: "spawn-issued" })]);
                }
                if (capabilityNumber === 100) {
                  return directorStatus(
                    [],
                    [
                      reviewStatus({
                        status: "reported",
                        association: "associated",
                        providerThreadId: "review-thread",
                        settlementEvidence: "native-closed",
                        axes: [
                          reviewAxis("standards", "native-closed"),
                          reviewAxis("spec", "native-closed"),
                        ],
                      }),
                    ],
                  );
                }
                if (capabilityNumber === 110) {
                  return directorStatus(
                    [],
                    [
                      reviewStatus({
                        status: "reported",
                        association: "associated",
                        providerThreadId: "review-thread",
                        settlementEvidence: "native-closed",
                        axes: [reviewAxis("standards", "native-closed"), reviewAxis("spec", null)],
                      }),
                    ],
                  );
                }
                if (capabilityNumber === 120) {
                  return directorStatus([], [reviewStatus({ status: "prepared" })]);
                }
                if (capabilityNumber === 130) {
                  return directorStatus(
                    [],
                    [
                      reviewStatus({
                        status: "reported",
                        association: "associated",
                        providerThreadId: "review-thread",
                      }),
                    ],
                  );
                }
                return directorStatus([]);
              }),
          }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect(
    "retires historical detail paths from cadence refreshes and marks them stale on return",
    () =>
      Effect.gen(function* () {
        let rootReads = 0;
        let detailReads = 0;
        const program = Effect.gen(function* () {
          const monitor = yield* WorkflowMonitor.WorkflowMonitor;
          const events = yield* Queue.unbounded<WorkflowSyncState>();
          const watcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
            Stream.runForEach((state) => Queue.offer(events, state)),
            Effect.forkScoped,
          );
          yield* nextStatus(events, (state) => state.status === "fresh");
          yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 });
          const detailReadsAfterOpen = detailReads;

          yield* TestClock.adjust("61 seconds");
          yield* monitor.roots({ projectId: projectOne, repository });
          yield* TestClock.adjust("29 seconds");
          yield* nextStatus(events, (state) => state.status === "fresh");
          expect(rootReads).toBeGreaterThan(1);
          expect(detailReads).toBe(detailReadsAfterOpen + 2);

          yield* TestClock.adjust("31 seconds");
          yield* monitor.issueDetail({ projectId: projectOne, repository, number: 11 });
          const stale = yield* nextStatus(events, (state) => state.status === "stale");
          expect(stale.message).toContain("previously viewed");
          yield* nextStatus(events, (state) => state.status === "fresh");
          expect(detailReads).toBe(detailReadsAfterOpen + 3);
          yield* Fiber.interrupt(watcher);
        });
        yield* program.pipe(
          Effect.provide(
            monitorLayer({
              loadRoots: () => {
                rootReads += 1;
                return Effect.succeed(roots("current"));
              },
              onRead: (operation) =>
                Effect.sync(() => {
                  if (operation === "detail") detailReads += 1;
                }),
            }),
          ),
          Effect.scoped,
        );
      }),
  );

  it.effect("coalesces repeated reads of old cached data and preserves outage backoff", () =>
    Effect.gen(function* () {
      let outage = false;
      let repositoryReads = 0;
      let controlReads = 0;
      const program = Effect.gen(function* () {
        const monitor = yield* WorkflowMonitor.WorkflowMonitor;
        yield* monitor.roots({ projectId: projectOne, repository });
        outage = true;
        yield* TestClock.adjust("61 seconds");

        expect((yield* monitor.roots({ projectId: projectOne, repository })).roots[0]?.title).toBe(
          "cached",
        );
        const firstFailure = yield* monitor.refresh({ projectId: projectOne, repository });
        expect(firstFailure.status).toBe("unavailable");
        for (let index = 0; index < 5; index += 1) {
          yield* monitor.roots({ projectId: projectOne, repository });
        }
        expect(repositoryReads).toBe(2);

        const failedEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const failedWatcher = yield* monitor.watch({ projectId: projectOne, repository }).pipe(
          Stream.runForEach((state) => Queue.offer(failedEvents, state)),
          Effect.forkScoped,
        );
        yield* nextStatus(failedEvents, (state) => state.status === "unavailable");
        for (let index = 0; index < 5; index += 1) {
          yield* monitor.roots({ projectId: projectOne, repository });
        }
        expect(repositoryReads).toBe(2);

        const controlEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const controlWatcher = yield* monitor
          .watch({ projectId: projectOne, repository: "Flow-Fly/control" })
          .pipe(
            Stream.runForEach((state) => Queue.offer(controlEvents, state)),
            Effect.forkScoped,
          );
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        yield* TestClock.adjust("59 seconds");
        yield* nextStatus(
          failedEvents,
          (state) => state.status === "unavailable" && state.revision > firstFailure.revision,
        );
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        expect(controlReads).toBeGreaterThan(1);
        expect(repositoryReads).toBe(3);

        yield* TestClock.adjust("59 seconds");
        expect(repositoryReads).toBe(3);
        yield* TestClock.adjust("1 second");
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        expect(repositoryReads).toBe(4);
        yield* Fiber.interrupt(failedWatcher);
        yield* Fiber.interrupt(controlWatcher);
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: ({ repository: requestedRepository }) => {
              if (requestedRepository.toLowerCase() === "flow-fly/control") {
                controlReads += 1;
                return Effect.succeed(roots("control"));
              }
              repositoryReads += 1;
              if (!outage) return Effect.succeed(roots("cached"));
              return Effect.fail(
                new WorkflowQueryError({
                  failure: "request-failed",
                  message: "GitHub is unavailable.",
                }),
              );
            },
          }),
        ),
        Effect.scoped,
      );
    }),
  );
});
