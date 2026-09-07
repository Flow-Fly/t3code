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

function directorStatus(workers: WorkflowDirectorStatus["workers"]): WorkflowDirectorStatus {
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
    observation: "Director is active.",
    actions: ["open"],
    createdAt: "2026-09-07T10:00:00.000Z",
    updatedAt: "2026-09-07T10:00:00.000Z",
    message: "Director is active.",
  };
}

function monitorLayer(input: {
  readonly loadRoots: WorkflowService.WorkflowService["Service"]["roots"];
  readonly onRead?: ((operation: "children" | "detail", repository: string) => void) | undefined;
  readonly status?:
    | WorkflowDirectorService.WorkflowDirectorService["Service"]["status"]
    | undefined;
}) {
  return WorkflowMonitor.layer.pipe(
    Layer.provideMerge(
      Layer.mock(WorkflowService.WorkflowService)({
        roots: input.loadRoots,
        children: ({ repository, parentNumber }) => {
          input.onRead?.("children", repository);
          return Effect.succeed({
            parentNumber,
            children: [],
            frontier: {
              status: "empty",
              message: "No immediate work is visible in this branch.",
              readyIssueIds: [],
            },
          });
        },
        issueDetail: ({ repository, number }) => {
          input.onRead?.("detail", repository);
          const detail: WorkflowIssueDetail = {
            ...roots("Capability").roots[0]!,
            repository,
            number,
            body: "",
            blockedBy: [],
          };
          return Effect.succeed(detail);
        },
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

const nextStatus = (
  queue: Queue.Queue<WorkflowSyncState>,
  predicate: (state: WorkflowSyncState) => boolean,
) =>
  Effect.gen(function* () {
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
            onRead: (operation) => {
              if (operation === "detail") detailReads += 1;
            },
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

  it.effect("keeps archived directors only while durable child execution is unsettled", () =>
    Effect.gen(function* () {
      const reads = new Array<{ operation: string; repository: string }>();
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
              VALUES (${`thread-${entry.suffix}`}, ${projectOne}, ${entry.suffix}, ${now}, ${now}, ${entry.archived}, ${entry.deleted})
            `;
          }
          yield* sql`
            INSERT INTO workflow_directors
              (director_id, batch_id, environment_id, project_id, repository, root_number,
               capability_number, thread_id, command_id, message_id, worktree_path,
               worktree_branch, status, requested_model, requested_instance_id,
               requested_effort, observed_model, observed_effort, observed_match, sequence,
               initial_turn_disposition, is_current, created_at, updated_at)
            VALUES (${`director-${entry.suffix}`}, 'batch', ${environmentId}, ${projectOne}, ${repo}, 1,
              ${entry.capability}, ${`thread-${entry.suffix}`},
              ${`command-${entry.suffix}`}, ${`message-${entry.suffix}`}, '/tmp/worktree', 'branch', ${entry.status},
              'gpt-6-astra', 'codex-workflow', 'high', 'gpt-6-astra', 'high', 'match', 1,
              ${entry.disposition}, 1, ${now}, ${now})
          `;
        }
        yield* TestClock.adjust("30 seconds");
        yield* Effect.yieldNow;
        for (const suffix of ["archived-held", "missing-held", "deleted-held", "open"]) {
          expect(reads.filter((read) => read.repository === `Flow-Fly/${suffix}`)).toHaveLength(2);
        }
        for (const suffix of ["archived-closed", "deleted-closed", "failed-setup"]) {
          expect(reads.some((read) => read.repository === `Flow-Fly/${suffix}`)).toBe(false);
        }
      });
      yield* program.pipe(
        Effect.provide(
          monitorLayer({
            loadRoots: () => Effect.succeed(roots("unused")),
            onRead: (operation, repo) => reads.push({ operation, repository: repo }),
            status: ({ capabilityNumber }) =>
              Effect.succeed(
                directorStatus(
                  capabilityNumber !== undefined && [10, 30, 40].includes(capabilityNumber)
                    ? [
                        {
                          dispatchId: "dispatch-1",
                          admissionId: "admission-1",
                          ticketNumber: 21,
                          providerThreadId: null,
                          parentProviderThreadId: null,
                          ownership: "server",
                          writePaths: ["apps/server"],
                          writeReservation: "held",
                          settlementEvidence: null,
                          association: "unconfirmed",
                          providerStatus: "prepared",
                          requestedProfile: null,
                          observedProfile: { model: null, effort: null, match: "unknown" },
                          handoff: {
                            outcome: "succeeded",
                            summary: "Reported",
                            commits: [],
                            checks: [],
                          },
                          title: "Ticket 21",
                          role: "implement",
                          updatedAt: "2026-09-07T10:00:00.000Z",
                        },
                      ]
                    : [],
                ),
              ),
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
              onRead: (operation) => {
                if (operation === "detail") detailReads += 1;
              },
            }),
          ),
          Effect.scoped,
        );
      }),
  );

  it.effect("coalesces repeated reads of old cached data and preserves outage backoff", () =>
    Effect.gen(function* () {
      const scheduledFailure = yield* Deferred.make<void>();
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
        yield* nextStatus(
          failedEvents,
          (state) => state.status === "unavailable" && state.revision > firstFailure.revision,
        );
        for (let index = 0; index < 5; index += 1) {
          yield* monitor.roots({ projectId: projectOne, repository });
        }
        expect(repositoryReads).toBe(3);

        const controlEvents = yield* Queue.unbounded<WorkflowSyncState>();
        const controlWatcher = yield* monitor
          .watch({ projectId: projectOne, repository: "Flow-Fly/control" })
          .pipe(
            Stream.runForEach((state) => Queue.offer(controlEvents, state)),
            Effect.forkScoped,
          );
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        yield* TestClock.adjust("59 seconds");
        yield* nextStatus(controlEvents, (state) => state.status === "fresh");
        expect(controlReads).toBeGreaterThan(1);
        expect(repositoryReads).toBe(3);

        yield* TestClock.adjust("30 seconds");
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
                return (controlReads >= 3 ? Deferred.await(scheduledFailure) : Effect.void).pipe(
                  Effect.as(roots("control")),
                );
              }
              repositoryReads += 1;
              if (!outage) return Effect.succeed(roots("cached"));
              if (repositoryReads === 4) {
                return Deferred.succeed(scheduledFailure, undefined).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new WorkflowQueryError({
                        failure: "request-failed",
                        message: "GitHub is unavailable.",
                      }),
                    ),
                  ),
                );
              }
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
