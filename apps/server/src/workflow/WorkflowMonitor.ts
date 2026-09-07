import {
  WorkflowDirectorError,
  type ProjectId,
  type WorkflowChildrenInput,
  type WorkflowChildrenResult,
  type WorkflowDirectorStatus,
  type WorkflowIssueDetail,
  type WorkflowIssueDetailInput,
  type WorkflowMonitorInput,
  type WorkflowQueryError,
  type WorkflowRepositoryNameWithOwner,
  type WorkflowRootsInput,
  type WorkflowRootsResult,
  type WorkflowSyncState,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as WorkflowDirectorService from "./WorkflowDirectorService.ts";
import * as WorkflowService from "./WorkflowService.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";

const REFRESH_INTERVAL = Duration.seconds(30);
const MAX_BACKOFF = Duration.minutes(15);
const CACHE_CAPACITY = 256;
const CACHE_REFRESH_RETENTION = Duration.minutes(1);

type CachedValue = WorkflowRootsResult | WorkflowChildrenResult | WorkflowIssueDetail;

type RefreshEntryResult = {
  readonly error: WorkflowQueryError | null;
  readonly capability: WorkflowIssueDetail | null;
};

type CacheEntry =
  | {
      readonly kind: "roots";
      input: WorkflowRootsInput;
      value: WorkflowRootsResult | null;
      error: WorkflowQueryError | null;
      lastRequestedAtMs?: number;
      lastRefreshedAtMs?: number;
      refreshRequestedGeneration?: number;
      refreshingGeneration?: number;
      pendingGeneration?: number;
    }
  | {
      readonly kind: "children";
      input: WorkflowChildrenInput;
      value: WorkflowChildrenResult | null;
      error: WorkflowQueryError | null;
      lastRequestedAtMs?: number;
      lastRefreshedAtMs?: number;
      refreshRequestedGeneration?: number;
      refreshingGeneration?: number;
      pendingGeneration?: number;
    }
  | {
      readonly kind: "detail";
      input: WorkflowIssueDetailInput;
      value: WorkflowIssueDetail | null;
      error: WorkflowQueryError | null;
      lastRequestedAtMs?: number;
      lastRefreshedAtMs?: number;
      refreshRequestedGeneration?: number;
      refreshingGeneration?: number;
      pendingGeneration?: number;
    };

interface RepositoryState {
  readonly key: string;
  readonly repository: WorkflowRepositoryNameWithOwner;
  projectId: ProjectId;
  watchers: number;
  epoch: number;
  attemptedEpoch: number;
  generation: number;
  revision: number;
  failureCount: number;
  lastAttemptAtMs: number | null;
  lastSuccessfulAtMs: number | null;
  nextRefreshAtMs: number;
  status: WorkflowSyncState["status"];
  message: string;
  refreshQueued: boolean;
  readonly refreshLock: Semaphore.Semaphore;
  readonly sync: SubscriptionRef.SubscriptionRef<WorkflowSyncState>;
}

interface ActiveDirectorRow {
  readonly projectId: string;
  readonly repository: string;
  readonly capabilityNumber: number;
  readonly threadId: string;
  readonly projectionThreadId: string | null;
  readonly archivedAt: string | null;
  readonly deletedAt: string | null;
}

function repositoryKey(repository: string): string {
  return repository.trim().toLocaleLowerCase();
}

function iso(millis: number | null): string | null {
  if (millis === null) return null;
  return Option.match(DateTime.make(millis), {
    onNone: () => null,
    onSome: DateTime.formatIso,
  });
}

function cacheKey(entry: CacheEntry): string {
  switch (entry.kind) {
    case "roots":
      return `${repositoryKey(entry.input.repository)}\0roots`;
    case "children":
      return `${repositoryKey(entry.input.repository)}\0children\0${entry.input.parentNumber}`;
    case "detail":
      return `${repositoryKey(entry.input.repository)}\0detail\0${entry.input.number}`;
  }
}

function failureStatus(error: WorkflowQueryError): WorkflowSyncState["status"] {
  switch (error.failure) {
    case "github-unauthenticated":
    case "github-forbidden":
    case "repository-not-found":
      return "access-denied";
    case "github-rate-limited":
      return "rate-limited";
    default:
      return "unavailable";
  }
}

function backoffMillis(failureCount: number): number {
  return Math.min(
    Duration.toMillis(REFRESH_INTERVAL) * 2 ** Math.max(0, failureCount - 1),
    Duration.toMillis(MAX_BACKOFF),
  );
}

function directorHasUnsettledExecution(director: WorkflowDirectorStatus): boolean {
  if (
    director.workers.some(
      (worker) =>
        worker.writeReservation === "held" ||
        (worker.providerThreadId !== null && worker.settlementEvidence !== "native-closed"),
    )
  ) {
    return true;
  }
  return (
    director.reviews?.some(
      (review) =>
        (review.status === "spawn-issued" ||
          review.status === "associated" ||
          review.status === "reported") &&
        (review.settlementEvidence !== "native-closed" ||
          review.axes.some((axis) => axis.settlementEvidence !== "native-closed")),
    ) === true
  );
}

export class WorkflowMonitor extends Context.Service<
  WorkflowMonitor,
  {
    readonly roots: (
      input: WorkflowRootsInput,
    ) => Effect.Effect<WorkflowRootsResult, WorkflowQueryError>;
    readonly children: (
      input: WorkflowChildrenInput,
    ) => Effect.Effect<WorkflowChildrenResult, WorkflowQueryError>;
    readonly issueDetail: (
      input: WorkflowIssueDetailInput,
    ) => Effect.Effect<WorkflowIssueDetail, WorkflowQueryError>;
    readonly watch: (
      input: WorkflowMonitorInput,
    ) => Stream.Stream<WorkflowSyncState, WorkflowQueryError>;
    readonly refresh: (
      input: WorkflowMonitorInput,
    ) => Effect.Effect<WorkflowSyncState, WorkflowQueryError>;
    readonly invalidate: (input: WorkflowMonitorInput) => Effect.Effect<void, WorkflowQueryError>;
    readonly invalidateDirector: (input: {
      readonly environmentId: string;
      readonly threadId: string;
    }) => Effect.Effect<void>;
    readonly invalidateAll: Effect.Effect<void>;
  }
>()("t3/workflow/WorkflowMonitor") {}

export class WorkflowRefreshNotifier extends Context.Reference<{
  readonly directorChanged: (input: {
    readonly environmentId: string;
    readonly threadId: string;
  }) => Effect.Effect<void>;
}>("t3/workflow/WorkflowRefreshNotifier", {
  defaultValue: () => ({ directorChanged: () => Effect.void }),
}) {}

export const make = Effect.gen(function* () {
  const workflow = yield* WorkflowService.WorkflowService;
  const directors = yield* WorkflowDirectorService.WorkflowDirectorService;
  const orchestration = yield* OrchestrationEngine.OrchestrationEngineService;
  const sql = yield* SqlClient.SqlClient;
  const stateLock = yield* Semaphore.make(1);
  const refreshRequests = yield* Queue.unbounded<{
    readonly state: RepositoryState;
    readonly targetEpoch: number;
    readonly minimumGeneration: number;
  }>();
  const states = new Map<string, RepositoryState>();
  const cache = new Map<string, CacheEntry>();

  const snapshot = Effect.fn("WorkflowMonitor.snapshot")(function* (state: RepositoryState) {
    const now = yield* Clock.currentTimeMillis;
    return {
      repository: state.repository,
      status: state.status,
      lastAttemptAt: iso(state.lastAttemptAtMs),
      lastSuccessfulAt: iso(state.lastSuccessfulAtMs),
      cacheAgeMs:
        state.lastSuccessfulAtMs === null ? null : Math.max(0, now - state.lastSuccessfulAtMs),
      retryAt:
        state.failureCount > 0 && state.nextRefreshAtMs > now ? iso(state.nextRefreshAtMs) : null,
      revision: state.revision,
      message: state.message,
    } satisfies WorkflowSyncState;
  });

  const publish = Effect.fn("WorkflowMonitor.publish")(function* (state: RepositoryState) {
    state.revision += 1;
    yield* SubscriptionRef.set(state.sync, yield* snapshot(state));
  });

  const rememberRoutingFailure = Effect.fn("WorkflowMonitor.rememberRoutingFailure")(function* (
    state: RepositoryState,
    error: WorkflowQueryError,
  ) {
    const failedAt = yield* Clock.currentTimeMillis;
    state.lastAttemptAtMs = failedAt;
    state.failureCount += 1;
    state.nextRefreshAtMs = failedAt + backoffMillis(state.failureCount);
    state.status = failureStatus(error);
    state.message =
      state.lastSuccessfulAtMs === null
        ? error.message
        : `${error.message} Showing the last successful Workflow data.`;
    yield* publish(state);
  });

  const ensureState = Effect.fn("WorkflowMonitor.ensureState")(function* (
    input: WorkflowMonitorInput,
  ) {
    yield* workflow.validateProject(input.projectId);
    return yield* stateLock.withPermits(1)(
      Effect.gen(function* () {
        const key = repositoryKey(input.repository);
        const existing = states.get(key);
        if (existing) {
          existing.projectId = input.projectId;
          return existing;
        }
        const refreshLock = yield* Semaphore.make(1);
        const initial: WorkflowSyncState = {
          repository: input.repository,
          status: "refreshing",
          lastAttemptAt: null,
          lastSuccessfulAt: null,
          cacheAgeMs: null,
          retryAt: null,
          revision: 0,
          message: "Workflow has not synced with GitHub yet.",
        } satisfies WorkflowSyncState;
        const state: RepositoryState = {
          key,
          repository: input.repository,
          projectId: input.projectId,
          watchers: 0,
          epoch: 0,
          attemptedEpoch: -1,
          generation: 0,
          revision: 0,
          failureCount: 0,
          lastAttemptAtMs: null,
          lastSuccessfulAtMs: null,
          nextRefreshAtMs: 0,
          status: "refreshing",
          message: initial.message,
          refreshQueued: false,
          refreshLock,
          sync: yield* SubscriptionRef.make<WorkflowSyncState>(initial),
        };
        states.set(key, state);
        return state;
      }),
    );
  });

  const remember = Effect.fn("WorkflowMonitor.remember")(function* (entry: CacheEntry) {
    const now = yield* Clock.currentTimeMillis;
    const key = cacheKey(entry);
    const previous = cache.get(key);
    if (previous) {
      previous.lastRequestedAtMs = now;
      if (previous.kind === "roots" && entry.kind === "roots") previous.input = entry.input;
      if (previous.kind === "children" && entry.kind === "children") previous.input = entry.input;
      if (previous.kind === "detail" && entry.kind === "detail") previous.input = entry.input;
      return previous;
    }
    entry.lastRequestedAtMs = now;
    cache.set(key, entry);
    if (cache.size > CACHE_CAPACITY) {
      const oldest = cache.keys().next().value;
      if (typeof oldest === "string") cache.delete(oldest);
    }
    return entry;
  });

  const entriesFor = Effect.fn("WorkflowMonitor.entriesFor")(function* (state: RepositoryState) {
    const now = yield* Clock.currentTimeMillis;
    const oldest = now - Duration.toMillis(CACHE_REFRESH_RETENTION);
    return [...cache.values()].filter(
      (entry) =>
        repositoryKey(entry.input.repository) === state.key &&
        (entry.lastRequestedAtMs ?? 0) >= oldest,
    );
  });

  const refreshEntry = Effect.fn("WorkflowMonitor.refreshEntry")(function* (
    entry: CacheEntry,
    generation: number,
  ) {
    const finishRequest = Effect.sync(() => {
      if ((entry.refreshRequestedGeneration ?? Number.POSITIVE_INFINITY) <= generation) {
        delete entry.refreshRequestedGeneration;
      }
    });
    return yield* Effect.gen(function* () {
      switch (entry.kind) {
        case "roots": {
          const result = yield* workflow.roots(entry.input).pipe(Effect.result);
          if (result._tag === "Failure") {
            entry.error = result.failure;
            return { error: result.failure, capability: null } satisfies RefreshEntryResult;
          }
          entry.value = result.success;
          entry.error = null;
          entry.lastRefreshedAtMs = yield* Clock.currentTimeMillis;
          return { error: null, capability: null } satisfies RefreshEntryResult;
        }
        case "children": {
          const result = yield* workflow.children(entry.input).pipe(Effect.result);
          if (result._tag === "Failure") {
            entry.error = result.failure;
            return { error: result.failure, capability: null } satisfies RefreshEntryResult;
          }
          entry.value = result.success;
          entry.error = null;
          entry.lastRefreshedAtMs = yield* Clock.currentTimeMillis;
          return { error: null, capability: null } satisfies RefreshEntryResult;
        }
        case "detail": {
          const result = yield* workflow.issueDetail(entry.input).pipe(Effect.result);
          if (result._tag === "Failure") {
            entry.error = result.failure;
            return { error: result.failure, capability: null } satisfies RefreshEntryResult;
          }
          entry.value = result.success;
          entry.error = null;
          entry.lastRefreshedAtMs = yield* Clock.currentTimeMillis;
          return {
            error: null,
            capability: result.success.kind === "capability" ? result.success : null,
          } satisfies RefreshEntryResult;
        }
      }
    }).pipe(Effect.ensuring(finishRequest));
  });

  const refreshRepository = Effect.fn("WorkflowMonitor.refreshRepository")(function* (
    state: RepositoryState,
    targetEpoch: number,
    minimumGeneration: number,
  ) {
    return yield* state.refreshLock.withPermits(1)(
      Effect.gen(function* () {
        while (state.attemptedEpoch < targetEpoch || state.generation < minimumGeneration) {
          const entries = yield* entriesFor(state);
          if (entries.length === 0) return yield* snapshot(state);
          const capturedEpoch = state.epoch;
          state.generation += 1;
          const generation = state.generation;
          for (const entry of entries) entry.refreshingGeneration = generation;
          const finishGeneration = Effect.sync(() => {
            for (const entry of entries) {
              if (entry.refreshingGeneration === generation) delete entry.refreshingGeneration;
            }
          });
          const completed = yield* Effect.gen(function* () {
            state.status = "refreshing";
            state.message =
              state.lastSuccessfulAtMs === null
                ? "Syncing Workflow with GitHub…"
                : "Refreshing Workflow from GitHub…";
            yield* publish(state);
            const attemptAt = yield* Clock.currentTimeMillis;
            state.lastAttemptAtMs = attemptAt;
            const errors = yield* Effect.forEach(
              entries,
              (entry) => refreshEntry(entry, generation),
              { concurrency: 4 },
            );
            const error = errors.find((candidate) => candidate.error !== null)?.error ?? null;
            const completedAt = yield* Clock.currentTimeMillis;
            if (capturedEpoch !== state.epoch) {
              state.status = "stale";
              state.message =
                "Workflow changed while GitHub was refreshing. Waiting for a newer read.";
              yield* publish(state);
              targetEpoch = state.epoch;
              minimumGeneration = generation + 1;
              return null;
            }
            const requestedGeneration = Math.max(
              0,
              ...(yield* entriesFor(state)).map((entry) => entry.refreshRequestedGeneration ?? 0),
            );
            if (requestedGeneration > generation) {
              minimumGeneration = requestedGeneration;
              return null;
            }
            for (const capability of errors.flatMap((candidate) =>
              candidate.capability ? [candidate.capability] : [],
            )) {
              yield* directors
                .reassess({ projectId: state.projectId, issue: capability }, (command) =>
                  orchestration.dispatch(command).pipe(
                    Effect.mapError(
                      (dispatchError) =>
                        new WorkflowDirectorError({
                          failure: "dispatch-failed",
                          message: "The reassessment interrupt command could not be submitted.",
                          detail: String(dispatchError),
                        }),
                    ),
                  ),
                )
                .pipe(
                  Effect.catch((reassessmentError) =>
                    Effect.logWarning("Workflow reassessment could not be recorded.", {
                      capabilityNumber: capability.number,
                      error: reassessmentError,
                    }),
                  ),
                );
            }
            if (error !== null) {
              state.attemptedEpoch = capturedEpoch;
              state.failureCount += 1;
              state.nextRefreshAtMs = completedAt + backoffMillis(state.failureCount);
              state.status = failureStatus(error);
              state.message =
                state.lastSuccessfulAtMs === null
                  ? error.message
                  : `${error.message} Showing the last successful Workflow data.`;
              yield* publish(state);
              return yield* snapshot(state);
            }
            state.attemptedEpoch = capturedEpoch;
            state.failureCount = 0;
            state.lastSuccessfulAtMs = completedAt;
            state.nextRefreshAtMs = completedAt + Duration.toMillis(REFRESH_INTERVAL);
            state.status = "fresh";
            state.message = "Workflow is current with GitHub.";
            yield* publish(state);
            return yield* snapshot(state);
          }).pipe(Effect.ensuring(finishGeneration));
          if (completed !== null) return completed;
        }
        return yield* snapshot(state);
      }),
    );
  });

  const requestRefresh = Effect.fn("WorkflowMonitor.requestRefresh")(function* (
    state: RepositoryState,
    targetEpoch = state.epoch,
    minimumGeneration = state.generation + 1,
  ) {
    if (state.refreshQueued) return;
    state.refreshQueued = true;
    yield* Queue.offer(refreshRequests, {
      state,
      targetEpoch,
      minimumGeneration,
    });
  });

  const seedRoots = Effect.fn("WorkflowMonitor.seedRoots")(function* (state: RepositoryState) {
    yield* remember({
      kind: "roots",
      input: { projectId: state.projectId, repository: state.repository },
      value: null,
      error: null,
    });
  });

  const activeDirectors = Effect.fn("WorkflowMonitor.activeDirectors")(function* () {
    const rows = yield* sql<ActiveDirectorRow>`
      SELECT d.project_id AS "projectId", d.repository,
        d.capability_number AS "capabilityNumber", d.thread_id AS "threadId",
        t.thread_id AS "projectionThreadId",
        t.archived_at AS "archivedAt", t.deleted_at AS "deletedAt"
      FROM workflow_directors d
      LEFT JOIN projection_threads t ON t.thread_id = d.thread_id
      WHERE d.is_current = 1
        AND (d.status = 'active' OR EXISTS (
          SELECT 1 FROM workflow_reassessments r
          WHERE r.director_id = d.director_id AND r.status != 'cleared'
        ))
        AND d.initial_turn_disposition = 'accepted'
    `;
    const active = [];
    for (const row of rows) {
      if (row.projectionThreadId !== null && row.archivedAt === null && row.deletedAt === null) {
        active.push(row);
        continue;
      }
      const status = yield* directors
        .status({
          projectId: row.projectId as ProjectId,
          repository: row.repository as WorkflowRepositoryNameWithOwner,
          capabilityNumber: row.capabilityNumber,
        })
        .pipe(Effect.option);
      if (Option.isSome(status) && directorHasUnsettledExecution(status.value)) active.push(row);
    }
    return active;
  });

  const seedDirector = Effect.fn("WorkflowMonitor.seedDirector")(function* (
    row: ActiveDirectorRow,
  ) {
    const input = {
      projectId: row.projectId as ProjectId,
      repository: row.repository as WorkflowRepositoryNameWithOwner,
    };
    const state = yield* ensureState(input).pipe(
      Effect.catch((error) => {
        const existing = states.get(repositoryKey(row.repository));
        if (!existing) return error;
        return workflow.validateProject(existing.projectId).pipe(
          Effect.as(existing),
          Effect.catch((fallbackError) =>
            rememberRoutingFailure(existing, fallbackError).pipe(Effect.andThen(fallbackError)),
          ),
        );
      }),
    );
    yield* remember({
      kind: "detail",
      input: {
        projectId: state.projectId,
        repository: state.repository,
        number: row.capabilityNumber,
      },
      value: null,
      error: null,
    });
    yield* remember({
      kind: "children",
      input: {
        projectId: state.projectId,
        repository: state.repository,
        parentNumber: row.capabilityNumber,
      },
      value: null,
      error: null,
    });
    return state;
  });

  const refreshDueRepositories = Effect.fn("WorkflowMonitor.refreshDueRepositories")(function* () {
    const activeRows = yield* activeDirectors();
    const activeStates = (yield* Effect.forEach(activeRows, (row) =>
      seedDirector(row).pipe(Effect.option),
    )).flatMap(Option.toArray);
    const activeKeys = new Set(activeStates.map((state) => state.key));
    for (const row of activeRows) {
      const key = repositoryKey(row.repository);
      if (states.has(key)) activeKeys.add(key);
    }
    const candidates = new Map(
      [...states.values(), ...activeStates]
        .filter((state) => state.watchers > 0 || activeKeys.has(state.key))
        .map((state) => [state.key, state]),
    );
    const now = yield* Clock.currentTimeMillis;
    yield* Effect.forEach(
      [...candidates.values()].filter(
        (state) => state.watchers > 0 && (state.failureCount === 0 || state.nextRefreshAtMs <= now),
      ),
      (state) =>
        workflow.validateProject(state.projectId).pipe(
          Effect.andThen(seedRoots(state)),
          Effect.catch((error) => rememberRoutingFailure(state, error)),
        ),
      { discard: true },
    );
    yield* Effect.forEach(
      [...candidates.values()].filter((state) => state.nextRefreshAtMs <= now),
      (state) => refreshRepository(state, state.epoch, state.generation + 1),
      { concurrency: 4, discard: true },
    );
  });

  const refreshWorker = Effect.forever(
    Queue.take(refreshRequests).pipe(
      Effect.tap(({ state }) =>
        Effect.sync(() => {
          state.refreshQueued = false;
        }),
      ),
      Effect.flatMap(({ state, targetEpoch, minimumGeneration }) =>
        refreshRepository(state, targetEpoch, minimumGeneration).pipe(
          Effect.ignoreCause({ log: true }),
        ),
      ),
    ),
  );
  yield* Effect.forEach([0, 1, 2, 3], () => refreshWorker.pipe(Effect.forkScoped), {
    discard: true,
  });
  yield* Effect.forever(
    Effect.sleep(REFRESH_INTERVAL).pipe(
      Effect.andThen(refreshDueRepositories),
      Effect.ignoreCause({ log: true }),
    ),
  ).pipe(Effect.forkScoped);

  const cachedRead = Effect.fn("WorkflowMonitor.cachedRead")(function* <A extends CachedValue>(
    entry: CacheEntry,
  ) {
    const state = yield* ensureState({
      projectId: entry.input.projectId,
      repository: entry.input.repository,
    });
    const held = yield* remember(entry);
    const now = yield* Clock.currentTimeMillis;
    if (held.value !== null) {
      if (
        held.lastRefreshedAtMs === undefined ||
        now - held.lastRefreshedAtMs > Duration.toMillis(CACHE_REFRESH_RETENTION)
      ) {
        if (state.failureCount === 0 || state.nextRefreshAtMs <= now) {
          const requestedGeneration =
            held.refreshingGeneration === state.generation
              ? state.generation
              : state.generation + 1;
          if ((held.refreshRequestedGeneration ?? 0) < requestedGeneration) {
            held.refreshRequestedGeneration = requestedGeneration;
          }
          if (state.status !== "refreshing" && state.status !== "stale") {
            state.epoch += 1;
            state.status = "stale";
            state.message = "Refreshing a previously viewed Workflow path from GitHub…";
            yield* publish(state);
          }
          yield* requestRefresh(state, state.epoch, requestedGeneration);
        }
      }
      return held.value as A;
    }
    if (held.error !== null && state.failureCount > 0 && state.nextRefreshAtMs > now) {
      return yield* held.error;
    }
    const pendingGeneration = held.pendingGeneration;
    if (pendingGeneration !== undefined) {
      yield* refreshRepository(state, state.epoch, pendingGeneration);
    } else {
      const requestedGeneration =
        held.refreshingGeneration === state.generation ? state.generation : state.generation + 1;
      held.pendingGeneration = requestedGeneration;
      held.refreshRequestedGeneration = Math.max(
        held.refreshRequestedGeneration ?? 0,
        requestedGeneration,
      );
      yield* refreshRepository(state, state.epoch, requestedGeneration).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (held.pendingGeneration === requestedGeneration) {
              delete held.pendingGeneration;
            }
          }),
        ),
      );
    }
    if (held.value !== null) return held.value as A;
    if (held.error !== null) return yield* held.error;
    return yield* Effect.die("Workflow refresh finished without data or an error.");
  });

  const roots: WorkflowMonitor["Service"]["roots"] = (input) =>
    cachedRead<WorkflowRootsResult>({ kind: "roots", input, value: null, error: null });
  const children: WorkflowMonitor["Service"]["children"] = (input) =>
    cachedRead<WorkflowChildrenResult>({ kind: "children", input, value: null, error: null });
  const issueDetail: WorkflowMonitor["Service"]["issueDetail"] = (input) =>
    cachedRead<WorkflowIssueDetail>({ kind: "detail", input, value: null, error: null });

  const watch: WorkflowMonitor["Service"]["watch"] = (input) =>
    Stream.unwrap(
      Effect.acquireRelease(
        Effect.gen(function* () {
          const state = yield* ensureState(input);
          state.watchers += 1;
          yield* seedRoots(state);
          const now = yield* Clock.currentTimeMillis;
          const backoffActive = state.failureCount > 0 && state.nextRefreshAtMs > now;
          if (state.status === "fresh") {
            state.epoch += 1;
            state.status = "stale";
            state.message = "Checking GitHub for Workflow updates…";
            yield* publish(state);
          }
          if (!backoffActive) {
            yield* requestRefresh(
              state,
              state.epoch,
              state.status === "refreshing" || state.status === "stale"
                ? Math.max(1, state.generation)
                : state.generation + 1,
            );
          }
          return state;
        }),
        (state) =>
          Effect.sync(() => {
            state.watchers = Math.max(0, state.watchers - 1);
          }),
      ).pipe(Effect.map((state) => SubscriptionRef.changes(state.sync))),
    );

  const refresh: WorkflowMonitor["Service"]["refresh"] = Effect.fn("WorkflowMonitor.refresh")(
    function* (input) {
      const state = yield* ensureState(input);
      yield* seedRoots(state);
      if (state.status !== "refreshing" && state.status !== "stale") {
        state.epoch += 1;
        state.status = state.lastSuccessfulAtMs === null ? "refreshing" : "stale";
        state.message = "Retrying Workflow sync with GitHub…";
        yield* publish(state);
      }
      return yield* refreshRepository(
        state,
        state.epoch,
        state.status === "refreshing" || state.status === "stale"
          ? Math.max(1, state.generation)
          : state.generation + 1,
      );
    },
  );

  const invalidate: WorkflowMonitor["Service"]["invalidate"] = Effect.fn(
    "WorkflowMonitor.invalidate",
  )(function* (input) {
    const state = yield* ensureState(input);
    yield* seedRoots(state);
    state.epoch += 1;
    state.status = state.lastSuccessfulAtMs === null ? "refreshing" : "stale";
    state.message = "Workflow changed. Waiting for GitHub confirmation…";
    yield* publish(state);
    yield* requestRefresh(state);
  });

  const invalidateDirector: WorkflowMonitor["Service"]["invalidateDirector"] = Effect.fn(
    "WorkflowMonitor.invalidateDirector",
  )(function* (input) {
    yield* Effect.gen(function* () {
      const rows = yield* sql<{
        readonly projectId: string;
        readonly repository: string;
      }>`
        SELECT project_id AS "projectId", repository FROM workflow_directors
        WHERE environment_id = ${input.environmentId} AND thread_id = ${input.threadId}
          AND is_current = 1 LIMIT 1
      `;
      const row = rows[0];
      if (!row) return;
      yield* invalidate({
        projectId: row.projectId as ProjectId,
        repository: row.repository as WorkflowRepositoryNameWithOwner,
      });
    }).pipe(Effect.ignore);
  });

  const invalidateAll: WorkflowMonitor["Service"]["invalidateAll"] = Effect.suspend(() =>
    Effect.forEach(
      [...states.values()],
      (state) =>
        invalidate({ projectId: state.projectId, repository: state.repository }).pipe(
          Effect.ignore,
        ),
      { discard: true },
    ),
  );

  return WorkflowMonitor.of({
    roots,
    children,
    issueDetail,
    watch,
    refresh,
    invalidate,
    invalidateDirector,
    invalidateAll,
  });
});

export const layer = Layer.effect(WorkflowMonitor, make);

export const notifierLayer = Layer.effect(
  WorkflowRefreshNotifier,
  WorkflowMonitor.pipe(Effect.map((monitor) => ({ directorChanged: monitor.invalidateDirector }))),
);
