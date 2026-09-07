import * as NodeAssert from "node:assert/strict";
import { it } from "@effect/vitest";
import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeTaskId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { recordWorkflowWorkerObservation } from "./WorkflowWorkerPersistence.ts";

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workflow_directors (director_id,batch_id,environment_id,project_id,repository,root_number,capability_number,thread_id,command_id,message_id,worktree_path,worktree_branch,status,requested_model,requested_instance_id,requested_effort,observed_match,initial_turn_disposition,created_at,updated_at)
    VALUES ('director','batch','environment','project','owner/repo',1,1,'thread','command','message','/tmp','capability','held','gpt-6-astra','codex','high','unknown','accepted','2026-09-07T00:00:00Z','2026-09-07T00:00:00Z')`;
  yield* sql`INSERT INTO workflow_reassessments (reassessment_id,director_id,trigger_kind,trigger_issue_number,trigger_source,status,required_action,stop_request_status,tracker_status,created_at,updated_at)
    VALUES ('reassessment','director','scope-change',1,'https://github.com/owner/repo/issues/1','held','Reassess','submitted','not-written','2026-09-07T00:00:00Z','2026-09-07T00:00:00Z')`;
  return sql;
});

function childEvent(
  status: "running" | "interrupted",
  second: number,
  completed: boolean,
): ProviderRuntimeEvent {
  return {
    type: "task.updated",
    eventId: EventId.make(`child-event-${second}`),
    provider: ProviderDriverKind.make("codex"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    threadId: ThreadId.make("thread"),
    createdAt: `2026-09-07T00:00:0${second}Z`,
    payload: {
      taskId: RuntimeTaskId.make("child"),
      status,
      timelineBypass: true,
      ...(completed
        ? {
            nativeTurn: {
              sessionId: "native-session",
              turnId: "old-turn",
              status: "interrupted" as const,
            },
            nativeInterruption: {
              attemptId: "old-attempt",
              sessionId: "native-session",
              turnId: "old-turn",
              requestStatus: "acknowledged" as const,
              completionStatus: "interrupted" as const,
            },
          }
        : {}),
    },
  };
}

it.effect("a late old child completion cannot settle newer observed running work", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    yield* recordWorkflowWorkerObservation(childEvent("interrupted", 1, true));
    yield* recordWorkflowWorkerObservation(childEvent("running", 2, false));
    yield* recordWorkflowWorkerObservation(childEvent("interrupted", 3, true));
    const subjects = yield* sql<{
      outcome: string;
    }>`SELECT outcome FROM workflow_interruption_subjects WHERE subject_id = 'child'`;
    NodeAssert.equal(subjects.length, 1);
    NodeAssert.notEqual(
      subjects[0]!.outcome,
      "stopped",
      "old native completion must not erase a newer running observation",
    );
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("a new native director turn unsettles its previously stopped subject", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    yield* sql`INSERT INTO workflow_interruption_subjects (reassessment_id,subject_id,subject_kind,native_session_id,native_turn_id,request_status,outcome,discovered_at,updated_at)
      VALUES ('reassessment','director','director','native-session','old-turn','acknowledged','stopped','2026-09-07T00:00:01Z','2026-09-07T00:00:01Z')`;
    yield* recordWorkflowWorkerObservation({
      type: "turn.started",
      eventId: EventId.make("new-parent-turn"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      threadId: ThreadId.make("thread"),
      turnId: TurnId.make("new-turn"),
      createdAt: "2026-09-07T00:00:02Z",
      payload: {},
      raw: {
        source: "codex.app-server.notification",
        method: "turn/started",
        payload: { threadId: "native-session", turn: { id: "new-turn" } },
      },
    });
    const subjects = yield* sql<{
      outcome: string;
    }>`SELECT outcome FROM workflow_interruption_subjects WHERE subject_id = 'director'`;
    NodeAssert.notEqual(
      subjects[0]!.outcome,
      "stopped",
      "new native director activity must unsettle the existing reassessment",
    );
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("a matching native director completion after the hold settles the root subject", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    yield* recordWorkflowWorkerObservation({
      type: "turn.started",
      eventId: EventId.make("parent-turn-started"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      threadId: ThreadId.make("thread"),
      turnId: TurnId.make("root-turn"),
      createdAt: "2026-09-07T00:00:01Z",
      payload: {},
      raw: {
        source: "codex.app-server.notification",
        method: "turn/started",
        payload: { threadId: "native-session", turn: { id: "root-turn" } },
      },
    });
    yield* sql`INSERT INTO workflow_interruption_subjects
      (reassessment_id,subject_id,subject_kind,native_session_id,native_turn_id,request_status,outcome,discovered_at,updated_at)
      VALUES ('reassessment','director','director','native-session','root-turn','not-issued','unknown','2026-09-07T00:00:01Z','2026-09-07T00:00:01Z')`;
    yield* recordWorkflowWorkerObservation({
      type: "turn.completed",
      eventId: EventId.make("parent-turn-completed"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      threadId: ThreadId.make("thread"),
      turnId: TurnId.make("root-turn"),
      createdAt: "2026-09-07T00:00:02Z",
      payload: { state: "completed" },
      raw: {
        source: "codex.app-server.notification",
        method: "turn/completed",
        payload: { threadId: "native-session", turn: { id: "root-turn" } },
      },
    });
    const subjects = yield* sql<{ outcome: string; detail: string | null }>`
      SELECT outcome, detail FROM workflow_interruption_subjects WHERE subject_id = 'director'
    `;
    NodeAssert.equal(subjects[0]?.outcome, "stopped");
    NodeAssert.match(subjects[0]?.detail ?? "", /completed/u);
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("a late old child completion cannot settle a newer identified native turn", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    yield* recordWorkflowWorkerObservation(childEvent("interrupted", 1, true));
    yield* recordWorkflowWorkerObservation({
      ...childEvent("running", 2, false),
      payload: {
        taskId: RuntimeTaskId.make("child"),
        status: "running",
        timelineBypass: true,
        nativeTurn: { sessionId: "native-session", turnId: "new-turn", status: "running" },
      },
    });
    yield* recordWorkflowWorkerObservation(childEvent("interrupted", 3, true));
    const subjects = yield* sql<{ outcome: string; nativeTurnId: string | null }>`
      SELECT outcome, native_turn_id AS "nativeTurnId"
      FROM workflow_interruption_subjects WHERE subject_id = 'child'
    `;
    NodeAssert.equal(subjects[0]?.outcome, "resumed");
    NodeAssert.equal(subjects[0]?.nativeTurnId, "new-turn");
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("a late request acknowledgment cannot downgrade matching native stop evidence", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    yield* recordWorkflowWorkerObservation(childEvent("interrupted", 1, true));
    yield* recordWorkflowWorkerObservation({
      ...childEvent("interrupted", 2, false),
      payload: {
        taskId: RuntimeTaskId.make("child"),
        status: "interrupted",
        timelineBypass: true,
        nativeInterruption: {
          attemptId: "old-attempt",
          sessionId: "native-session",
          turnId: "old-turn",
          requestStatus: "acknowledged",
        },
      },
    });
    const subjects = yield* sql<{ outcome: string; requestStatus: string }>`
      SELECT outcome, request_status AS "requestStatus"
      FROM workflow_interruption_subjects WHERE subject_id = 'child'
    `;
    NodeAssert.equal(subjects[0]?.outcome, "stopped");
    NodeAssert.equal(subjects[0]?.requestStatus, "acknowledged");
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("a late old terminal event cannot restore identity after newer generic activity", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    yield* sql`DELETE FROM workflow_reassessments WHERE reassessment_id = 'reassessment'`;
    yield* recordWorkflowWorkerObservation({
      ...childEvent("running", 1, false),
      payload: {
        taskId: RuntimeTaskId.make("child"),
        status: "running",
        timelineBypass: true,
        nativeTurn: { sessionId: "native-session", turnId: "old-turn", status: "running" },
      },
    });
    yield* recordWorkflowWorkerObservation(childEvent("running", 2, false));
    yield* recordWorkflowWorkerObservation(childEvent("interrupted", 3, true));
    const observations = yield* sql<{
      nativeSessionId: string | null;
      nativeTurnId: string | null;
      nativeTurnStatus: string | null;
    }>`SELECT native_session_id AS "nativeSessionId", native_turn_id AS "nativeTurnId",
        native_turn_status AS "nativeTurnStatus"
      FROM workflow_worker_observations WHERE provider_thread_id = 'child'`;
    NodeAssert.equal(observations[0]?.nativeSessionId, null);
    NodeAssert.equal(observations[0]?.nativeTurnId, null);
    NodeAssert.equal(observations[0]?.nativeTurnStatus, "unknown");
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);
