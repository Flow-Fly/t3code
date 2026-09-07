import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

type WorkerEvent = Extract<
  ProviderRuntimeEvent,
  { readonly type: "task.started" | "task.updated" | "task.completed" }
>;

const NativeSessionPayload = Schema.Struct({ threadId: Schema.String });
const NativeNestedTurnPayload = Schema.Struct({ turn: Schema.Struct({ id: Schema.String }) });
const NativeFlatTurnPayload = Schema.Struct({ turnId: Schema.String });
const isNativeSessionPayload = Schema.is(NativeSessionPayload);
const isNativeNestedTurnPayload = Schema.is(NativeNestedTurnPayload);
const isNativeFlatTurnPayload = Schema.is(NativeFlatTurnPayload);

function nativeTurnIdentity(event: ProviderRuntimeEvent) {
  const raw = event.raw?.payload;
  if (!isNativeSessionPayload(raw)) return null;
  const nativeSessionId = raw.threadId;
  const nativeTurnId = isNativeNestedTurnPayload(raw)
    ? raw.turn.id
    : isNativeFlatTurnPayload(raw)
      ? raw.turnId
      : (event.providerRefs?.providerTurnId ?? null);
  return nativeSessionId && nativeTurnId ? { nativeSessionId, nativeTurnId } : null;
}

function providerStatus(event: WorkerEvent): string {
  if (event.type === "task.started") return "running";
  if (event.type === "task.completed") return event.payload.status;
  return event.payload.status ?? "observed";
}

/** Persist native child identity separately from bounded timeline activity. */
export const recordWorkflowWorkerObservation = Effect.fn(
  "WorkflowWorkerPersistence.recordObservation",
)(function* (event: ProviderRuntimeEvent) {
  const sql = yield* SqlClient.SqlClient;
  if (
    event.type === "turn.started" ||
    event.type === "turn.completed" ||
    event.type === "turn.aborted"
  ) {
    const identity = nativeTurnIdentity(event);
    if (!identity) return;
    const status =
      event.type === "turn.started"
        ? "running"
        : event.type === "turn.aborted" || event.payload.state === "interrupted"
          ? "interrupted"
          : event.payload.state === "failed"
            ? "failed"
            : "completed";
    const rootStarted = status === "running" ? 1 : 0;
    yield* sql`
      INSERT INTO workflow_director_native_turns
        (director_id, native_session_id, native_turn_id, status, updated_at)
      SELECT director_id, ${identity.nativeSessionId}, ${identity.nativeTurnId}, ${status}, ${event.createdAt}
      FROM workflow_directors
      WHERE thread_id = ${event.threadId} AND requested_instance_id = ${event.providerInstanceId ?? ""}
        AND is_current = 1
      ON CONFLICT(director_id) DO UPDATE SET
        native_session_id = excluded.native_session_id,
        native_turn_id = excluded.native_turn_id,
        status = excluded.status,
        updated_at = excluded.updated_at
      WHERE ${rootStarted}
        OR (workflow_director_native_turns.native_session_id = excluded.native_session_id
          AND workflow_director_native_turns.native_turn_id = excluded.native_turn_id)
    `;
    yield* sql`
      UPDATE workflow_interruption_subjects
      SET native_session_id = ${identity.nativeSessionId}, native_turn_id = ${identity.nativeTurnId},
        interrupt_attempt_id = CASE WHEN ${rootStarted} THEN NULL ELSE interrupt_attempt_id END,
        request_status = CASE WHEN ${rootStarted} THEN 'not-issued' ELSE request_status END,
        outcome = ${status === "running" ? "resumed" : "stopped"},
        detail = ${status === "interrupted" ? "The matching native director turn reported interrupted." : status === "running" ? "The director started another native turn while reassessment remains active." : `The matching native director turn ended with status ${status}.`},
        updated_at = ${event.createdAt}
      WHERE subject_kind = 'director'
        AND reassessment_id IN (
          SELECT reassessment_id FROM workflow_reassessments
          WHERE director_id IN (
            SELECT director_id FROM workflow_directors
            WHERE thread_id = ${event.threadId} AND requested_instance_id = ${event.providerInstanceId ?? ""}
              AND is_current = 1
          ) AND status != 'cleared'
        )
        AND (${rootStarted} OR (
          native_session_id = ${identity.nativeSessionId} AND native_turn_id = ${identity.nativeTurnId}
        ))
    `;
    return;
  }
  if (
    event.type !== "task.started" &&
    event.type !== "task.updated" &&
    event.type !== "task.completed"
  ) {
    return;
  }
  if (event.payload.timelineBypass !== true) return;
  const payload = event.payload;
  const title = "title" in payload ? (payload.title ?? null) : null;
  const role = payload.role ?? null;
  const agentPath = payload.agentPath ?? null;
  const parentProviderThreadId = payload.parentAgentId ?? null;
  const model = payload.model ?? null;
  const effort = payload.effort ?? null;
  const status = providerStatus(event);
  const nativeLifecycle = payload.nativeLifecycle ?? null;
  const nativeTurn = payload.nativeTurn;
  const childStarted = nativeTurn?.status === "running" ? 1 : 0;
  const genericRunning = !nativeTurn && status === "running" ? 1 : 0;
  const updatesLifecycle =
    event.type === "task.started" ||
    event.type === "task.completed" ||
    (event.type === "task.updated" && event.payload.status !== undefined)
      ? 1
      : 0;

  yield* sql`
    INSERT INTO workflow_worker_observations (
      director_id, provider_thread_id, parent_provider_thread_id, title, role, agent_path,
      observed_model, observed_effort, provider_status, native_lifecycle, last_event_kind,
      native_session_id, native_turn_id, native_turn_status,
      first_observed_at, updated_at
    )
    SELECT director_id, ${payload.taskId}, ${parentProviderThreadId}, ${title}, ${role}, ${agentPath},
      ${model}, ${effort}, ${status}, ${nativeLifecycle}, ${event.type},
      ${nativeTurn?.sessionId ?? null}, ${nativeTurn?.turnId ?? null}, ${nativeTurn?.status ?? (genericRunning ? "unknown" : null)},
      ${event.createdAt}, ${event.createdAt}
    FROM workflow_directors
    WHERE thread_id = ${event.threadId} AND requested_instance_id = ${event.providerInstanceId ?? ""}
      AND is_current = 1
    ON CONFLICT(director_id, provider_thread_id) DO UPDATE SET
      parent_provider_thread_id = COALESCE(excluded.parent_provider_thread_id, parent_provider_thread_id),
      title = COALESCE(excluded.title, title),
      role = COALESCE(excluded.role, role),
      agent_path = COALESCE(excluded.agent_path, agent_path),
      observed_model = COALESCE(excluded.observed_model, observed_model),
      observed_effort = COALESCE(excluded.observed_effort, observed_effort),
      provider_status = CASE
        WHEN excluded.provider_status = 'observed' THEN provider_status
        ELSE excluded.provider_status
      END,
      native_lifecycle = CASE
        WHEN excluded.native_lifecycle = 'closed' THEN 'closed'
        WHEN ${updatesLifecycle} THEN NULL
        ELSE native_lifecycle
      END,
      native_session_id = CASE
        WHEN ${childStarted} OR ${genericRunning} THEN excluded.native_session_id
        WHEN native_turn_status IS NULL OR (
          native_session_id = excluded.native_session_id AND native_turn_id = excluded.native_turn_id
        ) THEN excluded.native_session_id
        ELSE native_session_id
      END,
      native_turn_id = CASE
        WHEN ${childStarted} OR ${genericRunning} THEN excluded.native_turn_id
        WHEN native_turn_status IS NULL OR (
          native_session_id = excluded.native_session_id AND native_turn_id = excluded.native_turn_id
        ) THEN excluded.native_turn_id
        ELSE native_turn_id
      END,
      native_turn_status = CASE
        WHEN ${childStarted} OR ${genericRunning} THEN excluded.native_turn_status
        WHEN native_turn_status IS NULL OR (
          native_session_id = excluded.native_session_id AND native_turn_id = excluded.native_turn_id
        ) THEN excluded.native_turn_status
        ELSE native_turn_status
      END,
      last_event_kind = excluded.last_event_kind,
      updated_at = excluded.updated_at
    WHERE excluded.updated_at >= workflow_worker_observations.updated_at
  `;

  if (nativeTurn) {
    yield* sql`
      INSERT INTO workflow_interruption_subjects (
        reassessment_id, subject_id, subject_kind, provider_thread_id, parent_provider_thread_id,
        native_session_id, native_turn_id, interrupt_attempt_id, request_status, outcome, detail,
        discovered_at, updated_at
      )
      SELECT r.reassessment_id, ${payload.taskId},
        CASE
          WHEN EXISTS (
            SELECT 1 FROM workflow_worker_dispatches d
            WHERE d.director_id = r.director_id AND d.provider_thread_id = ${payload.taskId}
          ) THEN 'worker'
          WHEN EXISTS (
            SELECT 1 FROM workflow_ticket_reviews v
            WHERE v.director_id = r.director_id AND v.provider_thread_id = ${payload.taskId}
          ) OR EXISTS (
            SELECT 1 FROM workflow_review_axes a
            JOIN workflow_ticket_reviews v ON v.review_id = a.review_id
            WHERE v.director_id = r.director_id AND a.provider_thread_id = ${payload.taskId}
          ) THEN 'reviewer'
          ELSE 'unknown-child'
        END,
        ${payload.taskId}, ${parentProviderThreadId},
        ${nativeTurn.sessionId}, ${nativeTurn.turnId}, NULL, 'not-issued',
        ${nativeTurn.status === "running" ? "resumed" : "stopped"},
        ${nativeTurn.status === "running" ? "Child activity appeared while reassessment remains active." : `The matching native child turn ended with status ${nativeTurn.status}.`},
        ${event.createdAt}, ${event.createdAt}
      FROM workflow_reassessments r
      JOIN workflow_directors d ON d.director_id = r.director_id
      WHERE d.thread_id = ${event.threadId} AND d.requested_instance_id = ${event.providerInstanceId ?? ""}
        AND d.is_current = 1 AND r.status != 'cleared'
      ON CONFLICT(reassessment_id, subject_id) DO UPDATE SET
        subject_kind = excluded.subject_kind,
        parent_provider_thread_id = COALESCE(excluded.parent_provider_thread_id, parent_provider_thread_id),
        native_session_id = excluded.native_session_id,
        native_turn_id = excluded.native_turn_id,
        interrupt_attempt_id = CASE WHEN ${childStarted} THEN NULL ELSE interrupt_attempt_id END,
        request_status = CASE WHEN ${childStarted} THEN 'not-issued' ELSE request_status END,
        outcome = excluded.outcome, detail = excluded.detail,
        updated_at = excluded.updated_at
      WHERE ${childStarted}
        OR (native_session_id = excluded.native_session_id AND native_turn_id = excluded.native_turn_id)
    `;
  }
  if (!nativeTurn && status === "running") {
    yield* sql`
      INSERT INTO workflow_interruption_subjects (
        reassessment_id, subject_id, subject_kind, provider_thread_id, parent_provider_thread_id,
        request_status, outcome, detail, discovered_at, updated_at
      )
      SELECT r.reassessment_id, ${payload.taskId},
        CASE
          WHEN EXISTS (
            SELECT 1 FROM workflow_worker_dispatches d
            WHERE d.director_id = r.director_id AND d.provider_thread_id = ${payload.taskId}
          ) THEN 'worker'
          WHEN EXISTS (
            SELECT 1 FROM workflow_ticket_reviews v
            WHERE v.director_id = r.director_id AND v.provider_thread_id = ${payload.taskId}
          ) OR EXISTS (
            SELECT 1 FROM workflow_review_axes a
            JOIN workflow_ticket_reviews v ON v.review_id = a.review_id
            WHERE v.director_id = r.director_id AND a.provider_thread_id = ${payload.taskId}
          ) THEN 'reviewer'
          ELSE 'unknown-child'
        END,
        ${payload.taskId}, ${parentProviderThreadId}, 'not-issued', 'resumed',
        'Child activity appeared without a current native turn identity.',
        ${event.createdAt}, ${event.createdAt}
      FROM workflow_reassessments r
      JOIN workflow_directors d ON d.director_id = r.director_id
      WHERE d.thread_id = ${event.threadId} AND d.requested_instance_id = ${event.providerInstanceId ?? ""}
        AND d.is_current = 1 AND r.status != 'cleared'
      ON CONFLICT(reassessment_id, subject_id) DO UPDATE SET
        parent_provider_thread_id = COALESCE(excluded.parent_provider_thread_id, parent_provider_thread_id),
        native_session_id = NULL, native_turn_id = NULL, interrupt_attempt_id = NULL,
        request_status = 'not-issued', outcome = 'resumed', detail = excluded.detail,
        updated_at = excluded.updated_at
      WHERE excluded.updated_at >= workflow_interruption_subjects.updated_at
    `;
  }
  const interruption = payload.nativeInterruption;
  if (interruption) {
    const confirmsStopped = interruption.completionStatus === "interrupted" ? 1 : 0;
    const outcome =
      interruption.completionStatus === "interrupted"
        ? "stopped"
        : interruption.requestStatus === "failed"
          ? "failed"
          : interruption.requestStatus === "unknown" || interruption.requestStatus === "not-issued"
            ? "unknown"
            : "stopping";
    yield* sql`
      UPDATE workflow_interruption_subjects
      SET interrupt_attempt_id = ${interruption.attemptId},
        request_status = ${interruption.requestStatus},
        outcome = CASE
          WHEN ${confirmsStopped} THEN 'stopped'
          WHEN outcome IN ('stopped', 'closed') THEN outcome
          ELSE ${outcome}
        END,
        detail = CASE
          WHEN ${confirmsStopped} THEN 'The current native child turn reported interrupted.'
          WHEN outcome IN ('stopped', 'closed') THEN detail
          ELSE ${interruption.detail ?? null}
        END,
        updated_at = ${event.createdAt}
      WHERE subject_id = ${payload.taskId}
        AND reassessment_id IN (
          SELECT r.reassessment_id FROM workflow_reassessments r
          JOIN workflow_directors d ON d.director_id = r.director_id
          WHERE d.thread_id = ${event.threadId}
            AND d.requested_instance_id = ${event.providerInstanceId ?? ""}
            AND d.is_current = 1 AND r.status != 'cleared'
        )
        AND native_session_id = ${interruption.sessionId}
        AND native_turn_id = ${interruption.turnId}
    `;
  }
  if (nativeLifecycle === "closed") {
    yield* sql`
      UPDATE workflow_interruption_subjects SET outcome = 'closed',
        detail = 'The native child thread closed.', updated_at = ${event.createdAt}
      WHERE subject_id = ${payload.taskId}
        AND reassessment_id IN (
          SELECT r.reassessment_id FROM workflow_reassessments r
          JOIN workflow_directors d ON d.director_id = r.director_id
          WHERE d.thread_id = ${event.threadId}
            AND d.requested_instance_id = ${event.providerInstanceId ?? ""}
            AND d.is_current = 1 AND r.status != 'cleared'
        )
    `;
  }
});
