import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

type WorkerEvent = Extract<
  ProviderRuntimeEvent,
  { readonly type: "task.started" | "task.updated" | "task.completed" }
>;

function providerStatus(event: WorkerEvent): string {
  if (event.type === "task.started") return "running";
  if (event.type === "task.completed") return event.payload.status;
  return event.payload.status ?? "observed";
}

/** Persist native child identity separately from bounded timeline activity. */
export const recordWorkflowWorkerObservation = Effect.fn(
  "WorkflowWorkerPersistence.recordObservation",
)(function* (event: WorkerEvent) {
  if (event.payload.timelineBypass !== true) return;
  const sql = yield* SqlClient.SqlClient;
  const payload = event.payload;
  const title = "title" in payload ? (payload.title ?? null) : null;
  const role = payload.role ?? null;
  const agentPath = payload.agentPath ?? null;
  const parentProviderThreadId = payload.parentAgentId ?? null;
  const model = payload.model ?? null;
  const effort = payload.effort ?? null;
  const status = providerStatus(event);

  yield* sql`
    INSERT INTO workflow_worker_observations (
      director_id, provider_thread_id, parent_provider_thread_id, title, role, agent_path,
      observed_model, observed_effort, provider_status, last_event_kind,
      first_observed_at, updated_at
    )
    SELECT director_id, ${payload.taskId}, ${parentProviderThreadId}, ${title}, ${role}, ${agentPath},
      ${model}, ${effort}, ${status}, ${event.type}, ${event.createdAt}, ${event.createdAt}
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
      last_event_kind = excluded.last_event_kind,
      updated_at = excluded.updated_at
    WHERE excluded.updated_at >= workflow_worker_observations.updated_at
  `;
});
