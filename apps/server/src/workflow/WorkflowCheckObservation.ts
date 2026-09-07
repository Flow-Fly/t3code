import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const NativeCommandData = Schema.Struct({
  item: Schema.Struct({
    type: Schema.Literal("commandExecution"),
    command: Schema.String,
    cwd: Schema.optional(Schema.NullOr(Schema.String)),
    exitCode: Schema.optional(Schema.NullOr(Schema.Number)),
    status: Schema.optional(Schema.String),
    aggregatedOutput: Schema.optional(Schema.String),
  }),
});
const decodeNativeCommandData = Schema.decodeUnknownEffect(NativeCommandData);

type CommandEvent = Extract<
  ProviderRuntimeEvent,
  { readonly type: "item.started" | "item.completed" }
>;

/** Retains compact native command evidence with its provider session identity. */
export const recordWorkflowCheckObservation = Effect.fn("WorkflowCheckObservation.record")(
  function* (event: CommandEvent) {
    if (
      event.payload.itemType !== "command_execution" ||
      !event.itemId ||
      !event.providerInstanceId
    ) {
      return;
    }
    const decoded = yield* decodeNativeCommandData(event.payload.data).pipe(Effect.option);
    if (Option.isNone(decoded)) return;
    const item = decoded.value.item;
    const sql = yield* SqlClient.SqlClient;
    const active = yield* sql<{ readonly registered: number }>`
    SELECT 1 AS registered
    FROM workflow_directors d
    JOIN workflow_ticket_reviews r ON r.director_id = d.director_id
    JOIN workflow_review_checks c ON c.review_id = r.review_id
    WHERE d.thread_id = ${event.threadId}
      AND d.requested_instance_id = ${event.providerInstanceId}
      AND c.verification_status <> 'passed'
      AND ${event.createdAt} > c.created_at
    LIMIT 1
  `;
    if (!active[0]) return;
    yield* sql`
    INSERT INTO workflow_native_command_observations (
      thread_id, provider_instance_id, tool_call_id, lifecycle, command, cwd,
      status, exit_code, output, created_at
    ) VALUES (
      ${event.threadId}, ${event.providerInstanceId}, ${event.itemId},
      ${event.type === "item.started" ? "started" : "completed"}, ${item.command},
      ${item.cwd ?? null}, ${event.payload.status ?? item.status ?? null},
      ${item.exitCode ?? null}, ${(item.aggregatedOutput ?? "").slice(-4_000)}, ${event.createdAt}
    )
    ON CONFLICT(thread_id, provider_instance_id, tool_call_id, lifecycle) DO UPDATE SET
      command = excluded.command, cwd = excluded.cwd, status = excluded.status,
      exit_code = excluded.exit_code, output = excluded.output, created_at = excluded.created_at
    WHERE excluded.created_at >= workflow_native_command_observations.created_at
  `;
  },
);
