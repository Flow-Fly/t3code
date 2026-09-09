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
    aggregatedOutput: Schema.optional(Schema.NullOr(Schema.String)),
  }),
});
const decodeNativeCommandData = Schema.decodeUnknownEffect(NativeCommandData);

type CommandEvent = Extract<
  ProviderRuntimeEvent,
  { readonly type: "item.started" | "item.completed" }
>;

const literalPosixShells = ["/bin/zsh", "/bin/bash", "/bin/sh"] as const;

function quotePosixLiteral(command: string) {
  return `'${command.replaceAll("'", "'\\''")}'`;
}

function matchesRegisteredCommand(observed: string, registered: string) {
  if (observed === registered) return true;
  const literal = quotePosixLiteral(registered);
  return literalPosixShells.some((shell) => observed === `${shell} -c ${literal}`);
}

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
    const cwd = item.cwd ?? null;
    if (!cwd) return;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      DELETE FROM workflow_native_command_observations
      WHERE thread_id = ${event.threadId}
        AND provider_instance_id = ${event.providerInstanceId}
        AND (
          EXISTS (
            SELECT 1 FROM workflow_review_checks bound
            WHERE bound.tool_call_id = workflow_native_command_observations.tool_call_id
              AND bound.thread_id = workflow_native_command_observations.thread_id
              AND bound.provider_instance_id =
                workflow_native_command_observations.provider_instance_id
          )
          OR EXISTS (
            SELECT 1 FROM workflow_capability_checks bound
            WHERE bound.tool_call_id = workflow_native_command_observations.tool_call_id
              AND bound.thread_id = workflow_native_command_observations.thread_id
              AND bound.provider_instance_id =
                workflow_native_command_observations.provider_instance_id
          )
          OR NOT EXISTS (
            SELECT 1 FROM (
              SELECT c.created_at AS registered_at
              FROM workflow_directors d
              JOIN workflow_ticket_reviews r ON r.director_id = d.director_id
              JOIN workflow_review_checks c ON c.review_id = r.review_id
              WHERE d.thread_id = workflow_native_command_observations.thread_id
                AND d.requested_instance_id = workflow_native_command_observations.provider_instance_id
                AND d.is_current = 1
                AND c.verification_status <> 'passed'
                AND c.command = workflow_native_command_observations.command
                AND d.worktree_path = workflow_native_command_observations.cwd
                AND NOT EXISTS (
                  SELECT 1 FROM workflow_ticket_reviews newer
                  WHERE newer.director_id = r.director_id
                    AND newer.ticket_number = r.ticket_number
                    AND (
                      newer.created_at > r.created_at
                      OR (newer.created_at = r.created_at AND newer.rowid > r.rowid)
                    )
                )
              UNION ALL
              SELECT c.created_at AS registered_at
              FROM workflow_directors d
              JOIN workflow_capability_completions completion
                ON completion.director_id = d.director_id
              JOIN workflow_capability_checks c
                ON c.completion_id = completion.completion_id
              WHERE d.thread_id = workflow_native_command_observations.thread_id
                AND d.requested_instance_id = workflow_native_command_observations.provider_instance_id
                AND d.is_current = 1
                AND completion.status IN ('checks-pending', 'checks-failed')
                AND c.verification_status <> 'passed'
                AND c.command = workflow_native_command_observations.command
                AND d.worktree_path = workflow_native_command_observations.cwd
            ) eligible
            WHERE workflow_native_command_observations.created_at > eligible.registered_at
          )
        )
    `;
    const registeredRows = yield* sql<{
      readonly command: string;
      readonly registeredAt: string;
    }>`
      SELECT c.command, c.created_at AS "registeredAt"
      FROM workflow_directors d
      JOIN workflow_ticket_reviews r ON r.director_id = d.director_id
      JOIN workflow_review_checks c ON c.review_id = r.review_id
      WHERE d.thread_id = ${event.threadId}
        AND d.requested_instance_id = ${event.providerInstanceId}
        AND d.is_current = 1
        AND c.verification_status <> 'passed'
        AND d.worktree_path = ${cwd}
        AND NOT EXISTS (
          SELECT 1 FROM workflow_ticket_reviews newer
          WHERE newer.director_id = r.director_id
            AND newer.ticket_number = r.ticket_number
            AND (
              newer.created_at > r.created_at
              OR (newer.created_at = r.created_at AND newer.rowid > r.rowid)
            )
        )
      UNION ALL
      SELECT c.command, c.created_at AS "registeredAt"
      FROM workflow_directors d
      JOIN workflow_capability_completions completion
        ON completion.director_id = d.director_id
      JOIN workflow_capability_checks c ON c.completion_id = completion.completion_id
      WHERE d.thread_id = ${event.threadId}
        AND d.requested_instance_id = ${event.providerInstanceId}
        AND d.is_current = 1
        AND completion.status IN ('checks-pending', 'checks-failed')
        AND c.verification_status <> 'passed'
        AND d.worktree_path = ${cwd}
    `;
    const matchingRows = registeredRows.filter((candidate) =>
      matchesRegisteredCommand(item.command, candidate.command),
    );
    const matchingCommands = new Set(matchingRows.map((candidate) => candidate.command));
    if (
      matchingCommands.size !== 1 ||
      !matchingRows.some((candidate) => event.createdAt > candidate.registeredAt)
    ) {
      return;
    }
    const command = matchingRows[0]!.command;
    const registered = matchingRows.length;
    if (event.type === "item.completed") {
      const started = yield* sql<{ readonly observed: number }>`
        SELECT 1 AS observed FROM workflow_native_command_observations
        WHERE thread_id = ${event.threadId}
          AND provider_instance_id = ${event.providerInstanceId}
          AND tool_call_id = ${event.itemId}
          AND lifecycle = 'started'
          AND command = ${command}
          AND cwd = ${cwd}
        LIMIT 1
      `;
      if (!started[0]) return;
    }
    yield* sql`
      INSERT INTO workflow_native_command_observations (
        thread_id, provider_instance_id, tool_call_id, lifecycle, command, cwd,
        status, exit_code, output, created_at
      ) VALUES (
        ${event.threadId}, ${event.providerInstanceId}, ${event.itemId},
        ${event.type === "item.started" ? "started" : "completed"}, ${command},
        ${cwd}, ${event.payload.status ?? item.status ?? null},
        ${item.exitCode ?? null}, ${(item.aggregatedOutput ?? "").slice(-4_000)}, ${event.createdAt}
      )
      ON CONFLICT(thread_id, provider_instance_id, tool_call_id, lifecycle) DO UPDATE SET
        command = excluded.command, cwd = excluded.cwd, status = excluded.status,
        exit_code = excluded.exit_code, output = excluded.output, created_at = excluded.created_at
      WHERE (
        excluded.lifecycle = 'started'
        AND excluded.created_at < workflow_native_command_observations.created_at
      ) OR (
        excluded.lifecycle = 'completed'
        AND excluded.created_at >= workflow_native_command_observations.created_at
      )
    `;
    if (event.type === "item.started") {
      yield* sql`
        DELETE FROM workflow_native_command_observations
        WHERE thread_id = ${event.threadId}
          AND provider_instance_id = ${event.providerInstanceId}
          AND command = ${command}
          AND cwd = ${cwd}
          AND tool_call_id IN (
            SELECT candidate.tool_call_id
            FROM workflow_native_command_observations candidate
            WHERE candidate.thread_id = ${event.threadId}
              AND candidate.provider_instance_id = ${event.providerInstanceId}
              AND candidate.command = ${command}
              AND candidate.cwd = ${cwd}
              AND NOT EXISTS (
                SELECT 1 FROM workflow_review_checks bound
                WHERE bound.tool_call_id = candidate.tool_call_id
                  AND bound.thread_id = candidate.thread_id
                  AND bound.provider_instance_id = candidate.provider_instance_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM workflow_capability_checks bound
                WHERE bound.tool_call_id = candidate.tool_call_id
                  AND bound.thread_id = candidate.thread_id
                  AND bound.provider_instance_id = candidate.provider_instance_id
              )
            GROUP BY candidate.tool_call_id
            ORDER BY MAX(candidate.created_at) DESC, candidate.tool_call_id DESC
            LIMIT -1 OFFSET ${registered}
          )
      `;
    }
  },
);
