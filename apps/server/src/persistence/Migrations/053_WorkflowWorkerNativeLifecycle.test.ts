import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("053_WorkflowWorkerNativeLifecycle", (it) => {
  it.effect("upgrades existing worker observations without inventing closure evidence", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 52 });
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path,
          worktree_branch, status, requested_model, requested_instance_id,
          requested_effort, observed_match, initial_turn_disposition, created_at, updated_at
        ) VALUES (
          'director-1', 'batch-1', 'environment-1', 'project-1', 'Flow-Fly/t3code', 10,
          17, 'thread-1', 'command-1', 'message-1', '/tmp/worktree',
          'capability/workflow-10', 'active', 'gpt-6-astra', 'codex',
          'high', 'unknown', 'accepted', '2026-09-06T10:00:00.000Z',
          '2026-09-06T10:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_worker_observations (
          director_id, provider_thread_id, provider_status, last_event_kind,
          first_observed_at, updated_at
        ) VALUES (
          'director-1', 'worker-1', 'idle', 'task.updated',
          '2026-09-06T11:00:00.000Z', '2026-09-06T11:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 53 });
      const observations = yield* sql<{
        readonly providerThreadId: string;
        readonly nativeLifecycle: string | null;
      }>`
        SELECT provider_thread_id AS "providerThreadId",
          native_lifecycle AS "nativeLifecycle"
        FROM workflow_worker_observations
      `;

      assert.deepStrictEqual(observations, [
        { providerThreadId: "worker-1", nativeLifecycle: null },
      ]);
    }),
  );
});
