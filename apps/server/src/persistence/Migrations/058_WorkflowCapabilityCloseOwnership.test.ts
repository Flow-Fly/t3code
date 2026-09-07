import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("058_WorkflowCapabilityCloseOwnership", (it) => {
  it.effect("does not infer close ownership for existing confirmed completions", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 57 });
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path,
          worktree_branch, status, requested_model, requested_instance_id,
          requested_effort, observed_match, initial_turn_disposition,
          specification_fingerprint, breakdown_fingerprint, created_at, updated_at
        ) VALUES (
          'director-1', 'batch-1', 'environment-1', 'project-1', 'Flow-Fly/t3code', 10,
          17, 'thread-1', 'command-1', 'message-1', '/tmp/worktree',
          'capability/workflow-10', 'active', 'gpt-6-astra', 'codex',
          'high', 'unknown', 'accepted', 'specification', 'breakdown',
          '2026-09-06T10:00:00.000Z', '2026-09-06T10:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_capability_completions (
          completion_id, director_id, repository, capability_number, resulting_head,
          specification_fingerprint, breakdown_fingerprint, comment_body, status,
          close_confirmed, required_action, created_at, updated_at
        ) VALUES (
          'completion-1', 'director-1', 'Flow-Fly/t3code', 17,
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'specification', 'breakdown',
          'evidence', 'completed', 1, 'No action required.',
          '2026-09-06T11:00:00.000Z', '2026-09-06T11:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 58 });
      const completions = yield* sql<{
        readonly closeConfirmed: number;
        readonly closeOwned: number;
      }>`
        SELECT close_confirmed AS "closeConfirmed", close_owned AS "closeOwned"
        FROM workflow_capability_completions
      `;

      assert.deepStrictEqual(completions, [{ closeConfirmed: 1, closeOwned: 0 }]);
    }),
  );
});
