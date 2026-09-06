import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("050_WorkflowRecovery", (it) => {
  it.effect("preserves the existing attempt and permits history with one current attempt", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 49 });
      yield* sql`
        INSERT INTO workflow_start_attempts (
          attempt_id, environment_id, project_id, repository, root_number,
          issue_number, phase, thread_id, command_id, message_id, status,
          claim_login, claim_owned, sequence, created_at, updated_at, detail
        ) VALUES (
          'attempt-original', 'environment-1', 'project-1', 'Flow-Fly/t3code', 10,
          15, 'decision', 'thread-original', 'command-original', 'message-original', 'submitted',
          'Flow-Fly', 1, 42, '2026-09-06T10:00:00.000Z', '2026-09-06T10:01:00.000Z', NULL
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 50 });
      const preserved = yield* sql<{
        readonly attemptId: string;
        readonly threadId: string;
        readonly claimLogin: string;
        readonly claimOwned: number;
        readonly initialTurnDisposition: string | null;
        readonly isCurrent: number;
      }>`
        SELECT attempt_id AS "attemptId", thread_id AS "threadId",
          claim_login AS "claimLogin", claim_owned AS "claimOwned",
          initial_turn_disposition AS "initialTurnDisposition", is_current AS "isCurrent"
        FROM workflow_start_attempts
      `;
      assert.deepStrictEqual(preserved, [
        {
          attemptId: "attempt-original",
          threadId: "thread-original",
          claimLogin: "Flow-Fly",
          claimOwned: 1,
          initialTurnDisposition: null,
          isCurrent: 1,
        },
      ]);

      yield* sql`UPDATE workflow_start_attempts SET is_current = 0 WHERE attempt_id = 'attempt-original'`;
      yield* sql`
        INSERT INTO workflow_start_attempts (
          attempt_id, environment_id, project_id, repository, root_number,
          issue_number, phase, thread_id, command_id, message_id, status,
          created_at, updated_at, is_current
        ) VALUES (
          'attempt-fresh', 'environment-1', 'project-1', 'Flow-Fly/t3code', 10,
          15, 'decision', 'thread-fresh', 'command-fresh', 'message-fresh', 'claiming',
          '2026-09-06T11:00:00.000Z', '2026-09-06T11:00:00.000Z', 1
        )
      `;
      const duplicateCurrent = sql`
        INSERT INTO workflow_start_attempts (
          attempt_id, environment_id, project_id, repository, root_number,
          issue_number, phase, thread_id, command_id, message_id, status,
          created_at, updated_at, is_current
        ) VALUES (
          'attempt-conflict', 'environment-1', 'project-1', 'Flow-Fly/t3code', 10,
          15, 'decision', 'thread-conflict', 'command-conflict', 'message-conflict', 'claiming',
          '2026-09-06T12:00:00.000Z', '2026-09-06T12:00:00.000Z', 1
        )
      `;
      assert.isTrue(yield* Effect.isFailure(duplicateCurrent));
      assert.equal((yield* sql`SELECT attempt_id FROM workflow_start_attempts`).length, 2);
    }),
  );
});
