import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("055_WorkflowReviewCheckIdentity", (it) => {
  it.effect("backfills receipt scope and permits reuse only across provider sessions", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      const seedReview = (suffix: string) =>
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO workflow_directors (
              director_id, batch_id, environment_id, project_id, repository, root_number,
              capability_number, thread_id, command_id, message_id, worktree_path,
              worktree_branch, status, requested_model, requested_instance_id,
              requested_effort, observed_match, initial_turn_disposition, created_at, updated_at
            ) VALUES (
              ${`director-${suffix}`}, ${`batch-${suffix}`}, ${`environment-${suffix}`}, 'project',
              'Flow-Fly/t3code', 10, ${suffix === "one" ? 17 : 18}, ${`thread-${suffix}`},
              ${`command-${suffix}`}, ${`message-${suffix}`}, ${`/tmp/worktree-${suffix}`},
              ${`capability/workflow-${suffix}`}, 'active', 'gpt-6-astra', ${`codex-${suffix}`},
              'high', 'unknown', 'accepted', '2026-09-07T09:00:00.000Z',
              '2026-09-07T09:00:00.000Z'
            )
          `;
          yield* sql`
            INSERT INTO workflow_director_admissions (
              admission_id, director_id, batch_id, repository, ticket_id, ticket_number,
              slot_ticket_number, purpose, ownership, claim_status, created_at, updated_at
            ) VALUES (
              ${`admission-${suffix}`}, ${`director-${suffix}`}, ${`batch-${suffix}`},
              'Flow-Fly/t3code', ${`ticket-${suffix}`}, ${suffix === "one" ? 20 : 21},
              ${suffix === "one" ? 20 : 21}, 'implement', 'ticket', 'confirmed',
              '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
            )
          `;
          yield* sql`
            INSERT INTO workflow_worker_dispatches (
              dispatch_id, association_token, director_id, batch_id, admission_id, repository,
              ticket_number, ownership, write_paths_json, requested_model, requested_effort,
              requested_skill_path, status, created_at, updated_at
            ) VALUES (
              ${`dispatch-${suffix}`}, ${`worker-token-${suffix}`}, ${`director-${suffix}`},
              ${`batch-${suffix}`}, ${`admission-${suffix}`}, 'Flow-Fly/t3code',
              ${suffix === "one" ? 20 : 21}, 'ticket', '["ticket"]', 'gpt-5.6-sol', 'high',
              '/skills/implement/SKILL.md', 'reported-succeeded',
              '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
            )
          `;
          yield* sql`
            INSERT INTO workflow_ticket_reviews (
              review_id, association_token, director_id, batch_id, admission_id,
              implementation_dispatch_id, repository, ticket_number, fixed_base,
              implementation_head, scope_body, requested_model, requested_effort,
              requested_skill_path, status, created_at, updated_at
            ) VALUES (
              ${`review-${suffix}`}, ${`review-token-${suffix}`}, ${`director-${suffix}`},
              ${`batch-${suffix}`}, ${`admission-${suffix}`}, ${`dispatch-${suffix}`},
              'Flow-Fly/t3code', ${suffix === "one" ? 20 : 21}, 'base', 'head', 'scope',
              'gpt-6-astra', 'medium', '/skills/code-review/SKILL.md', 'prepared',
              '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
            )
          `;
        });

      yield* seedReview("one");
      yield* sql`
        INSERT INTO workflow_review_checks (
          review_id, label, command, tool_call_id, exit_code, output, started_head,
          finished_head, started_clean, finished_clean, verification_status,
          created_at, updated_at
        ) VALUES (
          'review-one', 'focused', 'vp test run focused.test.ts', 'native-command',
          0, 'passed', 'head', 'head', 1, 1, 'passed',
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 55 });
      const backfilled = yield* sql<{
        readonly threadId: string | null;
        readonly providerInstanceId: string | null;
      }>`
        SELECT thread_id AS "threadId", provider_instance_id AS "providerInstanceId"
        FROM workflow_review_checks WHERE review_id = 'review-one'
      `;
      assert.deepStrictEqual(backfilled, [
        { threadId: "thread-one", providerInstanceId: "codex-one" },
      ]);

      yield* seedReview("two");
      const otherSession = yield* sql`
        INSERT INTO workflow_review_checks (
          review_id, label, command, tool_call_id, exit_code, output, started_head,
          finished_head, started_clean, finished_clean, verification_status,
          thread_id, provider_instance_id, created_at, updated_at
        ) VALUES (
          'review-two', 'focused', 'vp test run focused.test.ts', 'native-command',
          0, 'passed', 'head', 'head', 1, 1, 'passed', 'thread-two', 'codex-two',
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `.pipe(Effect.result);
      assert.strictEqual(otherSession._tag, "Success");

      yield* sql`
        INSERT INTO workflow_ticket_reviews (
          review_id, association_token, director_id, batch_id, admission_id,
          implementation_dispatch_id, repository, ticket_number, fixed_base,
          implementation_head, scope_body, requested_model, requested_effort,
          requested_skill_path, status, created_at, updated_at
        ) VALUES (
          'review-one-retry', 'review-token-one-retry', 'director-one', 'batch-one',
          'admission-one', 'dispatch-one', 'Flow-Fly/t3code', 20, 'base', 'head-two',
          'scope', 'gpt-6-astra', 'medium', '/skills/code-review/SKILL.md', 'prepared',
          '2026-09-07T10:00:00.000Z', '2026-09-07T10:00:00.000Z'
        )
      `;
      const sameSession = yield* sql`
        INSERT INTO workflow_review_checks (
          review_id, label, command, tool_call_id, exit_code, output, started_head,
          finished_head, started_clean, finished_clean, verification_status,
          thread_id, provider_instance_id, created_at, updated_at
        ) VALUES (
          'review-one-retry', 'focused', 'vp test run focused.test.ts', 'native-command',
          0, 'passed', 'head-two', 'head-two', 1, 1, 'passed', 'thread-one', 'codex-one',
          '2026-09-07T10:00:00.000Z', '2026-09-07T10:00:00.000Z'
        )
      `.pipe(Effect.result);
      assert.strictEqual(sameSession._tag, "Failure");
    }),
  );
});
