import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { recordWorkflowCheckObservation } from "./WorkflowCheckObservation.ts";

const layer = it.layer(SqlitePersistenceMemory);

layer("WorkflowCheckObservation", (it) => {
  it.effect("retains compact native start and completion under the exact provider session", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path, worktree_branch,
          status, requested_model, requested_instance_id, requested_effort, observed_match,
          initial_turn_disposition, is_current, created_at, updated_at
        ) VALUES (
          'director', 'batch', 'environment', 'project', 'Flow-Fly/t3code', 10, 17,
          'director-thread', 'command', 'message', '/tmp/capability', 'capability/workflow-17',
          'active', 'gpt-6-astra', 'codex-review', 'high', 'unknown', 'accepted', 1,
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_director_admissions (
          admission_id, director_id, batch_id, repository, ticket_id, ticket_number, slot_ticket_number,
          purpose, ownership, claim_status, created_at, updated_at
        ) VALUES (
          'admission', 'director', 'batch', 'Flow-Fly/t3code', 'ticket-20', 20, 20,
          'implement', 'ticket', 'confirmed',
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_worker_dispatches (
          dispatch_id, association_token, director_id, batch_id, admission_id, repository,
          ticket_number, ownership, write_paths_json, requested_model, requested_effort,
          requested_skill_path, status, created_at, updated_at
        ) VALUES (
          'dispatch', 'worker-token', 'director', 'batch', 'admission', 'Flow-Fly/t3code',
          20, 'ticket', '["ticket"]', 'gpt-5.6-sol', 'high', '/skills/implement/SKILL.md',
          'reported-succeeded', '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_ticket_reviews (
          review_id, association_token, director_id, batch_id, admission_id,
          implementation_dispatch_id, repository, ticket_number, fixed_base,
          implementation_head, scope_body, requested_model, requested_effort,
          requested_skill_path, status, created_at, updated_at
        ) VALUES (
          'review', 'review-token', 'director', 'batch', 'admission', 'dispatch',
          'Flow-Fly/t3code', 20, 'base', 'head', 'scope', 'gpt-6-astra', 'medium',
          '/skills/code-review/SKILL.md', 'checks-pending',
          '2026-09-07T09:30:00.000Z', '2026-09-07T09:30:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_review_checks (
          review_id, label, command, output, started_head, started_clean,
          verification_status, created_at, updated_at
        ) VALUES (
          'review', 'focused', 'vp test run focused.test.ts', '', 'head', 1,
          'pending', '2026-09-07T09:30:00.000Z', '2026-09-07T09:30:00.000Z'
        )
      `;
      const base = {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex-review"),
        threadId: ThreadId.make("director-thread"),
        itemId: RuntimeItemId.make("check-item"),
      } as const;
      yield* recordWorkflowCheckObservation({
        ...base,
        type: "item.started",
        eventId: EventId.make("check-started"),
        createdAt: "2026-09-07T10:00:00.000Z",
        payload: {
          itemType: "command_execution",
          status: "inProgress",
          data: {
            item: {
              type: "commandExecution",
              command: "vp test run focused.test.ts",
              cwd: "/tmp/capability",
            },
          },
        },
      });
      yield* recordWorkflowCheckObservation({
        ...base,
        type: "item.completed",
        eventId: EventId.make("check-completed"),
        createdAt: "2026-09-07T10:00:01.000Z",
        payload: {
          itemType: "command_execution",
          status: "completed",
          data: {
            item: {
              type: "commandExecution",
              command: "vp test run focused.test.ts",
              cwd: "/tmp/capability",
              status: "completed",
              exitCode: 0,
              aggregatedOutput: "1 passed",
            },
          },
        },
      });

      const rows = yield* sql<{
        readonly lifecycle: string;
        readonly providerInstanceId: string;
        readonly command: string;
        readonly exitCode: number | null;
      }>`
        SELECT lifecycle, provider_instance_id AS "providerInstanceId", command,
          exit_code AS "exitCode"
        FROM workflow_native_command_observations ORDER BY created_at
      `;
      assert.deepStrictEqual(rows, [
        {
          lifecycle: "started",
          providerInstanceId: "codex-review",
          command: "vp test run focused.test.ts",
          exitCode: null,
        },
        {
          lifecycle: "completed",
          providerInstanceId: "codex-review",
          command: "vp test run focused.test.ts",
          exitCode: 0,
        },
      ]);
    }),
  );
});
