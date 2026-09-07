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
  it.effect("retains compact native completion through exact start replay", () =>
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

  it.effect("retains only the newest bounded candidates for the current latest review", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path, worktree_branch,
          status, requested_model, requested_instance_id, requested_effort, observed_match,
          initial_turn_disposition, is_current, created_at, updated_at
        ) VALUES (
          'director-bounded', 'batch-bounded', 'environment-bounded', 'project', 'Flow-Fly/t3code', 10, 18,
          'director-thread-bounded', 'command-bounded', 'message-bounded', '/tmp/capability', 'capability/workflow-18',
          'active', 'gpt-6-astra', 'codex-review', 'high', 'unknown', 'accepted', 1,
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_director_admissions (
          admission_id, director_id, batch_id, repository, ticket_id, ticket_number,
          slot_ticket_number, purpose, ownership, claim_status, created_at, updated_at
        ) VALUES (
          'admission-bounded', 'director-bounded', 'batch-bounded', 'Flow-Fly/t3code', 'ticket-20', 20,
          20, 'implement', 'ticket', 'confirmed',
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_worker_dispatches (
          dispatch_id, association_token, director_id, batch_id, admission_id, repository,
          ticket_number, ownership, write_paths_json, requested_model, requested_effort,
          requested_skill_path, status, created_at, updated_at
        ) VALUES (
          'dispatch-bounded', 'worker-token-bounded', 'director-bounded', 'batch-bounded', 'admission-bounded', 'Flow-Fly/t3code',
          20, 'ticket', '["ticket"]', 'gpt-5.6-sol', 'high', '/skills/implement/SKILL.md',
          'reported-succeeded', '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      for (const [reviewId, associationToken, command, createdAt] of [
        [
          "review-old",
          "review-token-old",
          "vp test run obsolete.test.ts",
          "2026-09-07T09:30:00.000Z",
        ],
        ["review-z", "review-token-z", "vp test run tied-away.test.ts", "2026-09-07T10:00:00.000Z"],
        ["review-a", "review-token-a", "vp test run active.test.ts", "2026-09-07T10:00:00.000Z"],
      ] as const) {
        yield* sql`
          INSERT INTO workflow_ticket_reviews (
            review_id, association_token, director_id, batch_id, admission_id,
            implementation_dispatch_id, repository, ticket_number, fixed_base,
            implementation_head, scope_body, requested_model, requested_effort,
            requested_skill_path, status, created_at, updated_at
          ) VALUES (
            ${reviewId}, ${associationToken}, 'director-bounded', 'batch-bounded', 'admission-bounded', 'dispatch-bounded',
            'Flow-Fly/t3code', 20, 'base', 'head', 'scope', 'gpt-6-astra', 'medium',
            '/skills/code-review/SKILL.md', 'checks-pending', ${createdAt}, ${createdAt}
          )
        `;
        yield* sql`
          INSERT INTO workflow_review_checks (
            review_id, label, command, output, started_head, started_clean,
            verification_status, created_at, updated_at
          ) VALUES (
            ${reviewId}, 'focused', ${command}, '', 'head', 1, 'pending', ${createdAt}, ${createdAt}
          )
        `;
      }
      yield* sql`
        INSERT INTO workflow_review_checks (
          review_id, label, command, output, started_head, started_clean,
          verification_status, created_at, updated_at
        ) VALUES (
          'review-a', 'focused-duplicate', 'vp test run active.test.ts', '', 'head', 1,
          'pending', '2026-09-07T10:00:00.000Z', '2026-09-07T10:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_directors (
          director_id, batch_id, environment_id, project_id, repository, root_number,
          capability_number, thread_id, command_id, message_id, worktree_path, worktree_branch,
          status, requested_model, requested_instance_id, requested_effort, observed_match,
          initial_turn_disposition, is_current, created_at, updated_at
        ) VALUES (
          'director-other-session', 'batch-other-session', 'environment-other-session', 'project',
          'Flow-Fly/t3code', 10, 19, 'director-thread-other-session', 'command-other-session',
          'message-other-session', '/tmp/other-capability', 'capability/workflow-19',
          'active', 'gpt-6-astra', 'codex-other', 'high', 'unknown', 'accepted', 1,
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_director_admissions (
          admission_id, director_id, batch_id, repository, ticket_id, ticket_number,
          slot_ticket_number, purpose, ownership, claim_status, created_at, updated_at
        ) VALUES (
          'admission-other-session', 'director-other-session', 'batch-other-session',
          'Flow-Fly/t3code', 'ticket-21', 21, 21, 'implement', 'ticket', 'confirmed',
          '2026-09-07T09:00:00.000Z', '2026-09-07T09:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_worker_dispatches (
          dispatch_id, association_token, director_id, batch_id, admission_id, repository,
          ticket_number, ownership, write_paths_json, requested_model, requested_effort,
          requested_skill_path, status, created_at, updated_at
        ) VALUES (
          'dispatch-other-session', 'worker-token-other-session', 'director-other-session',
          'batch-other-session', 'admission-other-session', 'Flow-Fly/t3code', 21, 'ticket',
          '["ticket"]', 'gpt-5.6-sol', 'high', '/skills/implement/SKILL.md',
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
          'review-other-session', 'review-token-other-session', 'director-other-session',
          'batch-other-session', 'admission-other-session', 'dispatch-other-session',
          'Flow-Fly/t3code', 21, 'base', 'head', 'scope', 'gpt-6-astra', 'medium',
          '/skills/code-review/SKILL.md', 'prepared',
          '2026-09-07T10:00:00.000Z', '2026-09-07T10:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO workflow_review_checks (
          review_id, label, command, tool_call_id, exit_code, output, started_head,
          finished_head, started_clean, finished_clean, verification_status,
          native_started_at, native_completed_at, thread_id, provider_instance_id,
          created_at, updated_at
        ) VALUES (
          'review-other-session', 'focused', 'vp test run other.test.ts', 'candidate-1',
          0, 'passed', 'head', 'head', 1, 1, 'passed',
          '2026-09-07T10:01:00.000Z', '2026-09-07T10:01:01.000Z',
          'director-thread-other-session', 'codex-other',
          '2026-09-07T10:00:00.000Z', '2026-09-07T10:01:01.000Z'
        )
      `;
      const observe = (
        toolCallId: string,
        command: string,
        startedAt: string,
        completedAt?: string,
      ) =>
        Effect.gen(function* () {
          const base = {
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: ProviderInstanceId.make("codex-review"),
            threadId: ThreadId.make("director-thread-bounded"),
            itemId: RuntimeItemId.make(toolCallId),
          } as const;
          yield* recordWorkflowCheckObservation({
            ...base,
            type: "item.started",
            eventId: EventId.make(`${toolCallId}-started`),
            createdAt: startedAt,
            payload: {
              itemType: "command_execution",
              status: "inProgress",
              data: { item: { type: "commandExecution", command, cwd: "/tmp/capability" } },
            },
          });
          if (!completedAt) return;
          yield* recordWorkflowCheckObservation({
            ...base,
            type: "item.completed",
            eventId: EventId.make(`${toolCallId}-completed`),
            createdAt: completedAt,
            payload: {
              itemType: "command_execution",
              status: "completed",
              data: {
                item: {
                  type: "commandExecution",
                  command,
                  cwd: "/tmp/capability",
                  status: "completed",
                  exitCode: 0,
                  aggregatedOutput: "passed",
                },
              },
            },
          });
        });

      yield* observe(
        "obsolete",
        "vp test run obsolete.test.ts",
        "2026-09-07T11:00:00.000Z",
        "2026-09-07T11:00:01.000Z",
      );
      yield* observe(
        "tied-away",
        "vp test run tied-away.test.ts",
        "2026-09-07T11:00:02.000Z",
        "2026-09-07T11:00:03.000Z",
      );
      yield* observe(
        "unrelated",
        "git status --short",
        "2026-09-07T11:00:04.000Z",
        "2026-09-07T11:00:05.000Z",
      );
      yield* observe(
        "candidate-1",
        "vp test run active.test.ts",
        "2026-09-07T11:01:00.000Z",
        "2026-09-07T11:01:01.000Z",
      );
      const crossSessionCandidate = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM workflow_native_command_observations
        WHERE thread_id = 'director-thread-bounded'
          AND provider_instance_id = 'codex-review'
          AND tool_call_id = 'candidate-1'
      `;
      assert.strictEqual(crossSessionCandidate[0]?.count, 2);
      for (const [index, toolCallId] of ["candidate-2", "candidate-3"].entries()) {
        yield* observe(
          toolCallId,
          "vp test run active.test.ts",
          `2026-09-07T11:01:0${index * 2 + 2}.000Z`,
          `2026-09-07T11:01:0${index * 2 + 3}.000Z`,
        );
      }
      yield* observe(
        "candidate-late-old",
        "vp test run active.test.ts",
        "2026-09-07T11:00:59.000Z",
      );
      yield* recordWorkflowCheckObservation({
        type: "item.completed",
        eventId: EventId.make("completion-without-start"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: ProviderInstanceId.make("codex-review"),
        threadId: ThreadId.make("director-thread-bounded"),
        itemId: RuntimeItemId.make("completion-only"),
        createdAt: "2026-09-07T11:01:10.000Z",
        payload: {
          itemType: "command_execution",
          status: "completed",
          data: {
            item: {
              type: "commandExecution",
              command: "vp test run active.test.ts",
              cwd: "/tmp/capability",
              status: "completed",
              exitCode: 0,
              aggregatedOutput: "passed",
            },
          },
        },
      });
      const boundedRows = yield* sql<{
        readonly toolCallId: string;
        readonly lifecycle: string;
      }>`
        SELECT tool_call_id AS "toolCallId", lifecycle
        FROM workflow_native_command_observations
        WHERE thread_id = 'director-thread-bounded'
        ORDER BY tool_call_id, lifecycle DESC
      `;
      assert.deepStrictEqual(boundedRows, [
        { toolCallId: "candidate-2", lifecycle: "started" },
        { toolCallId: "candidate-2", lifecycle: "completed" },
        { toolCallId: "candidate-3", lifecycle: "started" },
        { toolCallId: "candidate-3", lifecycle: "completed" },
      ]);
      yield* sql`
        UPDATE workflow_directors SET is_current = 0 WHERE director_id = 'director-bounded'
      `;
      yield* observe(
        "inactive",
        "vp test run active.test.ts",
        "2026-09-07T11:02:00.000Z",
        "2026-09-07T11:02:01.000Z",
      );

      const rows = yield* sql<{ readonly toolCallId: string; readonly lifecycle: string }>`
        SELECT tool_call_id AS "toolCallId", lifecycle
        FROM workflow_native_command_observations
        WHERE thread_id = 'director-thread-bounded'
        ORDER BY tool_call_id, lifecycle DESC
      `;
      assert.deepStrictEqual(rows, []);
    }),
  );
});
