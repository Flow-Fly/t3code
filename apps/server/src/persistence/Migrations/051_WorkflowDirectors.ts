import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE workflow_directors (
      director_id TEXT PRIMARY KEY,
      batch_id TEXT NOT NULL,
      environment_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      repository TEXT NOT NULL,
      root_number INTEGER NOT NULL,
      capability_number INTEGER NOT NULL,
      thread_id TEXT NOT NULL,
      command_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      worktree_path TEXT NOT NULL,
      worktree_branch TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('preparing-worktree', 'submitting', 'active', 'held', 'waiting')),
      requested_model TEXT NOT NULL,
      requested_instance_id TEXT NOT NULL,
      requested_effort TEXT NOT NULL,
      observed_model TEXT,
      observed_effort TEXT,
      observed_match TEXT NOT NULL CHECK(observed_match IN ('match', 'mismatch', 'unknown')),
      sequence INTEGER,
      initial_turn_disposition TEXT NOT NULL CHECK(initial_turn_disposition IN ('not-attempted', 'unknown', 'accepted', 'not-accepted')),
      detail TEXT,
      is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0, 1)),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_directors_current
    ON workflow_directors(environment_id, repository COLLATE NOCASE, capability_number)
    WHERE is_current = 1
  `;
  yield* sql`
    CREATE INDEX idx_workflow_directors_thread
    ON workflow_directors(thread_id)
  `;

  yield* sql`
    CREATE TABLE workflow_director_resumes (
      resume_id TEXT PRIMARY KEY,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      source_turn_id TEXT NOT NULL,
      command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('submitting', 'submitted', 'held')),
      sequence INTEGER,
      detail TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(director_id, source_turn_id)
    )
  `;

  yield* sql`
    CREATE TABLE workflow_director_admissions (
      admission_id TEXT PRIMARY KEY,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      batch_id TEXT NOT NULL,
      repository TEXT NOT NULL,
      ticket_id TEXT NOT NULL,
      ticket_number INTEGER NOT NULL,
      slot_ticket_number INTEGER NOT NULL,
      purpose TEXT NOT NULL CHECK(purpose IN ('implement', 'retry', 'review')),
      ownership TEXT NOT NULL,
      claim_login TEXT,
      claim_status TEXT NOT NULL CHECK(claim_status IN ('pending', 'confirmed', 'uncertain', 'conflict')),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_director_admissions_ticket
    ON workflow_director_admissions(director_id, repository COLLATE NOCASE, ticket_number)
  `;
  yield* sql`
    CREATE INDEX idx_workflow_director_admission_slots
    ON workflow_director_admissions(director_id, slot_ticket_number)
  `;
});
