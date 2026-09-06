import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`ALTER TABLE workflow_start_attempts RENAME TO workflow_start_attempts_legacy`;
  yield* sql`
    CREATE TABLE workflow_start_attempts (
      attempt_id TEXT PRIMARY KEY,
      environment_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      repository TEXT NOT NULL,
      root_number INTEGER NOT NULL,
      issue_number INTEGER NOT NULL,
      phase TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      command_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      status TEXT NOT NULL,
      claim_login TEXT,
      claim_owned INTEGER NOT NULL DEFAULT 0,
      sequence INTEGER,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      detail TEXT,
      initial_turn_disposition TEXT CHECK (
        initial_turn_disposition IS NULL OR
        initial_turn_disposition IN ('not-attempted', 'unknown', 'accepted', 'not-accepted')
      ),
      is_current INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1))
    )
  `;
  yield* sql`
    INSERT INTO workflow_start_attempts (
      attempt_id, environment_id, project_id, repository, root_number,
      issue_number, phase, thread_id, command_id, message_id, status,
      claim_login, claim_owned, sequence, created_at, updated_at, detail,
      initial_turn_disposition, is_current
    )
    SELECT attempt_id, environment_id, project_id, repository, root_number,
      issue_number, phase, thread_id, command_id, message_id, status,
      claim_login, claim_owned, sequence, created_at, updated_at, detail, NULL, 1
    FROM workflow_start_attempts_legacy
  `;
  yield* sql`DROP TABLE workflow_start_attempts_legacy`;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_start_attempts_current
    ON workflow_start_attempts(project_id, repository, issue_number, phase)
    WHERE is_current = 1
  `;
  yield* sql`
    CREATE INDEX idx_workflow_start_attempts_history
    ON workflow_start_attempts(project_id, repository, issue_number, phase, created_at DESC)
  `;
  yield* sql`
    CREATE INDEX idx_workflow_start_attempts_thread
    ON workflow_start_attempts(thread_id)
  `;

  yield* sql`
    CREATE TABLE workflow_resume_attempts (
      resume_id TEXT PRIMARY KEY,
      workflow_attempt_id TEXT NOT NULL REFERENCES workflow_start_attempts(attempt_id),
      source_turn_id TEXT NOT NULL,
      command_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      status TEXT NOT NULL,
      sequence INTEGER,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      detail TEXT,
      UNIQUE (workflow_attempt_id, source_turn_id)
    )
  `;
});
