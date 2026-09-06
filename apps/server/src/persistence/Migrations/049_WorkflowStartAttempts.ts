import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_start_attempts (
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
      UNIQUE (project_id, repository, issue_number, phase)
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_workflow_start_attempts_thread
    ON workflow_start_attempts(thread_id)
  `;
});
