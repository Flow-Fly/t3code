import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE workflow_capability_completions (
      completion_id TEXT PRIMARY KEY,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      repository TEXT NOT NULL,
      capability_number INTEGER NOT NULL,
      resulting_head TEXT NOT NULL,
      specification_fingerprint TEXT NOT NULL,
      breakdown_fingerprint TEXT NOT NULL,
      comment_body TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN (
        'checks-pending', 'checks-failed', 'comment-pending', 'comment-uncertain',
        'close-pending', 'close-uncertain', 'reopen-pending', 'reopen-uncertain',
        'invalidated', 'completed'
      )),
      comment_url TEXT,
      close_confirmed INTEGER NOT NULL DEFAULT 0 CHECK(close_confirmed IN (0, 1)),
      required_action TEXT NOT NULL,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_capability_completion_active
    ON workflow_capability_completions(director_id)
    WHERE status NOT IN ('invalidated', 'completed')
  `;
  yield* sql`
    CREATE INDEX idx_workflow_capability_completion_history
    ON workflow_capability_completions(director_id, created_at)
  `;

  yield* sql`
    CREATE TABLE workflow_capability_checks (
      completion_id TEXT NOT NULL REFERENCES workflow_capability_completions(completion_id),
      label TEXT NOT NULL,
      command TEXT NOT NULL,
      tool_call_id TEXT,
      exit_code INTEGER,
      output TEXT NOT NULL,
      started_head TEXT NOT NULL,
      finished_head TEXT,
      started_clean INTEGER NOT NULL CHECK(started_clean IN (0, 1)),
      finished_clean INTEGER CHECK(finished_clean IN (0, 1)),
      verification_status TEXT NOT NULL CHECK(verification_status IN ('pending', 'passed', 'failed')),
      verification_error TEXT,
      native_started_at TEXT,
      native_completed_at TEXT,
      thread_id TEXT NOT NULL,
      provider_instance_id TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(completion_id, label)
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_capability_check_tool_call
    ON workflow_capability_checks(thread_id, provider_instance_id, tool_call_id)
    WHERE tool_call_id IS NOT NULL
  `;
});
