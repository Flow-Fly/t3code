import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE workflow_worker_dispatches (
      dispatch_id TEXT PRIMARY KEY,
      association_token TEXT NOT NULL UNIQUE,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      batch_id TEXT NOT NULL,
      admission_id TEXT NOT NULL REFERENCES workflow_director_admissions(admission_id),
      repository TEXT NOT NULL,
      ticket_number INTEGER NOT NULL,
      ownership TEXT NOT NULL,
      write_paths_json TEXT NOT NULL,
      requested_model TEXT NOT NULL,
      requested_effort TEXT NOT NULL,
      requested_skill_path TEXT NOT NULL,
      provider_thread_id TEXT,
      status TEXT NOT NULL CHECK(status IN ('prepared', 'associated', 'reported-failed', 'reported-succeeded', 'unconfirmed')),
      handoff_summary TEXT,
      handoff_commits_json TEXT,
      handoff_checks_json TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_worker_dispatch_provider_thread
    ON workflow_worker_dispatches(director_id, provider_thread_id)
    WHERE provider_thread_id IS NOT NULL
  `;
  yield* sql`
    CREATE INDEX idx_workflow_worker_dispatch_admission
    ON workflow_worker_dispatches(admission_id, created_at)
  `;

  yield* sql`
    CREATE TABLE workflow_worker_observations (
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      provider_thread_id TEXT NOT NULL,
      parent_provider_thread_id TEXT,
      title TEXT,
      role TEXT,
      agent_path TEXT,
      observed_model TEXT,
      observed_effort TEXT,
      provider_status TEXT NOT NULL,
      last_event_kind TEXT NOT NULL,
      first_observed_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(director_id, provider_thread_id)
    )
  `;
});
