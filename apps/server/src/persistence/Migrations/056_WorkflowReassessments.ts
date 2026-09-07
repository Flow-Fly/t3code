import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`ALTER TABLE workflow_directors ADD COLUMN specification_fingerprint TEXT`;
  yield* sql`ALTER TABLE workflow_directors ADD COLUMN breakdown_fingerprint TEXT`;
  yield* sql`ALTER TABLE workflow_director_admissions ADD COLUMN scope_body TEXT`;
  yield* sql`ALTER TABLE workflow_director_admissions ADD COLUMN scope_fingerprint TEXT`;
  yield* sql`ALTER TABLE workflow_director_admissions ADD COLUMN current_scope_body TEXT`;
  yield* sql`ALTER TABLE workflow_director_admissions ADD COLUMN current_scope_fingerprint TEXT`;
  yield* sql`ALTER TABLE workflow_worker_observations ADD COLUMN native_session_id TEXT`;
  yield* sql`ALTER TABLE workflow_worker_observations ADD COLUMN native_turn_id TEXT`;
  yield* sql`ALTER TABLE workflow_worker_observations ADD COLUMN native_turn_status TEXT`;
  yield* sql`
    CREATE TABLE workflow_reassessments (
      reassessment_id TEXT PRIMARY KEY,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      trigger_kind TEXT NOT NULL CHECK(trigger_kind IN ('scope-change', 'prerequisite')),
      trigger_issue_number INTEGER NOT NULL,
      trigger_source TEXT NOT NULL,
      previous_fingerprint TEXT,
      current_fingerprint TEXT,
      status TEXT NOT NULL CHECK(status IN ('stopping', 'held', 'clearing', 'cleared')),
      required_action TEXT NOT NULL,
      stop_command_id TEXT,
      stop_request_status TEXT NOT NULL CHECK(stop_request_status IN ('not-issued', 'submitted', 'failed', 'unknown')),
      tracker_status TEXT NOT NULL CHECK(tracker_status IN ('not-written', 'pending', 'uncertain', 'confirmed')),
      tracker_body TEXT,
      tracker_url TEXT,
      supersedes_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`ALTER TABLE workflow_director_resumes ADD COLUMN reassessment_id TEXT REFERENCES workflow_reassessments(reassessment_id)`;
  yield* sql`ALTER TABLE workflow_director_resumes ADD COLUMN specification_fingerprint TEXT`;
  yield* sql`ALTER TABLE workflow_director_resumes ADD COLUMN breakdown_fingerprint TEXT`;
  yield* sql`ALTER TABLE workflow_director_resumes ADD COLUMN admission_scopes_json TEXT`;
  yield* sql`ALTER TABLE workflow_director_resumes ADD COLUMN reassessment_trigger_count INTEGER`;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_reassessments_open
    ON workflow_reassessments(director_id)
    WHERE status != 'cleared'
  `;

  yield* sql`
    CREATE TABLE workflow_reassessment_triggers (
      reassessment_id TEXT NOT NULL REFERENCES workflow_reassessments(reassessment_id),
      trigger_kind TEXT NOT NULL CHECK(trigger_kind IN ('scope-change', 'prerequisite')),
      trigger_issue_number INTEGER NOT NULL,
      trigger_source TEXT NOT NULL,
      previous_fingerprint TEXT,
      current_fingerprint TEXT,
      required_action TEXT NOT NULL,
      discovered_at TEXT NOT NULL,
      PRIMARY KEY(reassessment_id, trigger_kind, trigger_issue_number, trigger_source)
    )
  `;

  yield* sql`
    CREATE TABLE workflow_interruption_subjects (
      reassessment_id TEXT NOT NULL REFERENCES workflow_reassessments(reassessment_id),
      subject_id TEXT NOT NULL,
      subject_kind TEXT NOT NULL CHECK(subject_kind IN ('director', 'worker', 'reviewer', 'unknown-child')),
      provider_thread_id TEXT,
      parent_provider_thread_id TEXT,
      native_session_id TEXT,
      native_turn_id TEXT,
      interrupt_attempt_id TEXT,
      request_status TEXT NOT NULL CHECK(request_status IN ('not-issued', 'requested', 'acknowledged', 'failed', 'unknown')),
      outcome TEXT NOT NULL CHECK(outcome IN ('stopping', 'stopped', 'failed', 'unknown', 'closed', 'resumed')),
      detail TEXT,
      discovered_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(reassessment_id, subject_id)
    )
  `;

  yield* sql`
    CREATE TABLE workflow_director_native_turns (
      director_id TEXT PRIMARY KEY REFERENCES workflow_directors(director_id),
      native_session_id TEXT NOT NULL,
      native_turn_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'interrupted')),
      updated_at TEXT NOT NULL
    )
  `;
});
