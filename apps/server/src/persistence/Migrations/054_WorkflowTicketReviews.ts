import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE workflow_ticket_reviews (
      review_id TEXT PRIMARY KEY,
      association_token TEXT NOT NULL UNIQUE,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      batch_id TEXT NOT NULL,
      admission_id TEXT NOT NULL REFERENCES workflow_director_admissions(admission_id),
      implementation_dispatch_id TEXT NOT NULL REFERENCES workflow_worker_dispatches(dispatch_id),
      repository TEXT NOT NULL,
      ticket_number INTEGER NOT NULL,
      fixed_base TEXT NOT NULL,
      implementation_head TEXT NOT NULL,
      scope_body TEXT NOT NULL,
      requested_model TEXT NOT NULL,
      requested_effort TEXT NOT NULL,
      requested_skill_path TEXT NOT NULL,
      provider_thread_id TEXT,
      status TEXT NOT NULL CHECK(status IN ('checks-pending', 'checks-failed', 'prepared', 'spawn-issued', 'associated', 'reported')),
      report_summary TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_ticket_review_provider_thread
    ON workflow_ticket_reviews(director_id, provider_thread_id)
    WHERE provider_thread_id IS NOT NULL
  `;
  yield* sql`
    CREATE INDEX idx_workflow_ticket_review_admission
    ON workflow_ticket_reviews(admission_id, created_at)
  `;

  yield* sql`
    CREATE TABLE workflow_review_checks (
      review_id TEXT NOT NULL REFERENCES workflow_ticket_reviews(review_id),
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
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(review_id, label)
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_review_check_tool_call
    ON workflow_review_checks(tool_call_id)
    WHERE tool_call_id IS NOT NULL
  `;

  yield* sql`
    CREATE TABLE workflow_native_command_observations (
      thread_id TEXT NOT NULL,
      provider_instance_id TEXT NOT NULL,
      tool_call_id TEXT NOT NULL,
      lifecycle TEXT NOT NULL CHECK(lifecycle IN ('started', 'completed')),
      command TEXT NOT NULL,
      cwd TEXT,
      status TEXT,
      exit_code INTEGER,
      output TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY(thread_id, provider_instance_id, tool_call_id, lifecycle)
    )
  `;

  yield* sql`
    CREATE TABLE workflow_review_axes (
      review_id TEXT NOT NULL REFERENCES workflow_ticket_reviews(review_id),
      axis TEXT NOT NULL CHECK(axis IN ('standards', 'spec')),
      provider_thread_id TEXT NOT NULL,
      PRIMARY KEY(review_id, axis),
      UNIQUE(review_id, provider_thread_id)
    )
  `;

  yield* sql`
    CREATE TABLE workflow_review_findings (
      review_id TEXT NOT NULL REFERENCES workflow_ticket_reviews(review_id),
      finding_id TEXT NOT NULL,
      axis TEXT NOT NULL CHECK(axis IN ('standards', 'spec')),
      severity TEXT NOT NULL CHECK(severity IN ('critical', 'high', 'medium', 'low')),
      summary TEXT NOT NULL,
      location TEXT,
      disposition TEXT CHECK(disposition IN ('fixed', 'dismissed', 'owner-accepted')),
      disposition_rationale TEXT,
      disposition_evidence_source TEXT,
      disposition_evidence_quote TEXT,
      resulting_review_id TEXT REFERENCES workflow_ticket_reviews(review_id),
      updated_at TEXT NOT NULL,
      PRIMARY KEY(review_id, finding_id)
    )
  `;

  yield* sql`
    CREATE TABLE workflow_ticket_resolution_intents (
      resolution_id TEXT PRIMARY KEY,
      director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      admission_id TEXT NOT NULL REFERENCES workflow_director_admissions(admission_id),
      review_id TEXT NOT NULL UNIQUE REFERENCES workflow_ticket_reviews(review_id),
      repository TEXT NOT NULL,
      ticket_number INTEGER NOT NULL,
      scope_body TEXT NOT NULL,
      final_head TEXT NOT NULL,
      comment_body TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('comment-pending', 'comment-uncertain', 'close-pending', 'close-uncertain', 'resolved')),
      comment_url TEXT,
      frontier_json TEXT,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
});
