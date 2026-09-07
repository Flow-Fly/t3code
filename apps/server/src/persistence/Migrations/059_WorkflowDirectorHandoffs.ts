import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE workflow_director_handoffs (
      handoff_id TEXT PRIMARY KEY,
      source_director_id TEXT NOT NULL UNIQUE REFERENCES workflow_directors(director_id),
      successor_director_id TEXT UNIQUE REFERENCES workflow_directors(director_id),
      source_thread_id TEXT NOT NULL,
      source_batch_id TEXT NOT NULL,
      admissions_json TEXT NOT NULL,
      settlements_json TEXT NOT NULL,
      implementation_head TEXT,
      worktree_path TEXT NOT NULL,
      worktree_branch TEXT NOT NULL,
      specification_links_json TEXT NOT NULL,
      issue_links_json TEXT NOT NULL,
      review_links_json TEXT NOT NULL,
      commit_links_json TEXT NOT NULL,
      suggested_skills_json TEXT NOT NULL,
      suggested_staffing_json TEXT NOT NULL,
      lessons_json TEXT NOT NULL,
      unresolved_context_json TEXT NOT NULL,
      successor_thread_id TEXT UNIQUE,
      successor_command_id TEXT UNIQUE,
      successor_message_id TEXT UNIQUE,
      successor_prompt TEXT,
      status TEXT NOT NULL CHECK(status IN ('waiting-settlement', 'submitting', 'submitted', 'held')),
      detail TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
});
