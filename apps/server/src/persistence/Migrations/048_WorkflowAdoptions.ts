import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_adoption_previews (
      preview_id TEXT PRIMARY KEY,
      repository TEXT NOT NULL,
      root_number INTEGER NOT NULL,
      preview_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_adoptions (
      adoption_id TEXT PRIMARY KEY,
      preview_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      repository TEXT NOT NULL,
      root_number INTEGER NOT NULL,
      record_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY (preview_id) REFERENCES workflow_adoption_previews(preview_id)
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_workflow_adoptions_root
    ON workflow_adoptions(project_id, repository, root_number, created_at DESC)
  `;
});
