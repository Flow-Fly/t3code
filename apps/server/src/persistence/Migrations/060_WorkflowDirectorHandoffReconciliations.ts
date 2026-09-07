import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE workflow_director_handoff_reconciliations (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      reconciliation_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL REFERENCES workflow_director_handoffs(handoff_id),
      acknowledged_by_director_id TEXT NOT NULL REFERENCES workflow_directors(director_id),
      settlements_json TEXT NOT NULL,
      implementation_head TEXT NOT NULL,
      summary TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX workflow_director_handoff_reconciliations_latest
    ON workflow_director_handoff_reconciliations(handoff_id, sequence DESC)
  `;
});
