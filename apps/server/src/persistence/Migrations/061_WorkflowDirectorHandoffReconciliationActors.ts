import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`ALTER TABLE workflow_director_handoff_reconciliations
    ADD COLUMN actor_kind TEXT NOT NULL DEFAULT 'director'
      CHECK(actor_kind IN ('director', 'owner-session'))`;
  yield* sql`ALTER TABLE workflow_director_handoff_reconciliations
    ADD COLUMN actor_subject TEXT`;
  yield* sql`UPDATE workflow_director_handoff_reconciliations
    SET actor_subject = acknowledged_by_director_id
    WHERE actor_subject IS NULL`;
});
