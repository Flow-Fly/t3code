import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE workflow_capability_completions
    ADD COLUMN close_owned INTEGER NOT NULL DEFAULT 0 CHECK(close_owned IN (0, 1))
  `;
});
