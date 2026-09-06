import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE workflow_worker_observations
    ADD COLUMN native_lifecycle TEXT CHECK(native_lifecycle IN ('closed'))
  `;
});
