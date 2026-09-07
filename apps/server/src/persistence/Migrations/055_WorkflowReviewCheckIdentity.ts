import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`ALTER TABLE workflow_review_checks ADD COLUMN thread_id TEXT`;
  yield* sql`ALTER TABLE workflow_review_checks ADD COLUMN provider_instance_id TEXT`;
  yield* sql`
    UPDATE workflow_review_checks
    SET thread_id = (
      SELECT d.thread_id
      FROM workflow_ticket_reviews r
      JOIN workflow_directors d ON d.director_id = r.director_id
      WHERE r.review_id = workflow_review_checks.review_id
    ), provider_instance_id = (
      SELECT d.requested_instance_id
      FROM workflow_ticket_reviews r
      JOIN workflow_directors d ON d.director_id = r.director_id
      WHERE r.review_id = workflow_review_checks.review_id
    )
  `;
  yield* sql`DROP INDEX idx_workflow_review_check_tool_call`;
  yield* sql`
    CREATE UNIQUE INDEX idx_workflow_review_check_tool_call
    ON workflow_review_checks(thread_id, provider_instance_id, tool_call_id)
    WHERE tool_call_id IS NOT NULL
  `;
});
