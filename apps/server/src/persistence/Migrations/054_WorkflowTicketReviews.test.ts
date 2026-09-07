import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("054_WorkflowTicketReviews", (it) => {
  it.effect("adds durable review, native check, finding and resolution records", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 53 });
      yield* runMigrations({ toMigrationInclusive: 54 });

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'workflow_%'
        ORDER BY name
      `;
      assert.deepStrictEqual(
        tables
          .map((table) => table.name)
          .filter((name) =>
            [
              "workflow_native_command_observations",
              "workflow_review_axes",
              "workflow_review_checks",
              "workflow_review_findings",
              "workflow_ticket_resolution_intents",
              "workflow_ticket_reviews",
            ].includes(name),
          ),
        [
          "workflow_native_command_observations",
          "workflow_review_axes",
          "workflow_review_checks",
          "workflow_review_findings",
          "workflow_ticket_resolution_intents",
          "workflow_ticket_reviews",
        ],
      );
    }),
  );
});
