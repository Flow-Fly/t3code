// @effect-diagnostics nodeBuiltinImport:off - subprocess integration exercises the fixture's Node process and filesystem boundary.

import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProjectId, type OrchestrationProjectShell } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkflowService from "./WorkflowService.ts";

const fixtureDirectory = NodePath.resolve(
  import.meta.dirname,
  "../testUtils/workflowGitHubFixture",
);
const controlPath = NodePath.join(fixtureDirectory, "control.mjs");
const projectId = ProjectId.make("project-1");

interface Fixture {
  readonly directory: string;
  readonly statePath: string;
  readonly binDirectory: string;
  readonly ghPath: string;
}

function run(
  command: string,
  args: ReadonlyArray<string>,
  options?: { readonly input?: string; readonly env?: NodeJS.ProcessEnv },
) {
  return NodeChildProcess.spawnSync(command, args, {
    encoding: "utf8",
    input: options?.input,
    env: options?.env,
  });
}

function control(fixture: Fixture, ...args: ReadonlyArray<string>) {
  return run(process.execPath, [controlPath, ...args, "--state", fixture.statePath]);
}

function createFixture(): Fixture {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-workflow-gh-"));
  const statePath = NodePath.join(directory, "state.json");
  const binDirectory = NodePath.join(directory, "bin");
  const initialized = run(process.execPath, [
    controlPath,
    "init",
    "--state",
    statePath,
    "--bin",
    binDirectory,
  ]);
  expect(initialized.status, initialized.stderr).toBe(0);
  expect(JSON.parse(initialized.stdout)).toMatchObject({
    repositories: {
      large: "fixture/workflow-demo",
      secondary: "fixture/workflow-dependency",
      live: "fixture/workflow-live",
    },
    largeSliceCount: 52,
  });
  return {
    directory,
    statePath,
    binDirectory,
    ghPath: NodePath.join(
      binDirectory,
      HostProcessPlatform.defaultValue() === "win32" ? "gh.cmd" : "gh",
    ),
  };
}

function fixtureEnv(fixture: Fixture) {
  return {
    ...process.env,
    PATH: `${fixture.binDirectory}${NodePath.delimiter}${process.env.PATH ?? ""}`,
    T3_WORKFLOW_FIXTURE_STATE: fixture.statePath,
  };
}

function gh(fixture: Fixture, ...args: ReadonlyArray<string>) {
  return run(fixture.ghPath, args, { env: fixtureEnv(fixture) });
}

describe("Workflow GitHub subprocess fixture", () => {
  it("keeps synthetic reads and writes closed, stateful, and recoverable", () => {
    const fixture = createFixture();
    try {
      expect(gh(fixture, "--version").stdout).toContain("gh version 2.83.0");
      expect(
        JSON.parse(gh(fixture, "auth", "status", "--json", "hosts").stdout).hosts["github.com"][0],
      ).toMatchObject({ active: true, login: "workflow-fixture" });

      expect(gh(fixture, "api", "repos/fixture/workflow-demo/issues/701").stdout).toContain(
        "Adoptable delivery slice",
      );
      expect(
        gh(
          fixture,
          "api",
          "--method",
          "POST",
          "repos/fixture/workflow-demo/issues/701/labels",
          "--input",
          "-",
        ).status,
      ).not.toBe(0);
      const addLabel = run(
        fixture.ghPath,
        [
          "api",
          "--method",
          "POST",
          "repos/fixture/workflow-demo/issues/701/labels",
          "--input",
          "-",
        ],
        { input: JSON.stringify({ labels: ["workflow:ticket"] }), env: fixtureEnv(fixture) },
      );
      expect(addLabel.status, addLabel.stderr).toBe(0);
      expect(gh(fixture, "api", "repos/fixture/workflow-demo/issues/701").stdout).toContain(
        "workflow:ticket",
      );

      expect(
        gh(
          fixture,
          "issue",
          "edit",
          "152",
          "--repo",
          "fixture/workflow-demo",
          "--add-assignee",
          "@me",
        ).status,
      ).toBe(0);
      expect(
        gh(
          fixture,
          "issue",
          "view",
          "152",
          "--repo",
          "fixture/workflow-demo",
          "--json",
          "assignees",
          "--jq",
          ".assignees[].login",
        ).stdout.trim(),
      ).toBe("workflow-fixture");

      expect(control(fixture, "fail-next", "before", "issue-edit").status).toBe(0);
      expect(
        gh(
          fixture,
          "issue",
          "edit",
          "152",
          "--repo",
          "fixture/workflow-demo",
          "--add-label",
          "workflow:needs-reassessment",
        ).status,
      ).not.toBe(0);
      expect(gh(fixture, "api", "repos/fixture/workflow-demo/issues/152").stdout).not.toContain(
        "workflow:needs-reassessment",
      );

      expect(control(fixture, "fail-next", "after", "issue-comment").status).toBe(0);
      expect(
        run(
          fixture.ghPath,
          ["issue", "comment", "152", "--repo", "fixture/workflow-demo", "--body-file", "-"],
          { input: "Synthetic uncertain evidence", env: fixtureEnv(fixture) },
        ).status,
      ).not.toBe(0);
      expect(
        JSON.parse(control(fixture, "inspect", "fixture/workflow-demo", "152").stdout).comments,
      ).toContainEqual(expect.objectContaining({ body: "Synthetic uncertain evidence" }));

      expect(control(fixture, "offline", "on").status).toBe(0);
      expect(gh(fixture, "api", "user", "--jq", ".login").status).not.toBe(0);
      expect(control(fixture, "offline", "off").status).toBe(0);
      expect(gh(fixture, "api", "user", "--jq", ".login").stdout.trim()).toBe("workflow-fixture");

      expect(gh(fixture, "repo", "delete", "fixture/workflow-demo").status).not.toBe(0);
      expect(gh(fixture, "api", "repos/fixture/unknown/issues/1").status).not.toBe(0);
      expect(
        gh(
          fixture,
          "issue",
          "edit",
          "152",
          "--repo",
          "fixture/workflow-demo",
          "--add-label",
          "must-not-land",
          "--body",
          "unsupported",
        ).status,
      ).not.toBe(0);
      expect(gh(fixture, "api", "repos/fixture/workflow-demo/issues/152").stdout).not.toContain(
        "must-not-land",
      );
      expect(
        gh(fixture, "issue", "edit", "152", "--repo", "fixture/unknown", "--add-label", "unsafe")
          .status,
      ).not.toBe(0);
    } finally {
      NodeFS.rmSync(fixture.directory, { recursive: true, force: true });
    }
  });

  it.effect("feeds the actual Workflow service through the gh process boundary", () => {
    const fixture = createFixture();
    const previousPath = process.env.PATH;
    const previousState = process.env.T3_WORKFLOW_FIXTURE_STATE;
    process.env.PATH = fixtureEnv(fixture).PATH;
    process.env.T3_WORKFLOW_FIXTURE_STATE = fixture.statePath;

    const processLayer = ProcessRunner.layer.pipe(Layer.provide(NodeServices.layer));
    const githubLayer = GitHubCli.layer.pipe(
      Layer.provide(VcsProcess.layer.pipe(Layer.provide(NodeServices.layer))),
    );
    const workflowLayer = Layer.effect(WorkflowService.WorkflowService, WorkflowService.make).pipe(
      Layer.provide(
        Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
          getProjectShellById: () =>
            Effect.succeed(
              Option.some({
                id: projectId,
                title: "Workflow fixture",
                workspaceRoot: fixture.directory,
                defaultModelSelection: null,
                scripts: [],
                createdAt: "2026-09-09T12:00:00.000Z",
                updatedAt: "2026-09-09T12:00:00.000Z",
              } satisfies OrchestrationProjectShell),
            ),
        }),
      ),
      Layer.provide(githubLayer),
      Layer.provide(processLayer),
    );

    return Effect.gen(function* () {
      const service = yield* WorkflowService.WorkflowService;
      const roots = yield* service.roots({
        projectId,
        repository: "fixture/workflow-demo",
      });
      expect(roots.roots.map((root) => root.number)).toContain(1);

      const rootChildren = yield* service.children({
        projectId,
        repository: "fixture/workflow-demo",
        parentNumber: 1,
      });
      expect(rootChildren.children.map((child) => child.number)).toEqual(
        expect.arrayContaining([2, 10]),
      );

      const largeCapability = yield* service.issueDetail({
        projectId,
        repository: "fixture/workflow-demo",
        number: 10,
      });
      expect(
        largeCapability.evidence?.records
          .filter((record) => record.kind === "approval")
          .map((record) => [record.approvalKind, record.scope]),
      ).toEqual([
        ["specification", "current"],
        ["ticket-breakdown", "not-applicable"],
      ]);

      const slices = yield* service.children({
        projectId,
        repository: "fixture/workflow-demo",
        parentNumber: 10,
      });
      expect(slices.children).toHaveLength(52);
      expect(slices.children.find((child) => child.number === 152)?.readiness?.status).toBe(
        "ready",
      );

      const detail = yield* service.issueDetail({
        projectId,
        repository: "fixture/workflow-demo",
        number: 151,
      });
      expect(detail.evidence?.records.length).toBeGreaterThan(100);

      const search = yield* service.search({
        projectId,
        repository: "fixture/workflow-demo",
        query: "slice 52",
      });
      expect(search.matches[0]?.issue.number).toBe(152);

      const located = yield* service.locate({
        projectId,
        repository: "fixture/workflow-demo",
        number: 151,
      });
      expect(located.ancestry.map((item) => item.number)).toEqual([1, 10]);

      const liveCapability = yield* service.issueDetail({
        projectId,
        repository: "fixture/workflow-live",
        number: 1,
      });
      expect(
        liveCapability.evidence?.records
          .filter((record) => record.kind === "approval")
          .map((record) => [record.approvalKind, record.scope]),
      ).toEqual([
        ["specification", "current"],
        ["ticket-breakdown", "not-applicable"],
      ]);
      const liveTicket = yield* service.issueDetail({
        projectId,
        repository: "fixture/workflow-live",
        number: 2,
      });
      expect(liveTicket.title).toBe("Format check summaries");
      expect(liveTicket.body).toContain("formatCheckSummary");
      expect(liveTicket.body).toContain("No checks ran.");
      expect(liveTicket.body).toContain("2 checks passed.");
      expect(liveTicket.body).toContain("1 passed, 2 failed: lint, typecheck.");
      expect(
        liveTicket.evidence?.records.find(
          (record) => record.kind === "approval" && record.approvalKind === "ticket-breakdown",
        )?.scope,
      ).toBe("current");
    }).pipe(
      Effect.provide(workflowLayer),
      Effect.ensuring(
        Effect.sync(() => {
          if (previousPath === undefined) delete process.env.PATH;
          else process.env.PATH = previousPath;
          if (previousState === undefined) delete process.env.T3_WORKFLOW_FIXTURE_STATE;
          else process.env.T3_WORKFLOW_FIXTURE_STATE = previousState;
          NodeFS.rmSync(fixture.directory, { recursive: true, force: true });
        }),
      ),
    );
  });
});
