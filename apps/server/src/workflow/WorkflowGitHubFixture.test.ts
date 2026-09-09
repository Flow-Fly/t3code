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
import { fixPath } from "../os-jank.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkflowService from "./WorkflowService.ts";
// @ts-expect-error The subprocess fixture is intentionally a plain Node module.
import * as FixtureState from "../testUtils/workflowGitHubFixture/fixture.mjs";

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
  readonly loginShellPath: string;
  readonly profileShellPath: string;
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
  const decoyBinDirectory = NodePath.join(directory, "decoy-bin");
  const profileShellPath = NodePath.join(directory, "profile-shell");
  NodeFS.mkdirSync(decoyBinDirectory);
  NodeFS.writeFileSync(
    NodePath.join(decoyBinDirectory, "gh"),
    "#!/bin/sh\nprintf '%s\\n' 'gh version 0.0.0 (decoy)'\n",
    { encoding: "utf8", mode: 0o755 },
  );
  NodeFS.writeFileSync(
    profileShellPath,
    `#!/bin/sh\nexport PATH='${decoyBinDirectory}:/usr/bin:/bin'\ncase "$1" in\n  -ilc|-lc|-c) exec /bin/sh -c "$2" ;;\n  *) exec /bin/sh "$@" ;;\nesac\n`,
    { encoding: "utf8", mode: 0o755 },
  );
  const initialized = run(process.execPath, [
    controlPath,
    "init",
    "--state",
    statePath,
    "--bin",
    binDirectory,
    "--shell",
    profileShellPath,
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
    loginShellPath: NodePath.join(binDirectory, "login-shell"),
    profileShellPath,
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

async function startLockHolder(fixture: Fixture) {
  const child = NodeChildProcess.spawn(
    process.execPath,
    [controlPath, "hold-lock", "--state", fixture.statePath],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const owner = await new Promise<{ readonly pid: number; readonly token: string }>(
    (resolve, reject) => {
      const onExit = (code: number | null) => {
        reject(new Error(`Fixture lock holder exited ${code}: ${stderr}`));
      };
      child.once("exit", onExit);
      child.stdout.on("data", (chunk: string) => {
        stdout += chunk;
        const newline = stdout.indexOf("\n");
        if (newline === -1) return;
        child.off("exit", onExit);
        resolve(JSON.parse(stdout.slice(0, newline)));
      });
    },
  );
  return { child, owner };
}

async function killLockHolder(child: NodeChildProcess.ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
  });
  child.kill("SIGKILL");
  await exited;
}

describe("Workflow GitHub subprocess fixture", () => {
  it("keeps synthetic reads and writes closed, stateful, and recoverable", async () => {
    const fixture = createFixture();
    let lockHolder: NodeChildProcess.ChildProcess | undefined;
    try {
      const stateBeforeLauncherInstall = NodeFS.readFileSync(fixture.statePath, "utf8");
      expect(
        control(
          fixture,
          "install-launcher",
          "--bin",
          fixture.binDirectory,
          "--shell",
          fixture.profileShellPath,
        ).status,
      ).toBe(0);
      expect(NodeFS.readFileSync(fixture.statePath, "utf8")).toBe(stateBeforeLauncherInstall);

      const lockPath = `${fixture.statePath}.lock`;
      NodeFS.mkdirSync(lockPath);
      NodeFS.utimesSync(lockPath, 0, 0);
      const ownerlessAttempt = gh(fixture, "--version");
      expect(ownerlessAttempt.status).not.toBe(0);
      expect(ownerlessAttempt.stderr).toContain("Fixture state lock is stale");
      expect(NodeFS.existsSync(lockPath)).toBe(true);
      NodeFS.rmdirSync(lockPath);

      const held = await startLockHolder(fixture);
      lockHolder = held.child;
      NodeFS.utimesSync(lockPath, 0, 0);
      const liveOwnerAttempt = gh(fixture, "--version");
      expect(liveOwnerAttempt.status).not.toBe(0);
      expect(liveOwnerAttempt.stderr).toContain("Fixture state is busy");

      await killLockHolder(lockHolder);
      lockHolder = undefined;
      expect(gh(fixture, "--version").stdout).toContain("gh version 2.83.0");

      const successor = FixtureState.acquireFixtureStateLock(fixture.statePath);
      try {
        const recoveryPath = `${lockPath}.recovery`;
        NodeFS.mkdirSync(recoveryPath);
        expect(FixtureState.tryRecoverFixtureStateLock(fixture.statePath, held.owner)).toBe(false);
        expect(
          JSON.parse(NodeFS.readFileSync(NodePath.join(lockPath, "owner.json"), "utf8")),
        ).toMatchObject({ token: successor.owner.token });
        NodeFS.rmdirSync(recoveryPath);
        expect(FixtureState.tryRecoverFixtureStateLock(fixture.statePath, held.owner)).toBe(false);
        expect(
          JSON.parse(NodeFS.readFileSync(NodePath.join(lockPath, "owner.json"), "utf8")),
        ).toMatchObject({ token: successor.owner.token });
      } finally {
        FixtureState.releaseFixtureStateLock(successor);
      }

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
      if (lockHolder) await killLockHolder(lockHolder);
      NodeFS.rmSync(fixture.directory, { recursive: true, force: true });
    }
  });

  it.effect("keeps fixture gh selected through startup hydration", () => {
    const fixture = createFixture();
    const previousPath = process.env.PATH;
    const previousShell = process.env.SHELL;
    const previousState = process.env.T3_WORKFLOW_FIXTURE_STATE;
    process.env.PATH = fixtureEnv(fixture).PATH;
    process.env.SHELL = fixture.profileShellPath;
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
      if (HostProcessPlatform.defaultValue() !== "win32") {
        yield* fixPath().pipe(Effect.provide(NodeServices.layer));
        expect(run("gh", ["--version"], { env: process.env }).stdout).toContain("(decoy)");

        expect(NodeFS.existsSync(fixture.loginShellPath)).toBe(true);
        process.env.PATH = fixtureEnv(fixture).PATH;
        process.env.SHELL = fixture.loginShellPath;
        yield* fixPath().pipe(Effect.provide(NodeServices.layer));
        expect(run("gh", ["--version"], { env: process.env }).stdout).toContain(
          "(workflow fixture)",
        );
        for (const mode of ["-lc", "-c"] as const) {
          expect(
            run(fixture.loginShellPath, [mode, "gh --version"], { env: process.env }).stdout,
          ).toContain("(workflow fixture)");
        }
      }

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

      const adoptionChildren = yield* service.children({
        projectId,
        repository: "fixture/workflow-demo",
        parentNumber: 700,
      });
      expect(adoptionChildren.children.map((child) => child.number)).toEqual([701]);
      const nestedAdoptionChildren = yield* service.children({
        projectId,
        repository: "fixture/workflow-demo",
        parentNumber: 701,
      });
      expect(nestedAdoptionChildren.children.map((child) => child.number)).toEqual([702]);

      const largeCapability = yield* service.issueDetail({
        projectId,
        repository: "fixture/workflow-demo",
        number: 10,
      });
      expect(
        largeCapability.evidence?.records
          .filter((record) => record.kind === "approval")
          .map((record) => [record.approvalKind, record.scope, record.sourceAccess]),
      ).toEqual([
        ["specification", "current", "verified"],
        ["ticket-breakdown", "not-applicable", "verified"],
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
          .map((record) => [record.approvalKind, record.scope, record.sourceAccess]),
      ).toEqual([
        ["specification", "current", "verified"],
        ["ticket-breakdown", "not-applicable", "verified"],
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
        ),
      ).toMatchObject({ scope: "current", sourceAccess: "verified" });

      expect(
        control(
          fixture,
          "reassessment",
          "fixture/workflow-demo",
          "10",
          "scope-change",
          "Outside preparation reopened.",
        ).status,
      ).toBe(0);
      expect(
        control(
          fixture,
          "reassessment",
          "fixture/workflow-demo",
          "10",
          "cleared",
          "Outside preparation restored.",
        ).status,
      ).toBe(0);
      const reassessedCapability = yield* service.issueDetail({
        projectId,
        repository: "fixture/workflow-demo",
        number: 10,
      });
      expect(
        reassessedCapability.evidence?.records
          .filter((record) => record.kind === "reassessment")
          .map((record) => [record.outcome, record.state, record.scope, record.sourceAccess]),
      ).toEqual([
        ["scope-change", "superseded", "current", "verified"],
        ["cleared", "current", "current", "verified"],
      ]);
      expect(
        (HostProcessPlatform.defaultValue() === "win32"
          ? gh(fixture, "repo", "delete", "fixture/workflow-demo")
          : run("gh", ["repo", "delete", "fixture/workflow-demo"], { env: process.env })
        ).stderr,
      ).toContain("Fixture denies unsupported gh command");
    }).pipe(
      Effect.provide(workflowLayer),
      Effect.ensuring(
        Effect.sync(() => {
          if (previousPath === undefined) delete process.env.PATH;
          else process.env.PATH = previousPath;
          if (previousShell === undefined) delete process.env.SHELL;
          else process.env.SHELL = previousShell;
          if (previousState === undefined) delete process.env.T3_WORKFLOW_FIXTURE_STATE;
          else process.env.T3_WORKFLOW_FIXTURE_STATE = previousState;
          NodeFS.rmSync(fixture.directory, { recursive: true, force: true });
        }),
      ),
    );
  });
});
