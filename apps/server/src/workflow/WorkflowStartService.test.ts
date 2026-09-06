import {
  EnvironmentId,
  OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type OrchestrationCommand,
  type ServerProvider,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import { ProviderDriverError } from "../provider/Errors.ts";
import { makeProviderRegistryMock } from "../provider/testUtils/providerRegistryMock.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowService from "./WorkflowService.ts";
import * as WorkflowStartService from "./WorkflowStartService.ts";

const projectId = ProjectId.make("project-1");
const environmentId = EnvironmentId.make("environment-1");
const instanceId = ProviderInstanceId.make("codex-workflow");
const repository = "Flow-Fly/t3code" as const;
const workspaceRoot = "/tmp";
const modelSelection = {
  instanceId,
  model: "gpt-6-astra",
  options: [{ id: "reasoningEffort", value: "high" }],
} as const;

function summary(
  number: number,
  kind: WorkflowIssueSummary["kind"],
  parentNumber: number | null,
  labels: ReadonlyArray<string>,
): WorkflowIssueSummary {
  return {
    id: `issue-${number}`,
    repository,
    number,
    title: number === 15 ? "Choose the workflow launch contract" : `Workflow ${kind}`,
    url: `https://github.com/${repository}/issues/${number}`,
    kind,
    state: "open",
    stateReason: null,
    updatedAt: "2026-09-06T10:00:00.000Z",
    childCount: 1,
    parentNumber,
    labels: [...labels],
    readiness: { status: "ready", reasons: [] },
  };
}

const root = summary(10, "container", null, ["workflow:container"]);
const map = summary(12, "map", 10, ["wayfinder:map"]);
const decision = summary(15, "decision", 12, ["wayfinder:research"]);

function detail(issue: WorkflowIssueSummary): WorkflowIssueDetail {
  return { ...issue, body: "Decision context", blockedBy: [] };
}

function output(stdout: string) {
  return {
    stdout,
    stderr: "",
    exitCode: ChildProcessSpawner.ExitCode(0),
    timedOut: false,
    stdoutTruncated: false,
    stderrTruncated: false,
    stdoutInvalidUtf8: false,
    stderrInvalidUtf8: false,
  };
}

function provider(skills = ["wayfinder", "research"]): ServerProvider {
  return {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    status: "ready",
    enabled: true,
    installed: true,
    auth: { status: "authenticated" },
    checkedAt: "2026-09-06T10:00:00.000Z",
    version: "1.0.0",
    models: [
      {
        slug: "gpt-6-astra",
        name: "GPT-6 Astra",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "reasoningEffort",
              label: "Reasoning effort",
              type: "select",
              options: [
                { id: "medium", label: "Medium" },
                { id: "high", label: "High" },
              ],
            },
          ],
        },
      },
    ],
    slashCommands: [],
    skills: skills.map((name) => ({
      name,
      path: `/skills/${name}/SKILL.md`,
      enabled: true,
    })),
  };
}

function harness(
  options: {
    readonly provider?: ServerProvider | undefined;
    readonly dispatchFailure?: OrchestrationDispatchCommandError | undefined;
    readonly probeFailure?: boolean | undefined;
    readonly selectedIssue?: WorkflowIssueSummary | undefined;
    readonly ancestry?: ReadonlyArray<WorkflowIssueSummary> | undefined;
  } = {},
) {
  const commands = new Array<Extract<OrchestrationCommand, { type: "thread.turn.start" }>>();
  const githubCalls = new Array<ReadonlyArray<string>>();
  const assignees = new Set<string>();
  let sequence = 100;
  let probeCount = 0;

  const selectedIssue = options.selectedIssue ?? decision;
  const ancestry = options.ancestry ?? [root, map];
  const workflowLayer = Layer.mock(WorkflowService.WorkflowService)({
    issueDetail: ({ number }) => Effect.succeed(detail(number === 10 ? root : selectedIssue)),
    locate: () =>
      Effect.succeed({
        issue: selectedIssue,
        ancestry,
        ancestryComplete: true,
      }),
  });
  const githubLayer = Layer.mock(GitHubCli.GitHubCli)({
    execute: ({ args }) =>
      Effect.sync(() => {
        githubCalls.push(args);
        if (args[0] === "api") return output("Flow-Fly\n");
        if (args.includes("--json")) return output([...assignees].join("\n"));
        const addAt = args.indexOf("--add-assignee");
        if (addAt >= 0) assignees.add(args[addAt + 1]!);
        const removeAt = args.indexOf("--remove-assignee");
        if (removeAt >= 0) assignees.delete(args[removeAt + 1]!);
        return output("");
      }),
  });
  const selectedProvider = Object.hasOwn(options, "provider") ? options.provider : provider();
  const registry = makeProviderRegistryMock(selectedProvider ? [selectedProvider] : []);
  const providerLayer = Layer.succeed(ProviderRegistry.ProviderRegistry, {
    ...registry,
    probeWorkspaceSnapshot: () =>
      Effect.suspend(() => {
        probeCount += 1;
        return options.probeFailure
          ? Effect.fail(
              new ProviderDriverError({
                driver: "codex",
                instanceId,
                detail: "controlled discovery failure",
              }),
            )
          : Effect.succeed(selectedProvider);
      }),
  });

  const serviceLayer = Layer.effect(
    WorkflowStartService.WorkflowStartService,
    WorkflowStartService.make,
  ).pipe(
    Layer.provide(workflowLayer),
    Layer.provide(
      Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
        getProjectShellById: () =>
          Effect.succeed(Option.some({ id: projectId, title: "T3 Code", workspaceRoot } as never)),
      }),
    ),
    Layer.provide(providerLayer),
    Layer.provide(githubLayer),
    Layer.provide(
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getEnvironmentId: Effect.succeed(environmentId),
      }),
    ),
    Layer.provide(SqlitePersistenceMemory),
    Layer.provide(NodeServices.layer),
  );

  const dispatch = (command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>) =>
    Effect.suspend(() => {
      commands.push(command);
      if (options.dispatchFailure) return Effect.fail(options.dispatchFailure);
      sequence += 1;
      return Effect.succeed({ sequence });
    });

  const input = { projectId, repository, rootNumber: 10, issueNumber: 15, modelSelection };

  return {
    assignees,
    commands,
    dispatch,
    githubCalls,
    input,
    layer: serviceLayer,
    probeCount: () => probeCount,
  };
}

describe("WorkflowStartService", () => {
  it.effect(
    "persists one attempt before claiming and submits the existing bootstrap with explicit skills",
    () => {
      const test = harness();
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const started = yield* service.start(test.input, test.dispatch);
        const repeated = yield* service.start(test.input, test.dispatch);

        expect(started).toMatchObject({ disposition: "started", status: "submitted" });
        expect(repeated).toMatchObject({
          disposition: "existing",
          status: "submitted",
          attemptId: started.attemptId,
          threadId: started.threadId,
        });
        expect(test.commands).toHaveLength(1);
        expect(test.probeCount()).toBe(1);
        expect(test.commands[0]).toMatchObject({
          threadId: started.threadId,
          bootstrap: { createThread: { projectId, title: decision.title } },
          modelSelection,
          skills: [
            { name: "wayfinder", path: "/skills/wayfinder/SKILL.md" },
            { name: "research", path: "/skills/research/SKILL.md" },
          ],
        });
        expect(test.commands[0]?.message.text).toContain("Map: Flow-Fly/t3code#12");
        expect(test.commands[0]?.message.text).toContain("Decision: Flow-Fly/t3code#15");
        expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("serializes two clients onto the same durable attempt", () => {
    const test = harness();
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const results = yield* Effect.all(
        [service.start(test.input, test.dispatch), service.start(test.input, test.dispatch)],
        { concurrency: "unbounded" },
      );

      expect(results.map((result) => result.disposition).sort()).toEqual(["existing", "started"]);
      expect(new Set(results.map((result) => result.attemptId))).toHaveLength(1);
      expect(new Set(results.map((result) => result.threadId))).toHaveLength(1);
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("refuses a fresh start when live workspace discovery lacks a required skill", () => {
    const test = harness({ provider: provider(["wayfinder"]) });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "skill-unavailable" });
      expect(test.probeCount()).toBe(1);
      expect(test.githubCalls).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects an ambiguous process label before claiming", () => {
    const test = harness({
      selectedIssue: {
        ...decision,
        labels: ["wayfinder:research", "wayfinder:prototype"],
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "unsupported-issue" });
      expect(test.probeCount()).toBe(0);
      expect(test.githubCalls).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects work outside the viewed workflow branch", () => {
    const unrelatedRoot = summary(20, "container", null, ["workflow:container"]);
    const test = harness({ ancestry: [unrelatedRoot, map] });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "unsupported-issue" });
      expect(test.probeCount()).toBe(0);
      expect(test.githubCalls).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("rejects a reasoning effort the selected model does not support", () => {
    const codex = provider();
    const test = harness({
      provider: {
        ...codex,
        models: codex.models.map((model) => ({
          ...model,
          capabilities: {
            optionDescriptors: [
              {
                id: "reasoningEffort",
                label: "Reasoning effort",
                type: "select" as const,
                options: [{ id: "medium", label: "Medium" }],
              },
            ],
          },
        })),
      },
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "effort-required" });
      expect(test.githubCalls).toHaveLength(0);
      expect(test.commands).toHaveLength(0);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect(
    "refuses a start when fresh discovery fails instead of trusting cached provider data",
    () => {
      const test = harness({ probeFailure: true });
      return Effect.gen(function* () {
        const service = yield* WorkflowStartService.WorkflowStartService;
        const error = yield* Effect.flip(service.start(test.input, test.dispatch));

        expect(error).toMatchObject({
          _tag: "WorkflowStartError",
          failure: "provider-unavailable",
        });
        expect(test.probeCount()).toBe(1);
        expect(test.githubCalls).toHaveLength(0);
        expect(test.commands).toHaveLength(0);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect("holds an uncertain first submission and never dispatches it again", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({ message: "response lost" }),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const repeated = yield* service.start(test.input, test.dispatch);

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "dispatch-failed" });
      expect(repeated).toMatchObject({ disposition: "held", status: "held" });
      expect(repeated.message).toContain("uncertain");
      expect(test.commands).toHaveLength(1);
      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("releases only its own claim when bootstrap deletion confirms no turn started", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({
        message: "provider rejected turn",
        bootstrapThreadDisposition: "deleted",
      }),
    });
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      const error = yield* Effect.flip(service.start(test.input, test.dispatch));
      const repeated = yield* service.start(test.input, test.dispatch);

      expect(error).toMatchObject({ _tag: "WorkflowStartError", failure: "dispatch-failed" });
      expect(error.message).toContain("claim added by this attempt was released");
      expect(repeated.message).toContain("claim added by this attempt was released");
      expect(test.assignees).toEqual(new Set());
      expect(test.githubCalls.some((args) => args.includes("--remove-assignee"))).toBe(true);
      expect(test.commands).toHaveLength(1);
    }).pipe(Effect.provide(test.layer));
  });

  it.effect("preserves a pre-existing assignment after bootstrap deletion", () => {
    const test = harness({
      dispatchFailure: new OrchestrationDispatchCommandError({
        message: "provider rejected turn",
        bootstrapThreadDisposition: "deleted",
      }),
    });
    test.assignees.add("Flow-Fly");
    return Effect.gen(function* () {
      const service = yield* WorkflowStartService.WorkflowStartService;
      yield* Effect.flip(service.start(test.input, test.dispatch));

      expect(test.assignees).toEqual(new Set(["Flow-Fly"]));
      expect(test.githubCalls.some((args) => args.includes("--remove-assignee"))).toBe(false);
    }).pipe(Effect.provide(test.layer));
  });
});
