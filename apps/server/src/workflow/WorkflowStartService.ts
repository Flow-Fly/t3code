import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  type OrchestrationCommand,
  type OrchestrationDispatchCommandError,
  type ProjectId,
  ProviderDriverKind,
  ThreadId,
  WorkflowQueryError,
  WorkflowStartError,
  type WorkflowStartInput,
  type WorkflowStartResult,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowService from "./WorkflowService.ts";

type Dispatch = (
  command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;

const AttemptRow = Schema.Struct({
  attemptId: Schema.String,
  environmentId: Schema.String,
  projectId: Schema.String,
  repository: Schema.String,
  rootNumber: Schema.Number,
  issueNumber: Schema.Number,
  threadId: Schema.String,
  status: Schema.String,
  createdAt: Schema.String,
  detail: Schema.NullOr(Schema.String),
});
type AttemptRow = typeof AttemptRow.Type;

function startError(failure: WorkflowStartError["failure"], message: string, detail?: string) {
  return new WorkflowStartError({ failure, message, ...(detail ? { detail } : {}) });
}

function requiredSkillNames(labels: ReadonlyArray<string>): ReadonlyArray<string> {
  if (labels.includes("wayfinder:grilling"))
    return ["wayfinder", "grill-with-docs", "grilling", "domain-modeling"];
  if (labels.includes("wayfinder:research")) return ["wayfinder", "research"];
  if (labels.includes("wayfinder:prototype")) return ["wayfinder", "prototype"];
  if (labels.includes("wayfinder:task")) return ["wayfinder"];
  return ["wayfinder"];
}

const PROCESS_LABELS = [
  "wayfinder:grilling",
  "wayfinder:research",
  "wayfinder:prototype",
  "wayfinder:task",
] as const;

const WORKFLOW_KIND_LABELS = [
  "wayfinder:map",
  ...PROCESS_LABELS,
  "workflow:map",
  "workflow:decision",
  "workflow:capability",
  "workflow:container",
  "workflow:ticket",
  "workflow:task",
] as const;

function decisionProcess(labels: ReadonlyArray<string>): string {
  if (labels.includes("wayfinder:grilling"))
    return "Run grill-with-docs: grill the owner interactively with the grilling skill and use domain-modeling to document the durable decision.";
  if (labels.includes("wayfinder:research"))
    return "Research the question from authoritative sources and present evidence for the owner to decide.";
  if (labels.includes("wayfinder:prototype"))
    return "Build a throwaway prototype that answers the decision question and review it with the owner.";
  return "Complete the prerequisite task and bring the result back to the map before advancing dependent work.";
}

export function workflowDecisionInstructions(input: {
  readonly repository: string;
  readonly mapNumber: number;
  readonly mapTitle: string;
  readonly issueNumber: number;
  readonly issueTitle: string;
  readonly issueUrl: string;
  readonly labels: ReadonlyArray<string>;
}): string {
  return [
    "Use Wayfinder to work this selected workflow decision.",
    "",
    `Map: ${input.repository}#${input.mapNumber} — ${input.mapTitle}`,
    `Decision: ${input.repository}#${input.issueNumber} — ${input.issueTitle}`,
    `Source: ${input.issueUrl}`,
    "",
    decisionProcess(input.labels),
    "Use every attached skill explicitly. Do not substitute another process or skill.",
    "Keep required human participation in the loop. Do not infer approval from labels or silence.",
    "Before reporting this decision resolved, record the outcome and current evidence in a Resolution record, then update the map's decision index and remaining unknowns.",
    "Recheck live prerequisites before changing tracker state or advancing dependent work.",
  ].join("\n");
}

function resultFromRow(
  row: AttemptRow,
  disposition: WorkflowStartResult["disposition"],
): WorkflowStartResult {
  const held = row.status !== "submitted";
  return {
    disposition: held ? "held" : disposition,
    attemptId: row.attemptId,
    environmentId: row.environmentId,
    projectId: row.projectId,
    repository: row.repository,
    rootNumber: row.rootNumber,
    issueNumber: row.issueNumber,
    phase: "decision",
    threadId: row.threadId,
    status: held ? "held" : "submitted",
    createdAt: row.createdAt,
    message: held
      ? (row.detail ?? "The first submission is uncertain. Reconcile this attempt before retrying.")
      : disposition === "started"
        ? "Decision work started."
        : "Opening the existing decision attempt.",
  } as WorkflowStartResult;
}

export class WorkflowStartService extends Context.Service<
  WorkflowStartService,
  {
    readonly start: (
      input: WorkflowStartInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowStartResult, WorkflowQueryError | WorkflowStartError>;
  }
>()("t3/workflow/WorkflowStartService") {}

export const make = Effect.gen(function* () {
  const workflow = yield* WorkflowService.WorkflowService;
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const github = yield* GitHubCli.GitHubCli;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const lock = yield* Semaphore.make(1);

  const loadAttempt = Effect.fn("WorkflowStartService.loadAttempt")(function* (
    input: WorkflowStartInput,
  ) {
    const rows = yield* sql<AttemptRow>`
      SELECT attempt_id AS "attemptId", environment_id AS "environmentId",
        project_id AS "projectId", repository, root_number AS "rootNumber",
        issue_number AS "issueNumber", thread_id AS "threadId", status,
        created_at AS "createdAt", detail
      FROM workflow_start_attempts
      WHERE project_id = ${input.projectId} AND repository = ${input.repository}
        AND issue_number = ${input.issueNumber} AND phase = 'decision'
      LIMIT 1
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The existing workflow attempt could not be read.",
          String(error),
        ),
      ),
    );
    return rows[0];
  });

  const selectedProject = Effect.fn("WorkflowStartService.selectedProject")(function* (
    projectId: ProjectId,
  ) {
    const project = yield* projection
      .getProjectShellById(projectId)
      .pipe(
        Effect.mapError((error) =>
          startError(
            "workspace-unavailable",
            "The target project could not be read.",
            error.message,
          ),
        ),
      );
    return yield* Option.match(project, {
      onNone: () =>
        Effect.fail(
          startError("workspace-unavailable", "The target project is no longer available."),
        ),
      onSome: Effect.succeed,
    });
  });

  const executeClaim = Effect.fn("WorkflowStartService.executeClaim")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
  ) {
    return yield* github
      .execute({ cwd, args, maxOutputBytes: 100_000 })
      .pipe(
        Effect.mapError((error) =>
          startError("claim-failed", "GitHub could not claim this decision.", error.message),
        ),
      );
  });

  const startUnlocked = Effect.fn("WorkflowStartService.start")(function* (
    input: WorkflowStartInput,
    dispatch: Dispatch,
  ) {
    const existing = yield* loadAttempt(input);
    if (existing) return resultFromRow(existing, "existing");

    const project = yield* selectedProject(input.projectId);
    const workspace = yield* fileSystem
      .stat(project.workspaceRoot)
      .pipe(
        Effect.mapError((error) =>
          startError(
            "workspace-unavailable",
            "The target workspace is not accessible in this environment.",
            String(error),
          ),
        ),
      );
    if (workspace.type !== "Directory") {
      return yield* startError(
        "workspace-unavailable",
        "The target workspace is not a directory in this environment.",
      );
    }
    const [root, issue] = yield* Effect.all([
      workflow.issueDetail({
        projectId: input.projectId,
        repository: input.repository,
        number: input.rootNumber,
      }),
      workflow.issueDetail({
        projectId: input.projectId,
        repository: input.repository,
        number: input.issueNumber,
      }),
    ]);
    if (issue.kind !== "decision" && !issue.labels.includes("wayfinder:task")) {
      return yield* startError(
        "unsupported-issue",
        "Start is available for workflow decisions and prerequisite tasks.",
      );
    }
    const processLabels = PROCESS_LABELS.filter((label) => issue.labels.includes(label));
    const kindLabels = WORKFLOW_KIND_LABELS.filter((label) => issue.labels.includes(label));
    if (processLabels.length !== 1 || kindLabels.length !== 1) {
      return yield* startError(
        "unsupported-issue",
        "The decision must have exactly one Wayfinder process label before it can start.",
      );
    }
    const located = yield* workflow.locate({
      projectId: input.projectId,
      repository: input.repository,
      id: issue.id,
      number: issue.number,
    });
    const locatedRoot = located.ancestry[0] ?? located.issue;
    if (!located.ancestryComplete || locatedRoot.id !== root.id) {
      return yield* startError(
        "unsupported-issue",
        "The selected decision does not belong to this workflow root.",
      );
    }
    const selectedMap = located.ancestry.toReversed().find((ancestor) => ancestor.kind === "map");
    if (!selectedMap) {
      return yield* startError(
        "unsupported-issue",
        "The selected decision is not contained by a Wayfinder map.",
      );
    }
    if (issue.readiness?.status !== "ready") {
      return yield* startError(
        "not-ready",
        "This decision is not ready to start.",
        issue.readiness?.reasons.map((reason) => reason.message).join(" ") ??
          "Refresh Workflow to load current readiness evidence.",
      );
    }

    const scopedProvider = yield* providerRegistry
      .probeWorkspaceSnapshot({
        instanceId: input.modelSelection.instanceId,
        cwd: project.workspaceRoot,
      })
      .pipe(
        Effect.mapError((error) =>
          startError(
            "provider-unavailable",
            "Codex workspace discovery failed. Check the provider and retry.",
            String(error),
          ),
        ),
      );
    if (
      !scopedProvider ||
      scopedProvider.driver !== ProviderDriverKind.make("codex") ||
      !scopedProvider.enabled ||
      !scopedProvider.installed ||
      scopedProvider.auth.status !== "authenticated" ||
      scopedProvider.status === "error" ||
      scopedProvider.status === "disabled"
    ) {
      return yield* startError(
        "provider-unavailable",
        "Choose an enabled, authenticated Codex provider in this environment.",
      );
    }
    if (!scopedProvider.models.some((model) => model.slug === input.modelSelection.model)) {
      return yield* startError(
        "model-unavailable",
        `Model '${input.modelSelection.model}' is not available from this Codex provider.`,
      );
    }
    const effort = getModelSelectionStringOptionValue(input.modelSelection, "reasoningEffort");
    if (!effort) {
      return yield* startError(
        "effort-required",
        "Choose a Codex reasoning effort before starting decision work.",
      );
    }
    const model = scopedProvider.models.find(
      (candidate) => candidate.slug === input.modelSelection.model,
    )!;
    const effortDescriptor = model.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "reasoningEffort",
    );
    if (
      effortDescriptor?.type !== "select" ||
      !effortDescriptor.options.some((option) => option.id === effort)
    ) {
      return yield* startError(
        "effort-required",
        `Reasoning effort '${effort}' is not supported by model '${model.slug}'.`,
      );
    }
    const skills = requiredSkillNames(issue.labels).map((name) => {
      const matches = scopedProvider.skills.filter((skill) => skill.name === name && skill.enabled);
      return matches.length === 1 ? matches[0] : undefined;
    });
    const missingSkill = requiredSkillNames(issue.labels).find((_, index) => !skills[index]);
    if (missingSkill) {
      return yield* startError(
        "skill-unavailable",
        `Enable the '${missingSkill}' skill at one unambiguous path in the target workspace.`,
      );
    }

    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        startError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const attemptId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const threadId = ThreadId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    yield* sql`
      INSERT INTO workflow_start_attempts (
        attempt_id, environment_id, project_id, repository, root_number,
        issue_number, phase, thread_id, command_id, message_id, status,
        created_at, updated_at
      ) VALUES (
        ${attemptId}, ${environmentId}, ${input.projectId}, ${input.repository}, ${input.rootNumber},
        ${input.issueNumber}, 'decision', ${threadId}, ${commandId}, ${messageId}, 'claiming',
        ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        startError("persistence-failed", "The workflow attempt could not be saved.", String(error)),
      ),
    );

    const identity = yield* executeClaim(project.workspaceRoot, ["api", "user", "--jq", ".login"]);
    const login = identity.stdout.trim();
    if (!login) {
      return yield* startError("claim-failed", "GitHub did not identify the authenticated user.");
    }
    yield* sql`
      UPDATE workflow_start_attempts SET claim_login = ${login}, updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The GitHub claim identity could not be saved.",
          String(error),
        ),
      ),
    );
    const assigneeArgs = [
      "issue",
      "view",
      String(input.issueNumber),
      "--repo",
      input.repository,
      "--json",
      "assignees",
      "--jq",
      ".assignees[].login",
    ] as const;
    const beforeAssignees = (yield* executeClaim(project.workspaceRoot, assigneeArgs)).stdout
      .split("\n")
      .map((value) => value.trim())
      .filter(Boolean);
    yield* executeClaim(project.workspaceRoot, [
      "issue",
      "edit",
      String(input.issueNumber),
      "--repo",
      input.repository,
      "--add-assignee",
      login,
    ]).pipe(
      Effect.catch((error) =>
        sql`
          UPDATE workflow_start_attempts SET status = 'held',
            detail = 'Claim outcome is uncertain. Reconcile the GitHub assignment before retrying.',
            updated_at = ${createdAt}
          WHERE attempt_id = ${attemptId}
        `.pipe(
          Effect.mapError((cause) =>
            startError(
              "persistence-failed",
              "The uncertain claim could not be recorded.",
              String(cause),
            ),
          ),
          Effect.andThen(Effect.fail(error)),
        ),
      ),
    );
    const afterAssignees = (yield* executeClaim(project.workspaceRoot, assigneeArgs)).stdout
      .split("\n")
      .map((value) => value.trim())
      .filter(Boolean);
    if (!afterAssignees.includes(login)) {
      yield* sql`
        UPDATE workflow_start_attempts SET status = 'held',
          detail = 'GitHub did not confirm the assignment. Reconcile the claim before retrying.',
          updated_at = ${createdAt}
        WHERE attempt_id = ${attemptId}
      `.pipe(
        Effect.mapError((error) =>
          startError(
            "persistence-failed",
            "The uncertain claim could not be recorded.",
            String(error),
          ),
        ),
      );
      return yield* startError(
        "claim-failed",
        "GitHub did not confirm the assignment. Reconcile the claim before retrying.",
      );
    }
    const claimOwned = !beforeAssignees.includes(login);
    const releaseOwnedClaim = () =>
      claimOwned
        ? executeClaim(project.workspaceRoot, [
            "issue",
            "edit",
            String(input.issueNumber),
            "--repo",
            input.repository,
            "--remove-assignee",
            login,
          ]).pipe(
            Effect.as(true),
            Effect.catch(() => Effect.succeed(false)),
          )
        : Effect.succeed(false);

    const instructions = workflowDecisionInstructions({
      repository: input.repository,
      mapNumber: selectedMap.number,
      mapTitle: selectedMap.title,
      issueNumber: input.issueNumber,
      issueTitle: issue.title,
      issueUrl: issue.url,
      labels: issue.labels,
    });
    yield* sql`
      UPDATE workflow_start_attempts SET status = 'submitting', claim_login = ${login},
        claim_owned = ${claimOwned ? 1 : 0}, updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The workflow submission intent could not be saved.",
          String(error),
        ),
      ),
      Effect.catch((error) => releaseOwnedClaim().pipe(Effect.andThen(Effect.fail(error)))),
    );

    const command = {
      type: "thread.turn.start" as const,
      commandId,
      threadId,
      message: { messageId, role: "user" as const, text: instructions, attachments: [] },
      modelSelection: input.modelSelection,
      skills: skills.map((skill) => ({ name: skill!.name, path: skill!.path })),
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      bootstrap: {
        createThread: {
          projectId: input.projectId,
          title: issue.title,
          modelSelection: input.modelSelection,
          runtimeMode: "approval-required" as const,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          branch: null,
          worktreePath: null,
          createdAt,
        },
      },
      createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;

    const dispatched = yield* dispatch(command).pipe(
      Effect.catch((error) => {
        const threadWasDeleted = error.bootstrapThreadDisposition === "deleted";
        const releaseClaim =
          threadWasDeleted && claimOwned ? releaseOwnedClaim() : Effect.succeed(false);

        return releaseClaim.pipe(
          Effect.flatMap((claimReleased) => {
            const detail = threadWasDeleted
              ? claimOwned && !claimReleased
                ? "The bootstrap thread was deleted, but the claim could not be released. Reconcile the assignment before retrying."
                : "The bootstrap thread was deleted before a turn started. The claim added by this attempt was released."
              : "The first submission is uncertain. Reconcile the durable command before retrying.";
            return sql`
              UPDATE workflow_start_attempts SET status = 'held', detail = ${detail},
                updated_at = ${createdAt}
              WHERE attempt_id = ${attemptId}
            `.pipe(
              Effect.mapError((cause) =>
                startError(
                  "persistence-failed",
                  "The failed submission could not be recorded.",
                  String(cause),
                ),
              ),
              Effect.andThen(
                Effect.fail(
                  startError(
                    "dispatch-failed",
                    threadWasDeleted
                      ? detail
                      : "The first submission is uncertain and will not be sent again automatically.",
                    String(error),
                  ),
                ),
              ),
            );
          }),
        );
      }),
    );
    yield* sql`
      UPDATE workflow_start_attempts SET status = 'submitted', sequence = ${dispatched.sequence},
        detail = NULL, updated_at = ${createdAt}
      WHERE attempt_id = ${attemptId}
    `.pipe(
      Effect.mapError((error) =>
        startError(
          "persistence-failed",
          "The submitted workflow attempt could not be saved.",
          String(error),
        ),
      ),
    );
    return resultFromRow(
      {
        attemptId,
        environmentId,
        projectId: input.projectId,
        repository: input.repository,
        rootNumber: input.rootNumber,
        issueNumber: input.issueNumber,
        threadId,
        status: "submitted",
        createdAt,
        detail: null,
      },
      "started",
    );
  });

  return WorkflowStartService.of({
    start: (input, dispatch) => lock.withPermits(1)(startUnlocked(input, dispatch)),
  });
});

export const layer = Layer.effect(WorkflowStartService, make);
