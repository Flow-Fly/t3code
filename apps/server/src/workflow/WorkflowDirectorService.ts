import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentId,
  MessageId,
  type OrchestrationCommand,
  type OrchestrationDispatchCommandError,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  WorkflowDirectorError,
  type WorkflowDirectorAdmission,
  type WorkflowDirectorAdmissionInput,
  type WorkflowDirectorAdmissionResult,
  type WorkflowDirectorResumeInput,
  type WorkflowDirectorStartInput,
  type WorkflowDirectorStartResult,
  type WorkflowDirectorStatus,
  type WorkflowDirectorStatusInput,
  type WorkflowEvidenceRecord,
  type WorkflowIssueDetail,
  type WorkflowIssueSummary,
  type WorkflowQueryError,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as WorkflowService from "./WorkflowService.ts";

const ADMISSION_LIMIT = 10;
const DIRECTOR_MODEL = "gpt-6-astra";
const DIRECTOR_EFFORT = "high";
const REQUIRED_SKILLS = ["implement", "code-review"] as const;

type Dispatch = (
  command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>,
) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;

const DirectorRow = Schema.Struct({
  directorId: Schema.String,
  batchId: Schema.String,
  environmentId: Schema.String,
  projectId: Schema.String,
  repository: Schema.String,
  rootNumber: Schema.Number,
  capabilityNumber: Schema.Number,
  threadId: Schema.String,
  commandId: Schema.String,
  messageId: Schema.String,
  worktreePath: Schema.String,
  worktreeBranch: Schema.String,
  status: Schema.String,
  requestedModel: Schema.String,
  requestedInstanceId: Schema.String,
  requestedEffort: Schema.String,
  observedModel: Schema.NullOr(Schema.String),
  observedEffort: Schema.NullOr(Schema.String),
  observedMatch: Schema.String,
  sequence: Schema.NullOr(Schema.Number),
  initialTurnDisposition: Schema.String,
  detail: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type DirectorRow = typeof DirectorRow.Type;
const decodeDirectorRow = Schema.decodeUnknownEffect(DirectorRow);

const AdmissionRow = Schema.Struct({
  admissionId: Schema.String,
  directorId: Schema.String,
  batchId: Schema.String,
  repository: Schema.String,
  ticketNumber: Schema.Number,
  slotTicketNumber: Schema.Number,
  purpose: Schema.String,
  ownership: Schema.String,
  claimLogin: Schema.NullOr(Schema.String),
  claimStatus: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
type AdmissionRow = typeof AdmissionRow.Type;
const decodeAdmissionRow = Schema.decodeUnknownEffect(AdmissionRow);

function directorError(
  failure: WorkflowDirectorError["failure"],
  message: string,
  detail?: string,
) {
  return new WorkflowDirectorError({ failure, message, ...(detail ? { detail } : {}) });
}

function sameContent(left: string, right: string): boolean {
  return left.trim() === right.trim();
}

function durableT3SourceReference(source: string | undefined) {
  if (!source) return null;
  const threadIds = [...source.matchAll(/\bT3(?: Code)? thread\s+`([^`]+)`/giu)];
  const messageIds = [...source.matchAll(/\b(?:user\s+)?message\s+`([^`]+)`/giu)];
  if (threadIds.length !== 1 || messageIds.length !== 1) return null;
  return { threadId: threadIds[0]![1]!, messageId: messageIds[0]![1]! };
}

function breakdownUnits(content: string) {
  const details = [
    ...content.matchAll(
      /<details>\s*<summary>\s*(?:(T\d+)\s+[—-]\s+)?([^<]+?)\s*<\/summary>([\s\S]*?)<\/details>/giu,
    ),
  ].map((match) => ({
    id: match[1]?.toUpperCase() ?? null,
    title: match[2]!.replace(/\s+/gu, " ").trim(),
    content: match[3]!.trim(),
  }));
  const listTitles =
    details.length > 0
      ? details
      : [...content.matchAll(/^\s*\d+\.\s+\*\*(.+?)\*\*\s*$/gmu)].map((match) => ({
          id: null,
          title: match[1]!.replace(/\s+/gu, " ").trim(),
          content: "",
        }));
  return [...new Map(listTitles.map((unit) => [`${unit.id ?? ""}:${unit.title}`, unit])).values()];
}

function normalizedUnitTitle(title: string): string {
  return title.replace(/\s+/gu, " ").trim().toLocaleLowerCase();
}

function sourceIssueReferences(body: string) {
  const section = /(?:^|\n)#{2,6}\s+Source map\s*\n([\s\S]*?)(?=\n#{1,6}\s|$)/iu.exec(body)?.[1];
  if (!section || /^\s*none(?:\s*\(standalone\))?\.?\s*$/iu.test(section)) return [];
  return [
    ...new Map(
      [...section.matchAll(/https:\/\/github\.com\/([^/\s]+\/[^/\s)]+)\/issues\/(\d+)/giu)].map(
        (match) => [
          `${match[1]!.toLocaleLowerCase()}#${Number(match[2])}`,
          { repository: match[1]!, number: Number(match[2]) },
        ],
      ),
    ).values(),
  ];
}

function admissionFromRow(row: AdmissionRow): WorkflowDirectorAdmission {
  return {
    admissionId: row.admissionId,
    directorId: row.directorId,
    batchId: row.batchId,
    repository: row.repository as WorkflowDirectorAdmission["repository"],
    ticketNumber: row.ticketNumber,
    slotTicketNumber: row.slotTicketNumber,
    purpose: row.purpose as WorkflowDirectorAdmission["purpose"],
    ownership: row.ownership,
    claimLogin: row.claimLogin,
    claimStatus: row.claimStatus as WorkflowDirectorAdmission["claimStatus"],
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

export function workflowDirectorInstructions(input: {
  readonly capability: WorkflowIssueDetail;
  readonly tickets: ReadonlyArray<WorkflowIssueSummary>;
  readonly approvalRecords: ReadonlyArray<WorkflowEvidenceRecord>;
  readonly sourceContext: ReadonlyArray<WorkflowIssueDetail>;
  readonly implementSkillPath: string;
  readonly reviewSkillPath: string;
}) {
  return [
    "Direct implementation of this approved capability from its isolated capability worktree.",
    "",
    `Capability: ${input.capability.repository}#${input.capability.number} — ${input.capability.title}`,
    `Source: ${input.capability.url}`,
    "Requested director profile: Astra/high.",
    `Delegate implementation in a fresh Sol/high role with $implement (${input.implementSkillPath}).`,
    `Delegate independent review in a separate fresh Astra/medium role with $code-review (${input.reviewSkillPath}).`,
    "The director coordinates those roles; it does not perform their child work itself.",
    "There is no workflow-director skill dependency.",
    "",
    "Approved capability:",
    input.capability.body,
    "",
    "Approval sources:",
    ...input.approvalRecords.map(
      (record) =>
        `- ${record.approvalKind}: ${record.url} by ${record.approvedBy ?? "unknown"}; source ${record.source ?? "unknown"}`,
    ),
    "",
    "Decisions and source context:",
    ...(input.sourceContext.length > 0
      ? input.sourceContext.map(
          (source) => `- ${source.repository}#${source.number} — ${source.title}\n${source.body}`,
        )
      : ["- Standalone capability; no source map was declared."]),
    "",
    "Live delivery frontier:",
    ...input.tickets.map(
      (ticket) =>
        `- ${ticket.repository}#${ticket.number} — ${ticket.title}: ${ticket.readiness?.status ?? "unknown"} (${ticket.url})`,
    ),
    "",
    "Before every ticket admission, use the host admission RPC so readiness, approval, slot and explicit ownership are persisted before delegation.",
    `This batch admits at most ${ADMISSION_LIMIT} distinct delivery slices. Failed or blocked admitted slices keep their slot; retry and review reuse it; nested tasks reuse their parent slice. At the limit, stop new admissions, finish or settle admitted work, and wait for a successor.`,
    "Re-read live tracker state before each admission. Do not infer approval from labels, assignment, closure, silence or unavailable evidence.",
    "GitHub assignment is observational and is not a cross-environment atomic lock.",
    "Arbitrary provider collaboration outside the admission RPC cannot be host-enforced; keep all directed delivery inside the callable boundary.",
  ].join("\n");
}

export class WorkflowDirectorService extends Context.Service<
  WorkflowDirectorService,
  {
    readonly start: (
      input: WorkflowDirectorStartInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowDirectorStartResult, WorkflowQueryError | WorkflowDirectorError>;
    readonly status: (
      input: WorkflowDirectorStatusInput,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly resume: (
      input: WorkflowDirectorResumeInput,
      dispatch: Dispatch,
    ) => Effect.Effect<WorkflowDirectorStatus, WorkflowQueryError | WorkflowDirectorError>;
    readonly admit: (
      input: WorkflowDirectorAdmissionInput,
    ) => Effect.Effect<WorkflowDirectorAdmissionResult, WorkflowQueryError | WorkflowDirectorError>;
  }
>()("t3/workflow/WorkflowDirectorService") {}

export const make = Effect.gen(function* () {
  const workflow = yield* WorkflowService.WorkflowService;
  const projection = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const git = yield* GitWorkflowService.GitWorkflowService;
  const github = yield* GitHubCli.GitHubCli;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* OrchestrationCommandReceiptRepository;
  const crypto = yield* Crypto.Crypto;
  const lock = yield* Semaphore.make(1);

  const selectedProject = Effect.fn("WorkflowDirectorService.selectedProject")(function* (
    projectId: ProjectId,
  ) {
    const project = yield* projection
      .getProjectShellById(projectId)
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The target project could not be read.",
            error.message,
          ),
        ),
      );
    return yield* Option.match(project, {
      onNone: () =>
        Effect.fail(directorError("workspace-unavailable", "The target project is unavailable.")),
      onSome: Effect.succeed,
    });
  });

  const loadDirectorByCapability = Effect.fn("WorkflowDirectorService.loadDirectorByCapability")(
    function* (input: WorkflowDirectorStatusInput, environmentId: EnvironmentId) {
      const rows = yield* sql<Record<string, unknown>>`
      SELECT director_id AS "directorId", batch_id AS "batchId", environment_id AS "environmentId",
        project_id AS "projectId", repository, root_number AS "rootNumber",
        capability_number AS "capabilityNumber", thread_id AS "threadId", command_id AS "commandId",
        message_id AS "messageId", worktree_path AS "worktreePath", worktree_branch AS "worktreeBranch",
        status, requested_model AS "requestedModel", requested_instance_id AS "requestedInstanceId",
        requested_effort AS "requestedEffort",
        observed_model AS "observedModel", observed_effort AS "observedEffort",
        observed_match AS "observedMatch", sequence, initial_turn_disposition AS "initialTurnDisposition",
        detail, created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_directors
      WHERE environment_id = ${environmentId} AND repository COLLATE NOCASE = ${input.repository}
        AND capability_number = ${input.capabilityNumber} AND is_current = 1
      LIMIT 1
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The capability director could not be read.",
            String(error),
          ),
        ),
      );
      return rows[0]
        ? yield* decodeDirectorRow(rows[0]).pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "The capability director record is invalid.",
                String(error),
              ),
            ),
          )
        : undefined;
    },
  );

  const loadDirectorById = Effect.fn("WorkflowDirectorService.loadDirectorById")(function* (
    directorId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT director_id AS "directorId", batch_id AS "batchId", environment_id AS "environmentId",
        project_id AS "projectId", repository, root_number AS "rootNumber",
        capability_number AS "capabilityNumber", thread_id AS "threadId", command_id AS "commandId",
        message_id AS "messageId", worktree_path AS "worktreePath", worktree_branch AS "worktreeBranch",
        status, requested_model AS "requestedModel", requested_instance_id AS "requestedInstanceId",
        requested_effort AS "requestedEffort",
        observed_model AS "observedModel", observed_effort AS "observedEffort",
        observed_match AS "observedMatch", sequence, initial_turn_disposition AS "initialTurnDisposition",
        detail, created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_directors WHERE director_id = ${directorId} AND is_current = 1 LIMIT 1
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The capability director could not be read.",
          String(error),
        ),
      ),
    );
    if (!rows[0])
      return yield* directorError(
        "director-not-found",
        "This capability director is no longer current in this environment.",
      );
    return yield* decodeDirectorRow(rows[0]).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The capability director record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const verifyApprovalSource = Effect.fn("WorkflowDirectorService.verifyApprovalSource")(function* (
    record: WorkflowEvidenceRecord,
  ) {
    if (record.sourceAccess === "verified") return true;
    if (record.sourceAccess === "unavailable") return false;
    const source = durableT3SourceReference(record.source);
    if (!source) return false;
    const detail = yield* projection
      .getThreadDetailById(ThreadId.make(source.threadId), {
        activityKinds: [],
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "An approval source thread could not be read.",
            String(error),
          ),
        ),
      );
    return (
      Option.isSome(detail) &&
      detail.value.messages.some(
        (message) => message.id === source.messageId && message.role === "user",
      )
    );
  });

  const currentApproval = Effect.fn("WorkflowDirectorService.currentApproval")(function* (
    capability: WorkflowIssueDetail,
    kind: "specification" | "ticket-breakdown",
  ) {
    const records =
      capability.evidence?.records.filter(
        (record) =>
          record.kind === "approval" &&
          record.approvalKind === kind &&
          record.state === "current" &&
          record.authority === "verified" &&
          typeof record.approvedBy === "string" &&
          typeof record.approvedContent === "string" &&
          (kind === "ticket-breakdown" ||
            (record.scope === "current" && sameContent(record.approvedContent, capability.body))),
      ) ?? [];
    for (const record of records.toReversed()) {
      if (yield* verifyApprovalSource(record)) return record;
    }
    return undefined;
  });

  const collectDeliveryTickets = Effect.fn("WorkflowDirectorService.collectDeliveryTickets")(
    function* (input: {
      readonly projectId: ProjectId;
      readonly repository: string;
      readonly capabilityNumber: number;
    }) {
      const tickets: WorkflowIssueSummary[] = [];
      const pending = [input.capabilityNumber];
      const visited = new Set<number>();
      while (pending.length > 0) {
        const parentNumber = pending.shift()!;
        if (visited.has(parentNumber)) continue;
        visited.add(parentNumber);
        const result = yield* workflow.children({
          projectId: input.projectId,
          repository: input.repository as WorkflowIssueSummary["repository"],
          parentNumber,
        });
        for (const child of result.children) {
          if (child.kind === "ticket") {
            tickets.push(child);
          } else if (child.kind === "container" || child.kind === "map") {
            pending.push(child.number);
          }
        }
      }
      return tickets;
    },
  );

  const verifyPublishedBreakdown = Effect.fn("WorkflowDirectorService.verifyPublishedBreakdown")(
    function* (approval: WorkflowEvidenceRecord, tickets: ReadonlyArray<WorkflowIssueDetail>) {
      const approved = breakdownUnits(approval.approvedContent ?? "");
      if (approved.length === 0) {
        return yield* directorError(
          "breakdown-incomplete",
          "The approved ticket breakdown does not identify any delivery units.",
        );
      }
      const matched = tickets.map((ticket) => {
        const approvedId = /Approved slice:\s*\*\*(T\d+)\*\*/iu
          .exec(ticket.body)?.[1]
          ?.toUpperCase();
        const unit = approvedId
          ? approved.find((candidate) => candidate.id === approvedId)
          : approved.find(
              (candidate) =>
                normalizedUnitTitle(candidate.title) === normalizedUnitTitle(ticket.title),
            );
        const approvalRecord = ticket.evidence?.records.find(
          (record) =>
            record.kind === "approval" &&
            record.approvalKind === "ticket-breakdown" &&
            record.state === "current" &&
            record.scope === "current" &&
            record.authority === "verified" &&
            sameContent(record.approvedContent ?? "", approval.approvedContent ?? ""),
        );
        return { ticket, unit, approvalRecord };
      });
      const matchedUnits = new Set(matched.flatMap(({ unit }) => (unit ? [unit] : [])));
      const missing = approved.filter((unit) => !matchedUnits.has(unit));
      const extra = matched.filter(({ unit }) => !unit).map(({ ticket }) => ticket.title);
      const changedScope: WorkflowIssueDetail[] = [];
      for (const entry of matched) {
        if (!entry.approvalRecord || !(yield* verifyApprovalSource(entry.approvalRecord))) {
          changedScope.push(entry.ticket);
        }
      }
      if (
        approved.length !== tickets.length ||
        missing.length > 0 ||
        extra.length > 0 ||
        changedScope.length > 0
      ) {
        return yield* directorError(
          "breakdown-incomplete",
          "Published delivery work does not match the complete approved breakdown.",
          [
            missing.length > 0 ? `Missing: ${missing.map((unit) => unit.title).join(", ")}.` : "",
            extra.length > 0 ? `Extra or renamed: ${extra.join(", ")}.` : "",
            changedScope.length > 0
              ? `Changed or unverified scope: ${changedScope.map((ticket) => `#${ticket.number}`).join(", ")}.`
              : "",
          ]
            .filter(Boolean)
            .join(" "),
        );
      }
    },
  );

  const providerPreflight = Effect.fn("WorkflowDirectorService.providerPreflight")(function* (
    cwd: string,
    modelSelection: WorkflowDirectorStartInput["modelSelection"],
  ) {
    if (modelSelection.model !== DIRECTOR_MODEL) {
      return yield* directorError(
        "model-unavailable",
        `Capability directors require '${DIRECTOR_MODEL}'. The requested model was '${modelSelection.model}'.`,
      );
    }
    const effort = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
    if (effort !== DIRECTOR_EFFORT) {
      return yield* directorError(
        "effort-required",
        `Capability directors require '${DIRECTOR_EFFORT}' reasoning effort.`,
      );
    }
    const provider = yield* providerRegistry
      .probeWorkspaceSnapshot({ instanceId: modelSelection.instanceId, cwd })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "provider-unavailable",
            "Codex workspace discovery failed. Check the selected provider and retry.",
            String(error),
          ),
        ),
      );
    if (
      !provider ||
      provider.driver !== ProviderDriverKind.make("codex") ||
      !provider.enabled ||
      !provider.installed ||
      provider.auth.status !== "authenticated" ||
      provider.status === "error" ||
      provider.status === "disabled"
    ) {
      return yield* directorError(
        "provider-unavailable",
        "Choose an enabled, installed and authenticated Codex provider.",
      );
    }
    const model = provider.models.find((candidate) => candidate.slug === DIRECTOR_MODEL);
    const descriptor = model?.capabilities?.optionDescriptors?.find(
      (candidate) => candidate.id === "reasoningEffort",
    );
    if (!model) {
      return yield* directorError(
        "model-unavailable",
        `Model '${DIRECTOR_MODEL}' is unavailable from the selected Codex provider.`,
      );
    }
    if (
      descriptor?.type !== "select" ||
      !descriptor.options.some((option) => option.id === DIRECTOR_EFFORT)
    ) {
      return yield* directorError(
        "effort-required",
        `Model '${DIRECTOR_MODEL}' does not support '${DIRECTOR_EFFORT}' reasoning effort.`,
      );
    }
    const skills = REQUIRED_SKILLS.map((name) => {
      const matches = provider.skills.filter((skill) => skill.name === name && skill.enabled);
      return matches.length === 1 ? matches[0] : undefined;
    });
    const missingSkill = REQUIRED_SKILLS.find((_, index) => !skills[index]);
    if (missingSkill) {
      return yield* directorError(
        "skill-unavailable",
        `Enable the '${missingSkill}' skill at one unambiguous path in the target workspace.`,
      );
    }
    return { provider, skills: skills.map((skill) => skill!) };
  });

  const verifyRepository = Effect.fn("WorkflowDirectorService.verifyRepository")(function* (
    cwd: string,
    repository: string,
  ) {
    const result = yield* github
      .execute({
        cwd,
        args: ["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
        maxOutputBytes: 100_000,
      })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "workspace-unavailable",
            "The target repository identity or tracker access could not be verified.",
            error.message,
          ),
        ),
      );
    if (result.stdout.trim().toLocaleLowerCase() !== repository.toLocaleLowerCase()) {
      return yield* directorError(
        "workspace-unavailable",
        "The target workspace belongs to a different repository.",
        `Expected ${repository}; observed ${result.stdout.trim() || "unknown"}.`,
      );
    }
  });

  const prepareCapability = Effect.fn("WorkflowDirectorService.prepareCapability")(function* (
    input: Pick<
      WorkflowDirectorStartInput,
      "projectId" | "repository" | "capabilityNumber" | "modelSelection"
    >,
    cwd: string,
    options?: { readonly allowClaimed?: boolean },
  ) {
    const capability = yield* workflow.issueDetail({
      projectId: input.projectId,
      repository: input.repository,
      number: input.capabilityNumber,
    });
    if (capability.kind !== "capability" || capability.state !== "open") {
      return yield* directorError(
        "not-ready",
        "Start implementation is available for an open workflow capability.",
      );
    }
    const specificationApproval = yield* currentApproval(capability, "specification");
    const breakdownApproval = yield* currentApproval(capability, "ticket-breakdown");
    if (!specificationApproval || !breakdownApproval) {
      return yield* directorError(
        "approval-unavailable",
        "Current specification and ticket-breakdown approvals with available owner sources are required.",
      );
    }
    const ticketSummaries = yield* collectDeliveryTickets(input);
    const tickets = yield* Effect.forEach(ticketSummaries, (ticket) =>
      workflow.issueDetail({
        projectId: input.projectId,
        repository: input.repository,
        number: ticket.number,
      }),
    );
    yield* verifyPublishedBreakdown(breakdownApproval, tickets);
    if (
      options?.allowClaimed !== true &&
      !tickets.some((ticket) => ticket.readiness?.status === "ready")
    ) {
      return yield* directorError(
        "not-ready",
        "No published delivery ticket is currently ready or owned by this capability.",
      );
    }
    yield* verifyRepository(cwd, input.repository);
    const provider = yield* providerPreflight(cwd, input.modelSelection);
    return { capability, specificationApproval, breakdownApproval, tickets, ...provider };
  });

  const ensureWorktree = Effect.fn("WorkflowDirectorService.ensureWorktree")(function* (
    row: DirectorRow,
    projectCwd: string,
  ) {
    const validate = Effect.fn("WorkflowDirectorService.validateWorktree")(function* () {
      const status = yield* git
        .localStatus({ cwd: row.worktreePath })
        .pipe(Effect.orElseSucceed(() => null));
      return status?.isRepo === true && status.refName === row.worktreeBranch;
    });
    if (yield* validate()) {
      yield* verifyRepository(row.worktreePath, row.repository);
      return;
    }
    const projectStatus = yield* git
      .localStatus({ cwd: projectCwd })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "worktree-failed",
            "The project Git branch could not be read.",
            String(error),
          ),
        ),
      );
    if (!projectStatus.isRepo || !projectStatus.refName) {
      return yield* directorError(
        "worktree-failed",
        "The capability workspace must be a Git repository on a branch.",
      );
    }
    const created = yield* git
      .createWorktree({
        cwd: projectCwd,
        refName: projectStatus.refName,
        newRefName: row.worktreeBranch,
        baseRefName: projectStatus.refName,
        path: row.worktreePath,
      })
      .pipe(Effect.result);
    if (created._tag === "Failure" && !(yield* validate())) {
      return yield* directorError(
        "worktree-failed",
        "Capability worktree creation is uncertain. The recorded path was retained for recovery.",
        String(created.failure),
      );
    }
    if (!(yield* validate())) {
      return yield* directorError(
        "worktree-failed",
        "The created capability worktree does not match its recorded Git branch.",
      );
    }
    yield* verifyRepository(row.worktreePath, row.repository);
  });

  const sourceContext = Effect.fn("WorkflowDirectorService.sourceContext")(function* (
    input: Pick<WorkflowDirectorStartInput, "projectId" | "repository">,
    capability: WorkflowIssueDetail,
  ) {
    return yield* Effect.forEach(sourceIssueReferences(capability.body), (reference) =>
      workflow.issueDetail({
        projectId: input.projectId,
        repository: reference.repository as WorkflowIssueSummary["repository"],
        number: reference.number,
      }),
    );
  });

  const admissions = Effect.fn("WorkflowDirectorService.admissions")(function* (
    directorId: string,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT admission_id AS "admissionId", director_id AS "directorId", batch_id AS "batchId",
        repository, ticket_number AS "ticketNumber", slot_ticket_number AS "slotTicketNumber",
        purpose, ownership, claim_login AS "claimLogin", claim_status AS "claimStatus",
        created_at AS "createdAt", updated_at AS "updatedAt"
      FROM workflow_director_admissions WHERE director_id = ${directorId} ORDER BY created_at
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "Director admissions could not be read.",
          String(error),
        ),
      ),
    );
    return yield* Effect.forEach(rows, (row) => decodeAdmissionRow(row)).pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "A director admission record is invalid.",
          String(error),
        ),
      ),
    );
  });

  const reconcileDirector = Effect.fn("WorkflowDirectorService.reconcileDirector")(function* (
    row: DirectorRow,
  ) {
    if (row.status !== "submitting" && row.status !== "held") return row;
    const receipt = yield* receipts
      .getByCommandId({ commandId: CommandId.make(row.commandId) })
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Director command evidence could not be read.",
            String(error),
          ),
        ),
      );
    if (Option.isSome(receipt) && receipt.value.status === "accepted") {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`
        UPDATE workflow_directors SET status = 'active', sequence = ${receipt.value.resultSequence},
          initial_turn_disposition = 'accepted', detail = NULL, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Accepted director evidence could not be saved.",
            String(error),
          ),
        ),
      );
      return {
        ...row,
        status: "active",
        sequence: receipt.value.resultSequence,
        detail: null,
        updatedAt,
      };
    }
    if (row.status === "submitting") {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      const detail =
        "The director submission is uncertain. Inspect the linked thread; it will not be sent again automatically.";
      yield* sql`
        UPDATE workflow_directors SET status = 'held', detail = ${detail}, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The uncertain director could not be saved.",
            String(error),
          ),
        ),
      );
      return { ...row, status: "held", detail, updatedAt };
    }
    return row;
  });

  const statusFromRow = Effect.fn("WorkflowDirectorService.statusFromRow")(function* (
    original: DirectorRow,
  ) {
    const row = yield* reconcileDirector(original);
    const admissionRows = yield* admissions(row.directorId);
    const admissionCount = new Set(admissionRows.map((admission) => admission.slotTicketNumber))
      .size;
    const shell = yield* projection
      .getThreadShellById(ThreadId.make(row.threadId))
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director thread state could not be read.",
            String(error),
          ),
        ),
      );
    const actions: Array<WorkflowDirectorStatus["actions"][number]> = [];
    if (Option.isSome(shell)) actions.push("open");
    if (
      (row.status === "held" || row.status === "preparing-worktree") &&
      row.initialTurnDisposition === "not-attempted"
    ) {
      actions.push("retry");
    }
    if (
      row.status === "active" &&
      Option.isSome(shell) &&
      (shell.value.latestTurn?.state === "interrupted" ||
        shell.value.latestTurn?.state === "error") &&
      shell.value.session?.activeTurnId == null &&
      shell.value.session?.status !== "running" &&
      shell.value.session?.status !== "starting"
    ) {
      actions.push("resume");
    }
    const status =
      admissionCount >= ADMISSION_LIMIT && row.status === "active" ? "waiting" : row.status;
    const observation = [
      row.directorId,
      row.updatedAt,
      status,
      Option.isSome(shell) ? (shell.value.latestTurn?.turnId ?? "no-turn") : "no-thread",
      admissionCount,
    ].join("|");
    return {
      directorId: row.directorId,
      batchId: row.batchId,
      environmentId: EnvironmentId.make(row.environmentId),
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowDirectorStatus["repository"],
      rootNumber: row.rootNumber,
      capabilityNumber: row.capabilityNumber,
      threadId: ThreadId.make(row.threadId),
      worktreePath: row.worktreePath,
      worktreeBranch: row.worktreeBranch,
      status: status as WorkflowDirectorStatus["status"],
      requestedProfile: {
        instanceId: row.requestedInstanceId,
        model: row.requestedModel,
        effort: row.requestedEffort,
      },
      observedProfile: {
        model: row.observedModel,
        effort: row.observedEffort,
        match: row.observedMatch as WorkflowDirectorStatus["observedProfile"]["match"],
      },
      admissionCount,
      admissionLimit: ADMISSION_LIMIT,
      observation,
      actions,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      message:
        status === "waiting"
          ? "This director has admitted ten delivery slices. Finish or settle admitted work, then wait for a successor."
          : status === "held"
            ? (row.detail ?? "Director startup is held for recovery.")
            : status === "preparing-worktree"
              ? "The recorded capability worktree needs recovery before the director can start."
              : status === "submitting"
                ? "The director submission is being reconciled."
                : "The capability director is active in its preserved worktree.",
    } satisfies WorkflowDirectorStatus;
  });

  const statusUnlocked = Effect.fn("WorkflowDirectorService.status")(function* (
    input: WorkflowDirectorStatusInput,
  ) {
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        directorError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const row = yield* loadDirectorByCapability(input, environmentId);
    if (!row) {
      return yield* directorError(
        "director-not-found",
        "No capability director is linked in this environment.",
      );
    }
    return yield* statusFromRow(row);
  });

  const continueInitialDirector = Effect.fn("WorkflowDirectorService.continueInitialDirector")(
    function* (
      row: DirectorRow,
      input: WorkflowDirectorStartInput,
      projectWorkspaceRoot: string,
      dispatch: Dispatch,
    ) {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      const worktree = yield* ensureWorktree(row, projectWorkspaceRoot).pipe(Effect.result);
      if (worktree._tag === "Failure") {
        const detail = worktree.failure.message;
        yield* sql`
        UPDATE workflow_directors SET status = 'held', detail = ${detail}, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The held worktree state could not be saved.",
              String(error),
            ),
          ),
        );
        return {
          disposition: "held",
          director: yield* statusFromRow({ ...row, status: "held", detail, updatedAt }),
        } satisfies WorkflowDirectorStartResult;
      }
      const preparedResult = yield* prepareCapability(input, row.worktreePath).pipe(Effect.result);
      if (preparedResult._tag === "Failure") {
        const detail = preparedResult.failure.message;
        yield* sql`
        UPDATE workflow_directors SET status = 'held', detail = ${detail}, updated_at = ${updatedAt}
        WHERE director_id = ${row.directorId}
      `.pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The held director state could not be saved.",
              String(error),
            ),
          ),
        );
        return {
          disposition: "held",
          director: yield* statusFromRow({ ...row, status: "held", detail, updatedAt }),
        } satisfies WorkflowDirectorStartResult;
      }
      const prepared = preparedResult.success;
      const sources = yield* sourceContext(input, prepared.capability);
      const instructions = workflowDirectorInstructions({
        capability: prepared.capability,
        tickets: prepared.tickets,
        approvalRecords: [prepared.specificationApproval, prepared.breakdownApproval],
        sourceContext: sources,
        implementSkillPath: prepared.skills[0]!.path,
        reviewSkillPath: prepared.skills[1]!.path,
      });
      yield* sql`
      UPDATE workflow_directors SET status = 'submitting', initial_turn_disposition = 'unknown',
        detail = NULL, updated_at = ${updatedAt} WHERE director_id = ${row.directorId}
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director submission intent could not be saved.",
            String(error),
          ),
        ),
      );
      const submitting = {
        ...row,
        status: "submitting",
        initialTurnDisposition: "unknown",
        detail: null,
        updatedAt,
      };
      const command = {
        type: "thread.turn.start" as const,
        commandId: CommandId.make(row.commandId),
        threadId: ThreadId.make(row.threadId),
        message: {
          messageId: MessageId.make(row.messageId),
          role: "user" as const,
          text: instructions,
          attachments: [],
        },
        modelSelection: input.modelSelection,
        runtimeMode: "approval-required" as const,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        bootstrap: {
          createThread: {
            projectId: ProjectId.make(row.projectId),
            title: `Director: ${prepared.capability.title}`,
            modelSelection: input.modelSelection,
            runtimeMode: "approval-required" as const,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: row.worktreeBranch,
            worktreePath: row.worktreePath,
            createdAt: updatedAt,
          },
          runSetupScript: true,
        },
        createdAt: updatedAt,
      } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
      const dispatched = yield* dispatch(command).pipe(Effect.result);
      if (dispatched._tag === "Failure") {
        const receipt = yield* receipts
          .getByCommandId({ commandId: command.commandId })
          .pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "Director command evidence could not be read.",
                String(error),
              ),
            ),
          );
        const accepted = Option.isSome(receipt) && receipt.value.status === "accepted";
        const notAccepted = dispatched.failure.bootstrapTurnDisposition === "not-accepted";
        const detail = accepted
          ? null
          : notAccepted
            ? "The director turn was not accepted. Its worktree and history were retained."
            : "The director submission is uncertain. Inspect the linked thread; it will not be sent again automatically.";
        yield* sql`
        UPDATE workflow_directors SET status = ${accepted ? "active" : "held"},
          sequence = ${accepted ? receipt.value.resultSequence : null},
          initial_turn_disposition = ${accepted ? "accepted" : notAccepted ? "not-accepted" : "unknown"},
          detail = ${detail}, updated_at = ${updatedAt} WHERE director_id = ${row.directorId}
      `.pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "The director outcome could not be saved.",
              String(error),
            ),
          ),
        );
        return {
          disposition: accepted ? "started" : "held",
          director: yield* statusFromRow({
            ...submitting,
            status: accepted ? "active" : "held",
            sequence: accepted ? receipt.value.resultSequence : null,
            initialTurnDisposition: accepted
              ? "accepted"
              : notAccepted
                ? "not-accepted"
                : "unknown",
            detail,
          }),
        } satisfies WorkflowDirectorStartResult;
      }
      yield* sql`
      UPDATE workflow_directors SET status = 'active', sequence = ${dispatched.success.sequence},
        initial_turn_disposition = 'accepted', detail = NULL, updated_at = ${updatedAt}
      WHERE director_id = ${row.directorId}
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The accepted director could not be saved.",
            String(error),
          ),
        ),
      );
      return {
        disposition: "started",
        director: yield* statusFromRow({
          ...submitting,
          status: "active",
          sequence: dispatched.success.sequence,
          initialTurnDisposition: "accepted",
        }),
      } satisfies WorkflowDirectorStartResult;
    },
  );

  const startUnlocked = Effect.fn("WorkflowDirectorService.start")(function* (
    input: WorkflowDirectorStartInput,
    dispatch: Dispatch,
  ) {
    const environmentId = yield* environment.getEnvironmentId.pipe(
      Effect.mapError((error) =>
        directorError(
          "workspace-unavailable",
          "The environment identity could not be read.",
          String(error),
        ),
      ),
    );
    const existing = yield* loadDirectorByCapability(input, environmentId);
    if (existing) {
      if (
        (existing.status === "held" || existing.status === "preparing-worktree") &&
        existing.initialTurnDisposition === "not-attempted"
      ) {
        if (existing.requestedInstanceId !== input.modelSelection.instanceId) {
          return yield* directorError(
            "provider-unavailable",
            "Retry with the Codex provider recorded for this director.",
          );
        }
        const project = yield* selectedProject(ProjectId.make(existing.projectId));
        const retryInput = {
          ...input,
          projectId: ProjectId.make(existing.projectId),
          repository: existing.repository as WorkflowDirectorStartInput["repository"],
          rootNumber: existing.rootNumber,
          capabilityNumber: existing.capabilityNumber,
        };
        yield* prepareCapability(retryInput, project.workspaceRoot);
        return yield* continueInitialDirector(
          existing,
          retryInput,
          project.workspaceRoot,
          dispatch,
        );
      }
      const director = yield* statusFromRow(existing);
      return {
        disposition: director.status === "held" ? "held" : "existing",
        director,
      } satisfies WorkflowDirectorStartResult;
    }
    const project = yield* selectedProject(input.projectId);
    yield* prepareCapability(input, project.workspaceRoot);
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const directorId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const batchId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const threadId = ThreadId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const worktreeBranch = `t3code/workflow-${input.capabilityNumber}`;
    const repositoryDirectory = input.repository.replace(/[^a-z0-9._-]+/giu, "-");
    const worktreePath = path.join(
      config.worktreesDir,
      repositoryDirectory,
      `capability-${input.capabilityNumber}`,
    );
    yield* sql`
      INSERT INTO workflow_directors (
        director_id, batch_id, environment_id, project_id, repository, root_number,
        capability_number, thread_id, command_id, message_id, worktree_path, worktree_branch,
        status, requested_model, requested_instance_id, requested_effort, observed_match,
        initial_turn_disposition,
        created_at, updated_at
      ) VALUES (
        ${directorId}, ${batchId}, ${environmentId}, ${input.projectId}, ${input.repository}, ${input.rootNumber},
        ${input.capabilityNumber}, ${threadId}, ${commandId}, ${messageId}, ${worktreePath}, ${worktreeBranch},
        'preparing-worktree', ${DIRECTOR_MODEL}, ${input.modelSelection.instanceId},
        ${DIRECTOR_EFFORT}, 'unknown', 'not-attempted',
        ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The director and worktree intent could not be saved.",
          String(error),
        ),
      ),
    );
    const row = (yield* loadDirectorByCapability(input, environmentId))!;
    return yield* continueInitialDirector(row, input, project.workspaceRoot, dispatch);
  });

  const resumeUnlocked = Effect.fn("WorkflowDirectorService.resume")(function* (
    input: WorkflowDirectorResumeInput,
    dispatch: Dispatch,
  ) {
    const row = yield* loadDirectorById(input.directorId);
    if (
      row.projectId !== input.projectId ||
      row.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase() ||
      row.capabilityNumber !== input.capabilityNumber
    ) {
      return yield* directorError(
        "director-not-found",
        "The selected capability director changed.",
      );
    }
    const shell = yield* projection
      .getThreadShellById(ThreadId.make(row.threadId))
      .pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director thread state could not be read.",
            String(error),
          ),
        ),
      );
    const latestTurn = Option.isSome(shell) ? shell.value.latestTurn : undefined;
    const observedSourceTurnId = input.observation.split("|")[3];
    if (
      observedSourceTurnId &&
      observedSourceTurnId !== "no-turn" &&
      observedSourceTurnId !== "no-thread"
    ) {
      const previous = yield* sql<{
        readonly commandId: string;
        readonly status: string;
        readonly sequence: number | null;
      }>`
        SELECT command_id AS "commandId", status, sequence FROM workflow_director_resumes
        WHERE director_id = ${row.directorId} AND source_turn_id = ${observedSourceTurnId} LIMIT 1
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "Director resume history could not be read.",
            String(error),
          ),
        ),
      );
      if (previous[0]) {
        const receipt = yield* receipts
          .getByCommandId({ commandId: CommandId.make(previous[0].commandId) })
          .pipe(
            Effect.mapError((error) =>
              directorError(
                "persistence-failed",
                "Director resume evidence could not be read.",
                String(error),
              ),
            ),
          );
        if (
          (Option.isSome(receipt) && receipt.value.status === "accepted") ||
          (previous[0].status === "submitted" && previous[0].sequence !== null)
        ) {
          return yield* statusFromRow(row);
        }
        return yield* directorError(
          "dispatch-failed",
          "The director resume is uncertain and will not be sent again automatically.",
        );
      }
    }
    const current = yield* statusFromRow(row);
    if (
      current.observation !== input.observation ||
      current.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase() ||
      current.capabilityNumber !== input.capabilityNumber
    ) {
      return yield* directorError(
        "not-ready",
        "The director changed. Refresh its status before resuming.",
      );
    }
    if (!current.actions.includes("resume")) {
      return yield* directorError(
        "not-ready",
        "Resume is available only for an interrupted director with no active turn.",
      );
    }
    const project = yield* selectedProject(ProjectId.make(row.projectId));
    yield* prepareCapability(
      {
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowDirectorStartInput["repository"],
        capabilityNumber: row.capabilityNumber,
        modelSelection: input.modelSelection,
      },
      row.worktreePath,
      { allowClaimed: true },
    );
    yield* ensureWorktree(row, project.workspaceRoot);
    if (Option.isNone(shell) || !latestTurn) {
      return yield* directorError("not-ready", "The interrupted director turn is unavailable.");
    }
    const sourceTurnId = latestTurn.turnId;
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const resumeId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const commandId = CommandId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const messageId = MessageId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    yield* sql`
      INSERT INTO workflow_director_resumes (
        resume_id, director_id, source_turn_id, command_id, message_id, status, created_at, updated_at
      ) VALUES (${resumeId}, ${row.directorId}, ${sourceTurnId}, ${commandId}, ${messageId}, 'submitting', ${createdAt}, ${createdAt})
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The director resume intent could not be saved.",
          String(error),
        ),
      ),
    );
    const command = {
      type: "thread.turn.start" as const,
      commandId,
      threadId: ThreadId.make(row.threadId),
      message: {
        messageId,
        role: "user" as const,
        text: `Resume the interrupted director for ${row.repository}#${row.capabilityNumber} in the preserved capability worktree. Re-read live readiness and use the host admission boundary before any new delegation.`,
        attachments: [],
      },
      modelSelection: input.modelSelection,
      runtimeMode: "approval-required" as const,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      createdAt,
    } satisfies Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
    const dispatched = yield* dispatch(command).pipe(Effect.result);
    if (dispatched._tag === "Failure") {
      const receipt = yield* receipts
        .getByCommandId({ commandId })
        .pipe(
          Effect.mapError((error) =>
            directorError(
              "persistence-failed",
              "Director resume evidence could not be read.",
              String(error),
            ),
          ),
        );
      const accepted = Option.isSome(receipt) && receipt.value.status === "accepted";
      yield* sql`
        UPDATE workflow_director_resumes SET status = ${accepted ? "submitted" : "held"},
          sequence = ${accepted ? receipt.value.resultSequence : null},
          detail = ${accepted ? null : "The director resume is uncertain and will not be sent again automatically."},
          updated_at = ${createdAt} WHERE resume_id = ${resumeId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The director resume outcome could not be saved.",
            String(error),
          ),
        ),
      );
      if (!accepted)
        return yield* directorError(
          "dispatch-failed",
          "The director resume is uncertain and will not be sent again automatically.",
        );
    } else {
      yield* sql`
        UPDATE workflow_director_resumes SET status = 'submitted', sequence = ${dispatched.success.sequence},
          detail = NULL, updated_at = ${createdAt} WHERE resume_id = ${resumeId}
      `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The accepted director resume could not be saved.",
            String(error),
          ),
        ),
      );
    }
    return yield* statusFromRow(row);
  });

  const executeGitHub = Effect.fn("WorkflowDirectorService.executeGitHub")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
  ) {
    return yield* github
      .execute({ cwd, args, maxOutputBytes: 100_000 })
      .pipe(
        Effect.mapError((error) =>
          directorError("claim-failed", "GitHub ownership could not be verified.", error.message),
        ),
      );
  });

  const reconcileExistingClaim = Effect.fn("WorkflowDirectorService.reconcileExistingClaim")(
    function* (row: DirectorRow, ticketNumber: number, existing: AdmissionRow) {
      let claimLogin = existing.claimLogin;
      let claimStatus: WorkflowDirectorAdmission["claimStatus"] = "uncertain";
      const identity = yield* executeGitHub(row.worktreePath, [
        "api",
        "user",
        "--jq",
        ".login",
      ]).pipe(Effect.result);
      if (identity._tag === "Success" && identity.success.stdout.trim()) {
        claimLogin = identity.success.stdout.trim();
        const assignees = yield* executeGitHub(row.worktreePath, [
          "issue",
          "view",
          String(ticketNumber),
          "--repo",
          row.repository,
          "--json",
          "assignees",
          "--jq",
          ".assignees[].login",
        ]).pipe(Effect.result);
        if (assignees._tag === "Success") {
          const current = assignees.success.stdout
            .split("\n")
            .map((value) => value.trim())
            .filter(Boolean);
          if (current.length === 0) {
            const claimed = yield* executeGitHub(row.worktreePath, [
              "issue",
              "edit",
              String(ticketNumber),
              "--repo",
              row.repository,
              "--add-assignee",
              claimLogin,
            ]).pipe(Effect.result);
            claimStatus = claimed._tag === "Success" ? "confirmed" : "uncertain";
          } else if (
            current.length === 1 &&
            current[0] === claimLogin &&
            existing.claimLogin === claimLogin
          ) {
            // A persisted local claim intent makes this observation safe to reconcile.
            claimStatus = "confirmed";
          } else {
            claimStatus = "conflict";
          }
        }
      }
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`
      UPDATE workflow_director_admissions SET claim_login = ${claimLogin}, claim_status = ${claimStatus},
        updated_at = ${updatedAt} WHERE admission_id = ${existing.admissionId}
    `.pipe(
        Effect.mapError((error) =>
          directorError(
            "persistence-failed",
            "The ticket ownership result could not be saved.",
            String(error),
          ),
        ),
      );
      return { ...existing, claimLogin, claimStatus, updatedAt };
    },
  );

  const admitUnlocked = Effect.fn("WorkflowDirectorService.admit")(function* (
    input: WorkflowDirectorAdmissionInput,
  ) {
    const row = yield* loadDirectorById(input.directorId);
    if (
      row.projectId !== input.projectId ||
      row.repository.toLocaleLowerCase() !== input.repository.toLocaleLowerCase()
    ) {
      return yield* directorError(
        "director-not-found",
        "The selected director does not own this project and repository.",
      );
    }
    const existingRows = yield* admissions(row.directorId);
    const existing = existingRows.find(
      (admission) =>
        admission.repository.toLocaleLowerCase() === input.repository.toLocaleLowerCase() &&
        admission.ticketNumber === input.ticketNumber,
    );
    const capabilityState = yield* prepareCapability(
      {
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowDirectorStartInput["repository"],
        capabilityNumber: row.capabilityNumber,
        modelSelection: {
          instanceId:
            row.requestedInstanceId as WorkflowDirectorStartInput["modelSelection"]["instanceId"],
          model: row.requestedModel,
          options: [{ id: "reasoningEffort", value: row.requestedEffort }],
        },
      },
      row.worktreePath,
      { allowClaimed: true },
    );
    const ticket = yield* workflow.issueDetail({
      projectId: ProjectId.make(row.projectId),
      repository: row.repository as WorkflowIssueSummary["repository"],
      number: input.ticketNumber,
    });
    const publishedTicket = capabilityState.tickets.find(
      (candidate) => candidate.id === ticket.id && candidate.number === ticket.number,
    );
    if (ticket.kind === "ticket" && !publishedTicket) {
      return yield* directorError(
        "not-ready",
        "This ticket is not a published delivery slice of the selected capability.",
      );
    }
    if (!existing && ticket.readiness?.status !== "ready") {
      return yield* directorError(
        "not-ready",
        "This delivery unit is not ready to admit.",
        ticket.readiness?.reasons.map((reason) => reason.message).join(" "),
      );
    }
    let slotTicketNumber: number;
    if (ticket.kind === "ticket") {
      if (input.parentTicketNumber && input.parentTicketNumber !== ticket.number) {
        return yield* directorError(
          "not-ready",
          "The supplied parent ticket does not match this delivery slice.",
        );
      }
      slotTicketNumber = ticket.number;
    } else if (ticket.kind === "task" && input.parentTicketNumber) {
      const located = yield* workflow.locate({
        projectId: ProjectId.make(row.projectId),
        repository: row.repository as WorkflowIssueSummary["repository"],
        id: ticket.id,
        number: ticket.number,
      });
      const parent = located.ancestry.find(
        (ancestor) =>
          ancestor.number === input.parentTicketNumber &&
          ancestor.kind === "ticket" &&
          capabilityState.tickets.some(
            (candidate) => candidate.id === ancestor.id && candidate.number === ancestor.number,
          ),
      );
      if (!located.ancestryComplete || !parent) {
        return yield* directorError(
          "not-ready",
          "The nested task does not belong to the supplied delivery slice.",
        );
      }
      slotTicketNumber = parent.number;
    } else {
      return yield* directorError(
        "not-ready",
        "Only delivery tickets, or their verified nested tasks, can be admitted.",
      );
    }
    if (existing) {
      const reconciled = yield* reconcileExistingClaim(row, ticket.number, existing);
      return {
        disposition: "existing",
        admission: admissionFromRow(reconciled),
        admissionCount: new Set(existingRows.map((admission) => admission.slotTicketNumber)).size,
        admissionLimit: ADMISSION_LIMIT,
        directorStatus: (yield* statusFromRow(row)).status,
        message: "This delivery unit already uses its existing batch slot.",
      } satisfies WorkflowDirectorAdmissionResult;
    }
    const slots = new Set(existingRows.map((admission) => admission.slotTicketNumber));
    if (!slots.has(slotTicketNumber) && slots.size >= ADMISSION_LIMIT) {
      return {
        disposition: "limit-reached",
        admission: null,
        admissionCount: slots.size,
        admissionLimit: ADMISSION_LIMIT,
        directorStatus: "waiting",
        message:
          "This director has admitted ten delivery slices. Finish or settle admitted work, then wait for a successor.",
      } satisfies WorkflowDirectorAdmissionResult;
    }
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    const admissionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    yield* sql`
      INSERT INTO workflow_director_admissions (
        admission_id, director_id, batch_id, repository, ticket_id, ticket_number,
        slot_ticket_number, purpose, ownership, claim_status, created_at, updated_at
      ) VALUES (
        ${admissionId}, ${row.directorId}, ${row.batchId}, ${row.repository}, ${ticket.id}, ${ticket.number},
        ${slotTicketNumber}, ${input.purpose}, ${input.ownership}, 'pending', ${createdAt}, ${createdAt}
      )
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The ticket admission could not be saved.",
          String(error),
        ),
      ),
    );
    let claimLogin: string | null = null;
    let claimStatus: WorkflowDirectorAdmission["claimStatus"] = "pending";
    const identity = yield* executeGitHub(row.worktreePath, ["api", "user", "--jq", ".login"]).pipe(
      Effect.result,
    );
    if (identity._tag === "Success" && identity.success.stdout.trim()) {
      claimLogin = identity.success.stdout.trim();
      const assignees = yield* executeGitHub(row.worktreePath, [
        "issue",
        "view",
        String(ticket.number),
        "--repo",
        row.repository,
        "--json",
        "assignees",
        "--jq",
        ".assignees[].login",
      ]).pipe(Effect.result);
      if (assignees._tag === "Failure") {
        claimStatus = "uncertain";
      } else {
        const current = assignees.success.stdout
          .split("\n")
          .map((value) => value.trim())
          .filter(Boolean);
        if (current.length === 0) {
          const claimed = yield* executeGitHub(row.worktreePath, [
            "issue",
            "edit",
            String(ticket.number),
            "--repo",
            row.repository,
            "--add-assignee",
            claimLogin,
          ]).pipe(Effect.result);
          claimStatus = claimed._tag === "Success" ? "confirmed" : "uncertain";
        } else {
          claimStatus = "conflict";
        }
      }
    } else {
      claimStatus = "uncertain";
    }
    yield* sql`
      UPDATE workflow_director_admissions SET claim_login = ${claimLogin}, claim_status = ${claimStatus},
        updated_at = ${createdAt} WHERE admission_id = ${admissionId}
    `.pipe(
      Effect.mapError((error) =>
        directorError(
          "persistence-failed",
          "The ticket ownership result could not be saved.",
          String(error),
        ),
      ),
    );
    const admission = admissionFromRow({
      admissionId,
      directorId: row.directorId,
      batchId: row.batchId,
      repository: row.repository,
      ticketNumber: ticket.number,
      slotTicketNumber,
      purpose: input.purpose,
      ownership: input.ownership,
      claimLogin,
      claimStatus,
      createdAt,
      updatedAt: createdAt,
    });
    return {
      disposition: "admitted",
      admission,
      admissionCount: new Set([...slots, slotTicketNumber]).size,
      admissionLimit: ADMISSION_LIMIT,
      directorStatus:
        new Set([...slots, slotTicketNumber]).size >= ADMISSION_LIMIT ? "waiting" : "active",
      message:
        claimStatus === "confirmed"
          ? "Admission and explicit ownership were persisted. Delegation may proceed."
          : "Admission was persisted and consumes its slot, but ownership is not confirmed. Resolve the GitHub assignment, then repeat this admission to recheck it before delegation.",
    } satisfies WorkflowDirectorAdmissionResult;
  });

  return WorkflowDirectorService.of({
    start: (input, dispatch) => lock.withPermits(1)(startUnlocked(input, dispatch)),
    status: (input) => lock.withPermits(1)(statusUnlocked(input)),
    resume: (input, dispatch) => lock.withPermits(1)(resumeUnlocked(input, dispatch)),
    admit: (input) => lock.withPermits(1)(admitUnlocked(input)),
  });
});

export const layer = Layer.effect(WorkflowDirectorService, make).pipe(
  Layer.provide(OrchestrationCommandReceiptRepositoryLive),
);
