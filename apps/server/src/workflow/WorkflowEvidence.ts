import type {
  WorkflowEvidence,
  WorkflowEvidenceRecord,
  WorkflowFrontier,
  WorkflowIssueKind,
  WorkflowIssueState,
  WorkflowIssueStateReason,
  WorkflowManualCondition,
  WorkflowReadiness,
  WorkflowReadinessReason,
} from "@t3tools/contracts";

export interface WorkflowEvidenceComment {
  readonly id: string;
  readonly url: string;
  readonly body: string;
  readonly createdAt: string;
  readonly author: string | null;
  readonly authorAssociation: string;
}

export interface WorkflowEvidenceIssue {
  readonly id: string;
  readonly url: string;
  readonly number: number;
  readonly title: string;
  readonly kind: WorkflowIssueKind;
  readonly state: WorkflowIssueState;
  readonly stateReason: WorkflowIssueStateReason | null;
  readonly labels: ReadonlyArray<string>;
  readonly assignees: ReadonlyArray<string>;
  readonly body: string;
  readonly comments: ReadonlyArray<WorkflowEvidenceComment>;
  readonly reopenedAt: ReadonlyArray<string>;
  readonly lastEditedAt?: string | null;
}

export interface WorkflowBlockerEvidence {
  readonly id: string;
  readonly number: number;
  readonly title: string;
  readonly url: string;
  readonly readiness: WorkflowReadiness;
}

const MARKERS = {
  approval: "<!-- t3-workflow:v1 approval -->",
  resolution: "<!-- t3-workflow:v1 resolution -->",
  reassessment: "<!-- t3-workflow:v1 reassessment -->",
} as const;

function field(body: string, name: string): string | undefined {
  return new RegExp(`^${name}:\\s*(.+)$`, "imu").exec(body)?.[1]?.trim();
}

function section(body: string, name: string): string | undefined {
  return new RegExp(`(?:^|\\n)#{2,6}\\s+${name}\\s*\\n([\\s\\S]*?)(?=\\n#{1,6}\\s|$)`, "iu")
    .exec(body)?.[1]
    ?.trim();
}

function approvalContent(body: string): string | undefined {
  return /(?:^|\n)#{2,6}\s+Approved content\s*\n([\s\S]+)$/iu.exec(body)?.[1]?.trim();
}

function recordKind(body: string): keyof typeof MARKERS | null {
  for (const [kind, marker] of Object.entries(MARKERS)) {
    if (body.includes(marker)) return kind as keyof typeof MARKERS;
  }
  return null;
}

function normalizeProse(value: string): string {
  return value
    .replace(/\[(?: |x|X)\]/g, "[ ]")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function approvedSlice(content: string, issue: WorkflowEvidenceIssue): string | undefined {
  const approvedId = /Approved slice:\s*\*\*(T\d+)\*\*/iu.exec(issue.body)?.[1];
  const details = [
    ...content.matchAll(
      /<details>\s*<summary>(T\d+)\s+[—-]\s+([^<]+)<\/summary>([\s\S]*?)<\/details>/giu,
    ),
  ];
  const match = approvedId
    ? details.find((candidate) => candidate[1]?.toUpperCase() === approvedId.toUpperCase())
    : details.find((candidate) => candidate[2]?.trim() === issue.title.trim());
  return match?.[3]?.trim();
}

function prerequisiteIds(body: string): ReadonlyArray<string> {
  const blockedBy = section(body, "Blocked by");
  if (!blockedBy) return [];
  if (/^none\b/iu.test(blockedBy)) return [];
  return [
    ...new Set([...blockedBy.matchAll(/\bT\d+\b/giu)].map((match) => match[0].toUpperCase())),
  ];
}

function declaredPrerequisites(body: string) {
  const blockedBy = section(body, "Blocked by") ?? "";
  return [
    ...blockedBy.matchAll(/\[[^\]]+\]\((https:\/\/github\.com\/[^/]+\/[^/]+\/issues\/(\d+))\)/giu),
  ].map((match) => ({ url: match[1]!, number: Number(match[2]) }));
}

function prerequisiteConditions(body: string): ReadonlyArray<string> {
  const conditions: string[] = [];
  let heading = "";
  for (const rawLine of body.split("\n")) {
    const headingMatch = /^#{1,6}\s+(.+)$/u.exec(rawLine);
    if (headingMatch) {
      heading = headingMatch[1]?.trim().toLowerCase() ?? "";
      continue;
    }
    const line = rawLine.replace(/^\s*[-*]\s+/u, "").trim();
    if (!line) continue;
    const inBlockedBy = /^blocked by$/iu.test(heading);
    const inPrerequisiteSection =
      /\b(?:manual|outside|additional|human|resource)?\s*(?:prerequisites?|conditions?)\b/iu.test(
        heading,
      );
    const explicitlyNamed = /^(?:prerequisite|requires?):\s+/iu.test(line);
    if (!inBlockedBy && !inPrerequisiteSection && !explicitlyNamed) continue;
    const description = line.replace(/^(?:prerequisite|requires?):\s+/iu, "").trim();
    if (!description || /^(?:none|n\/a|not applicable)\.?$/iu.test(description)) continue;
    if (inBlockedBy) {
      const unsupported = description
        .replace(/\[[^\]]+\]\(https:\/\/github\.com\/[^/]+\/[^/]+\/issues\/\d+\)/giu, "")
        .replace(/\bT\d+\b/giu, "")
        .replace(/[^\p{L}\p{N}]+/gu, "");
      if (!unsupported) continue;
    }
    conditions.push(description);
  }
  return [...new Set(conditions)];
}

function recordDeclarationLines(body: string): ReadonlyArray<string> {
  const lines: string[] = [];
  let fence: { character: string; length: number } | null = null;
  for (const line of body.split("\n")) {
    const fenceMatch = /^\s*(`{3,}|~{3,})/u.exec(line)?.[1];
    if (fenceMatch) {
      const character = fenceMatch[0]!;
      if (!fence) fence = { character, length: fenceMatch.length };
      else if (fence.character === character && fenceMatch.length >= fence.length) fence = null;
      continue;
    }
    if (fence || /^(?:\s*>| {4}|\t|\s*["'])/u.test(line)) continue;
    lines.push(line);
  }
  return lines;
}

function ticketScope(body: string): string | undefined {
  const what = section(body, "What to build");
  const acceptance = section(body, "Acceptance criteria");
  if (!what || !acceptance) return undefined;
  return normalizeProse(
    [
      what,
      acceptance,
      `Blocked by: ${prerequisiteIds(body).join(",") || "none"}`,
      `Manual conditions: ${prerequisiteConditions(body).join(" | ") || "none"}`,
    ].join("\n\n"),
  );
}

function approvalScope(
  approvalKind: string | undefined,
  approvedContent: string | undefined,
  issue: WorkflowEvidenceIssue,
): WorkflowEvidenceRecord["scope"] {
  if (!approvedContent) return "changed";
  if (approvalKind === "specification" && issue.kind === "capability") {
    return normalizeProse(approvedContent) === normalizeProse(issue.body) ? "current" : "changed";
  }
  if (approvalKind === "ticket-breakdown" && issue.kind === "ticket") {
    const slice = approvedSlice(approvedContent, issue);
    const approved = slice ? ticketScope(slice) : undefined;
    const current = ticketScope(issue.body);
    return approved && current && approved === current ? "current" : "changed";
  }
  return "not-applicable";
}

function sourceAccess(
  source: string | undefined,
  availableComments: ReadonlyArray<WorkflowEvidenceComment>,
): WorkflowEvidenceRecord["sourceAccess"] {
  if (!source) return "unavailable";
  if (/\bunavailable\b|\binaccessible\b/iu.test(source)) return "unavailable";
  if (availableComments.some((comment) => source.includes(comment.url))) return "verified";
  return "reported";
}

function hasIdentifiableEvidence(evidence: string | undefined): boolean {
  if (!evidence) return false;
  return (
    /\[[^\]]+\]\(https?:\/\/[^\s)]+\)/u.test(evidence) ||
    /https?:\/\/[^\s)]+/u.test(evidence) ||
    /\b(?:thread|message|commit|artifact|file|path)\s*:?\s*`[^`]+`/iu.test(evidence)
  );
}

function recordScope(
  comment: WorkflowEvidenceComment,
  issue: WorkflowEvidenceIssue,
): WorkflowEvidenceRecord["scope"] {
  if (issue.reopenedAt.some((createdAt) => createdAt > comment.createdAt)) return "changed";
  if (issue.lastEditedAt && issue.lastEditedAt > comment.createdAt) return "unknown";
  return "current";
}

function parseRecord(
  comment: WorkflowEvidenceComment,
  issue: WorkflowEvidenceIssue,
  availableComments: ReadonlyArray<WorkflowEvidenceComment>,
): WorkflowEvidenceRecord | null {
  const kind = recordKind(comment.body);
  if (!kind) return null;
  const source = field(comment.body, kind === "reassessment" ? "Trigger" : "Source");
  const approvedContent = kind === "approval" ? approvalContent(comment.body) : undefined;
  const approvalKind = kind === "approval" ? field(comment.body, "Kind") : undefined;
  const approvedBy = kind === "approval" ? field(comment.body, "Approved by") : undefined;
  const approvedLogin = approvedBy?.replace(/\s*\([^)]*\)\s*$/u, "").trim();
  const authority =
    kind !== "approval"
      ? undefined
      : approvedLogin &&
          comment.author?.toLowerCase() === approvedLogin.toLowerCase() &&
          comment.authorAssociation === "OWNER"
        ? ("verified" as const)
        : approvedBy
          ? ("reported" as const)
          : ("unknown" as const);
  const outcome = kind === "approval" ? undefined : field(comment.body, "Outcome")?.toLowerCase();
  const summary =
    section(comment.body, kind === "reassessment" ? "Changes" : "Summary") ??
    (kind === "approval" ? `Approval: ${approvalKind ?? "unknown kind"}` : `${kind} record`);
  const evidence = kind === "approval" ? undefined : section(comment.body, "Evidence");
  const evidenceAccess =
    evidence && /\bunavailable\b|\binaccessible\b/iu.test(evidence)
      ? ("unavailable" as const)
      : sourceAccess(source ?? evidence, availableComments);
  const valid =
    kind === "approval"
      ? (approvalKind === "specification" || approvalKind === "ticket-breakdown") &&
        Boolean(approvedBy && source && approvedContent)
      : kind === "resolution"
        ? (outcome === "resolved" || outcome === "cancelled" || outcome === "out-of-scope") &&
          Boolean(section(comment.body, "Summary") && hasIdentifiableEvidence(evidence))
        : (outcome === "cleared" || outcome === "scope-change") &&
          Boolean(source && section(comment.body, "Changes") && hasIdentifiableEvidence(evidence));
  return {
    id: comment.id,
    url: comment.url,
    createdAt: comment.createdAt,
    kind,
    state: valid ? "current" : "invalid",
    sourceAccess: evidenceAccess,
    scope:
      kind === "approval"
        ? approvalScope(approvalKind, approvedContent, issue)
        : recordScope(comment, issue),
    summary,
    ...(approvalKind === "specification" || approvalKind === "ticket-breakdown"
      ? { approvalKind }
      : {}),
    ...(authority ? { authority } : {}),
    ...(approvedBy ? { approvedBy } : {}),
    ...(source ? { source } : {}),
    ...(approvedContent ? { approvedContent } : {}),
    ...(outcome === "resolved" ||
    outcome === "cancelled" ||
    outcome === "out-of-scope" ||
    outcome === "cleared" ||
    outcome === "scope-change"
      ? { outcome }
      : {}),
    ...(evidence ? { evidence } : {}),
  };
}

function markSuperseded(
  records: ReadonlyArray<WorkflowEvidenceRecord>,
  comments: ReadonlyArray<WorkflowEvidenceComment>,
) {
  const superseded = new Set<string>();
  const commentsById = new Map(comments.map((comment) => [comment.id, comment]));
  for (const supersedingRecord of records) {
    if (supersedingRecord.state !== "current") continue;
    const comment = commentsById.get(supersedingRecord.id);
    if (!comment) continue;
    const targets = new Set(
      recordDeclarationLines(comment.body).flatMap((line) => {
        const value =
          /^\s*Supersedes:\s*(.+)$/iu.exec(line)?.[1] ??
          /^\s*This record supersedes\s+(.+)$/iu.exec(line)?.[1];
        return value
          ? [...value.matchAll(/https?:\/\/[^\s)]+/gu)].map((match) =>
              match[0].replace(/[.,;:]+$/u, ""),
            )
          : [];
      }),
    );
    for (const target of records) {
      if (
        targets.has(target.url) &&
        target.createdAt < supersedingRecord.createdAt &&
        target.kind === supersedingRecord.kind &&
        (target.kind !== "approval" || target.approvalKind === supersedingRecord.approvalKind)
      ) {
        superseded.add(target.id);
      }
    }
  }
  return records.map((record) =>
    superseded.has(record.id) && record.state === "current"
      ? { ...record, state: "superseded" as const }
      : record,
  );
}

function manualConditions(
  issue: WorkflowEvidenceIssue,
  records: ReadonlyArray<WorkflowEvidenceRecord>,
): ReadonlyArray<WorkflowManualCondition> {
  const conditions = prerequisiteConditions(issue.body).map((description) => {
    const source = /\[[^\]]+\]\((https?:\/\/[^\s)]+)\)/u.exec(description)?.[1] ?? issue.url;
    return { description, source };
  });
  return conditions.map((condition) => {
    const satisfiedBy = records.find(
      (record) =>
        record.kind === "reassessment" &&
        record.state === "current" &&
        record.scope === "current" &&
        record.sourceAccess !== "unavailable" &&
        record.outcome === "cleared" &&
        record.source?.includes(condition.source) &&
        record.summary.toLocaleLowerCase().includes(condition.description.toLocaleLowerCase()),
    );
    return {
      ...condition,
      status: satisfiedBy ? ("satisfied" as const) : ("review-required" as const),
      ...(satisfiedBy?.evidence ? { evidence: satisfiedBy.evidence } : {}),
    };
  });
}

function reason(
  kind: WorkflowReadinessReason["kind"],
  message: string,
  source?: string,
): WorkflowReadinessReason {
  return { kind, message, ...(source ? { source } : {}) };
}

function closedReadiness(
  issue: WorkflowEvidenceIssue,
  records: ReadonlyArray<WorkflowEvidenceRecord>,
  historyComplete: boolean,
): WorkflowReadiness {
  const current = records.filter(
    (record) =>
      record.kind === "resolution" && record.state === "current" && record.scope === "current",
  );
  if (issue.labels.includes("workflow:superseded")) {
    return {
      status: "superseded",
      reasons: [
        reason(
          "superseded",
          "This work was superseded; it does not satisfy dependents.",
          issue.url,
        ),
      ],
    };
  }
  const outcome = current.findLast((record) => record.outcome)?.outcome;
  if (outcome === "out-of-scope") {
    return {
      status: "out-of-scope",
      reasons: [reason("out-of-scope", "This work was closed as out of scope.", issue.url)],
    };
  }
  if (issue.stateReason === "not_planned" || outcome === "cancelled") {
    return {
      status: "cancelled",
      reasons: [
        reason("cancelled", "This work was cancelled; it does not satisfy dependents.", issue.url),
      ],
    };
  }
  if (!historyComplete) {
    return {
      status: "closed-unverified",
      reasons: [
        reason(
          "missing-resolution",
          "Closed evidence history is truncated; open details to verify the current resolution.",
          issue.url,
        ),
      ],
    };
  }
  const resolution = current.findLast(
    (record) => record.outcome === "resolved" && record.sourceAccess !== "unavailable",
  );
  if (issue.stateReason === "completed" && resolution) {
    return {
      status: "resolved",
      reasons: [
        reason("resolution", "Completed with current resolution evidence.", resolution.url),
      ],
    };
  }
  const uncertainResolution = records.findLast(
    (record) =>
      record.kind === "resolution" &&
      record.state === "current" &&
      (record.scope === "unknown" || record.sourceAccess === "unavailable"),
  );
  return {
    status: "closed-unverified",
    reasons: [
      reason(
        "missing-resolution",
        uncertainResolution
          ? uncertainResolution.sourceAccess === "unavailable"
            ? "Resolution evidence is unavailable; completion remains unverified."
            : "Resolution predates the latest issue edit; current scope needs verification."
          : issue.reopenedAt.length > 0
            ? "Closed after reopening without new current resolution evidence."
            : "Closed as completed without current resolution evidence.",
        uncertainResolution?.url ?? issue.url,
      ),
    ],
  };
}

export function interpretWorkflowEvidence(input: {
  readonly issue: WorkflowEvidenceIssue;
  readonly approvalComments?: ReadonlyArray<WorkflowEvidenceComment>;
  readonly blockers?: ReadonlyArray<WorkflowBlockerEvidence>;
  readonly historyComplete?: boolean;
}): { readonly evidence: WorkflowEvidence; readonly readiness: WorkflowReadiness } {
  const allComments = [...(input.approvalComments ?? []), ...input.issue.comments];
  const inheritedApprovals = (input.approvalComments ?? []).filter(
    (comment) => recordKind(comment.body) === "approval",
  );
  const ownedComments = input.issue.comments.filter(
    (comment) => input.issue.kind === "capability" || recordKind(comment.body) !== "approval",
  );
  const recordComments = [...inheritedApprovals, ...ownedComments];
  const records = markSuperseded(
    recordComments
      .map((comment) => parseRecord(comment, input.issue, allComments))
      .filter((record): record is WorkflowEvidenceRecord => record !== null)
      .toSorted((left, right) => left.createdAt.localeCompare(right.createdAt)),
    recordComments,
  );
  const conditions = manualConditions(input.issue, records);
  const historyComplete = input.historyComplete ?? true;
  const evidence = { records, manualConditions: conditions, historyComplete };
  if (input.issue.state === "closed") {
    return { evidence, readiness: closedReadiness(input.issue, records, historyComplete) };
  }

  const reasons: WorkflowReadinessReason[] = [];
  const relevantApprovalKind =
    input.issue.kind === "capability"
      ? "specification"
      : input.issue.kind === "ticket"
        ? "ticket-breakdown"
        : null;
  if (relevantApprovalKind) {
    const approvals = records.filter(
      (record) =>
        record.kind === "approval" &&
        record.approvalKind === relevantApprovalKind &&
        record.state === "current",
    );
    if (approvals.some((record) => record.scope === "current" && record.authority === "verified")) {
      const approval = approvals.findLast(
        (record) => record.scope === "current" && record.authority === "verified",
      )!;
      reasons.push(
        reason(
          "approved-scope",
          `${relevantApprovalKind} approval covers current scope.`,
          approval.url,
        ),
      );
    } else if (approvals.some((record) => record.scope === "changed")) {
      const approval = approvals.findLast((record) => record.scope === "changed")!;
      reasons.push(
        reason("scope-changed", "Current scope differs from the approved snapshot.", approval.url),
      );
    } else if (approvals.some((record) => record.scope === "current")) {
      const approval = approvals.findLast((record) => record.scope === "current")!;
      reasons.push(
        reason(
          "reassessment",
          `The ${relevantApprovalKind} record matches current scope, but owner authority is not verified.`,
          approval.url,
        ),
      );
    } else {
      reasons.push(
        reason(
          "missing-approval",
          `No current ${relevantApprovalKind} approval covers this work.`,
          input.issue.url,
        ),
      );
    }
  }

  if (
    input.issue.labels.includes("workflow:needs-reassessment") ||
    records.some(
      (record) =>
        record.kind === "reassessment" &&
        record.state === "current" &&
        record.outcome === "scope-change",
    )
  ) {
    const reassessment = records.findLast(
      (record) => record.kind === "reassessment" && record.state === "current",
    );
    reasons.push(
      reason(
        "reassessment",
        "This work needs reassessment before it can proceed.",
        reassessment?.url ?? input.issue.url,
      ),
    );
  }

  for (const condition of conditions.filter((item) => item.status === "review-required")) {
    reasons.push(
      reason("manual-condition", `Review prerequisite: ${condition.description}`, condition.source),
    );
  }

  for (const blocker of input.blockers ?? []) {
    if (blocker.readiness.status === "resolved") continue;
    const blockerKind =
      blocker.readiness.status === "cancelled" || blocker.readiness.status === "out-of-scope"
        ? "cancelled-blocker"
        : blocker.readiness.status === "superseded"
          ? "superseded-blocker"
          : blocker.readiness.status === "closed-unverified"
            ? "unverified-blocker"
            : "open-blocker";
    reasons.push(
      reason(
        blockerKind,
        `Prerequisite #${blocker.number} ${blocker.title} is ${blocker.readiness.status}.`,
        blocker.url,
      ),
    );
  }

  const nativeBlockerUrls = new Set(
    (input.blockers ?? []).map((blocker) => blocker.url.toLowerCase()),
  );
  for (const declared of declaredPrerequisites(input.issue.body)) {
    if (nativeBlockerUrls.has(declared.url.toLowerCase())) continue;
    reasons.push(
      reason(
        "reassessment",
        `Declared prerequisite #${declared.number} is not present as a native blocker and needs review.`,
        declared.url,
      ),
    );
  }

  if (input.issue.assignees.length > 0) {
    reasons.push(
      reason("claimed", `Claimed by ${input.issue.assignees.join(", ")}.`, input.issue.url),
    );
  }

  const status = reasons.some(
    (item) =>
      item.kind === "reassessment" ||
      item.kind === "scope-changed" ||
      item.kind === "manual-condition",
  )
    ? "needs-review"
    : reasons.some((item) => item.kind === "missing-approval")
      ? "unapproved"
      : reasons.some((item) => item.kind.endsWith("blocker"))
        ? "blocked"
        : reasons.some((item) => item.kind === "claimed")
          ? "claimed"
          : "ready";
  return { evidence, readiness: { status, reasons } };
}

export function workflowFrontier(
  issues: ReadonlyArray<{ readonly id: string; readonly readiness: WorkflowReadiness }>,
  options?: { readonly hasRemainingFog?: boolean },
): WorkflowFrontier {
  const readyIssueIds = issues
    .filter((issue) => issue.readiness.status === "ready")
    .map((issue) => issue.id);
  if (readyIssueIds.length > 0) {
    return {
      status: "available",
      message: `${readyIssueIds.length} ${readyIssueIds.length === 1 ? "item can" : "items can"} proceed.`,
      readyIssueIds,
    };
  }
  if (options?.hasRemainingFog) {
    return {
      status: "empty-review",
      message: "No immediate work can proceed while this map still records remaining unknowns.",
      readyIssueIds: [],
    };
  }
  if (issues.length > 0 && issues.every((issue) => issue.readiness.status === "resolved")) {
    return { status: "complete", message: "All visible work is resolved.", readyIssueIds: [] };
  }
  const statuses = new Set(issues.map((issue) => issue.readiness.status));
  if (statuses.has("needs-review"))
    return {
      status: "empty-reassessment",
      message: "Nothing can proceed until review or reassessment is complete.",
      readyIssueIds: [],
    };
  if (statuses.has("blocked"))
    return {
      status: "empty-blocked",
      message: "Nothing can proceed because prerequisites are blocking the visible work.",
      readyIssueIds: [],
    };
  if (statuses.has("unapproved"))
    return {
      status: "empty-unapproved",
      message: "Nothing can proceed within current approved scope.",
      readyIssueIds: [],
    };
  if (statuses.has("claimed"))
    return {
      status: "empty-claimed",
      message: "All otherwise available work is already claimed.",
      readyIssueIds: [],
    };
  if (
    [...statuses].every((status) =>
      ["resolved", "closed-unverified", "cancelled", "out-of-scope", "superseded"].includes(status),
    )
  ) {
    return {
      status: "empty-inactive",
      message:
        "No work can proceed; remaining items are closed, cancelled, out of scope, or superseded.",
      readyIssueIds: [],
    };
  }
  if (issues.length === 0)
    return {
      status: "empty",
      message: "No immediate work is visible in this branch.",
      readyIssueIds: [],
    };
  return {
    status: "empty-review",
    message: "Nothing can proceed; inspect the recorded reasons.",
    readyIssueIds: [],
  };
}

export function workflowHasRemainingFog(body: string): boolean {
  const remaining = section(body, "(?:Remaining fog|Remaining unknowns)");
  return Boolean(remaining && !/^none\b/iu.test(remaining));
}
