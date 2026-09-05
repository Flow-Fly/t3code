import type {
  WorkflowIssueDetail,
  WorkflowIssueSummary,
  WorkflowRepository,
} from "@t3tools/contracts";

export function resolveWorkflowRepository(
  selected: string | null,
  repositories: ReadonlyArray<WorkflowRepository>,
): string | null {
  if (selected && repositories.some((repository) => repository.nameWithOwner === selected)) {
    return selected;
  }
  return repositories.length === 1 ? (repositories[0]?.nameWithOwner ?? null) : null;
}

export function filterWorkflowRoots(
  roots: ReadonlyArray<WorkflowIssueSummary>,
  query: string,
): ReadonlyArray<WorkflowIssueSummary> {
  const normalized = query.trim().toLocaleLowerCase();
  if (!normalized) return roots;
  const tokens = normalized.split(/\s+/);
  return roots.filter((root) => {
    const searchable = `${root.title} ${root.number} ${root.kind}`.toLocaleLowerCase();
    return tokens.every((token) =>
      token.startsWith("#") ? token === `#${root.number}` : searchable.includes(token),
    );
  });
}

export function workflowIssueStateLabel(
  issue: Pick<WorkflowIssueSummary, "state" | "stateReason">,
): string {
  if (issue.state === "open") return "Open";
  if (issue.stateReason === "not_planned") return "Closed — not planned";
  return "Closed — unverified";
}

export function workflowIssueBrief(detail: WorkflowIssueDetail): string | null {
  const preferredSection = /(?:^|\n)## (?:Summary|What to build)\s*\n([\s\S]*?)(?=\n## |$)/i.exec(
    detail.body,
  )?.[1];
  const paragraph = (preferredSection ?? detail.body)
    .split(/\n\s*\n/)
    .map((part) => part.trim())
    .find((part) => part.length > 0 && !part.startsWith("#"));
  return paragraph ?? null;
}

export interface WorkflowSourceLink {
  readonly label: string;
  readonly url: string;
}

export function workflowSourceLinks(body: string): ReadonlyArray<WorkflowSourceLink> {
  const links = new Map<string, WorkflowSourceLink>();
  for (const match of body.matchAll(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g)) {
    const label = match[1]?.trim();
    const url = match[2];
    if (!label || !url || links.has(url)) continue;
    links.set(url, { label, url });
  }
  return [...links.values()];
}
