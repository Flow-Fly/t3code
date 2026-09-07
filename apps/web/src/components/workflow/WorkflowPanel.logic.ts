import type {
  WorkflowIssueDetail,
  WorkflowIssueSummary,
  WorkflowRepository,
} from "@t3tools/contracts";

interface WorkflowBrowserEventSource {
  addEventListener(type: string, listener: EventListener): void;
  removeEventListener(type: string, listener: EventListener): void;
}

export function listenForWorkflowBrowserReturn(
  documentSource: WorkflowBrowserEventSource & { readonly visibilityState: string },
  windowSource: WorkflowBrowserEventSource,
  refresh: () => void,
): () => void {
  const refreshWhenVisible = () => {
    if (documentSource.visibilityState === "visible") refresh();
  };
  documentSource.addEventListener("visibilitychange", refreshWhenVisible);
  windowSource.addEventListener("focus", refreshWhenVisible);
  return () => {
    documentSource.removeEventListener("visibilitychange", refreshWhenVisible);
    windowSource.removeEventListener("focus", refreshWhenVisible);
  };
}

export function resolveWorkflowRepository(
  selected: string | null,
  repositories: ReadonlyArray<WorkflowRepository>,
): string | null {
  if (selected) {
    const selectedKey = selected.toLowerCase();
    const discovered = repositories.find(
      (repository) => repository.nameWithOwner.toLowerCase() === selectedKey,
    );
    return discovered?.nameWithOwner ?? selected;
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
  issue: Pick<WorkflowIssueSummary, "state" | "stateReason" | "readiness">,
): string {
  if (issue.readiness) {
    const labels = {
      ready: "Ready",
      claimed: "Claimed",
      blocked: "Blocked",
      "needs-review": "Needs review",
      unapproved: "Approval required",
      resolved: "Resolved",
      "closed-unverified": "Closed — unverified",
      cancelled: "Cancelled",
      "out-of-scope": "Out of scope",
      superseded: "Superseded",
    } as const;
    return labels[issue.readiness.status];
  }
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
  readonly relationship: "source" | "specification" | "supersession" | "reference";
}

export function workflowSourceLinks(body: string): ReadonlyArray<WorkflowSourceLink> {
  const links = new Map<string, WorkflowSourceLink>();
  let section = "";
  for (const line of body.split("\n")) {
    const heading = /^#{1,6}\s+(.+)$/.exec(line);
    if (heading) section = heading[1]?.trim().toLocaleLowerCase() ?? "";
    for (const match of line.matchAll(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g)) {
      const label = match[1]?.trim();
      const url = match[2];
      if (!label || !url || links.has(url)) continue;
      const relationship = /superseded by|supersession/.test(section)
        ? "supersession"
        : /specification|approved spec/.test(section)
          ? "specification"
          : /source map|source/.test(section)
            ? "source"
            : "reference";
      links.set(url, { label, url, relationship });
    }
  }
  return [...links.values()];
}
