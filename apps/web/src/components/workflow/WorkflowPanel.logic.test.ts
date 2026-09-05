import type { WorkflowIssueSummary, WorkflowRepository } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  filterWorkflowRoots,
  resolveWorkflowRepository,
  workflowIssueBrief,
  workflowIssueStateLabel,
  workflowSourceLinks,
} from "./WorkflowPanel.logic";

const repositories: WorkflowRepository[] = [
  { nameWithOwner: "Flow-Fly/t3code", remoteNames: ["origin"] },
  { nameWithOwner: "pingdotgg/t3code", remoteNames: ["upstream"] },
];

const issue = (number: number, title: string, kind: WorkflowIssueSummary["kind"]) => ({
  number,
  title,
  kind,
  url: `https://github.com/Flow-Fly/t3code/issues/${number}`,
  state: "open" as const,
  stateReason: null,
  updatedAt: "2026-09-05T19:30:00Z",
  childCount: 0,
});

describe("Workflow panel browsing", () => {
  it("requires an explicit repository when fork and upstream are both present", () => {
    expect(resolveWorkflowRepository(null, repositories)).toBeNull();
    expect(resolveWorkflowRepository("Flow-Fly/t3code", repositories)).toBe("Flow-Fly/t3code");
    expect(resolveWorkflowRepository("missing/repository", repositories)).toBeNull();
    expect(resolveWorkflowRepository(null, repositories.slice(0, 1))).toBe("Flow-Fly/t3code");
  });

  it("searches roots by title, number, and kind", () => {
    const roots = [issue(1, "Workflow map", "map"), issue(10, "Right panel", "capability")];
    expect(filterWorkflowRoots(roots, "right capability").map((root) => root.number)).toEqual([10]);
    expect(filterWorkflowRoots(roots, "#1").map((root) => root.number)).toEqual([1]);
    expect(filterWorkflowRoots(roots, "1").map((root) => root.number)).toEqual([1, 10]);
  });

  it("does not present raw GitHub closure as verified resolution", () => {
    expect(workflowIssueStateLabel({ state: "open", stateReason: null })).toBe("Open");
    expect(workflowIssueStateLabel({ state: "closed", stateReason: "completed" })).toBe(
      "Closed — unverified",
    );
    expect(workflowIssueStateLabel({ state: "closed", stateReason: "not_planned" })).toBe(
      "Closed — not planned",
    );
  });

  it("derives the brief only after detail is loaded", () => {
    expect(
      workflowIssueBrief({
        ...issue(11, "Browse GitHub work", "ticket"),
        body: "## Summary\n\nBrowse work beside chat.\n\n## Details\n\nLong evidence.",
        labels: ["workflow:ticket"],
      }),
    ).toBe("Browse work beside chat.");
  });

  it("exposes source links from the loaded issue description", () => {
    expect(
      workflowSourceLinks(
        "## Parent\n\n[Capability](https://github.com/Flow-Fly/t3code/issues/10)\n\n[Approval](https://github.com/Flow-Fly/t3code/issues/10#issuecomment-1)",
      ),
    ).toEqual([
      { label: "Capability", url: "https://github.com/Flow-Fly/t3code/issues/10" },
      {
        label: "Approval",
        url: "https://github.com/Flow-Fly/t3code/issues/10#issuecomment-1",
      },
    ]);
  });
});
