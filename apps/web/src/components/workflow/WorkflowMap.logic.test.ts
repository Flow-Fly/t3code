import type { WorkflowIssueSummary } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildVisibleWorkflowMap,
  fitWorkflowViewport,
  issueIdentity,
  mergeWorkflowSearchMatch,
  workflowHistoryGroup,
} from "./WorkflowMap.logic";

const issue = (
  number: number,
  parentNumber: number | null,
  state: WorkflowIssueSummary["state"] = "open",
  stateReason: WorkflowIssueSummary["stateReason"] = null,
  repository = "Flow-Fly/t3code",
): WorkflowIssueSummary => ({
  id: `${repository}-${number}`,
  repository,
  number,
  parentNumber,
  title: `Issue ${number}`,
  url: `https://github.com/${repository}/issues/${number}`,
  kind: parentNumber === null ? "capability" : "ticket",
  state,
  stateReason,
  updatedAt: "2026-09-05T00:00:00Z",
  childCount: 0,
  labels: [],
});

describe("focused Workflow map", () => {
  it("keeps repository-qualified issue identities distinct", () => {
    expect(issueIdentity(issue(7, null, "open", null, "one/repo"))).not.toBe(
      issueIdentity(issue(7, null, "open", null, "two/repo")),
    );
  });

  it("shows the root and open children while folding completed and cancelled history separately", () => {
    const root = { ...issue(10, null), childCount: 3 };
    const open = issue(11, 10);
    const completed = issue(12, 10, "closed", "completed");
    const cancelled = issue(13, 10, "closed", "not_planned");
    const nodes = Object.fromEntries(
      [root, open, completed, cancelled].map((node) => [issueIdentity(node), node]),
    );
    const result = buildVisibleWorkflowMap({
      root,
      nodes,
      childrenByParent: {
        [issueIdentity(root)]: [
          issueIdentity(open),
          issueIdentity(completed),
          issueIdentity(cancelled),
        ],
      },
      expanded: [],
      openFolds: [],
      positions: {},
    });

    expect(result.nodes.map(({ issue: node }) => node.number)).toEqual([10, 11]);
    expect(result.folds.map((fold) => [fold.group, fold.count])).toEqual([
      ["completed", 1],
      ["cancelled", 1],
    ]);
    expect(workflowHistoryGroup(completed)).toBe("completed");
    expect(workflowHistoryGroup(cancelled)).toBe("cancelled");
  });

  it("reveals a collapsed search match and ancestry without loading unrelated work", () => {
    const root = issue(1, null);
    const branch = issue(2, 1);
    const match = issue(3, 2);
    const unrelated = issue(99, 1);
    const merged = mergeWorkflowSearchMatch(
      {
        nodes: { [issueIdentity(root)]: root, [issueIdentity(unrelated)]: unrelated },
        childrenByParent: {},
      },
      { issue: match, ancestry: [root, branch], ancestryComplete: true },
    );

    expect(
      Object.values(merged.nodes)
        .map((node) => node.number)
        .toSorted((a, b) => a - b),
    ).toEqual([1, 2, 3, 99]);
    expect(merged.expanded).toEqual([issueIdentity(root), issueIdentity(branch)]);
    expect(merged.childrenByParent[issueIdentity(branch)]).toEqual([issueIdentity(match)]);
  });

  it("keeps manual positions stable as a fifty-slice hierarchy updates", () => {
    const root = { ...issue(1, null), childCount: 50 };
    const children = Array.from({ length: 50 }, (_, index) => issue(index + 2, 1));
    const nodes = Object.fromEntries(
      [root, ...children].map((node) => [issueIdentity(node), node]),
    );
    const rootId = issueIdentity(root);
    const movedId = issueIdentity(children[8]!);
    const first = buildVisibleWorkflowMap({
      root,
      nodes,
      childrenByParent: { [rootId]: children.map(issueIdentity) },
      expanded: [],
      openFolds: [],
      positions: { [movedId]: { x: 725, y: 310 } },
    });
    const changed = {
      ...nodes,
      [issueIdentity(children[49]!)]: { ...children[49]!, title: "Updated" },
    };
    const second = buildVisibleWorkflowMap({
      root,
      nodes: changed,
      childrenByParent: { [rootId]: children.map(issueIdentity) },
      expanded: [],
      openFolds: [],
      positions: Object.fromEntries(first.nodes.map((node) => [node.id, node.position])),
    });

    expect(first.nodes).toHaveLength(51);
    expect(second.nodes.find((node) => node.id === movedId)?.position).toEqual({ x: 725, y: 310 });
    const fitted = fitWorkflowViewport(second.nodes, { width: 360, height: 420 });
    expect(fitted.zoom).toBeLessThan(0.04);
    const visibleWidth =
      (Math.max(...second.nodes.map((node) => node.position.x + 208)) - 32) * fitted.zoom;
    expect(visibleWidth).toBeLessThanOrEqual(360);
  });

  it("does not overlap grandchildren from separate expanded branches", () => {
    const root = { ...issue(1, null), childCount: 2 };
    const left = { ...issue(2, 1), childCount: 1 };
    const right = { ...issue(3, 1), childCount: 1 };
    const leftChild = issue(4, 2);
    const rightChild = issue(5, 3);
    const all = [root, left, right, leftChild, rightChild];
    const result = buildVisibleWorkflowMap({
      root,
      nodes: Object.fromEntries(all.map((node) => [issueIdentity(node), node])),
      childrenByParent: {
        [issueIdentity(root)]: [issueIdentity(left), issueIdentity(right)],
        [issueIdentity(left)]: [issueIdentity(leftChild)],
        [issueIdentity(right)]: [issueIdentity(rightChild)],
      },
      expanded: [issueIdentity(left), issueIdentity(right)],
      openFolds: [],
      positions: {},
    });
    expect(result.nodes.find((node) => node.issue.number === 4)?.position).not.toEqual(
      result.nodes.find((node) => node.issue.number === 5)?.position,
    );
  });

  it("reserves a saved branch when an earlier sibling expands later", () => {
    const root = { ...issue(1, null), childCount: 2 };
    const left = { ...issue(2, 1), childCount: 1 };
    const right = { ...issue(3, 1), childCount: 1 };
    const leftChild = issue(4, 2);
    const rightChild = issue(5, 3);
    const all = [root, left, right, leftChild, rightChild];
    const input = {
      root,
      nodes: Object.fromEntries(all.map((node) => [issueIdentity(node), node])),
      childrenByParent: {
        [issueIdentity(root)]: [issueIdentity(left), issueIdentity(right)],
        [issueIdentity(left)]: [issueIdentity(leftChild)],
        [issueIdentity(right)]: [issueIdentity(rightChild)],
      },
      openFolds: [],
    };
    const rightFirst = buildVisibleWorkflowMap({
      ...input,
      expanded: [issueIdentity(right)],
      positions: {},
    });
    const positions = Object.fromEntries(rightFirst.nodes.map((node) => [node.id, node.position]));
    const both = buildVisibleWorkflowMap({
      ...input,
      expanded: [issueIdentity(left), issueIdentity(right)],
      positions,
    });

    expect(both.nodes.find((node) => node.issue.number === 5)?.position).toEqual(
      positions[issueIdentity(rightChild)],
    );
    expect(both.nodes.find((node) => node.issue.number === 4)?.position).not.toEqual(
      positions[issueIdentity(rightChild)],
    );
  });

  it("places an inserted sibling without moving or covering saved siblings", () => {
    const root = { ...issue(1, null), childCount: 3 };
    const first = issue(2, 1);
    const second = issue(3, 1);
    const inserted = issue(4, 1);
    const nodes = Object.fromEntries(
      [root, first, second, inserted].map((node) => [issueIdentity(node), node]),
    );
    const original = buildVisibleWorkflowMap({
      root,
      nodes,
      childrenByParent: { [issueIdentity(root)]: [issueIdentity(first), issueIdentity(second)] },
      expanded: [],
      openFolds: [],
      positions: {},
    });
    const positions = Object.fromEntries(original.nodes.map((node) => [node.id, node.position]));
    const updated = buildVisibleWorkflowMap({
      root,
      nodes,
      childrenByParent: {
        [issueIdentity(root)]: [
          issueIdentity(inserted),
          issueIdentity(first),
          issueIdentity(second),
        ],
      },
      expanded: [],
      openFolds: [],
      positions,
    });

    expect(updated.nodes.find((node) => node.id === issueIdentity(first))?.position).toEqual(
      positions[issueIdentity(first)],
    );
    expect(updated.nodes.find((node) => node.id === issueIdentity(second))?.position).toEqual(
      positions[issueIdentity(second)],
    );
    const insertedPosition = updated.nodes.find(
      (node) => node.id === issueIdentity(inserted),
    )?.position;
    expect(insertedPosition).not.toEqual(positions[issueIdentity(first)]);
    expect(insertedPosition).not.toEqual(positions[issueIdentity(second)]);
  });
});
