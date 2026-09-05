import type { WorkflowIssueSummary, WorkflowSearchMatch } from "@t3tools/contracts";

export interface WorkflowPoint {
  readonly x: number;
  readonly y: number;
}

export interface WorkflowViewport extends WorkflowPoint {
  readonly zoom: number;
}

export type WorkflowHistoryGroup = "completed" | "cancelled";

export function issueIdentity(
  issue: Pick<WorkflowIssueSummary, "id" | "repository" | "number">,
): string {
  return issue.id || `${issue.repository}#${issue.number}`;
}

export function workflowHistoryGroup(issue: WorkflowIssueSummary): WorkflowHistoryGroup | null {
  if (issue.state === "open") return null;
  if (issue.stateReason === "not_planned" || issue.labels.includes("workflow:superseded")) {
    return "cancelled";
  }
  return "completed";
}

export function foldIdentity(parentId: string, group: WorkflowHistoryGroup): string {
  return `${parentId}:${group}`;
}

export interface VisibleWorkflowNode {
  readonly id: string;
  readonly issue: WorkflowIssueSummary;
  readonly parentId: string | null;
  readonly depth: number;
  readonly position: WorkflowPoint;
}

export interface WorkflowFold {
  readonly id: string;
  readonly parentId: string;
  readonly group: WorkflowHistoryGroup;
  readonly count: number;
  readonly open: boolean;
}

export function buildVisibleWorkflowMap(input: {
  root: WorkflowIssueSummary;
  nodes: Readonly<Record<string, WorkflowIssueSummary>>;
  childrenByParent: Readonly<Record<string, readonly string[]>>;
  expanded: readonly string[];
  openFolds: readonly string[];
  positions: Readonly<Record<string, WorkflowPoint>>;
}): { nodes: VisibleWorkflowNode[]; folds: WorkflowFold[] } {
  const visible: VisibleWorkflowNode[] = [];
  const folds: WorkflowFold[] = [];
  const expanded = new Set(input.expanded);
  const openFolds = new Set(input.openFolds);
  const rootId = issueIdentity(input.root);
  const nextColumnByDepth = new Map<number, number>();
  const occupied = Object.values(input.positions);
  const overlapsExistingNode = (position: WorkflowPoint) =>
    occupied.some(
      (existing) =>
        Math.abs(existing.x - position.x) < 224 && Math.abs(existing.y - position.y) < 108,
    );
  const visit = (id: string, parentId: string | null, depth: number) => {
    const issue = input.nodes[id];
    if (!issue) return;
    let column = nextColumnByDepth.get(depth) ?? 0;
    let fallback = { x: column * 240 + 32, y: depth * 150 + 28 };
    while (!input.positions[id] && overlapsExistingNode(fallback)) {
      column += 1;
      fallback = { x: column * 240 + 32, y: depth * 150 + 28 };
    }
    nextColumnByDepth.set(depth, column + 1);
    const position = input.positions[id] ?? fallback;
    if (!input.positions[id]) occupied.push(position);
    visible.push({ id, issue, parentId, depth, position });
    if (issue.childCount === 0) return;
    if (id !== rootId && !expanded.has(id)) return;
    const groups: Record<WorkflowHistoryGroup, string[]> = { completed: [], cancelled: [] };
    const current: string[] = [];
    for (const childId of input.childrenByParent[id] ?? []) {
      const child = input.nodes[childId];
      if (!child) continue;
      const group = workflowHistoryGroup(child);
      if (group) groups[group].push(childId);
      else current.push(childId);
    }
    const shown = [...current];
    for (const group of ["completed", "cancelled"] as const) {
      if (groups[group].length === 0) continue;
      const foldId = foldIdentity(id, group);
      const open = openFolds.has(foldId);
      folds.push({ id: foldId, parentId: id, group, count: groups[group].length, open });
      if (open) shown.push(...groups[group]);
    }
    shown.forEach((childId) => visit(childId, id, depth + 1));
  };
  visit(rootId, null, 0);
  return { nodes: visible, folds };
}

export function mergeWorkflowSearchMatch(
  current: {
    nodes: Readonly<Record<string, WorkflowIssueSummary>>;
    childrenByParent: Readonly<Record<string, readonly string[]>>;
  },
  match: WorkflowSearchMatch,
): {
  nodes: Record<string, WorkflowIssueSummary>;
  childrenByParent: Record<string, readonly string[]>;
  expanded: string[];
} {
  const path = [...match.ancestry, match.issue];
  const nodes = { ...current.nodes };
  const childrenByParent = { ...current.childrenByParent };
  for (const node of path) nodes[issueIdentity(node)] = node;
  for (let index = 0; index < path.length - 1; index += 1) {
    const parentId = issueIdentity(path[index]!);
    const childId = issueIdentity(path[index + 1]!);
    childrenByParent[parentId] = [...new Set([...(childrenByParent[parentId] ?? []), childId])];
  }
  return { nodes, childrenByParent, expanded: path.slice(0, -1).map(issueIdentity) };
}

export function fitWorkflowViewport(
  nodes: readonly VisibleWorkflowNode[],
  size: { readonly width: number; readonly height: number },
): WorkflowViewport {
  if (nodes.length === 0) return { x: 0, y: 0, zoom: 1 };
  const minX = Math.min(...nodes.map((node) => node.position.x));
  const maxX = Math.max(...nodes.map((node) => node.position.x + 208));
  const minY = Math.min(...nodes.map((node) => node.position.y));
  const maxY = Math.max(...nodes.map((node) => node.position.y + 92));
  const zoom = Math.min(
    1,
    Math.max(0.02, Math.min((size.width - 32) / (maxX - minX), (size.height - 32) / (maxY - minY))),
  );
  return { x: 16 - minX * zoom, y: 16 - minY * zoom, zoom };
}
