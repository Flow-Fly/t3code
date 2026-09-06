import type { WorkflowAdoptionItem, WorkflowIssueKind } from "@t3tools/contracts";

const KIND_BY_LABEL: Readonly<Record<string, WorkflowIssueKind>> = {
  "wayfinder:map": "map",
  "wayfinder:research": "decision",
  "wayfinder:prototype": "decision",
  "wayfinder:grilling": "decision",
  "wayfinder:task": "task",
  "workflow:map": "map",
  "workflow:decision": "decision",
  "workflow:capability": "capability",
  "workflow:container": "container",
  "workflow:ticket": "ticket",
  "workflow:task": "task",
};

export function workflowAdoptionItemChanges(item: WorkflowAdoptionItem) {
  const changes = new Array<string>();
  if (!item.labels.some((label) => KIND_BY_LABEL[label] === item.proposedKind))
    changes.push(`Add workflow:${item.proposedKind}`);
  for (const label of item.labels) {
    const kind = KIND_BY_LABEL[label];
    if (kind !== undefined && kind !== item.proposedKind)
      changes.push(`Remove conflicting ${label}`);
  }
  if (item.currentParentNumber !== item.proposedParentNumber)
    changes.push(
      `Parent ${item.currentParentNumber === null ? "none" : `#${item.currentParentNumber}`} → ${item.proposedParentNumber === null ? "none" : `#${item.proposedParentNumber}`}`,
    );
  return changes;
}
