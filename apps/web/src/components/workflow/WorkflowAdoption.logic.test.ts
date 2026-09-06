import type { WorkflowAdoptionItem } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { workflowAdoptionItemChanges } from "./WorkflowAdoption.logic";

const item: WorkflowAdoptionItem = {
  id: "issue-11",
  repository: "Flow-Fly/t3code",
  number: 11,
  title: "Existing slice",
  url: "https://github.com/Flow-Fly/t3code/issues/11",
  updatedAt: "2026-09-06T10:00:00Z",
  sourceBody: "## What to build\n\nShip it.",
  currentKind: "task",
  proposedKind: "ticket",
  currentParentNumber: 10,
  proposedParentNumber: null,
  labels: ["wayfinder:task", "keep-me"],
  relationships: [],
  changes: [],
  included: true,
  parentChangeConfirmed: true,
};

describe("workflow adoption exact changes", () => {
  it("shows canonical classification cleanup and native parent changes", () => {
    expect(workflowAdoptionItemChanges(item)).toEqual([
      "Add workflow:ticket",
      "Remove conflicting wayfinder:task",
      "Parent #10 → none",
    ]);
  });

  it("preserves a matching Wayfinder subtype without adding a duplicate label", () => {
    expect(
      workflowAdoptionItemChanges({
        ...item,
        currentKind: "decision",
        proposedKind: "decision",
        currentParentNumber: 10,
        proposedParentNumber: 10,
        labels: ["wayfinder:research", "keep-me"],
      }),
    ).toEqual([]);
  });
});
