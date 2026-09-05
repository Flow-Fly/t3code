import { describe, expect, it } from "@effect/vitest";

import {
  interpretWorkflowEvidence,
  type WorkflowEvidenceComment,
  type WorkflowEvidenceIssue,
} from "./WorkflowEvidence.ts";

function comment(input: { id: string; body: string; createdAt?: string }): WorkflowEvidenceComment {
  return {
    id: input.id,
    url: `https://github.com/Flow-Fly/t3code/issues/2#issuecomment-${input.id}`,
    body: input.body,
    createdAt: input.createdAt ?? "2026-09-05T20:00:00Z",
    author: "Flow-Fly",
    authorAssociation: "OWNER",
  };
}

function resolution(id: string, evidence: string, createdAt?: string): WorkflowEvidenceComment {
  return comment({
    id,
    ...(createdAt ? { createdAt } : {}),
    body: [
      "## Resolution",
      "<!-- t3-workflow:v1 resolution -->",
      "Outcome: resolved",
      "### Summary",
      "The work was completed.",
      "### Evidence",
      evidence,
    ].join("\n"),
  });
}

function issue(overrides: Partial<WorkflowEvidenceIssue> = {}): WorkflowEvidenceIssue {
  return {
    id: "issue-2",
    url: "https://github.com/Flow-Fly/t3code/issues/2",
    number: 2,
    title: "Child",
    kind: "decision",
    state: "open",
    stateReason: null,
    labels: [],
    assignees: [],
    body: "Child scope",
    comments: [],
    reopenedAt: [],
    ...overrides,
  };
}

describe("interpretWorkflowEvidence", () => {
  it("does not inherit a parent resolution", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({ state: "closed", stateReason: "completed" }),
      approvalComments: [
        resolution(
          "parent-resolution",
          "[Parent evidence](https://github.com/Flow-Fly/t3code/commit/abc)",
        ),
      ],
    });

    expect(result.readiness.status).toBe("closed-unverified");
    expect(result.evidence.records).toEqual([]);
  });

  it("does not accept an unsupported resolution assertion", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({
        state: "closed",
        stateReason: "completed",
        comments: [resolution("assertion", "Trust me")],
      }),
    });

    expect(result.readiness.status).toBe("closed-unverified");
    expect(result.evidence.records[0]).toMatchObject({ state: "invalid" });
  });

  it("preserves explicitly unavailable resolution evidence without resolving work", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({
        state: "closed",
        stateReason: "completed",
        comments: [
          resolution(
            "unavailable",
            "Evidence unavailable: [proof](https://github.com/Flow-Fly/t3code/commit/abc)",
          ),
        ],
      }),
    });

    expect(result.readiness.status).toBe("closed-unverified");
    expect(result.evidence.records[0]).toMatchObject({
      state: "current",
      sourceAccess: "unavailable",
    });
  });

  it("holds a resolution when the issue scope was edited after the record", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({
        state: "closed",
        stateReason: "completed",
        lastEditedAt: "2026-09-06T00:00:00Z",
        comments: [
          resolution(
            "before-edit",
            "[Commit](https://github.com/Flow-Fly/t3code/commit/abc)",
            "2026-09-05T20:00:00Z",
          ),
        ],
      }),
    });

    expect(result.readiness.status).toBe("closed-unverified");
    expect(result.evidence.records[0]).toMatchObject({ scope: "unknown" });
  });

  it("accepts current evidence recorded after the latest issue edit", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({
        state: "closed",
        stateReason: "completed",
        lastEditedAt: "2026-09-05T19:00:00Z",
        comments: [
          resolution(
            "after-edit",
            "[Commit](https://github.com/Flow-Fly/t3code/commit/abc)",
            "2026-09-05T20:00:00Z",
          ),
        ],
      }),
    });

    expect(result.readiness.status).toBe("resolved");
    expect(result.evidence.records[0]).toMatchObject({ scope: "current" });
  });

  it("holds ambiguous Blocked by prose for review", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({ body: "## Blocked by\n\nstaging access or approved fixtures" }),
    });

    expect(result.readiness).toMatchObject({ status: "needs-review" });
    expect(result.evidence.manualConditions).toEqual([
      {
        description: "staging access or approved fixtures",
        source: "https://github.com/Flow-Fly/t3code/issues/2",
        status: "review-required",
      },
    ]);
  });

  it("keeps a substantive condition that begins with None", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({
        body: "## Manual prerequisites\n\nNone of the required staging credentials are available",
      }),
    });

    expect(result.readiness.status).toBe("needs-review");
    expect(result.evidence.manualConditions[0]).toMatchObject({
      description: "None of the required staging credentials are available",
      status: "review-required",
    });
  });

  it("recognizes an explicit None sentinel as an empty prerequisite section", () => {
    const result = interpretWorkflowEvidence({
      issue: issue({ body: "## Manual prerequisites\n\nNone." }),
    });

    expect(result.readiness.status).toBe("ready");
    expect(result.evidence.manualConditions).toEqual([]);
  });

  it("does not use a pre-reopening reassessment to clear a condition", () => {
    const condition = "Access must be enabled";
    const clearance = comment({
      id: "clearance",
      body: [
        "## Reassessment",
        "<!-- t3-workflow:v1 reassessment -->",
        "Outcome: cleared",
        "Trigger: https://github.com/Flow-Fly/t3code/issues/2",
        "### Changes",
        `${condition} — verified`,
        "### Evidence",
        "[Proof](https://github.com/Flow-Fly/t3code/issues/1)",
      ].join("\n"),
    });
    const result = interpretWorkflowEvidence({
      issue: issue({
        body: `## Manual prerequisites\n\n${condition}`,
        comments: [clearance],
        reopenedAt: ["2026-09-06T00:00:00Z"],
      }),
    });

    expect(result.readiness.status).toBe("needs-review");
    expect(result.evidence.manualConditions[0]).toMatchObject({ status: "review-required" });
    expect(result.evidence.records[0]).toMatchObject({ scope: "changed" });
  });

  it("uses a condition-specific reassessment recorded after the latest edit", () => {
    const condition = "Access must be enabled";
    const clearance = comment({
      id: "current-clearance",
      createdAt: "2026-09-06T00:00:00Z",
      body: [
        "## Reassessment",
        "<!-- t3-workflow:v1 reassessment -->",
        "Outcome: cleared",
        "Trigger: https://github.com/Flow-Fly/t3code/issues/2",
        "### Changes",
        `${condition} — verified`,
        "### Evidence",
        "[Proof](https://github.com/Flow-Fly/t3code/issues/1)",
      ].join("\n"),
    });
    const result = interpretWorkflowEvidence({
      issue: issue({
        body: `## Manual prerequisites\n\n${condition}`,
        comments: [clearance],
        lastEditedAt: "2026-09-05T21:00:00Z",
      }),
    });

    expect(result.readiness.status).toBe("ready");
    expect(result.evidence.manualConditions[0]).toMatchObject({ status: "satisfied" });
    expect(result.evidence.records[0]).toMatchObject({ scope: "current" });
  });

  it("does not use unavailable reassessment evidence to clear a condition", () => {
    const condition = "Access must be enabled";
    const clearance = comment({
      id: "unavailable-clearance",
      body: [
        "## Reassessment",
        "<!-- t3-workflow:v1 reassessment -->",
        "Outcome: cleared",
        "Trigger: https://github.com/Flow-Fly/t3code/issues/2",
        "### Changes",
        `${condition} — verified`,
        "### Evidence",
        "Evidence unavailable: [Proof](https://github.com/Flow-Fly/t3code/issues/1)",
      ].join("\n"),
    });
    const result = interpretWorkflowEvidence({
      issue: issue({
        body: `## Manual prerequisites\n\n${condition}`,
        comments: [clearance],
      }),
    });

    expect(result.readiness.status).toBe("needs-review");
    expect(result.evidence.manualConditions[0]).toMatchObject({ status: "review-required" });
    expect(result.evidence.records[0]).toMatchObject({ sourceAccess: "unavailable" });
  });

  it("requires an affirmative newer versioned record to supersede evidence", () => {
    const original = resolution(
      "original",
      "[Commit](https://github.com/Flow-Fly/t3code/commit/abc)",
    );
    const denial = comment({
      id: "denial",
      createdAt: "2026-09-06T00:00:00Z",
      body: `This does not supersede ${original.url}`,
    });
    const unrelated = comment({
      id: "unrelated",
      createdAt: "2026-09-06T01:00:00Z",
      body: [
        "## Reassessment",
        "<!-- t3-workflow:v1 reassessment -->",
        "Outcome: cleared",
        `Trigger: ${original.url}`,
        "### Changes",
        `A quoted note said: "This record supersedes ${original.url}"`,
        "### Evidence",
        "[Proof](https://github.com/Flow-Fly/t3code/issues/1)",
      ].join("\n"),
    });
    const result = interpretWorkflowEvidence({
      issue: issue({
        state: "closed",
        stateReason: "completed",
        comments: [original, denial, unrelated],
      }),
    });

    expect(result.readiness.status).toBe("resolved");
    expect(result.evidence.records.find((record) => record.id === original.id)).toMatchObject({
      state: "current",
    });
  });

  it("ignores supersession declarations inside backtick and tilde fences", () => {
    const original = resolution(
      "fenced-original",
      "[Commit](https://github.com/Flow-Fly/t3code/commit/abc)",
    );
    const newer = resolution(
      "fenced-newer",
      [
        "[Commit](https://github.com/Flow-Fly/t3code/commit/def)",
        "```text",
        `Supersedes: ${original.url}`,
        "```",
        "~~~text",
        `This record supersedes ${original.url}`,
        "~~~",
      ].join("\n"),
      "2026-09-06T00:00:00Z",
    );
    const result = interpretWorkflowEvidence({
      issue: issue({
        state: "closed",
        stateReason: "completed",
        comments: [original, newer],
      }),
    });

    expect(result.evidence.records.map((record) => [record.id, record.state])).toEqual([
      ["fenced-original", "current"],
      ["fenced-newer", "current"],
    ]);
  });
});
