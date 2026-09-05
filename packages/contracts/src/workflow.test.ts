import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { WorkflowIssueSummary, WorkflowRepositoriesResult } from "./workflow.ts";

describe("workflow contracts", () => {
  it("keeps repository selection explicit when a project has fork and upstream remotes", () => {
    const decode = Schema.decodeUnknownSync(WorkflowRepositoriesResult);
    const result = decode({
      projectId: "project-1",
      projectTitle: "T3 Code",
      repositories: [
        { nameWithOwner: "Flow-Fly/t3code", remoteNames: ["origin"] },
        { nameWithOwner: "pingdotgg/t3code", remoteNames: ["upstream"] },
      ],
    });

    expect(result.repositories.map((repository) => repository.nameWithOwner)).toEqual([
      "Flow-Fly/t3code",
      "pingdotgg/t3code",
    ]);
  });

  it("preserves raw closed state without claiming verified resolution", () => {
    const decode = Schema.decodeUnknownSync(WorkflowIssueSummary);
    const issue = decode({
      number: 11,
      title: "Browse GitHub work",
      url: "https://github.com/Flow-Fly/t3code/issues/11",
      kind: "ticket",
      state: "closed",
      stateReason: "completed",
      updatedAt: "2026-09-05T19:30:00Z",
      childCount: 0,
    });

    expect(issue.state).toBe("closed");
    expect(issue).not.toHaveProperty("resolved");
  });
});
