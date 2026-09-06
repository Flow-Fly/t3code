import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  WorkflowChildrenResult,
  WorkflowIssueDetail,
  WorkflowIssueSummary,
  WorkflowDirectorAdmissionInput,
  WorkflowDirectorStartInput,
  WorkflowDirectorStatus,
  WorkflowRepositoriesResult,
  WorkflowRootsInput,
  WorkflowStartInput,
  WorkflowStartResult,
} from "./workflow.ts";

const decodeWorkflowStartInput = Schema.decodeUnknownSync(WorkflowStartInput);
const decodeWorkflowStartResult = Schema.decodeUnknownSync(WorkflowStartResult);
const decodeDirectorStart = Schema.decodeUnknownSync(WorkflowDirectorStartInput);
const decodeDirectorStatus = Schema.decodeUnknownSync(WorkflowDirectorStatus);
const decodeAdmission = Schema.decodeUnknownSync(WorkflowDirectorAdmissionInput);

describe("workflow contracts", () => {
  it("keeps director profile observations and admission identity explicit", () => {
    const input = decodeDirectorStart({
      projectId: "project-1",
      repository: "Flow-Fly/t3code",
      rootNumber: 10,
      capabilityNumber: 17,
      modelSelection: {
        instanceId: "codex-workflow",
        model: "gpt-6-astra",
        options: [{ id: "reasoningEffort", value: "high" }],
      },
    });
    const status = decodeDirectorStatus({
      directorId: "director-17",
      batchId: "batch-17-1",
      environmentId: "environment-1",
      projectId: input.projectId,
      repository: input.repository,
      rootNumber: input.rootNumber,
      capabilityNumber: input.capabilityNumber,
      threadId: "thread-17",
      worktreePath: "/tmp/t3/workflow-17",
      worktreeBranch: "t3code/workflow-17",
      status: "active",
      requestedProfile: {
        instanceId: "codex-workflow",
        model: "gpt-6-astra",
        effort: "high",
      },
      observedProfile: { model: null, effort: null, match: "unknown" },
      admissionCount: 1,
      admissionLimit: 10,
      observation: "director-17|active|thread-17|1",
      actions: ["open"],
      createdAt: "2026-09-06T10:00:00.000Z",
      updatedAt: "2026-09-06T10:01:00.000Z",
      message: "Director is active.",
    });
    const admission = decodeAdmission({
      projectId: input.projectId,
      directorId: status.directorId,
      repository: input.repository,
      ticketNumber: 18,
      purpose: "retry",
      ownership: "apps/server/src/workflow",
    });

    expect(status.observedProfile.match).toBe("unknown");
    expect(admission).toMatchObject({ directorId: "director-17", ticketNumber: 18 });
  });

  it("carries the explicit environment model choice and durable start identity", () => {
    const input = decodeWorkflowStartInput({
      projectId: "project-1",
      repository: "Flow-Fly/t3code",
      rootNumber: 10,
      issueNumber: 15,
      modelSelection: {
        instanceId: "codex-workflow",
        model: "gpt-6-astra",
        options: [{ id: "reasoningEffort", value: "high" }],
      },
    });
    const result = decodeWorkflowStartResult({
      disposition: "started",
      attemptId: "attempt-15",
      environmentId: "environment-1",
      projectId: input.projectId,
      repository: input.repository,
      rootNumber: input.rootNumber,
      issueNumber: input.issueNumber,
      phase: "decision",
      threadId: "thread-15",
      status: "submitted",
      createdAt: "2026-09-06T10:00:00.000Z",
      message: "Decision work started.",
    });

    expect(input.modelSelection.options).toEqual([{ id: "reasoningEffort", value: "high" }]);
    expect(result).toMatchObject({ attemptId: "attempt-15", threadId: "thread-15" });
  });

  it("requires an owner and repository separated by one slash", () => {
    const decode = Schema.decodeUnknownSync(WorkflowRootsInput);

    expect(() =>
      decode({ projectId: "project-1", repository: "github.com/Flow-Fly/t3code" }),
    ).toThrow();
    expect(() => decode({ projectId: "project-1", repository: "Flow-Fly/t3code" })).not.toThrow();
  });

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
      id: "issue-11",
      repository: "Flow-Fly/t3code",
      number: 11,
      title: "Browse GitHub work",
      url: "https://github.com/Flow-Fly/t3code/issues/11",
      kind: "ticket",
      state: "closed",
      stateReason: "completed",
      updatedAt: "2026-09-05T19:30:00Z",
      childCount: 0,
      parentNumber: 10,
      labels: ["workflow:ticket"],
    });

    expect(issue.state).toBe("closed");
    expect(issue).not.toHaveProperty("resolved");
  });

  it("decodes additive readiness while remaining compatible with older summaries", () => {
    const summary = {
      id: "issue-13",
      repository: "Flow-Fly/t3code",
      number: 13,
      title: "Explain evidence",
      url: "https://github.com/Flow-Fly/t3code/issues/13",
      kind: "ticket",
      state: "open",
      stateReason: null,
      updatedAt: "2026-09-05T19:30:00Z",
      childCount: 0,
      parentNumber: 10,
      labels: ["workflow:ticket"],
    };
    const decodeChildren = Schema.decodeUnknownSync(WorkflowChildrenResult);

    expect(
      decodeChildren({ parentNumber: 10, children: [summary] }).children[0],
    ).not.toHaveProperty("readiness");
    const current = decodeChildren({
      parentNumber: 10,
      children: [
        {
          ...summary,
          readiness: {
            status: "ready",
            reasons: [
              {
                kind: "approved-scope",
                message: "Ticket-breakdown approval covers current scope.",
              },
            ],
          },
        },
      ],
      frontier: {
        status: "available",
        message: "1 item can proceed.",
        readyIssueIds: ["issue-13"],
      },
    });
    expect(current.children[0]?.readiness?.status).toBe("ready");
    expect(current.frontier?.readyIssueIds).toEqual(["issue-13"]);
  });

  it("keeps approval authority separate from reported source access", () => {
    const decode = Schema.decodeUnknownSync(WorkflowIssueDetail);
    const detail = decode({
      id: "issue-13",
      repository: "Flow-Fly/t3code",
      number: 13,
      title: "Explain evidence",
      url: "https://github.com/Flow-Fly/t3code/issues/13",
      kind: "ticket",
      state: "open",
      stateReason: null,
      updatedAt: "2026-09-05T19:30:00Z",
      childCount: 0,
      parentNumber: 10,
      labels: ["workflow:ticket"],
      body: "Scope",
      blockedBy: [],
      evidence: {
        records: [
          {
            id: "approval-1",
            url: "https://github.com/Flow-Fly/t3code/issues/10#issuecomment-1",
            createdAt: "2026-09-05T19:32:15Z",
            kind: "approval",
            state: "current",
            sourceAccess: "reported",
            scope: "unknown",
            summary: "Approval: ticket-breakdown",
            approvalKind: "ticket-breakdown",
            authority: "verified",
            approvedBy: "Flow-Fly",
            source: "T3 thread thread-1",
            approvedContent: "Approved scope",
          },
        ],
        manualConditions: [],
        historyComplete: false,
      },
    });

    expect(detail.evidence?.records[0]).toMatchObject({
      authority: "verified",
      sourceAccess: "reported",
      scope: "unknown",
    });
    expect(detail.evidence?.historyComplete).toBe(false);
  });
});
