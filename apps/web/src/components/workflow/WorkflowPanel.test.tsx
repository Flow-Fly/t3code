import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { useWorkflowMapStore } from "~/workflowMapStore";
import { useRightPanelStore } from "~/rightPanelStore";

vi.mock("./WorkflowActiveWork", () => ({ WorkflowActiveWork: () => null }));

const query = vi.hoisted(() => {
  const calls = new Array<{ kind: string; request: unknown }>();
  const descriptor = (kind: string, request: unknown) => {
    calls.push({ kind, request });
    return { kind, request };
  };
  return {
    calls,
    descriptor,
    moved: false,
    evidence: false,
    startReady: false,
    recoveryState: false,
    externalClaim: false,
    heldAfterRefresh: false,
    planningKind: null as "map" | "capability" | null,
    startFailure: false,
    startCalls: new Array<unknown>(),
    refreshCalls: new Array<unknown>(),
    syncStatus: "fresh" as "fresh" | "stale" | "rate-limited" | "unavailable",
    navigateCalls: new Array<unknown>(),
    threadDetailRefs: new Array<unknown>(),
    directorActivities: new Array<{
      id: string;
      kind: string;
      summary?: string;
      payload: unknown;
      createdAt: string;
    }>(),
    directorRefresh: vi.fn(),
    workerHistory: false,
    destinationRepository: "Flow-Fly/t3code",
  };
});

vi.mock("@tanstack/react-router", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-router")>()),
  useNavigate: () => (request: unknown) => {
    query.navigateCalls.push(request);
  },
}));

vi.mock("~/state/entities", () => ({
  useProject: () => ({
    defaultModelSelection: {
      instanceId: "codex-workflow",
      model: "gpt-6-astra",
      options: [{ id: "reasoningEffort", value: "high" }],
    },
  }),
  useServerConfigs: () =>
    new Map([
      [
        "remote-environment",
        {
          providers: [
            {
              instanceId: "codex-workflow",
              driver: "codex",
              status: "ready",
              enabled: true,
              installed: true,
              models: [
                {
                  slug: "gpt-6-astra",
                  capabilities: {
                    optionDescriptors: [
                      {
                        id: "reasoningEffort",
                        label: "Reasoning effort",
                        type: "select",
                        options: [{ id: "high", label: "High" }],
                      },
                    ],
                  },
                },
              ],
            },
          ],
        },
      ],
    ]),
  useThreadDetail: (reference: unknown) => {
    query.threadDetailRefs.push(reference);
    return reference ? { activities: query.directorActivities } : null;
  },
}));

vi.mock("~/state/use-atom-command", () => ({
  useAtomCommand: (command: { label: string }) => async (request: unknown) => {
    query.startCalls.push(request);
    if (command.label === "workflow:refresh") query.refreshCalls.push(request);
    if (command.label === "workflow:start" && query.startFailure) {
      return { _tag: "Failure", cause: Cause.fail(new Error("Planning link is unavailable.")) };
    }
    if (command.label === "workflow:start" && query.heldAfterRefresh) {
      return {
        _tag: "Success",
        value: {
          disposition: "started",
          status: "held",
          attemptId: "attempt-held",
          environmentId: "remote-environment",
          projectId: "project-draft",
          repository: "Flow-Fly/t3code",
          rootNumber: 10,
          issueNumber: 11,
          phase: "decision",
          threadId: "workflow-thread-held",
          createdAt: "2026-09-06T10:00:00.000Z",
          message: "The initial submission is uncertain.",
        },
      };
    }
    const input = (
      request as {
        input?: { phase?: "specification" | "ticket-breakdown"; planningThreadId?: string };
      }
    ).input;
    return {
      _tag: "Success",
      value: {
        disposition: "started",
        status: "submitted",
        attemptId: "attempt-15",
        environmentId: "remote-environment",
        projectId: "project-draft",
        repository: "Flow-Fly/t3code",
        rootNumber: 10,
        issueNumber: 11,
        phase: input?.phase ?? "decision",
        threadId: input?.planningThreadId ?? "workflow-thread-15",
        createdAt: "2026-09-06T10:00:00.000Z",
        message: "Decision work started.",
      },
    };
  },
}));

vi.mock("~/state/workflow", () => ({
  workflowEnvironment: {
    repositories: (request: unknown) => query.descriptor("repositories", request),
    roots: (request: unknown) => query.descriptor("roots", request),
    sync: (request: unknown) => query.descriptor("sync", request),
    children: (request: unknown) => query.descriptor("children", request),
    issueDetail: (request: unknown) => query.descriptor("detail", request),
    search: (request: unknown) => query.descriptor("search", request),
    locate: (request: unknown) => query.descriptor("locate", request),
    recovery: (request: unknown) => query.descriptor("recovery", request),
    directorStatus: (request: unknown) => query.descriptor("directorStatus", request),
    start: { label: "workflow:start", run: vi.fn() },
    recover: { label: "workflow:recover", run: vi.fn() },
    directorStart: { label: "workflow:director-start", run: vi.fn() },
    directorResume: { label: "workflow:director-resume", run: vi.fn() },
    refresh: { label: "workflow:refresh", run: vi.fn() },
  },
}));

vi.mock("~/state/query", () => ({
  useEnvironmentQuery: (descriptor: { kind: string; request?: unknown } | null) => {
    const idle = { data: null, error: null, isPending: false, refresh: vi.fn() };
    if (!descriptor) return idle;
    if (descriptor.kind === "repositories") {
      return {
        ...idle,
        data: {
          projectId: "project-draft",
          projectTitle: "Draft project",
          repositories: [{ nameWithOwner: "flow-fly/t3code", remoteNames: ["origin"] }],
        },
      };
    }
    if (descriptor.kind === "roots") {
      const repository = (descriptor.request as { input?: { repository?: string } }).input
        ?.repository;
      const destinationIsCurrent = query.destinationRepository.toLowerCase() === "flow-fly/t3code";
      return {
        ...idle,
        data: {
          repository: repository ?? "flow-fly/t3code",
          roots:
            repository?.toLowerCase() !== "flow-fly/t3code"
              ? [
                  {
                    id: "issue-20",
                    repository: query.destinationRepository,
                    number: 20,
                    title: "New capability",
                    url: `https://github.com/${query.destinationRepository}/issues/20`,
                    kind: "capability",
                    state: "open",
                    stateReason: null,
                    updatedAt: "2026-09-05T00:00:00Z",
                    childCount: 1,
                    parentNumber: null,
                    labels: ["workflow:capability"],
                  },
                ]
              : [
                  {
                    id: "issue-10",
                    repository: "Flow-Fly/t3code",
                    number: 10,
                    title: "Capability",
                    url: "https://github.com/Flow-Fly/t3code/issues/10",
                    kind: query.planningKind ?? (query.startReady ? "map" : "capability"),
                    state: "open",
                    stateReason: null,
                    updatedAt: "2026-09-05T00:00:00Z",
                    childCount: 1,
                    parentNumber: null,
                    labels: [
                      query.planningKind === "map" || query.startReady
                        ? "wayfinder:map"
                        : "workflow:capability",
                    ],
                  },
                  ...(query.moved && destinationIsCurrent
                    ? [
                        {
                          id: "issue-20",
                          repository: query.destinationRepository,
                          number: 20,
                          title: "New capability",
                          url: `https://github.com/${query.destinationRepository}/issues/20`,
                          kind: "capability",
                          state: "open",
                          stateReason: null,
                          updatedAt: "2026-09-05T00:00:00Z",
                          childCount: 1,
                          parentNumber: null,
                          labels: ["workflow:capability"],
                        },
                      ]
                    : []),
                ],
        },
      };
    }
    if (descriptor.kind === "sync") {
      return {
        ...idle,
        data: {
          repository: "Flow-Fly/t3code",
          status: query.syncStatus,
          lastAttemptAt: "2026-09-06T10:00:00.000Z",
          lastSuccessfulAt: "2026-09-06T10:00:00.000Z",
          cacheAgeMs: 0,
          retryAt: null,
          revision: 1,
          message: "Workflow is current with GitHub.",
        },
      };
    }
    if (descriptor.kind === "children") {
      const parentNumber = (descriptor.request as { input?: { parentNumber?: number } })?.input
        ?.parentNumber;
      return {
        ...idle,
        data: {
          parentNumber: parentNumber ?? 10,
          ...(query.evidence
            ? {
                frontier: {
                  status: "empty-blocked",
                  message:
                    "Nothing can proceed because prerequisites are blocking the visible work.",
                  readyIssueIds: [],
                },
              }
            : {}),
          children:
            query.moved && parentNumber === 11
              ? []
              : [
                  {
                    id: parentNumber === 11 || parentNumber === 20 ? "issue-12" : "issue-11",
                    repository: "Flow-Fly/t3code",
                    number: parentNumber === 11 || parentNumber === 20 ? 12 : 11,
                    title:
                      parentNumber === 11 || parentNumber === 20 ? "Nested task" : "Browse work",
                    url: `https://github.com/Flow-Fly/t3code/issues/${parentNumber === 11 || parentNumber === 20 ? 12 : 11}`,
                    kind: query.startReady ? "decision" : "ticket",
                    state: parentNumber === 11 || parentNumber === 20 ? "closed" : "open",
                    stateReason: parentNumber === 11 || parentNumber === 20 ? "completed" : null,
                    updatedAt: "2026-09-05T00:00:00Z",
                    childCount: parentNumber === 11 || parentNumber === 20 ? 0 : 1,
                    parentNumber: parentNumber ?? 10,
                    labels: [query.startReady ? "wayfinder:research" : "workflow:ticket"],
                    ...(query.evidence
                      ? {
                          readiness: {
                            status: "blocked",
                            reasons: [
                              {
                                kind: "unverified-blocker",
                                message: "Prerequisite #1 Map is closed-unverified.",
                                source: "https://github.com/Flow-Fly/t3code/issues/1",
                              },
                            ],
                          },
                        }
                      : {}),
                  },
                ],
        },
      };
    }
    if (descriptor.kind === "detail") {
      if (query.planningKind) {
        return {
          ...idle,
          data: {
            id: "issue-10",
            repository: "Flow-Fly/t3code",
            number: 10,
            title: "Capability",
            url: "https://github.com/Flow-Fly/t3code/issues/10",
            kind: query.planningKind,
            state: "open",
            stateReason: null,
            updatedAt: "2026-09-05T00:00:00Z",
            childCount: 1,
            parentNumber: null,
            body: "## Summary\n\nPlan the capability.",
            labels: [query.planningKind === "map" ? "wayfinder:map" : "workflow:capability"],
            blockedBy: [],
            ...(query.planningKind === "capability"
              ? { readiness: { status: "ready", reasons: [] } }
              : {}),
          },
        };
      }
      return {
        ...idle,
        data: {
          id: "issue-11",
          repository: "Flow-Fly/t3code",
          number: 11,
          title: "Browse work",
          url: "https://github.com/Flow-Fly/t3code/issues/11",
          kind: query.startReady ? "decision" : "ticket",
          state: "open",
          stateReason: null,
          updatedAt: "2026-09-05T00:00:00Z",
          childCount: 0,
          parentNumber: 10,
          body: "## Summary\n\nBrowse work without starting an agent.\n\n## Source map\n\n[Map](https://github.com/Flow-Fly/t3code/issues/1)\n\n## Notes\n\n[Related](https://github.com/Flow-Fly/t3code/issues/2)",
          labels: [query.startReady ? "wayfinder:research" : "wayfinder:task"],
          blockedBy: [
            {
              id: "issue-1",
              repository: "Flow-Fly/t3code",
              number: 1,
              title: "Map",
              url: "https://github.com/Flow-Fly/t3code/issues/1",
              kind: "map",
              state: "open",
              stateReason: null,
              updatedAt: "2026-09-05T00:00:00Z",
              childCount: 1,
              parentNumber: null,
              labels: ["wayfinder:map"],
            },
          ],
          ...(query.startReady
            ? { readiness: { status: "ready", reasons: [] } }
            : query.evidence
              ? {
                  readiness: {
                    status: "needs-review",
                    reasons: [
                      {
                        kind: "reassessment",
                        message:
                          "Ticket approval matches scope, but owner authority is not verified.",
                        source:
                          "https://github.com/Flow-Fly/t3code/issues/10#issuecomment-approval",
                      },
                    ],
                  },
                  evidence: {
                    records: [
                      {
                        id: "approval-record",
                        url: "https://github.com/Flow-Fly/t3code/issues/10#issuecomment-approval",
                        createdAt: "2026-09-05T19:32:15Z",
                        kind: "approval",
                        state: "current",
                        sourceAccess: "reported",
                        scope: "current",
                        summary: "Approval: ticket-breakdown",
                        approvalKind: "ticket-breakdown",
                        approvedBy: "Flow-Fly",
                        authority: "reported",
                        source: "T3 thread thread-1",
                        approvedContent: "Approved ticket snapshot",
                      },
                    ],
                    manualConditions: [
                      {
                        description: "Provide a staging account or an approved fixture.",
                        source: "https://github.com/acme/runbook/issues/4",
                        status: "review-required",
                      },
                    ],
                  },
                }
              : {}),
        },
      };
    }
    if (descriptor.kind === "recovery") {
      const currentAttempt = query.recoveryState
        ? {
            attemptId: query.heldAfterRefresh ? "attempt-held" : "attempt-15",
            environmentId: "remote-environment",
            projectId: "project-draft",
            repository: "Flow-Fly/t3code",
            rootNumber: 10,
            issueNumber: 11,
            phase: "decision",
            threadId: query.heldAfterRefresh ? "workflow-thread-held" : "workflow-thread-15",
            status: query.heldAfterRefresh ? "held" : "submitted",
            evidence: query.heldAfterRefresh ? "unknown" : "accepted",
            claimLogin: "Flow-Fly",
            isCurrent: true,
            createdAt: "2026-09-06T10:00:00.000Z",
            updatedAt: "2026-09-06T10:00:00.000Z",
            detail: query.heldAfterRefresh ? "The initial submission is uncertain." : null,
          }
        : null;
      return {
        ...idle,
        refresh: vi.fn(() => {
          if (query.heldAfterRefresh) query.recoveryState = true;
        }),
        data:
          query.recoveryState || query.externalClaim
            ? {
                environmentId: "remote-environment",
                projectId: "project-draft",
                repository: "Flow-Fly/t3code",
                issueNumber: 11,
                attempts: currentAttempt ? [currentAttempt] : [],
                currentAttempt,
                assignees: query.externalClaim ? ["outside-owner"] : ["Flow-Fly"],
                observation: query.externalClaim
                  ? "none|none|none|outside-owner|"
                  : query.heldAfterRefresh
                    ? "attempt-held|held|unknown|Flow-Fly|"
                    : "attempt-15|submitted|accepted|Flow-Fly|",
                actions: query.externalClaim
                  ? ["takeover"]
                  : query.heldAfterRefresh
                    ? ["open"]
                    : ["open", "resume", "start-fresh"],
                message: query.externalClaim
                  ? "GitHub shows an existing assignment. Confirm the handoff outside T3 Code or explicitly take over after checking the other environment."
                  : query.heldAfterRefresh
                    ? "Initial submission evidence is unavailable. Open the linked thread to inspect it; a new first turn is held."
                    : "This environment has accepted work linked to the preserved thread.",
              }
            : null,
      };
    }
    if (descriptor.kind === "directorStatus") {
      return {
        ...idle,
        refresh: query.directorRefresh,
        data: query.workerHistory
          ? {
              directorId: "director-1",
              batchId: "batch-1",
              environmentId: "remote-environment",
              projectId: "project-draft",
              repository: "Flow-Fly/t3code",
              rootNumber: 10,
              capabilityNumber: 10,
              threadId: "director-thread",
              worktreePath: "/tmp/t3code-workflow-10",
              worktreeBranch: "capability/workflow-10",
              status: "active",
              requestedProfile: {
                instanceId: "codex-workflow",
                model: "gpt-6-astra",
                effort: "high",
              },
              observedProfile: { model: "gpt-6-astra", effort: "high", match: "match" },
              admissionCount: 1,
              admissionLimit: 10,
              workers: [
                {
                  dispatchId: "dispatch-1",
                  admissionId: "admission-1",
                  ticketNumber: 11,
                  providerThreadId: "provider-worker-1",
                  parentProviderThreadId: "provider-director",
                  ownership: "workflow panel",
                  writePaths: ["apps/web/src/components/workflow"],
                  writeReservation: "released",
                  settlementEvidence: "native-closed",
                  association: "associated",
                  providerStatus: "interrupted",
                  requestedProfile: {
                    model: "gpt-5.6-sol",
                    effort: "high",
                    skillPath: "/skills/implement/SKILL.md",
                  },
                  observedProfile: {
                    model: "gpt-5.6-sol",
                    effort: "high",
                    match: "match",
                  },
                  handoff: null,
                  title: "Implement workflow panel",
                  role: "worker",
                  updatedAt: "2026-09-06T11:00:00.000Z",
                },
              ],
              reviews: [
                {
                  reviewId: "review-1",
                  admissionId: "admission-1",
                  ticketNumber: 11,
                  implementationProviderThreadId: "provider-worker-1",
                  fixedBase: "base-head",
                  implementationHead: "final-head",
                  status: "reported",
                  association: "associated",
                  providerThreadId: "review-coordinator-1",
                  parentProviderThreadId: "director-thread",
                  providerStatus: "interrupted",
                  settlementEvidence: "native-closed",
                  requestedProfile: {
                    model: "gpt-6-astra",
                    effort: "medium",
                    skillPath: "/skills/code-review/SKILL.md",
                  },
                  observedProfile: { model: "gpt-6-astra", effort: "medium", match: "match" },
                  checks: [
                    {
                      label: "focused",
                      command: "vp test run focused.test.ts",
                      toolCallId: "tool-1",
                      exitCode: 0,
                      output: "passed",
                      startedHead: "final-head",
                      finishedHead: "final-head",
                      startedClean: true,
                      finishedClean: true,
                      status: "passed",
                      verificationError: null,
                    },
                  ],
                  axes: [
                    {
                      axis: "standards",
                      providerThreadId: "standards-1",
                      parentProviderThreadId: "review-coordinator-1",
                      providerStatus: "interrupted",
                      settlementEvidence: "native-closed",
                      observedProfile: {
                        model: "gpt-6-astra",
                        effort: "medium",
                        match: "match",
                      },
                    },
                    {
                      axis: "spec",
                      providerThreadId: "spec-1",
                      parentProviderThreadId: "review-coordinator-1",
                      providerStatus: "interrupted",
                      settlementEvidence: "native-closed",
                      observedProfile: {
                        model: "gpt-6-astra",
                        effort: "medium",
                        match: "match",
                      },
                    },
                  ],
                  findings: [
                    {
                      id: "finding-1",
                      axis: "standards",
                      severity: "low",
                      summary: "Clarify the status copy.",
                      location: "WorkflowFocusedMap.tsx",
                      disposition: {
                        outcome: "dismissed",
                        rationale: "Existing copy is intentional.",
                        evidenceSource: null,
                        evidenceQuote: null,
                        resultingReviewId: null,
                      },
                    },
                  ],
                  summary: "Both axes completed.",
                  updatedAt: "2026-09-06T11:00:00.000Z",
                },
              ],
              resolutions: [
                {
                  resolutionId: "resolution-1",
                  reviewId: "review-1",
                  ticketNumber: 11,
                  finalHead: "final-head",
                  status: "comment-uncertain",
                  commentUrl: null,
                  lastError: "GitHub has not returned the comment yet.",
                  readyIssueIds: [],
                  updatedAt: "2026-09-06T11:00:00.000Z",
                },
              ],
              observation: "accepted",
              actions: ["open"],
              createdAt: "2026-09-06T10:00:00.000Z",
              updatedAt: "2026-09-06T11:00:00.000Z",
              message: "The capability director is active.",
            }
          : null,
      };
    }
    if (descriptor.kind === "search") {
      return {
        ...idle,
        data: {
          matches: [
            {
              ancestryComplete: true,
              ancestry: [
                {
                  id: "issue-10",
                  repository: "Flow-Fly/t3code",
                  number: 10,
                  title: "Capability",
                  url: "https://github.com/Flow-Fly/t3code/issues/10",
                  kind: "capability",
                  state: "open",
                  stateReason: null,
                  updatedAt: "2026-09-05T00:00:00Z",
                  childCount: 1,
                  parentNumber: null,
                  labels: ["workflow:capability"],
                },
                {
                  id: "issue-11",
                  repository: "Flow-Fly/t3code",
                  number: 11,
                  title: "Browse work",
                  url: "https://github.com/Flow-Fly/t3code/issues/11",
                  kind: "ticket",
                  state: "open",
                  stateReason: null,
                  updatedAt: "2026-09-05T00:00:00Z",
                  childCount: 1,
                  parentNumber: 10,
                  labels: ["workflow:ticket"],
                },
              ],
              issue: {
                id: "issue-12",
                repository: "Flow-Fly/t3code",
                number: 12,
                title: "Nested task",
                url: "https://github.com/Flow-Fly/t3code/issues/12",
                kind: "ticket",
                state: "closed",
                stateReason: "completed",
                updatedAt: "2026-09-05T00:00:00Z",
                childCount: 0,
                parentNumber: 11,
                labels: ["workflow:ticket"],
              },
            },
          ],
          hasMore: false,
        },
      };
    }
    if (descriptor.kind === "locate") {
      return {
        ...idle,
        data: {
          ancestryComplete: true,
          ancestry: [
            {
              id: "issue-20",
              repository: query.destinationRepository,
              number: 20,
              title: "New capability",
              url: `https://github.com/${query.destinationRepository}/issues/20`,
              kind: "capability",
              state: "open",
              stateReason: null,
              updatedAt: "2026-09-05T00:00:00Z",
              childCount: 1,
              parentNumber: null,
              labels: ["workflow:capability"],
            },
          ],
          issue: {
            id: "issue-12",
            repository: "Flow-Fly/t3code",
            number: 12,
            title: "Nested task",
            url: "https://github.com/Flow-Fly/t3code/issues/12",
            kind: "ticket",
            state: "closed",
            stateReason: "completed",
            updatedAt: "2026-09-05T00:00:00Z",
            childCount: 0,
            parentNumber: 20,
            labels: ["workflow:ticket"],
          },
        },
      };
    }
    throw new Error(`Unexpected Workflow operation: ${descriptor.kind}`);
  },
}));

import { WorkflowPanel } from "./WorkflowPanel";

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  useWorkflowMapStore.setState({
    repositoryByProject: {},
    focusedRootByContext: {},
    locationByThread: {},
    navigationTargetByThread: {},
    views: {},
  });
  useRightPanelStore.setState({ byThreadKey: {} });
  query.moved = false;
  query.evidence = false;
  query.startReady = false;
  query.recoveryState = false;
  query.externalClaim = false;
  query.heldAfterRefresh = false;
  query.planningKind = null;
  query.startFailure = false;
  query.startCalls.length = 0;
  query.refreshCalls.length = 0;
  query.syncStatus = "fresh";
  query.navigateCalls.length = 0;
  query.threadDetailRefs.length = 0;
  query.directorActivities.length = 0;
  query.directorRefresh.mockClear();
  query.workerHistory = false;
  query.destinationRepository = "Flow-Fly/t3code";
});

afterEach(() => {
  query.calls.length = 0;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("WorkflowPanel browsing", () => {
  it("restores an unloaded per-thread root before the shared project root", async () => {
    const environmentId = EnvironmentId.make("remote-environment");
    const projectId = ProjectId.make("project-draft");
    const threadId = ThreadId.make("return-thread");
    const store = useWorkflowMapStore.getState();
    const sharedContext = "remote-environment:project-draft:flow-fly/t3code";
    const unloadedScope = `${sharedContext}:issue-20`;
    store.focusRoot(sharedContext, "issue-10");
    store.setThreadLocation(
      { environmentId, threadId },
      { projectId, repository: "Flow-Fly/t3code", rootNumber: 20 },
    );
    store.patchView(unloadedScope, {
      positions: { "issue-12": { x: 90, y: 45 } },
      viewport: { x: 12, y: 24, zoom: 0.7 },
    });

    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={environmentId}
          environmentLabel="Remote environment"
          projectId={projectId}
          projectTitle="Draft project"
          planningThreadId={threadId}
          supported
        />,
      );
    });

    expect(
      renderer!.root
        .findByProps({ "aria-label": "Choose another workflow root" })
        .children.join(""),
    ).toContain("#20 New capability");
    expect(useWorkflowMapStore.getState().views[unloadedScope]).toMatchObject({
      positions: { "issue-12": { x: 90, y: 45 } },
      viewport: { x: 12, y: 24, zoom: 0.7 },
    });
    await act(() => renderer?.unmount());
  });

  it("shows a planning failure and restores the capability action", async () => {
    query.planningKind = "map";
    query.startFailure = true;
    const environmentId = EnvironmentId.make("remote-environment");
    const projectId = ProjectId.make("project-draft");
    const planningThreadId = ThreadId.make("planning-thread");
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={environmentId}
          environmentLabel="Remote environment"
          projectId={projectId}
          projectTitle="Draft project"
          planningThreadId={planningThreadId}
          supported
        />,
      );
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const rootButton = renderer!.root
        .findByProps({ "aria-label": "Synchronized workflow outline" })
        .findAllByType("button")
        .find((button) => button.children.join("").includes("#10 Capability"));
      await act(() => rootButton!.props.onClick());
      const start = renderer!.root.findByProps({ "aria-label": "Start workflow specification" });
      await act(() => start.findByType("button").props.onClick());

      expect(start.findByProps({ role: "alert" }).children.join("")).toContain(
        "Planning link is unavailable.",
      );
      expect(start.findByType("button").children.join("")).toBe("Create capability");
      expect(start.findByType("button").props.disabled).toBe(false);
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("starts ready decision work and opens Workflow on the durable destination thread", async () => {
    query.startReady = true;
    const environmentId = EnvironmentId.make("remote-environment");
    const projectId = ProjectId.make("project-draft");
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={environmentId}
          environmentLabel="Remote environment"
          projectId={projectId}
          projectTitle="Draft project"
          supported
        />,
      );
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const issueButton = renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.join("").includes("#11")),
        );
      await act(() => issueButton!.props.onClick());
      const start = renderer!.root.findByProps({ "aria-label": "Start workflow decision" });
      await act(() => start.findByType("button").props.onClick());

      expect(query.startCalls).toEqual([
        {
          environmentId,
          input: {
            projectId,
            repository: "Flow-Fly/t3code",
            rootNumber: 10,
            issueNumber: 11,
            modelSelection: {
              instanceId: "codex-workflow",
              model: "gpt-6-astra",
              options: [{ id: "reasoningEffort", value: "high" }],
            },
          },
        },
      ]);
      expect(query.navigateCalls).toEqual([
        {
          to: "/$environmentId/$threadId",
          params: { environmentId, threadId: "workflow-thread-15" },
        },
      ]);
      expect(Object.values(useRightPanelStore.getState().byThreadKey)[0]).toMatchObject({
        isOpen: true,
        activeSurfaceId: "workflow",
        surfaces: [{ id: "workflow", kind: "workflow" }],
      });
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("shows recovery target and resumes the preserved thread", async () => {
    query.startReady = true;
    query.recoveryState = true;
    const environmentId = EnvironmentId.make("remote-environment");
    const projectId = ProjectId.make("project-draft");
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={environmentId}
          environmentLabel="Remote environment"
          projectId={projectId}
          projectTitle="Draft project"
          supported
        />,
      );
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const issueButton = renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.join("").includes("#11")),
        );
      await act(() => issueButton!.props.onClick());
      const execution = renderer!.root.findByProps({ "aria-label": "Workflow execution" });
      expect(execution.findAllByType("p")[0]?.children.join("")).toContain(
        "remote-environment · project project-draft",
      );
      const resume = execution
        .findAllByType("button")
        .find((button) => button.children.join("") === "Resume");
      await act(() => resume!.props.onClick());

      expect(query.startCalls.at(-1)).toMatchObject({
        environmentId,
        input: {
          projectId,
          repository: "Flow-Fly/t3code",
          rootNumber: 10,
          issueNumber: 11,
          attemptId: "attempt-15",
          action: "resume",
          observation: "attempt-15|submitted|accepted|Flow-Fly|",
        },
      });
      expect(query.navigateCalls.at(-1)).toEqual({
        to: "/$environmentId/$threadId",
        params: { environmentId, threadId: "workflow-thread-15" },
      });
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("shows takeover for an external claim without a local attempt", async () => {
    query.startReady = true;
    query.externalClaim = true;
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={EnvironmentId.make("remote-environment")}
          environmentLabel="Remote environment"
          projectId={ProjectId.make("project-draft")}
          projectTitle="Draft project"
          supported
        />,
      );
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const issueButton = renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.join("").includes("#11")),
        );
      await act(() => issueButton!.props.onClick());

      const execution = renderer!.root.findByProps({ "aria-label": "Workflow execution" });
      expect(
        execution
          .findAllByType("p")
          .some((node) => node.children.join("").includes("outside-owner")),
      ).toBe(true);
      expect(
        execution
          .findAllByType("button")
          .some((button) => button.children.join("") === "Take over here"),
      ).toBe(true);
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("refreshes recovery after Start returns a held attempt", async () => {
    query.startReady = true;
    query.heldAfterRefresh = true;
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={EnvironmentId.make("remote-environment")}
          environmentLabel="Remote environment"
          projectId={ProjectId.make("project-draft")}
          projectTitle="Draft project"
          supported
        />,
      );
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const issueButton = renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.join("").includes("#11")),
        );
      await act(() => issueButton!.props.onClick());
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Start workflow decision" })
          .findByType("button")
          .props.onClick(),
      );

      const execution = renderer!.root.findByProps({ "aria-label": "Workflow execution" });
      expect(
        execution
          .findAllByType("button")
          .some((button) => button.children.join("") === "Open linked work"),
      ).toBe(true);
      expect(
        execution
          .findAllByType("p")
          .some((node) => node.children.join("").includes("current held")),
      ).toBe(true);
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("shows remote worker history and refreshes for the latest qualifying lifecycle activity", async () => {
    query.workerHistory = true;
    const props = {
      environmentId: EnvironmentId.make("remote-environment"),
      environmentLabel: "Remote environment",
      projectId: ProjectId.make("project-draft"),
      projectTitle: "Draft project",
      supported: true,
    };
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const rootButton = renderer!.root
        .findByProps({ "aria-label": "Synchronized workflow outline" })
        .findAllByType("button")
        .find((button) => button.children.join("").includes("#10 Capability"));
      await act(() => rootButton!.props.onClick());

      const director = renderer!.root.findByProps({ "aria-label": "Capability director" });
      expect(director.findAllByType("li")[0]!.findAllByType("p")[0]!.children.join("")).toContain(
        "Ticket #11",
      );
      expect(
        director
          .findAllByType("li")[0]!
          .findAllByType("p")
          .map((paragraph) => paragraph.children.join(""))
          .join(" "),
      ).toContain("write reservation released");
      expect(director.findAllByType("h4").map((heading) => heading.children.join(""))).toEqual([
        "Worker history",
        "Review history",
        "Resolution history",
      ]);
      expect(
        director
          .findAllByType("li")
          .map((item) => item.children.join(""))
          .join(" "),
      ).toContain("dismissed: Existing copy is intentional.");
      expect(
        director
          .findAllByType("p")
          .map((paragraph) => paragraph.children.join(""))
          .join(" "),
      ).toContain("comment-uncertain");
      expect(
        query.calls.some(
          (call) =>
            call.kind === "directorStatus" &&
            (call.request as { environmentId?: string }).environmentId === "remote-environment",
        ),
      ).toBe(true);
      expect(query.threadDetailRefs).toContainEqual({
        environmentId: "remote-environment",
        threadId: "director-thread",
      });

      query.directorActivities.push(
        {
          id: "worker-lifecycle-1",
          kind: "task.updated",
          payload: { taskId: "provider-worker-1", timelineBypass: true, status: "idle" },
          createdAt: "2026-09-06T11:01:00.000Z",
        },
        {
          id: "ordinary-activity-1",
          kind: "turn.plan.updated",
          payload: {},
          createdAt: "2026-09-06T11:01:01.000Z",
        },
      );
      await act(() => renderer!.update(<WorkflowPanel {...props} />));

      expect(query.directorRefresh).toHaveBeenCalledTimes(1);

      query.directorActivities.push({
        id: "workflow-resolution-1",
        kind: "tool.completed",
        summary: "t3code · workflow_record_review_dispositions",
        payload: { itemType: "mcp_tool_call", status: "completed" },
        createdAt: "2026-09-06T11:01:02.000Z",
      });
      await act(() => renderer!.update(<WorkflowPanel {...props} />));

      expect(query.directorRefresh).toHaveBeenCalledTimes(2);
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("browses an unsent draft through read queries without launching a provider", async () => {
    const draft = {
      environmentId: EnvironmentId.make("remote-environment"),
      projectId: ProjectId.make("project-draft"),
      threadId: ThreadId.make("draft-thread"),
      session: null,
    };
    let renderer: ReactTestRenderer | undefined;

    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={draft.environmentId}
          environmentLabel="Remote environment"
          projectId={draft.projectId}
          projectTitle="Draft project"
          supported
        />,
      );
    });

    try {
      const root = renderer!.root
        .findByProps({ "aria-label": "Workflow roots" })
        .findAllByType("button")[0]!;
      await act(() => root.props.onClick());

      const issueButton = renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.join("").includes("#11")),
        );
      expect(issueButton).toBeDefined();
      await act(() => issueButton!.props.onClick());

      expect(issueButton!.props["aria-current"]).toBe("true");
      expect(renderer!.root.findByType("article").findByType("h2").children.join("")).toContain(
        "#11 Browse work",
      );
      expect(renderer!.root.findByType("article").findAllByType("li")).toHaveLength(2);
      expect(new Set(query.calls.map((call) => call.kind))).toEqual(
        new Set(["repositories", "roots", "sync", "children", "detail", "directorStatus"]),
      );
      expect(query.calls.find((call) => call.kind === "repositories")?.request).toEqual({
        environmentId: draft.environmentId,
        input: { projectId: draft.projectId },
      });
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("explains an empty frontier and keeps approval provenance distinct in details", async () => {
    query.evidence = true;
    const props = {
      environmentId: EnvironmentId.make("remote-environment"),
      environmentLabel: "Remote environment",
      projectId: ProjectId.make("project-draft"),
      projectTitle: "Draft project",
      supported: true,
    };
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });

    try {
      await act(() =>
        renderer!.root
          .findByProps({ "aria-label": "Workflow roots" })
          .findAllByType("button")[0]!
          .props.onClick(),
      );
      const frontier = renderer!.root.findByProps({ "aria-label": "Workflow frontier" });
      const frontierText = frontier.findAllByType("span").map((span) => span.children.join(""));
      expect(frontierText).toContain("Frontier empty");
      expect(frontierText.join(" ")).toContain("prerequisites are blocking");

      const issueButton = renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.join("").includes("#11")),
        );
      await act(() => issueButton!.props.onClick());

      const articleText = renderer!.root
        .findByType("article")
        .findAllByType("p")
        .flatMap((node) => node.children.map(String));
      const readiness = renderer!.root.findByProps({ "aria-label": "Readiness evidence" });
      expect(readiness.findAllByType("a")[0]?.children.join("")).toContain(
        "owner authority is not verified",
      );
      const normalizedArticleText = articleText.join(" ").replace(/\s+/g, " ");
      expect(normalizedArticleText).toContain(
        "Source is reported by the record; this query did not independently verify it.",
      );
      expect(normalizedArticleText).toContain("authority reported");
      expect(
        renderer!.root
          .findByProps({ "aria-label": "Workflow evidence ledger" })
          .findAllByType("summary")
          .map((summary) => summary.children.join("")),
      ).toEqual(["Approved snapshot"]);
      expect(
        renderer!.root
          .findByProps({ "aria-label": "Workflow evidence ledger" })
          .findAllByType("a")
          .some((link) => link.children.join("").includes("Review required")),
      ).toBe(true);
    } finally {
      await act(() => renderer?.unmount());
    }
  });

  it("reveals collapsed ancestry and restores map interaction state after remount", async () => {
    const props = {
      environmentId: EnvironmentId.make("remote-environment"),
      environmentLabel: "Remote environment",
      projectId: ProjectId.make("project-draft"),
      projectTitle: "Draft project",
      supported: true,
    };
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });
    await act(() =>
      renderer!.root
        .findByProps({ "aria-label": "Workflow roots" })
        .findAllByType("button")[0]!
        .props.onClick(),
    );

    const search = renderer!.root.findByProps({ "aria-label": "Search this workflow" });
    await act(() => search.props.onChange({ target: { value: "Nested" } }));
    expect(query.calls.filter((call) => call.kind === "search")).toHaveLength(0);
    await act(() => search.props.onCompositionStart());
    await act(() =>
      renderer!.root.findByProps({ role: "search" }).props.onSubmit({ preventDefault: vi.fn() }),
    );
    expect(query.calls.filter((call) => call.kind === "search")).toHaveLength(0);
    await act(() => search.props.onCompositionEnd());
    const scope = Object.keys(useWorkflowMapStore.getState().views)[0]!;
    await act(() =>
      useWorkflowMapStore.getState().patchView(scope, {
        viewport: { x: 999, y: 999, zoom: 1 },
      }),
    );
    await act(() =>
      renderer!.root.findByProps({ role: "search" }).props.onSubmit({ preventDefault: vi.fn() }),
    );
    expect(query.calls.some((call) => call.kind === "search")).toBe(true);
    const result = renderer!.root
      .findByProps({ "aria-label": "Workflow search results" })
      .findAllByType("button")[0]!;
    await act(() => result.props.onClick());
    expect(useWorkflowMapStore.getState().views[scope]!.viewport).not.toEqual({
      x: 999,
      y: 999,
      zoom: 1,
    });
    const revealedViewport = useWorkflowMapStore.getState().views[scope]!.viewport;
    await act(() =>
      renderer!.root.findByProps({ "aria-label": "Refresh workflow map" }).props.onClick(),
    );
    expect(useWorkflowMapStore.getState().views[scope]!.viewport).toEqual(revealedViewport);
    const outline = renderer!.root.findByProps({ "aria-label": "Synchronized workflow outline" });
    expect(
      outline.findAllByType("button").some((button) => button.children.join("").includes("#12")),
    ).toBe(true);
    const breadcrumbs = renderer!.root.findByProps({ "aria-label": "Workflow breadcrumbs" });
    expect(breadcrumbs.findAllByType("button").map((button) => button.children.join(""))).toEqual([
      "#10 Capability",
      "#11 Browse work",
      "#12 Nested task",
    ]);

    const selectedPosition = useWorkflowMapStore.getState().views[scope]!.positions["issue-12"]!;
    await act(() =>
      renderer!.root.findByProps({ "aria-label": "Move selected node right" }).props.onClick(),
    );
    expect(useWorkflowMapStore.getState().views[scope]!.positions["issue-12"]).toEqual({
      x: selectedPosition.x + 16,
      y: selectedPosition.y,
    });

    await act(() => renderer!.root.findByProps({ "aria-label": "Zoom in" }).props.onClick());
    const canvas = renderer!.root.findByProps({ "aria-label": "Workflow map canvas" });
    const viewportBeforePan = useWorkflowMapStore.getState().views[scope]!.viewport;
    await act(() => {
      canvas.props.onPointerDown({
        target: { closest: () => null },
        currentTarget: { setPointerCapture: () => undefined },
        pointerId: 1,
        clientX: 10,
        clientY: 10,
      });
      canvas.props.onPointerMove({ clientX: 35, clientY: 28 });
      canvas.props.onPointerUp({ clientX: 35, clientY: 28 });
    });
    expect(useWorkflowMapStore.getState().views[scope]?.viewport).toMatchObject({
      x: viewportBeforePan.x + 25,
      y: viewportBeforePan.y + 18,
    });

    const node = renderer!.root.findAllByProps({ "data-workflow-node": true })[0]!;
    const rootPosition = useWorkflowMapStore.getState().views[scope]!.positions["issue-10"]!;
    await act(() => {
      node.props.onPointerDown({
        currentTarget: { setPointerCapture: () => undefined },
        stopPropagation: () => undefined,
        pointerId: 2,
        clientX: 50,
        clientY: 50,
      });
      node.props.onPointerMove({ clientX: 70, clientY: 80 });
      node.props.onPointerUp({ clientX: 70, clientY: 80 });
    });
    expect(useWorkflowMapStore.getState().views[scope]!.positions["issue-10"]).not.toEqual(
      rootPosition,
    );

    await act(() =>
      renderer!.root.findByProps({ "aria-label": "Fit workflow map" }).props.onClick(),
    );
    expect(useWorkflowMapStore.getState().views[scope]!.viewport.zoom).toBeLessThanOrEqual(1);
    await act(() =>
      renderer!.root.findByProps({ "aria-label": "Reset workflow layout" }).props.onClick(),
    );
    expect(useWorkflowMapStore.getState().views[scope]!.viewport).toEqual({ x: 0, y: 0, zoom: 1 });
    await act(() => renderer?.unmount());
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });
    expect(renderer!.root.findAllByProps({ "aria-label": "Workflow roots" })).toHaveLength(0);
    expect(renderer!.root.findByProps({ "aria-label": "Workflow map canvas" })).toBeDefined();
    expect(useWorkflowMapStore.getState().views[scope]?.selectedId).toBe("issue-12");
    await act(() => renderer?.unmount());
  });

  it("recovers a moved selection after remount across repository casing", async () => {
    const props = {
      environmentId: EnvironmentId.make("remote-environment"),
      environmentLabel: "Remote environment",
      projectId: ProjectId.make("project-draft"),
      projectTitle: "Draft project",
      supported: true,
    };
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });
    await act(() =>
      renderer!.root
        .findByProps({ "aria-label": "Workflow roots" })
        .findAllByType("button")[0]!
        .props.onClick(),
    );

    const search = renderer!.root.findByProps({ "aria-label": "Search this workflow" });
    await act(() => search.props.onChange({ target: { value: "Nested" } }));
    await act(() =>
      renderer!.root.findByProps({ role: "search" }).props.onSubmit({ preventDefault: vi.fn() }),
    );
    await act(() =>
      renderer!.root
        .findByProps({ "aria-label": "Workflow search results" })
        .findAllByType("button")[0]!
        .props.onClick(),
    );
    const oldScope = Object.keys(useWorkflowMapStore.getState().views)[0]!;
    await act(() =>
      useWorkflowMapStore.getState().patchView(oldScope, {
        viewport: { x: 45, y: 67, zoom: 0.75 },
      }),
    );
    expect(useWorkflowMapStore.getState().views[oldScope]!.selectedIssue).toEqual({
      id: "issue-12",
      repository: "Flow-Fly/t3code",
      number: 12,
    });

    query.moved = true;
    await act(() => renderer!.unmount());
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });
    const recovery = renderer!.root
      .findAllByType("button")
      .find((button) => button.children.join("") === "Find current context");
    expect(recovery).toBeDefined();
    await act(() => recovery!.props.onClick());
    const locateCall = query.calls.find((call) => call.kind === "locate");
    expect(locateCall?.request).toMatchObject({
      input: { repository: "Flow-Fly/t3code", id: "issue-12", number: 12 },
    });
    await act(() =>
      renderer!.root
        .findAllByType("button")
        .find((button) => button.children.join("").includes("Open current root #20"))!
        .props.onClick(),
    );

    expect(
      renderer!.root
        .findByProps({ "aria-label": "Choose another workflow root" })
        .children.join(""),
    ).toContain("#20 New capability");
    expect(
      renderer!.root
        .findByProps({ "aria-label": "Synchronized workflow outline" })
        .findAllByType("button")
        .some((button) => button.children.join("").includes("#12 Nested task")),
    ).toBe(true);
    expect(useWorkflowMapStore.getState().views[oldScope]!.viewport).toEqual({
      x: 45,
      y: 67,
      zoom: 0.75,
    });
    expect(renderer!.root.findByType("select").props.value).toBe("flow-fly/t3code");
    expect(
      Object.values(useWorkflowMapStore.getState().views).some(
        (view) =>
          view.selectedId === "issue-12" && view !== useWorkflowMapStore.getState().views[oldScope],
      ),
    ).toBe(true);
    await act(() => renderer?.unmount());
  });

  it("opens a located root from a repository absent from git remotes", async () => {
    const props = {
      environmentId: EnvironmentId.make("remote-environment"),
      environmentLabel: "Remote environment",
      projectId: ProjectId.make("project-draft"),
      projectTitle: "Draft project",
      supported: true,
    };
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(<WorkflowPanel {...props} />);
    });
    await act(() =>
      renderer!.root
        .findByProps({ "aria-label": "Workflow roots" })
        .findAllByType("button")[0]!
        .props.onClick(),
    );
    const search = renderer!.root.findByProps({ "aria-label": "Search this workflow" });
    await act(() => search.props.onChange({ target: { value: "Nested" } }));
    await act(() =>
      renderer!.root.findByProps({ role: "search" }).props.onSubmit({ preventDefault: vi.fn() }),
    );
    await act(() =>
      renderer!.root
        .findByProps({ "aria-label": "Workflow search results" })
        .findAllByType("button")[0]!
        .props.onClick(),
    );
    const oldScope = Object.keys(useWorkflowMapStore.getState().views)[0]!;
    await act(() =>
      useWorkflowMapStore.getState().patchView(oldScope, {
        viewport: { x: 21, y: 34, zoom: 0.6 },
      }),
    );

    query.destinationRepository = "outside/repository";
    query.moved = true;
    await act(() => renderer!.update(<WorkflowPanel {...props} />));
    await act(() =>
      renderer!.root
        .findAllByType("button")
        .find((button) => button.children.join("") === "Find current context")!
        .props.onClick(),
    );
    await act(() =>
      renderer!.root
        .findAllByType("button")
        .find((button) => button.children.join("").includes("Open current root #20"))!
        .props.onClick(),
    );

    const picker = renderer!.root.findByType("select");
    expect(picker.props.value).toBe("outside/repository");
    expect(
      picker.findAllByType("option").some((option) => option.children.join("").includes("linked")),
    ).toBe(true);
    expect(
      query.calls.some(
        (call) =>
          call.kind === "roots" &&
          (call.request as { input?: { repository?: string } }).input?.repository ===
            "outside/repository",
      ),
    ).toBe(true);
    expect(
      renderer!.root
        .findByProps({ "aria-label": "Choose another workflow root" })
        .children.join(""),
    ).toContain("#20 New capability");
    expect(useWorkflowMapStore.getState().views[oldScope]!.viewport).toEqual({
      x: 21,
      y: 34,
      zoom: 0.6,
    });
    await act(() => renderer?.unmount());
  });

  it("keeps cached workflow data visible while sync is stale and retries from the notice", async () => {
    query.syncStatus = "rate-limited";
    let renderer: ReactTestRenderer | undefined;
    await act(() => {
      renderer = create(
        <WorkflowPanel
          environmentId={EnvironmentId.make("remote-environment")}
          environmentLabel="Remote environment"
          projectId={ProjectId.make("project-draft")}
          projectTitle="Draft project"
          supported
        />,
      );
    });

    const notice = renderer!.root.findByProps({ role: "status" });
    expect(notice.findByProps({ className: "font-medium" }).children.join("")).toContain(
      "GitHub rate limit reached",
    );
    expect(renderer!.root.findByProps({ "aria-label": "Workflow roots" })).toBeDefined();
    await act(() => notice.findByType("button").props.onClick());
    expect(query.refreshCalls).toEqual([
      {
        environmentId: "remote-environment",
        input: { projectId: "project-draft", repository: "flow-fly/t3code" },
      },
    ]);
    await act(() => renderer?.unmount());
  });
});
