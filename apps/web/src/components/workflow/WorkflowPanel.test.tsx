import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { useWorkflowMapStore } from "~/workflowMapStore";

const query = vi.hoisted(() => {
  const calls = new Array<{ kind: string; request: unknown }>();
  const descriptor = (kind: string, request: unknown) => {
    calls.push({ kind, request });
    return { kind, request };
  };
  return { calls, descriptor };
});

vi.mock("~/state/workflow", () => ({
  workflowEnvironment: {
    repositories: (request: unknown) => query.descriptor("repositories", request),
    roots: (request: unknown) => query.descriptor("roots", request),
    children: (request: unknown) => query.descriptor("children", request),
    issueDetail: (request: unknown) => query.descriptor("detail", request),
    search: (request: unknown) => query.descriptor("search", request),
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
          repositories: [{ nameWithOwner: "Flow-Fly/t3code", remoteNames: ["origin"] }],
        },
      };
    }
    if (descriptor.kind === "roots") {
      return {
        ...idle,
        data: {
          repository: "Flow-Fly/t3code",
          roots: [
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
          ],
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
          children: [
            {
              id: parentNumber === 11 ? "issue-12" : "issue-11",
              repository: "Flow-Fly/t3code",
              number: parentNumber === 11 ? 12 : 11,
              title: parentNumber === 11 ? "Nested task" : "Browse work",
              url: `https://github.com/Flow-Fly/t3code/issues/${parentNumber === 11 ? 12 : 11}`,
              kind: "ticket",
              state: parentNumber === 11 ? "closed" : "open",
              stateReason: parentNumber === 11 ? "completed" : null,
              updatedAt: "2026-09-05T00:00:00Z",
              childCount: parentNumber === 11 ? 0 : 1,
              parentNumber: parentNumber ?? 10,
              labels: ["workflow:ticket"],
            },
          ],
        },
      };
    }
    if (descriptor.kind === "detail") {
      return {
        ...idle,
        data: {
          id: "issue-11",
          repository: "Flow-Fly/t3code",
          number: 11,
          title: "Browse work",
          url: "https://github.com/Flow-Fly/t3code/issues/11",
          kind: "ticket",
          state: "open",
          stateReason: null,
          updatedAt: "2026-09-05T00:00:00Z",
          childCount: 0,
          parentNumber: 10,
          body: "## Summary\n\nBrowse work without starting an agent.\n\n## Source map\n\n[Map](https://github.com/Flow-Fly/t3code/issues/1)\n\n## Notes\n\n[Related](https://github.com/Flow-Fly/t3code/issues/2)",
          labels: ["wayfinder:task"],
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
        },
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
    throw new Error(`Unexpected Workflow operation: ${descriptor.kind}`);
  },
}));

import { WorkflowPanel } from "./WorkflowPanel";

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  useWorkflowMapStore.setState({ repositoryByProject: {}, focusedRootByContext: {}, views: {} });
});

afterEach(() => {
  query.calls.length = 0;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("WorkflowPanel browsing", () => {
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
        .findByType("button");
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
        new Set(["repositories", "roots", "children", "detail"]),
      );
      expect(query.calls.find((call) => call.kind === "repositories")?.request).toEqual({
        environmentId: draft.environmentId,
        input: { projectId: draft.projectId },
      });
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
        .findByType("button")
        .props.onClick(),
    );

    const search = renderer!.root.findByProps({ "aria-label": "Search this workflow" });
    await act(() => search.props.onChange({ target: { value: "Nested" } }));
    const result = renderer!.root
      .findByProps({ "aria-label": "Workflow search results" })
      .findAllByType("button")[0]!;
    await act(() => result.props.onClick());
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

    await act(() => renderer!.root.findByProps({ "aria-label": "Zoom in" }).props.onClick());
    const scope = Object.keys(useWorkflowMapStore.getState().views)[0]!;
    const canvas = renderer!.root.findByProps({ "aria-label": "Workflow map canvas" });
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
    expect(useWorkflowMapStore.getState().views[scope]?.viewport).toMatchObject({ x: 25, y: 18 });

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
});
