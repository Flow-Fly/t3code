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
  return { calls, descriptor, moved: false, destinationRepository: "Flow-Fly/t3code" };
});

vi.mock("~/state/workflow", () => ({
  workflowEnvironment: {
    repositories: (request: unknown) => query.descriptor("repositories", request),
    roots: (request: unknown) => query.descriptor("roots", request),
    children: (request: unknown) => query.descriptor("children", request),
    issueDetail: (request: unknown) => query.descriptor("detail", request),
    search: (request: unknown) => query.descriptor("search", request),
    locate: (request: unknown) => query.descriptor("locate", request),
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
                    kind: "capability",
                    state: "open",
                    stateReason: null,
                    updatedAt: "2026-09-05T00:00:00Z",
                    childCount: 1,
                    parentNumber: null,
                    labels: ["workflow:capability"],
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
    if (descriptor.kind === "children") {
      const parentNumber = (descriptor.request as { input?: { parentNumber?: number } })?.input
        ?.parentNumber;
      return {
        ...idle,
        data: {
          parentNumber: parentNumber ?? 10,
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
                    kind: "ticket",
                    state: parentNumber === 11 || parentNumber === 20 ? "closed" : "open",
                    stateReason: parentNumber === 11 || parentNumber === 20 ? "completed" : null,
                    updatedAt: "2026-09-05T00:00:00Z",
                    childCount: parentNumber === 11 || parentNumber === 20 ? 0 : 1,
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
  useWorkflowMapStore.setState({ repositoryByProject: {}, focusedRootByContext: {}, views: {} });
  query.moved = false;
  query.destinationRepository = "Flow-Fly/t3code";
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
});
