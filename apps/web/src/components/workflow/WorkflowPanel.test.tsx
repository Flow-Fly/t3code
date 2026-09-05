import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

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
  },
}));

vi.mock("~/state/query", () => ({
  useEnvironmentQuery: (descriptor: { kind: string } | null) => {
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
              number: 10,
              title: "Capability",
              url: "https://github.com/Flow-Fly/t3code/issues/10",
              kind: "capability",
              state: "open",
              stateReason: null,
              updatedAt: "2026-09-05T00:00:00Z",
              childCount: 1,
            },
          ],
        },
      };
    }
    if (descriptor.kind === "children") {
      return {
        ...idle,
        data: {
          parentNumber: 10,
          children: [
            {
              number: 11,
              title: "Browse work",
              url: "https://github.com/Flow-Fly/t3code/issues/11",
              kind: "ticket",
              state: "open",
              stateReason: null,
              updatedAt: "2026-09-05T00:00:00Z",
              childCount: 0,
            },
          ],
        },
      };
    }
    if (descriptor.kind === "detail") {
      return {
        ...idle,
        data: {
          number: 11,
          title: "Browse work",
          url: "https://github.com/Flow-Fly/t3code/issues/11",
          kind: "ticket",
          state: "open",
          stateReason: null,
          updatedAt: "2026-09-05T00:00:00Z",
          childCount: 0,
          body: "## Summary\n\nBrowse work without starting an agent.",
          labels: ["wayfinder:task"],
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
      const expand = renderer!.root.findByProps({ "aria-label": "Expand Capability" });
      await act(() => expand.props.onClick());

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
});
