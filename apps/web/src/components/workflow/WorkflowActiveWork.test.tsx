import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type WorkflowActiveWorkResult,
} from "@t3tools/contracts";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";
import * as React from "react";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { WorkflowActiveWork } from "./WorkflowActiveWork";
import { useRightPanelStore } from "~/rightPanelStore";
import { useWorkflowMapStore } from "~/workflowMapStore";

const state = vi.hoisted(() => ({
  shells: [] as Array<Record<string, unknown>>,
  navigateCalls: [] as unknown[],
}));

vi.mock("~/state/entities", () => ({ useThreadShells: () => state.shells }));
vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => (request: unknown) => {
    state.navigateCalls.push(request);
    return Promise.resolve();
  },
}));
vi.mock("~/state/workflow", () => ({
  workflowEnvironment: {
    activeWork: (request: { input: { cursor?: unknown } }) =>
      request.input.cursor ? secondPageAtom : summaryAtom,
  },
}));
vi.mock("~/components/ui/popover", () => ({
  Popover: (props: { children: React.ReactNode; onOpenChange: (open: boolean) => void }) => (
    <div>
      <button type="button" onClick={() => props.onOpenChange(true)}>
        Open test menu
      </button>
      {props.children}
    </div>
  ),
  PopoverPopup: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  PopoverTrigger: ({ render }: { render: React.ReactNode }) => render,
}));
vi.mock("~/components/ui/button", () => ({
  Button: (props: { children: React.ReactNode; onClick?: () => void }) => (
    <button type="button" onClick={props.onClick}>
      {props.children}
    </button>
  ),
}));

const environmentId = EnvironmentId.make("environment-1");
const timestamp = "2026-09-09T00:00:00.000Z";
const entryCursor = {
  directorCreatedAt: timestamp,
  directorId: "director-1",
  entryOrder: 0,
  entryCreatedAt: timestamp,
  entryId: "director:director-1",
};
let response: WorkflowActiveWorkResult = {
  environmentId,
  nextCursor: null,
  refreshedAt: timestamp,
  entries: [
    {
      entryId: "director:director-1",
      kind: "director",
      environmentId,
      projectId: ProjectId.make("project-1"),
      projectTitle: "Synthetic project",
      repository: "Flow-Fly/t3code",
      rootNumber: 10,
      capabilityNumber: 10,
      issueNumber: 10,
      directorId: "director-1",
      ownerThreadId: ThreadId.make("thread-1"),
      navigationThreadId: ThreadId.make("thread-1"),
      title: null,
      providerThreadId: null,
      activity: "waiting",
      unresolved: true,
      updatedAt: timestamp,
      cursor: entryCursor,
    },
  ],
};
const summaryAtom = Atom.make(() => AsyncResult.success(response));
let secondPageResponse: WorkflowActiveWorkResult = { ...response, entries: [], nextCursor: null };
const secondPageAtom = Atom.make(() => AsyncResult.success(secondPageResponse));
let registry = AtomRegistry.make();
let renderer: ReactTestRenderer | null = null;

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = null;
  registry.dispose();
  registry = AtomRegistry.make();
  state.navigateCalls.length = 0;
  useWorkflowMapStore.setState({ locationByThread: {}, navigationTargetByThread: {} });
  useRightPanelStore.setState({ byThreadKey: {} });
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("refreshes visible work during uninterrupted selected-environment activity", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", {
    setTimeout: globalThis.setTimeout,
    clearTimeout: globalThis.clearTimeout,
  });
  state.shells = [{ id: "thread-1", environmentId, updatedAt: timestamp }];
  const render = () => (
    <RegistryContext.Provider value={registry}>
      <WorkflowActiveWork environmentId={environmentId} environmentLabel="Synthetic environment" />
    </RegistryContext.Provider>
  );
  await act(() => {
    renderer = create(render());
  });
  await act(() => {
    renderer!.root
      .findAllByType("button")
      .find((button) => button.children.includes("Open test menu"))!
      .props.onClick();
  });
  expect(JSON.stringify(renderer!.toJSON())).toContain("Waiting");

  response = {
    ...response,
    entries: response.entries.map((entry) => ({ ...entry, activity: "running" })),
  };
  for (let index = 1; index <= 10; index++) {
    await act(() => {
      vi.advanceTimersByTime(100);
    });
    state.shells = [
      {
        id: "thread-1",
        environmentId,
        updatedAt: `2026-09-09T00:00:00.${String(index).padStart(3, "0")}Z`,
      },
    ];
    await act(() => {
      renderer!.update(render());
    });
  }

  expect(JSON.stringify(renderer!.toJSON())).toContain("Running");
});

it("merges additional pages by stable entry identity", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", {
    setTimeout: globalThis.setTimeout,
    clearTimeout: globalThis.clearTimeout,
  });
  const duplicate = { ...response.entries[0]!, title: "Duplicate entry" };
  const uniqueCursor = {
    ...entryCursor,
    entryId: "director:director-2",
    directorId: "director-2",
  };
  const unique = {
    ...duplicate,
    entryId: uniqueCursor.entryId,
    directorId: uniqueCursor.directorId,
    title: "Unique second-page entry",
    cursor: uniqueCursor,
  };
  response = { ...response, entries: [duplicate], nextCursor: entryCursor };
  secondPageResponse = { ...response, entries: [duplicate, unique], nextCursor: null };
  state.shells = [];

  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <WorkflowActiveWork environmentId={environmentId} environmentLabel="Environment" />
      </RegistryContext.Provider>,
    );
  });
  await act(() => {
    renderer!.root
      .findAllByType("button")
      .find((button) => button.children.includes("Open test menu"))!
      .props.onClick();
  });
  await act(() => {
    renderer!.root
      .findAllByType("button")
      .find((button) => button.children.join("").includes("Load more active work"))!
      .props.onClick();
  });

  const rendered = JSON.stringify(renderer!.toJSON());
  expect(rendered.match(/Duplicate entry/g)).toHaveLength(1);
  expect(rendered).toContain("Unique second-page entry");
});

it("routes a native child through its owning T3 thread and saves its Workflow context", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", {
    setTimeout: globalThis.setTimeout,
    clearTimeout: globalThis.clearTimeout,
  });
  response = {
    ...response,
    nextCursor: null,
    entries: [
      {
        ...response.entries[0]!,
        kind: "worker",
        title: "Native implementation worker",
        issueNumber: 11,
        providerThreadId: "provider-child-not-a-t3-thread",
        navigationThreadId: ThreadId.make("owning-t3-thread"),
      },
    ],
  };

  await act(() => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <WorkflowActiveWork environmentId={environmentId} environmentLabel="Environment" />
      </RegistryContext.Provider>,
    );
  });
  await act(() => {
    renderer!.root
      .findAllByType("button")
      .find((button) => button.children.includes("Open test menu"))!
      .props.onClick();
  });
  await act(() => {
    renderer!.root
      .findAllByType("button")
      .find((button) =>
        button
          .findAllByType("span")
          .some((span) => span.children.includes("Native implementation worker")),
      )!
      .props.onClick();
  });

  expect(state.navigateCalls).toContainEqual(
    expect.objectContaining({ params: { environmentId, threadId: "owning-t3-thread" } }),
  );
  expect(
    useWorkflowMapStore.getState().navigationTargetByThread[`${environmentId}:owning-t3-thread`],
  ).toMatchObject({
    issueNumber: 11,
    activeWorkEntryId: "director:director-1",
    providerThreadId: "provider-child-not-a-t3-thread",
  });
});
