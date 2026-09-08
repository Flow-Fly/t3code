import { beforeEach, describe, expect, it } from "vite-plus/test";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";

import {
  selectWorkflowMapView,
  useWorkflowMapStore,
  workflowMapContextKey,
  workflowMapScopeKey,
} from "./workflowMapStore";

beforeEach(() =>
  useWorkflowMapStore.setState({
    repositoryByProject: {},
    focusedRootByContext: {},
    locationByThread: {},
    navigationTargetByThread: {},
    views: {},
  }),
);

describe("Workflow map view persistence", () => {
  it("separates graph views by environment, project, repository, and root", () => {
    const local = workflowMapContextKey({
      environmentId: "local",
      projectId: "project",
      repository: "one/repo",
    });
    const remote = workflowMapContextKey({
      environmentId: "remote",
      projectId: "project",
      repository: "one/repo",
    });
    const localScope = workflowMapScopeKey(local, "issue-root");
    const remoteScope = workflowMapScopeKey(remote, "issue-root");

    useWorkflowMapStore.getState().focusRoot(local, "issue-root");
    useWorkflowMapStore.getState().selectRepository("local:project", "one/repo");
    useWorkflowMapStore.getState().patchView(localScope, {
      selectedId: "issue-child",
      expanded: ["issue-child"],
      positions: { "issue-child": { x: 42, y: 84 } },
      viewport: { x: -10, y: 20, zoom: 0.75 },
    });

    expect(useWorkflowMapStore.getState().focusedRootByContext[local]).toBe("issue-root");
    expect(useWorkflowMapStore.getState().repositoryByProject["local:project"]).toBe("one/repo");
    expect(selectWorkflowMapView(useWorkflowMapStore.getState().views, localScope)).toMatchObject({
      selectedId: "issue-child",
      expanded: ["issue-child"],
      positions: { "issue-child": { x: 42, y: 84 } },
      viewport: { x: -10, y: 20, zoom: 0.75 },
    });
    expect(
      selectWorkflowMapView(useWorkflowMapStore.getState().views, remoteScope).selectedId,
    ).toBeNull();
  });

  it("uses one persisted view for GitHub repository casing variants", () => {
    const lower = workflowMapContextKey({
      environmentId: "remote",
      projectId: "project",
      repository: "flow-fly/t3code",
    });
    const canonical = workflowMapContextKey({
      environmentId: "remote",
      projectId: "project",
      repository: "Flow-Fly/T3Code",
    });

    expect(canonical).toBe(lower);
  });

  it("keeps roots and pending destinations scoped to canonical environment threads", () => {
    const local = scopeThreadRef(EnvironmentId.make("local"), ThreadId.make("thread-1"));
    const remote = scopeThreadRef(EnvironmentId.make("remote"), ThreadId.make("thread-1"));
    const store = useWorkflowMapStore.getState();

    store.setThreadLocation(local, {
      projectId: "project",
      repository: "one/repo",
      rootNumber: 10,
    });
    store.setNavigationTarget(remote, {
      requestId: "new-request",
      projectId: "other-project",
      repository: "two/repo",
      rootNumber: 20,
      issueNumber: 21,
      providerThreadId: "native-child",
    });
    store.clearNavigationTarget(remote, "old-request");

    expect(useWorkflowMapStore.getState().locationByThread).toEqual({
      "local:thread-1": { projectId: "project", repository: "one/repo", rootNumber: 10 },
      "remote:thread-1": { projectId: "other-project", repository: "two/repo", rootNumber: 20 },
    });
    expect(useWorkflowMapStore.getState()).toMatchObject({
      navigationTargetByThread: {
        "remote:thread-1": { requestId: "new-request", issueNumber: 21 },
      },
    });
  });
});
