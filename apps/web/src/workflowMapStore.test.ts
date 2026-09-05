import { beforeEach, describe, expect, it } from "vite-plus/test";

import {
  selectWorkflowMapView,
  useWorkflowMapStore,
  workflowMapContextKey,
  workflowMapScopeKey,
} from "./workflowMapStore";

beforeEach(() =>
  useWorkflowMapStore.setState({ repositoryByProject: {}, focusedRootByContext: {}, views: {} }),
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
});
