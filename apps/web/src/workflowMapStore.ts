import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";

import { resolveStorage } from "./lib/storage";
import type { WorkflowPoint, WorkflowViewport } from "./components/workflow/WorkflowMap.logic";

export interface WorkflowMapView {
  selectedId: string | null;
  selectedIssue: { id: string; repository: string; number: number } | null;
  expanded: string[];
  openFolds: string[];
  positions: Record<string, WorkflowPoint>;
  viewport: WorkflowViewport;
}

export interface WorkflowThreadLocation {
  projectId: string;
  repository: string;
  rootNumber: number;
}

export interface WorkflowNavigationTarget extends WorkflowThreadLocation {
  requestId: string;
  issueNumber: number;
  providerThreadId: string | null;
  activeWorkEntryId?: string;
}

const EMPTY_VIEW: WorkflowMapView = {
  selectedId: null,
  selectedIssue: null,
  expanded: [],
  openFolds: [],
  positions: {},
  viewport: { x: 0, y: 0, zoom: 1 },
};

interface WorkflowMapStoreState {
  repositoryByProject: Record<string, string>;
  focusedRootByContext: Record<string, string>;
  locationByThread: Record<string, WorkflowThreadLocation>;
  navigationTargetByThread: Record<string, WorkflowNavigationTarget>;
  views: Record<string, WorkflowMapView>;
  selectRepository: (projectScope: string, repository: string | null) => void;
  focusRoot: (context: string, rootId: string) => void;
  setThreadLocation: (ref: ScopedThreadRef, location: WorkflowThreadLocation) => void;
  clearThreadLocation: (ref: ScopedThreadRef) => void;
  setNavigationTarget: (ref: ScopedThreadRef, target: WorkflowNavigationTarget) => void;
  clearNavigationTarget: (ref: ScopedThreadRef, requestId: string) => void;
  patchView: (scope: string, patch: Partial<WorkflowMapView>) => void;
  toggleExpanded: (scope: string, id: string) => void;
  toggleFold: (scope: string, id: string) => void;
}

const toggle = (values: readonly string[], value: string) =>
  values.includes(value) ? values.filter((entry) => entry !== value) : [...values, value];

export const workflowMapContextKey = (input: {
  environmentId: string;
  projectId: string;
  repository: string;
}) => `${input.environmentId}:${input.projectId}:${input.repository.toLowerCase()}`;

export const workflowMapScopeKey = (context: string, rootId: string) => `${context}:${rootId}`;

export const useWorkflowMapStore = create<WorkflowMapStoreState>()(
  persist(
    (set) => ({
      repositoryByProject: {},
      focusedRootByContext: {},
      locationByThread: {},
      navigationTargetByThread: {},
      views: {},
      selectRepository: (projectScope, repository) =>
        set((state) => {
          if (repository) {
            return {
              repositoryByProject: {
                ...state.repositoryByProject,
                [projectScope]: repository,
              },
            };
          }
          const { [projectScope]: _removed, ...repositoryByProject } = state.repositoryByProject;
          return { repositoryByProject };
        }),
      focusRoot: (context, rootId) =>
        set((state) => ({
          focusedRootByContext: { ...state.focusedRootByContext, [context]: rootId },
        })),
      setThreadLocation: (ref, location) =>
        set((state) => ({
          locationByThread: { ...state.locationByThread, [scopedThreadKey(ref)]: location },
        })),
      clearThreadLocation: (ref) =>
        set((state) => {
          const key = scopedThreadKey(ref);
          const { [key]: _removed, ...locationByThread } = state.locationByThread;
          return { locationByThread };
        }),
      setNavigationTarget: (ref, target) =>
        set((state) => {
          const key = scopedThreadKey(ref);
          const location = {
            projectId: target.projectId,
            repository: target.repository,
            rootNumber: target.rootNumber,
          };
          return {
            locationByThread: { ...state.locationByThread, [key]: location },
            navigationTargetByThread: { ...state.navigationTargetByThread, [key]: target },
          };
        }),
      clearNavigationTarget: (ref, requestId) =>
        set((state) => {
          const key = scopedThreadKey(ref);
          if (state.navigationTargetByThread[key]?.requestId !== requestId) return state;
          const { [key]: _removed, ...navigationTargetByThread } = state.navigationTargetByThread;
          return { navigationTargetByThread };
        }),
      patchView: (scope, patch) =>
        set((state) => ({
          views: {
            ...state.views,
            [scope]: { ...(state.views[scope] ?? EMPTY_VIEW), ...patch },
          },
        })),
      toggleExpanded: (scope, id) =>
        set((state) => {
          const view = state.views[scope] ?? EMPTY_VIEW;
          return {
            views: { ...state.views, [scope]: { ...view, expanded: toggle(view.expanded, id) } },
          };
        }),
      toggleFold: (scope, id) =>
        set((state) => {
          const view = state.views[scope] ?? EMPTY_VIEW;
          return {
            views: { ...state.views, [scope]: { ...view, openFolds: toggle(view.openFolds, id) } },
          };
        }),
    }),
    {
      name: "t3code:workflow-map:v1",
      storage: createJSONStorage(() =>
        resolveStorage(typeof window !== "undefined" ? window.localStorage : undefined),
      ),
      partialize: ({ repositoryByProject, focusedRootByContext, locationByThread, views }) => ({
        repositoryByProject,
        focusedRootByContext,
        locationByThread,
        views,
      }),
    },
  ),
);

export function selectWorkflowMapView(
  views: Readonly<Record<string, WorkflowMapView>>,
  scope: string,
): WorkflowMapView {
  return views[scope] ?? EMPTY_VIEW;
}
