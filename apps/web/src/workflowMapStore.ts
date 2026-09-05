import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

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
  views: Record<string, WorkflowMapView>;
  selectRepository: (projectScope: string, repository: string | null) => void;
  focusRoot: (context: string, rootId: string) => void;
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
      partialize: ({ repositoryByProject, focusedRootByContext, views }) => ({
        repositoryByProject,
        focusedRootByContext,
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
