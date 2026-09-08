import type {
  EnvironmentId,
  ProjectId,
  ThreadId,
  WorkflowIssueSummary,
  WorkflowRepository,
  WorkflowSearchMatch,
  WorkflowSyncState,
} from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { useEnvironmentQuery } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";
import { workflowEnvironment } from "~/state/workflow";
import {
  selectWorkflowMapView,
  useWorkflowMapStore,
  workflowMapContextKey,
  workflowMapScopeKey,
} from "~/workflowMapStore";

import { WorkflowFocusedMap } from "./WorkflowFocusedMap";
import { WorkflowActiveWork } from "./WorkflowActiveWork";
import { WorkflowAdoptionPanel } from "./WorkflowAdoptionPanel";
import { foldIdentity, issueIdentity } from "./WorkflowMap.logic";
import {
  filterWorkflowRoots,
  listenForWorkflowBrowserReturn,
  resolveWorkflowRepository,
} from "./WorkflowPanel.logic";

interface WorkflowPanelProps {
  environmentId: EnvironmentId;
  environmentLabel: string;
  projectId: ProjectId;
  projectTitle: string;
  planningThreadId?: ThreadId;
  supported: boolean | null;
}

function QueryMessage(props: {
  title: string;
  description: string;
  retry?: (() => void) | undefined;
}) {
  return (
    <div className="flex min-h-36 flex-1 items-center justify-center px-6 py-10 text-center">
      <div className="max-w-sm">
        <p className="font-medium text-sm">{props.title}</p>
        <p className="mt-1 text-muted-foreground text-xs leading-relaxed">{props.description}</p>
        {props.retry ? (
          <Button className="mt-3" size="xs" variant="outline" onClick={props.retry}>
            Retry
          </Button>
        ) : null}
      </div>
    </div>
  );
}

function RepositoryPicker(props: {
  repositories: ReadonlyArray<WorkflowRepository>;
  value: string | null;
  onChange: (repository: string | null) => void;
}) {
  const hasSelectedRepository = props.repositories.some(
    (repository) => repository.nameWithOwner.toLowerCase() === props.value?.toLowerCase(),
  );
  return (
    <label className="grid min-w-44 gap-1 text-xs">
      <span className="font-medium text-muted-foreground">Tracker repository</span>
      <select
        className="h-8 w-full rounded-md border border-input bg-background px-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
        value={props.value ?? ""}
        onChange={(event) => props.onChange(event.target.value || null)}
      >
        {props.repositories.length > 1 ? <option value="">Choose a repository</option> : null}
        {props.value && !hasSelectedRepository ? (
          <option value={props.value}>{props.value} (linked)</option>
        ) : null}
        {props.repositories.map((repository) => (
          <option key={repository.nameWithOwner} value={repository.nameWithOwner}>
            {repository.nameWithOwner} ({repository.remoteNames.join(", ")})
          </option>
        ))}
      </select>
    </label>
  );
}

function WorkflowSyncNotice(props: {
  state: WorkflowSyncState | null;
  error: string | null;
  retry: () => void;
}) {
  if (props.state?.status === "fresh" && props.error === null) return null;
  const title = props.error
    ? "Workflow connection interrupted"
    : props.state === null || props.state.status === "refreshing"
      ? "Refreshing Workflow…"
      : props.state?.status === "stale"
        ? "Workflow data may be stale"
        : props.state?.status === "access-denied"
          ? "GitHub access unavailable"
          : props.state?.status === "rate-limited"
            ? "GitHub rate limit reached"
            : "GitHub is unavailable";
  const detail = props.error ?? props.state?.message ?? "Waiting for the first GitHub sync.";
  const lastSuccess = props.state?.lastSuccessfulAt
    ? ` Last synced ${new Date(props.state.lastSuccessfulAt).toLocaleString()}.`
    : "";
  const retryAt = props.state?.retryAt
    ? ` Automatic retry ${new Date(props.state.retryAt).toLocaleString()}.`
    : "";
  return (
    <div
      className="flex items-center gap-3 border-b border-border bg-muted/40 px-3 py-2 text-xs"
      role="status"
    >
      <p className="min-w-0 flex-1">
        <span className="font-medium">{title}</span>{" "}
        <span className="text-muted-foreground">
          {detail}
          {lastSuccess}
          {retryAt}
        </span>
      </p>
      <Button size="xs" variant="outline" onClick={props.retry}>
        Retry
      </Button>
    </div>
  );
}

export function WorkflowPanel(props: WorkflowPanelProps) {
  const repositoriesQuery = useEnvironmentQuery(
    props.supported === true
      ? workflowEnvironment.repositories({
          environmentId: props.environmentId,
          input: { projectId: props.projectId },
        })
      : null,
  );
  const mapStore = useWorkflowMapStore();
  const projectScope = `${props.environmentId}:${props.projectId}`;
  const threadRef = props.planningThreadId
    ? scopeThreadRef(props.environmentId, props.planningThreadId)
    : null;
  const threadKey = threadRef ? scopedThreadKey(threadRef) : null;
  const threadLocation = threadKey ? mapStore.locationByThread[threadKey] : undefined;
  const navigationTarget = threadKey ? mapStore.navigationTargetByThread[threadKey] : undefined;
  const repositoryChoice =
    threadLocation?.projectId === props.projectId
      ? threadLocation.repository
      : (mapStore.repositoryByProject[projectScope] ?? null);
  const repositories = repositoriesQuery.data?.repositories ?? [];
  const repository = resolveWorkflowRepository(repositoryChoice, repositories);
  const rootsQuery = useEnvironmentQuery(
    repository
      ? workflowEnvironment.roots({
          environmentId: props.environmentId,
          input: { projectId: props.projectId, repository },
        })
      : null,
  );
  const syncQuery = useEnvironmentQuery(
    repository
      ? workflowEnvironment.sync({
          environmentId: props.environmentId,
          input: { projectId: props.projectId, repository },
        })
      : null,
  );
  const refreshWorkflow = useAtomCommand(workflowEnvironment.refresh, {
    reportFailure: false,
  });
  const retryWorkflow = useCallback(() => {
    if (!repository) return;
    void refreshWorkflow({
      environmentId: props.environmentId,
      input: { projectId: props.projectId, repository },
    });
  }, [repository, props.environmentId, props.projectId, refreshWorkflow]);
  useEffect(() => {
    if (!repository || typeof document === "undefined" || typeof window === "undefined") return;
    return listenForWorkflowBrowserReturn(document, window, retryWorkflow);
  }, [repository, retryWorkflow]);
  const [rootSearch, setRootSearch] = useState("");
  const [adopting, setAdopting] = useState(false);
  const adoptButtonRef = useRef<HTMLButtonElement>(null);
  const wasAdopting = useRef(false);
  useEffect(() => {
    if (wasAdopting.current && !adopting) adoptButtonRef.current?.focus();
    wasAdopting.current = adopting;
  }, [adopting]);
  const roots = rootsQuery.data?.roots ?? [];
  const visibleRoots = useMemo(() => filterWorkflowRoots(roots, rootSearch), [rootSearch, roots]);
  const context = repository
    ? workflowMapContextKey({
        environmentId: props.environmentId,
        projectId: props.projectId,
        repository,
      })
    : null;
  const focusedId = context ? mapStore.focusedRootByContext[context] : null;
  const hasThreadLocation =
    threadLocation?.projectId === props.projectId &&
    threadLocation.repository.toLowerCase() === repository?.toLowerCase();
  const rootFromThread = hasThreadLocation
    ? roots.find((root) => root.number === threadLocation.rootNumber)
    : undefined;
  const locateNumber =
    navigationTarget?.projectId === props.projectId &&
    navigationTarget.repository.toLowerCase() === repository?.toLowerCase()
      ? navigationTarget.issueNumber
      : hasThreadLocation && !rootFromThread
        ? threadLocation.rootNumber
        : null;
  const locationQuery = useEnvironmentQuery(
    repository && locateNumber !== null
      ? workflowEnvironment.locate({
          environmentId: props.environmentId,
          input: { projectId: props.projectId, repository, number: locateNumber },
        })
      : null,
  );
  const locatedRoot = locationQuery.data
    ? (locationQuery.data.ancestry[0] ?? locationQuery.data.issue)
    : null;
  const focusedRoot = hasThreadLocation
    ? (rootFromThread ?? (locatedRoot?.number === threadLocation.rootNumber ? locatedRoot : null))
    : (roots.find((root) => issueIdentity(root) === focusedId) ?? null);
  const navigateToMatch = useCallback(
    (match: WorkflowSearchMatch) => {
      const root = match.ancestry[0] ?? match.issue;
      const destinationContext = workflowMapContextKey({
        environmentId: props.environmentId,
        projectId: props.projectId,
        repository: root.repository,
      });
      const destinationRootId = issueIdentity(root);
      const destinationScope = workflowMapScopeKey(destinationContext, destinationRootId);
      const destinationView = selectWorkflowMapView(mapStore.views, destinationScope);
      mapStore.selectRepository(projectScope, root.repository);
      mapStore.focusRoot(destinationContext, destinationRootId);
      if (threadRef) {
        mapStore.setThreadLocation(threadRef, {
          projectId: props.projectId,
          repository: root.repository,
          rootNumber: root.number,
        });
      }
      mapStore.patchView(destinationScope, {
        selectedId: issueIdentity(match.issue),
        selectedIssue: {
          id: match.issue.id,
          repository: match.issue.repository,
          number: match.issue.number,
        },
        expanded: [...new Set([...destinationView.expanded, ...match.ancestry.map(issueIdentity)])],
        openFolds: [
          ...new Set([
            ...destinationView.openFolds,
            ...match.ancestry.flatMap((parent) => [
              foldIdentity(issueIdentity(parent), "completed"),
              foldIdentity(issueIdentity(parent), "cancelled"),
            ]),
          ]),
        ],
      });
    },
    [mapStore, projectScope, props.environmentId, props.projectId, threadRef],
  );
  const handledNavigationRequest = useRef<string | null>(null);
  useEffect(() => {
    if (!threadRef || !navigationTarget || !locationQuery.data) return;
    if (handledNavigationRequest.current === navigationTarget.requestId) return;
    const current =
      useWorkflowMapStore.getState().navigationTargetByThread[scopedThreadKey(threadRef)];
    if (
      current?.requestId !== navigationTarget.requestId ||
      current.projectId !== props.projectId ||
      current.repository.toLowerCase() !== locationQuery.data.issue.repository.toLowerCase() ||
      current.issueNumber !== locationQuery.data.issue.number
    )
      return;
    handledNavigationRequest.current = navigationTarget.requestId;
    navigateToMatch(locationQuery.data);
  }, [locationQuery.data, navigateToMatch, navigationTarget, props.projectId, threadRef]);

  const chooseRepository = (value: string | null) => {
    mapStore.selectRepository(projectScope, value);
    if (!threadRef) return;
    mapStore.clearThreadLocation(threadRef);
    if (navigationTarget) mapStore.clearNavigationTarget(threadRef, navigationTarget.requestId);
  };

  const focusRoot = (root: WorkflowIssueSummary) => {
    if (!context) return;
    mapStore.focusRoot(context, issueIdentity(root));
    if (threadRef) {
      mapStore.setThreadLocation(threadRef, {
        projectId: props.projectId,
        repository: root.repository,
        rootNumber: root.number,
      });
      if (navigationTarget) mapStore.clearNavigationTarget(threadRef, navigationTarget.requestId);
    }
  };

  const header = (
    <header className="grid gap-2 border-b border-border px-3 py-2 @md/workflow:grid-cols-[1fr_auto] @md/workflow:items-end">
      <div className="grid min-w-0 grid-cols-2 gap-3 text-xs">
        <div className="min-w-0">
          <span className="block text-muted-foreground">Environment</span>
          <span className="block truncate font-medium">{props.environmentLabel}</span>
        </div>
        <div className="min-w-0">
          <span className="block text-muted-foreground">Project</span>
          <span className="block truncate font-medium">{props.projectTitle}</span>
        </div>
      </div>
      <div className="flex items-end justify-end gap-2">
        <WorkflowActiveWork
          environmentId={props.environmentId}
          environmentLabel={props.environmentLabel}
        />
        {repositoriesQuery.data && repositories.length > 0 ? (
          <RepositoryPicker
            repositories={repositories}
            value={repository}
            onChange={chooseRepository}
          />
        ) : null}
      </div>
    </header>
  );

  let content;
  if (props.supported === null)
    content = (
      <QueryMessage title="Loading Workflow…" description="Checking environment support." />
    );
  else if (!props.supported)
    content = (
      <QueryMessage
        title="Workflow unavailable"
        description="Update this environment's T3 Code server to browse GitHub workflow issues."
      />
    );
  else if (repositoriesQuery.isPending && !repositoriesQuery.data)
    content = (
      <QueryMessage
        title="Loading repositories…"
        description="Checking this project's GitHub remotes."
      />
    );
  else if (repositoriesQuery.error)
    content = (
      <QueryMessage
        title="Could not load repositories"
        description={repositoriesQuery.error}
        retry={repositoriesQuery.refresh}
      />
    );
  else if (repositoriesQuery.data && repositories.length === 0)
    content = (
      <QueryMessage
        title="No GitHub repository found"
        description="Add a GitHub remote to this project, then retry."
        retry={repositoriesQuery.refresh}
      />
    );
  else if (!repository)
    content = (
      <QueryMessage
        title="Choose the tracker repository"
        description="This project has multiple GitHub remotes. Select the repository that owns this workflow."
      />
    );
  else if (rootsQuery.isPending && !rootsQuery.data)
    content = (
      <QueryMessage title="Loading workflow roots…" description={`Reading ${repository}.`} />
    );
  else if (rootsQuery.error)
    content = (
      <QueryMessage
        title="Could not load workflow roots"
        description={rootsQuery.error}
        retry={rootsQuery.refresh}
      />
    );
  else if (locateNumber !== null && locationQuery.isPending && !locationQuery.data && !focusedRoot)
    content = (
      <QueryMessage title="Locating active work…" description={`Finding #${locateNumber}.`} />
    );
  else if (locateNumber !== null && locationQuery.error && !focusedRoot)
    content = (
      <QueryMessage
        title="Could not locate active work"
        description={`${locationQuery.error} The environment may need an updated T3 Code server.`}
        retry={locationQuery.refresh}
      />
    );
  else if (roots.length === 0 && !focusedRoot)
    content = (
      <QueryMessage
        title="No workflow roots"
        description="This repository has no top-level GitHub issues to browse."
        retry={rootsQuery.refresh}
      />
    );
  else if (!focusedRoot)
    content = (
      <div className="mx-auto grid w-full max-w-xl gap-3 overflow-y-auto p-4">
        <div>
          <h2 className="font-semibold text-sm">Choose a workflow root</h2>
          <p className="text-muted-foreground text-xs">
            {focusedId
              ? "The previously focused root moved or is no longer accessible. Choose its current context."
              : "Focus a map, capability, or container without starting an agent."}
          </p>
        </div>
        <Input
          aria-label="Search workflow roots"
          placeholder="Search roots"
          size="sm"
          type="search"
          value={rootSearch}
          onChange={(event) => setRootSearch(event.target.value)}
        />
        <ul aria-label="Workflow roots" className="grid gap-1">
          {visibleRoots.map((root) => (
            <li key={issueIdentity(root)}>
              <button
                type="button"
                className="w-full rounded-md border border-border p-3 text-left hover:bg-accent focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => focusRoot(root)}
              >
                <span className="block text-[10px] text-muted-foreground">
                  {root.repository} · {root.kind}
                </span>
                <span className="block font-medium text-xs">
                  #{root.number} {root.title}
                </span>
              </button>
            </li>
          ))}
        </ul>
      </div>
    );
  else if (adopting)
    content = (
      <WorkflowAdoptionPanel
        key={`${props.environmentId}:${props.projectId}:${focusedRoot.repository}:${focusedRoot.number}`}
        environmentId={props.environmentId}
        projectId={props.projectId}
        repository={focusedRoot.repository}
        rootNumber={focusedRoot.number}
        onClose={() => setAdopting(false)}
        onChanged={() => {
          rootsQuery.refresh();
        }}
      />
    );
  else
    content = (
      <>
        <div className="flex items-center gap-2 border-b border-border px-3 py-1.5 text-xs">
          <button
            type="button"
            className="truncate rounded font-medium focus-visible:ring-2 focus-visible:ring-ring"
            onClick={() => {
              if (context) mapStore.focusRoot(context, "");
              if (threadRef) {
                mapStore.clearThreadLocation(threadRef);
                if (navigationTarget)
                  mapStore.clearNavigationTarget(threadRef, navigationTarget.requestId);
              }
            }}
            aria-label="Choose another workflow root"
          >
            #{focusedRoot.number} {focusedRoot.title}
          </button>
          <span className="text-muted-foreground">Top-to-bottom map</span>
          <Button
            ref={adoptButtonRef}
            className="ml-auto"
            size="xs"
            variant="outline"
            onClick={() => setAdopting(true)}
          >
            Adopt branch
          </Button>
        </div>
        <WorkflowFocusedMap
          key={issueIdentity(focusedRoot)}
          environmentId={props.environmentId}
          projectId={props.projectId}
          {...(props.planningThreadId ? { planningThreadId: props.planningThreadId } : {})}
          root={focusedRoot}
          onRefreshRoot={retryWorkflow}
          onNavigateMatch={navigateToMatch}
          {...(navigationTarget
            ? {
                focusedActiveWorkEntryId: navigationTarget.activeWorkEntryId,
                focusedProviderThreadId: navigationTarget.providerThreadId,
              }
            : {})}
          onManualNavigation={() => {
            if (threadRef && navigationTarget)
              mapStore.clearNavigationTarget(threadRef, navigationTarget.requestId);
          }}
        />
      </>
    );

  return (
    <section
      className="@container/workflow relative flex min-h-0 flex-1 flex-col"
      aria-label="Workflow"
    >
      {header}
      {navigationTarget && locationQuery.isPending && !locationQuery.data ? (
        <div className="border-b border-border bg-muted/40 px-3 py-2 text-xs" role="status">
          Locating active work #{navigationTarget.issueNumber}…
        </div>
      ) : null}
      {navigationTarget && locationQuery.error ? (
        <div
          className="flex items-center gap-2 border-b border-destructive/40 px-3 py-2 text-xs"
          role="alert"
        >
          <span className="min-w-0 flex-1">
            Could not locate active work. {locationQuery.error} The environment may need an updated
            T3 Code server.
          </span>
          <Button size="xs" variant="outline" onClick={locationQuery.refresh}>
            Retry
          </Button>
        </div>
      ) : null}
      {repository ? (
        <WorkflowSyncNotice
          state={syncQuery.data ?? null}
          error={syncQuery.error}
          retry={retryWorkflow}
        />
      ) : null}
      {content}
    </section>
  );
}
