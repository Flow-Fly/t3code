import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  ProjectId,
  WorkflowChildrenResult,
  WorkflowFrontier,
  WorkflowIssueSummary,
  WorkflowSearchMatch,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import {
  ChevronDown,
  ChevronRight,
  ExternalLink,
  Focus,
  LocateFixed,
  Minus,
  Plus,
} from "lucide-react";
import {
  type PointerEvent as ReactPointerEvent,
  type FormEvent as ReactFormEvent,
  type WheelEvent as ReactWheelEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

import { useEnvironmentQuery } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";
import { useProject, useServerConfigs } from "~/state/entities";
import { workflowEnvironment } from "~/state/workflow";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { cn } from "~/lib/utils";
import { useRightPanelStore } from "~/rightPanelStore";
import { buildThreadRouteParams } from "~/threadRoutes";
import {
  selectWorkflowMapView,
  useWorkflowMapStore,
  workflowMapContextKey,
  workflowMapScopeKey,
} from "~/workflowMapStore";

import {
  buildVisibleWorkflowMap,
  fitWorkflowViewport,
  foldIdentity,
  issueIdentity,
  mergeWorkflowSearchMatch,
  type WorkflowPoint,
  type WorkflowViewport,
} from "./WorkflowMap.logic";
import {
  workflowIssueBrief,
  workflowIssueStateLabel,
  workflowSourceLinks,
} from "./WorkflowPanel.logic";
import { resolveWorkflowStartSelection } from "./WorkflowStart.logic";

function ChildrenLoader(props: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  parent: WorkflowIssueSummary;
  refreshRequest: number;
  onLoad: (parent: WorkflowIssueSummary, result: WorkflowChildrenResult) => void;
}) {
  const query = useEnvironmentQuery(
    workflowEnvironment.children({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        repository: props.parent.repository,
        parentNumber: props.parent.number,
      },
    }),
  );
  const lastRefreshRequest = useRef(props.refreshRequest);
  const refreshQuery = query.refresh;
  const refreshRequest = props.refreshRequest;
  useEffect(() => {
    if (lastRefreshRequest.current === refreshRequest) return;
    lastRefreshRequest.current = refreshRequest;
    refreshQuery();
  }, [refreshQuery, refreshRequest]);
  useEffect(() => {
    if (query.data) props.onLoad(props.parent, query.data);
  }, [props, query.data]);
  if (query.error)
    return (
      <div className="col-span-full flex items-center gap-2 border-b border-border px-3 py-1 text-destructive text-xs">
        <span>
          Could not load children for #{props.parent.number}: {query.error}
        </span>
        <Button size="micro" variant="ghost" onClick={query.refresh}>
          Retry
        </Button>
      </div>
    );
  if (query.isPending && !query.data)
    return (
      <p className="col-span-full border-b border-border px-3 py-1 text-muted-foreground text-xs">
        Loading children for #{props.parent.number}…
      </p>
    );
  return null;
}

function WorkflowDetails(props: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  rootNumber: number;
  issue: WorkflowIssueSummary;
  refreshRequest: number;
}) {
  const navigate = useNavigate();
  const project = useProject(scopeProjectRef(props.environmentId, props.projectId));
  const serverConfigs = useServerConfigs();
  const providers = serverConfigs.get(props.environmentId)?.providers ?? [];
  const startSelection = resolveWorkflowStartSelection(providers, project?.defaultModelSelection);
  const startWorkflow = useAtomCommand(workflowEnvironment.start, { reportFailure: false });
  const [startPending, setStartPending] = useState(false);
  const [startMessage, setStartMessage] = useState<string | null>(null);
  const query = useEnvironmentQuery(
    workflowEnvironment.issueDetail({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        repository: props.issue.repository,
        number: props.issue.number,
      },
    }),
  );
  const lastRefreshRequest = useRef(props.refreshRequest);
  const refreshQuery = query.refresh;
  const refreshRequest = props.refreshRequest;
  useEffect(() => {
    if (lastRefreshRequest.current === refreshRequest) return;
    lastRefreshRequest.current = refreshRequest;
    refreshQuery();
  }, [refreshQuery, refreshRequest]);
  if (query.isPending && !query.data)
    return <p className="p-3 text-muted-foreground text-xs">Loading issue details…</p>;
  if (query.error)
    return (
      <div className="flex items-center gap-2 p-3 text-destructive text-xs">
        <span>{query.error}</span>
        <Button size="micro" variant="ghost" onClick={query.refresh}>
          Retry
        </Button>
      </div>
    );
  if (!query.data) return null;
  const selectedIssue = query.data;
  const seen = new Set(query.data.blockedBy.map((issue) => `${issue.repository}#${issue.number}`));
  const links = workflowSourceLinks(query.data.body).filter((link) => {
    const match = /github\.com\/([^/]+\/[^/]+)\/issues\/(\d+)/.exec(link.url);
    if (!match) return true;
    const id = `${match[1]}#${Number(match[2])}`;
    if (seen.has(id)) return false;
    seen.add(id);
    return true;
  });
  const evidence = query.data.evidence;
  const canStart =
    query.data.readiness?.status === "ready" &&
    (query.data.kind === "decision" || query.data.labels.includes("wayfinder:task"));
  const handleStart = async () => {
    if (startPending || !startSelection.selection) return;
    setStartPending(true);
    setStartMessage(null);
    const result = await startWorkflow({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        repository: selectedIssue.repository,
        rootNumber: props.rootNumber,
        issueNumber: selectedIssue.number,
        modelSelection: startSelection.selection,
      },
    });
    setStartPending(false);
    if (result._tag === "Failure") {
      const failure = squashAtomCommandFailure(result);
      setStartMessage(
        failure instanceof Error
          ? failure.message
          : "Decision work could not start. Refresh Workflow and try again.",
      );
      return;
    }
    if (result.value.status === "held") {
      setStartMessage(result.value.message);
      return;
    }
    const threadRef = scopeThreadRef(result.value.environmentId, result.value.threadId);
    useRightPanelStore.getState().open(threadRef, "workflow");
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(threadRef),
    });
  };
  return (
    <article
      className="grid gap-3 border-t border-border p-3"
      aria-label="Selected workflow details"
    >
      <div>
        <p className="text-muted-foreground text-[10px] uppercase tracking-wide">
          {query.data.repository}
        </p>
        <h2 className="font-semibold text-sm">
          #{query.data.number} {query.data.title}
        </h2>
        <p className="text-muted-foreground text-xs">
          {query.data.kind} · {workflowIssueStateLabel(query.data)}
        </p>
        <a
          className="mt-1 inline-flex items-center gap-1 text-info text-xs hover:underline"
          href={query.data.url}
          target="_blank"
          rel="noreferrer noopener"
        >
          Open GitHub issue <ExternalLink className="size-3" />
        </a>
      </div>
      <p className="whitespace-pre-wrap text-muted-foreground text-xs leading-relaxed">
        {workflowIssueBrief(query.data) ?? "No description provided."}
      </p>
      {canStart ? (
        <section aria-label="Start workflow decision">
          <Button
            size="sm"
            disabled={startPending || startSelection.selection === null}
            onClick={() => void handleStart()}
          >
            {startPending ? "Starting…" : "Start"}
          </Button>
          {(startMessage ?? startSelection.message) ? (
            <p
              className={cn(
                "mt-1 text-xs",
                startMessage ? "text-destructive" : "text-muted-foreground",
              )}
              role={startMessage ? "alert" : undefined}
            >
              {startMessage ?? startSelection.message}
            </p>
          ) : (
            <p className="mt-1 text-muted-foreground text-xs">
              Claims this issue and starts Codex with its required Wayfinder skills.
            </p>
          )}
        </section>
      ) : null}
      {query.data.readiness ? (
        <section aria-label="Readiness evidence">
          <h3 className="font-medium text-xs">Readiness · {workflowIssueStateLabel(query.data)}</h3>
          <ul className="mt-1 grid gap-1 text-xs">
            {query.data.readiness.reasons.map((item, index) => (
              <li key={`${item.kind}-${item.source ?? index}`} className="text-muted-foreground">
                {item.source ? (
                  <a
                    className="text-info hover:underline"
                    href={item.source}
                    target="_blank"
                    rel="noreferrer noopener"
                  >
                    {item.message}
                  </a>
                ) : (
                  item.message
                )}
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      {query.data.blockedBy.length > 0 || links.length > 0 ? (
        <div>
          <h3 className="font-medium text-xs">Relationships</h3>
          <ul className="mt-1 grid gap-1 text-xs">
            {query.data.blockedBy.map((blocker) => (
              <li key={issueIdentity(blocker)}>
                <a
                  className="text-info hover:underline"
                  href={blocker.url}
                  target="_blank"
                  rel="noreferrer noopener"
                >
                  Prerequisite · {blocker.repository}#{blocker.number} {blocker.title}
                </a>
              </li>
            ))}
            {links.map((link) => (
              <li key={link.url}>
                <a
                  className="inline-flex items-center gap-1 text-info hover:underline"
                  href={link.url}
                  target="_blank"
                  rel="noreferrer noopener"
                >
                  <span className="capitalize">{link.relationship}</span> · {link.label}{" "}
                  <ExternalLink className="size-3" />
                </a>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      {evidence &&
      (evidence.records.length > 0 ||
        evidence.manualConditions.length > 0 ||
        evidence.historyComplete === false) ? (
        <section aria-label="Workflow evidence ledger">
          <h3 className="font-medium text-xs">Evidence</h3>
          {evidence.historyComplete === false ? (
            <p className="mt-1 text-muted-foreground text-xs">
              Evidence history is incomplete. Open or refresh details before relying on this status.
            </p>
          ) : null}
          {evidence.records.length > 0 ? (
            <ol className="mt-1 grid gap-2">
              {evidence.records.map((record) => (
                <li key={record.id} className="rounded-md border border-border p-2 text-xs">
                  <div className="flex flex-wrap items-center gap-x-1 font-medium">
                    <a
                      className="capitalize text-info hover:underline"
                      href={record.url}
                      target="_blank"
                      rel="noreferrer noopener"
                    >
                      {record.kind}
                      {record.approvalKind ? ` · ${record.approvalKind}` : ""}
                    </a>
                    <span className="text-muted-foreground">· {record.state}</span>
                    {record.scope !== "not-applicable" ? (
                      <span className="text-muted-foreground">· scope {record.scope}</span>
                    ) : null}
                  </div>
                  <p className="mt-1 whitespace-pre-wrap text-muted-foreground">{record.summary}</p>
                  {record.approvedBy ? (
                    <p className="mt-1 text-muted-foreground">
                      Approved by {record.approvedBy} · authority {record.authority ?? "unknown"}
                    </p>
                  ) : null}
                  {record.source ? (
                    <p className="mt-1 break-words">Source: {record.source}</p>
                  ) : null}
                  <p className="mt-1 text-muted-foreground">
                    {record.sourceAccess === "verified"
                      ? "Source is available in the loaded GitHub evidence."
                      : record.sourceAccess === "reported"
                        ? "Source is reported by the record; this query did not independently verify it."
                        : "Source is unavailable; provenance remains unknown and does not prove invalidation."}
                  </p>
                  {record.evidence ? (
                    <details className="mt-1">
                      <summary className="cursor-pointer font-medium">Recorded evidence</summary>
                      <p className="mt-1 whitespace-pre-wrap text-muted-foreground">
                        {record.evidence}
                      </p>
                    </details>
                  ) : null}
                  {record.approvedContent ? (
                    <details className="mt-1">
                      <summary className="cursor-pointer font-medium">Approved snapshot</summary>
                      <pre className="mt-1 whitespace-pre-wrap font-sans text-muted-foreground">
                        {record.approvedContent}
                      </pre>
                    </details>
                  ) : null}
                </li>
              ))}
            </ol>
          ) : null}
          {evidence.manualConditions.length > 0 ? (
            <div className="mt-2">
              <h4 className="font-medium text-xs">Reviewed conditions</h4>
              <ul className="mt-1 grid gap-1 text-xs">
                {evidence.manualConditions.map((condition) => (
                  <li key={`${condition.source}-${condition.description}`}>
                    <a
                      className="text-info hover:underline"
                      href={condition.source}
                      target="_blank"
                      rel="noreferrer noopener"
                    >
                      {condition.status === "satisfied" ? "Satisfied" : "Review required"} ·{" "}
                      {condition.description}
                    </a>
                    {condition.evidence ? (
                      <p className="text-muted-foreground">Evidence: {condition.evidence}</p>
                    ) : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </section>
      ) : null}
      {query.data.body.trim() ? (
        <details className="rounded-md border border-border p-2">
          <summary className="cursor-pointer font-medium text-xs">Full issue description</summary>
          <pre className="mt-2 whitespace-pre-wrap font-sans text-muted-foreground text-xs leading-relaxed">
            {query.data.body}
          </pre>
        </details>
      ) : null}
    </article>
  );
}

export function WorkflowFocusedMap(props: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  root: WorkflowIssueSummary;
  onRefreshRoot: () => void;
  onNavigateMatch: (match: WorkflowSearchMatch) => void;
}) {
  const context = workflowMapContextKey({
    environmentId: props.environmentId,
    projectId: props.projectId,
    repository: props.root.repository,
  });
  const rootId = issueIdentity(props.root);
  const scope = workflowMapScopeKey(context, rootId);
  const store = useWorkflowMapStore();
  const view = selectWorkflowMapView(store.views, scope);
  const [nodes, setNodes] = useState<Record<string, WorkflowIssueSummary>>({
    [rootId]: props.root,
  });
  const [childrenByParent, setChildrenByParent] = useState<Record<string, readonly string[]>>({});
  const [frontierByParent, setFrontierByParent] = useState<Record<string, WorkflowFrontier>>({});
  const [searchDraft, setSearchDraft] = useState("");
  const [submittedSearch, setSubmittedSearch] = useState("");
  const [recoverSelection, setRecoverSelection] = useState(false);
  const [refreshRequest, setRefreshRequest] = useState(0);
  const [transientViewport, setTransientViewport] = useState<WorkflowViewport | null>(null);
  const [transientPosition, setTransientPosition] = useState<{
    id: string;
    point: WorkflowPoint;
  } | null>(null);
  const canvasRef = useRef<HTMLDivElement>(null);
  const panRef = useRef<{ x: number; y: number; originX: number; originY: number } | null>(null);
  const dragRef = useRef<{
    id: string;
    x: number;
    y: number;
    originX: number;
    originY: number;
  } | null>(null);
  const searchCompositionRef = useRef(false);

  const onChildren = useCallback((parent: WorkflowIssueSummary, result: WorkflowChildrenResult) => {
    const parentId = issueIdentity(parent);
    setNodes((current) => {
      const changed = result.children.some((child) => {
        const existing = current[issueIdentity(child)];
        return (
          !existing ||
          existing.updatedAt !== child.updatedAt ||
          existing.state !== child.state ||
          existing.stateReason !== child.stateReason ||
          existing.title !== child.title ||
          existing.childCount !== child.childCount ||
          existing.readiness?.status !== child.readiness?.status ||
          existing.readiness?.reasons.length !== child.readiness?.reasons.length ||
          existing.readiness?.reasons.some(
            (reason, index) =>
              reason.kind !== child.readiness?.reasons[index]?.kind ||
              reason.message !== child.readiness?.reasons[index]?.message ||
              reason.source !== child.readiness?.reasons[index]?.source,
          )
        );
      });
      return changed
        ? {
            ...current,
            ...Object.fromEntries(result.children.map((child) => [issueIdentity(child), child])),
          }
        : current;
    });
    setChildrenByParent((current) => {
      const childIds = result.children.map(issueIdentity);
      const existing = current[parentId] ?? [];
      if (
        existing.length === childIds.length &&
        existing.every((childId, index) => childId === childIds[index])
      ) {
        return current;
      }
      return { ...current, [parentId]: childIds };
    });
    if (result.frontier) {
      const frontier = result.frontier;
      setFrontierByParent((current) =>
        current[parentId]?.status === frontier.status &&
        current[parentId]?.message === frontier.message &&
        current[parentId]?.readyIssueIds.join("\n") === frontier.readyIssueIds.join("\n")
          ? current
          : { ...current, [parentId]: frontier },
      );
    }
  }, []);

  const loadIds = [rootId, ...view.expanded].filter(
    (id, index, values) => values.indexOf(id) === index,
  );
  const currentNodes = nodes[rootId] === props.root ? nodes : { ...nodes, [rootId]: props.root };
  const currentPositions = transientPosition
    ? { ...view.positions, [transientPosition.id]: transientPosition.point }
    : view.positions;
  const currentViewport = transientViewport ?? view.viewport;
  const map = buildVisibleWorkflowMap({
    root: props.root,
    nodes: currentNodes,
    childrenByParent,
    expanded: view.expanded,
    openFolds: view.openFolds,
    positions: currentPositions,
  });
  const rootFrontier = frontierByParent[rootId];
  const nodeById = new Map(map.nodes.map((node) => [node.id, node]));
  const breadcrumbNodes = (() => {
    if (!view.selectedId) return [];
    const path = new Array<(typeof map.nodes)[number]>();
    let current = nodeById.get(view.selectedId);
    while (current) {
      path.unshift(current);
      current = current.parentId ? nodeById.get(current.parentId) : undefined;
    }
    return path;
  })();
  useEffect(() => {
    const missing = map.nodes.filter((node) => !(node.id in view.positions));
    if (missing.length === 0) return;
    store.patchView(scope, {
      positions: {
        ...view.positions,
        ...Object.fromEntries(missing.map((node) => [node.id, node.position])),
      },
    });
  }, [map.nodes, scope, store, view.positions]);

  const selected = view.selectedId ? currentNodes[view.selectedId] : undefined;
  const selectionHidden = Boolean(view.selectedId && !nodeById.has(view.selectedId));
  const searchQuery = useEnvironmentQuery(
    submittedSearch
      ? workflowEnvironment.search({
          environmentId: props.environmentId,
          input: {
            projectId: props.projectId,
            repository: props.root.repository,
            query: submittedSearch,
          },
        })
      : null,
  );
  const locateQuery = useEnvironmentQuery(
    selectionHidden && recoverSelection && view.selectedIssue
      ? workflowEnvironment.locate({
          environmentId: props.environmentId,
          input: {
            projectId: props.projectId,
            repository: view.selectedIssue.repository,
            id: view.selectedIssue.id,
            number: view.selectedIssue.number,
          },
        })
      : null,
  );
  const searchMatches =
    searchQuery.data?.matches.filter(
      (match) =>
        issueIdentity(match.issue) === rootId ||
        match.ancestry.some((ancestor) => issueIdentity(ancestor) === rootId),
    ) ?? [];

  const revealMatch = (match: WorkflowSearchMatch) => {
    const merged = mergeWorkflowSearchMatch({ nodes: currentNodes, childrenByParent }, match);
    const expanded = [...new Set([...view.expanded, ...merged.expanded])];
    const openFolds = [
      ...new Set([
        ...view.openFolds,
        ...match.ancestry.flatMap((parent) => [
          foldIdentity(issueIdentity(parent), "completed"),
          foldIdentity(issueIdentity(parent), "cancelled"),
        ]),
      ]),
    ];
    const revealedMap = buildVisibleWorkflowMap({
      root: props.root,
      nodes: merged.nodes,
      childrenByParent: merged.childrenByParent,
      expanded,
      openFolds,
      positions: currentPositions,
    });
    const pathIds = new Set([...match.ancestry, match.issue].map(issueIdentity));
    const pathNodes = revealedMap.nodes.filter((node) => pathIds.has(node.id));
    const rect = canvasRef.current?.getBoundingClientRect();
    setNodes(merged.nodes);
    setChildrenByParent(merged.childrenByParent);
    store.patchView(scope, {
      selectedId: issueIdentity(match.issue),
      selectedIssue: {
        id: match.issue.id,
        repository: match.issue.repository,
        number: match.issue.number,
      },
      expanded,
      openFolds,
      viewport: fitWorkflowViewport(pathNodes, {
        width: rect?.width ?? 600,
        height: rect?.height ?? 420,
      }),
    });
  };

  const submitSearch = (event: ReactFormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (searchCompositionRef.current) return;
    setSubmittedSearch(searchDraft.trim());
  };

  const moveSelected = (offset: WorkflowPoint) => {
    const node = view.selectedId ? nodeById.get(view.selectedId) : undefined;
    if (!node) return;
    const position = view.positions[node.id] ?? node.position;
    store.patchView(scope, {
      positions: {
        ...view.positions,
        [node.id]: { x: position.x + offset.x, y: position.y + offset.y },
      },
    });
  };

  const selectIssue = (issue: WorkflowIssueSummary) =>
    store.patchView(scope, {
      selectedId: issueIdentity(issue),
      selectedIssue: { id: issue.id, repository: issue.repository, number: issue.number },
    });

  const zoomBy = (factor: number) =>
    store.patchView(scope, {
      viewport: {
        ...view.viewport,
        zoom: Math.min(2, Math.max(0.02, view.viewport.zoom * factor)),
      },
    });
  const fit = () => {
    const rect = canvasRef.current?.getBoundingClientRect();
    store.patchView(scope, {
      viewport: fitWorkflowViewport(map.nodes, {
        width: rect?.width ?? 600,
        height: rect?.height ?? 420,
      }),
    });
  };
  const reset = () => store.patchView(scope, { positions: {}, viewport: { x: 0, y: 0, zoom: 1 } });
  const refresh = () => {
    props.onRefreshRoot();
    setRefreshRequest((current) => current + 1);
  };

  const panStart = (event: ReactPointerEvent<HTMLDivElement>) => {
    const target = event.target as { closest?: (selector: string) => unknown };
    if (target.closest?.("[data-workflow-node], button, input, a")) return;
    panRef.current = {
      x: event.clientX,
      y: event.clientY,
      originX: view.viewport.x,
      originY: view.viewport.y,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };
  const panMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const pan = panRef.current;
    if (!pan) return;
    setTransientViewport({
      ...view.viewport,
      x: pan.originX + event.clientX - pan.x,
      y: pan.originY + event.clientY - pan.y,
    });
  };
  const panEnd = (event: ReactPointerEvent<HTMLDivElement>) => {
    const pan = panRef.current;
    if (!pan) return;
    store.patchView(scope, {
      viewport: {
        ...view.viewport,
        x: pan.originX + event.clientX - pan.x,
        y: pan.originY + event.clientY - pan.y,
      },
    });
    panRef.current = null;
    setTransientViewport(null);
  };
  const dragStart = (id: string, event: ReactPointerEvent<HTMLDivElement>) => {
    const position = view.positions[id] ?? { x: 0, y: 0 };
    dragRef.current = {
      id,
      x: event.clientX,
      y: event.clientY,
      originX: position.x,
      originY: position.y,
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
    event.stopPropagation();
  };
  const dragMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (!drag) return;
    setTransientPosition({
      id: drag.id,
      point: {
        x: drag.originX + (event.clientX - drag.x) / view.viewport.zoom,
        y: drag.originY + (event.clientY - drag.y) / view.viewport.zoom,
      },
    });
  };
  const dragEnd = (event: ReactPointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (!drag) return;
    store.patchView(scope, {
      positions: {
        ...view.positions,
        [drag.id]: {
          x: drag.originX + (event.clientX - drag.x) / view.viewport.zoom,
          y: drag.originY + (event.clientY - drag.y) / view.viewport.zoom,
        },
      },
    });
    dragRef.current = null;
    setTransientPosition(null);
  };
  const wheel = (event: ReactWheelEvent<HTMLDivElement>) => {
    event.preventDefault();
    zoomBy(event.deltaY < 0 ? 1.1 : 0.9);
  };

  return (
    <div className="grid min-h-0 min-w-0 flex-1 grid-rows-[auto_minmax(16rem,1fr)_minmax(10rem,auto)] @lg/workflow:grid-cols-[minmax(0,1fr)_minmax(14rem,0.38fr)] @lg/workflow:grid-rows-[auto_minmax(0,1fr)]">
      {loadIds.map((id) =>
        currentNodes[id]?.childCount ? (
          <ChildrenLoader
            key={id}
            environmentId={props.environmentId}
            projectId={props.projectId}
            parent={currentNodes[id]!}
            refreshRequest={refreshRequest}
            onLoad={onChildren}
          />
        ) : null,
      )}
      <div className="col-span-full grid min-w-0 gap-2 border-b border-border p-2 @md/workflow:grid-cols-[minmax(12rem,1fr)_auto]">
        {rootFrontier ? (
          <div
            className="col-span-full flex flex-wrap items-center gap-x-2 rounded-md bg-muted px-2 py-1 text-xs"
            aria-label="Workflow frontier"
          >
            <span className="font-medium">
              {rootFrontier.status === "available" ? "Frontier available" : "Frontier empty"}
            </span>
            <span className="text-muted-foreground">{rootFrontier.message}</span>
          </div>
        ) : null}
        <form className="relative flex min-w-0 gap-1" role="search" onSubmit={submitSearch}>
          <Input
            aria-label="Search this workflow"
            placeholder="Search this workflow"
            size="sm"
            type="search"
            value={searchDraft}
            onChange={(event) => setSearchDraft(event.target.value)}
            onCompositionStart={() => {
              searchCompositionRef.current = true;
            }}
            onCompositionEnd={() => {
              searchCompositionRef.current = false;
            }}
          />
          <Button type="submit" size="xs" variant="outline">
            Search
          </Button>
          {submittedSearch ? (
            <div
              className="absolute inset-x-0 top-full z-30 mt-1 max-h-52 overflow-y-auto rounded-md border border-border bg-popover p-1 shadow-lg"
              aria-label="Workflow search results"
            >
              {searchQuery.isPending && !searchQuery.data ? (
                <p className="p-2 text-muted-foreground text-xs">Searching…</p>
              ) : null}
              {searchQuery.error ? (
                <div className="flex items-center gap-2 p-2 text-destructive text-xs">
                  <span>{searchQuery.error}</span>
                  <Button size="micro" variant="ghost" onClick={searchQuery.refresh}>
                    Retry
                  </Button>
                </div>
              ) : null}
              {searchMatches.map((match) => (
                <button
                  key={issueIdentity(match.issue)}
                  type="button"
                  className="block w-full rounded px-2 py-1.5 text-left text-xs hover:bg-accent focus-visible:ring-2 focus-visible:ring-ring"
                  onClick={() => {
                    revealMatch(match);
                    setSearchDraft("");
                    setSubmittedSearch("");
                  }}
                >
                  <span className="block truncate">
                    {match.issue.repository}#{match.issue.number} {match.issue.title}
                  </span>
                  <span className="block truncate text-muted-foreground">
                    {match.ancestry.map((item) => `#${item.number}`).join(" / ")}
                  </span>
                  {!match.ancestryComplete ? (
                    <span className="block text-amber-foreground">Earlier ancestry omitted</span>
                  ) : null}
                </button>
              ))}
              {searchQuery.data?.hasMore ? (
                <p className="p-2 text-muted-foreground text-[10px]">
                  More matches exist in {props.root.repository}; refine your search.
                </p>
              ) : null}
              {searchQuery.data && searchMatches.length === 0 ? (
                <p className="p-2 text-muted-foreground text-xs">
                  No matches in this focused root.
                </p>
              ) : null}
            </div>
          ) : null}
        </form>
        <div className="flex min-w-0 flex-wrap items-center gap-1" aria-label="Map controls">
          <Button size="icon-xs" variant="ghost" aria-label="Zoom out" onClick={() => zoomBy(0.9)}>
            <Minus />
          </Button>
          <Button size="icon-xs" variant="ghost" aria-label="Zoom in" onClick={() => zoomBy(1.1)}>
            <Plus />
          </Button>
          <Button size="xs" variant="ghost" aria-label="Fit workflow map" onClick={fit}>
            <Focus /> Fit
          </Button>
          <Button size="xs" variant="ghost" aria-label="Reset workflow layout" onClick={reset}>
            <LocateFixed /> Reset
          </Button>
          <Button size="xs" variant="ghost" aria-label="Refresh workflow map" onClick={refresh}>
            Refresh
          </Button>
          <div className="flex items-center" role="group" aria-label="Selected node position">
            <Button
              size="icon-xs"
              variant="ghost"
              aria-label="Move selected node left"
              disabled={!view.selectedId || !nodeById.has(view.selectedId)}
              onClick={() => moveSelected({ x: -16, y: 0 })}
            >
              ←
            </Button>
            <Button
              size="icon-xs"
              variant="ghost"
              aria-label="Move selected node up"
              disabled={!view.selectedId || !nodeById.has(view.selectedId)}
              onClick={() => moveSelected({ x: 0, y: -16 })}
            >
              ↑
            </Button>
            <Button
              size="icon-xs"
              variant="ghost"
              aria-label="Move selected node down"
              disabled={!view.selectedId || !nodeById.has(view.selectedId)}
              onClick={() => moveSelected({ x: 0, y: 16 })}
            >
              ↓
            </Button>
            <Button
              size="icon-xs"
              variant="ghost"
              aria-label="Move selected node right"
              disabled={!view.selectedId || !nodeById.has(view.selectedId)}
              onClick={() => moveSelected({ x: 16, y: 0 })}
            >
              →
            </Button>
          </div>
        </div>
      </div>

      <div
        ref={canvasRef}
        className="relative min-h-64 overflow-hidden bg-[radial-gradient(circle_at_center,var(--border)_1px,transparent_1px)] bg-[size:18px_18px] touch-none"
        aria-label="Workflow map canvas"
        onPointerDown={panStart}
        onPointerMove={panMove}
        onPointerUp={panEnd}
        onPointerCancel={panEnd}
        onWheel={wheel}
      >
        <div
          className="absolute inset-0 origin-top-left"
          style={{
            transform: `translate(${currentViewport.x}px, ${currentViewport.y}px) scale(${currentViewport.zoom})`,
          }}
        >
          <svg className="pointer-events-none absolute inset-0 overflow-visible" aria-hidden="true">
            {map.nodes
              .filter((node) => node.parentId)
              .map((node) => {
                const parent = node.parentId ? nodeById.get(node.parentId) : undefined;
                if (!parent) return null;
                return (
                  <path
                    key={node.id}
                    d={`M ${parent.position.x + 104} ${parent.position.y + 92} C ${parent.position.x + 104} ${parent.position.y + 120}, ${node.position.x + 104} ${node.position.y - 28}, ${node.position.x + 104} ${node.position.y}`}
                    fill="none"
                    stroke="currentColor"
                    className="text-border"
                  />
                );
              })}
          </svg>
          {map.nodes.map((node) => {
            const isSelected = node.id === view.selectedId;
            const isExpanded = view.expanded.includes(node.id);
            return (
              <div
                key={node.id}
                data-workflow-node
                className={cn(
                  "absolute w-52 rounded-lg border bg-background p-2 shadow-sm",
                  isSelected ? "border-ring ring-2 ring-ring/30" : "border-border",
                )}
                style={{ transform: `translate(${node.position.x}px, ${node.position.y}px)` }}
                onPointerDown={(event) => dragStart(node.id, event)}
                onPointerMove={dragMove}
                onPointerUp={dragEnd}
                onPointerCancel={dragEnd}
              >
                <button
                  type="button"
                  className="block w-full text-left outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  aria-current={isSelected ? "true" : undefined}
                  onClick={() => selectIssue(node.issue)}
                >
                  <span className="block truncate text-[10px] text-muted-foreground">
                    {node.issue.repository} · {node.issue.kind}
                  </span>
                  <span className="mt-1 line-clamp-2 block font-medium text-xs">
                    #{node.issue.number} {node.issue.title}
                  </span>
                  <span className="mt-1 block text-[10px] text-muted-foreground">
                    {workflowIssueStateLabel(node.issue)}
                  </span>
                </button>
                {node.issue.childCount > 0 && node.id !== rootId ? (
                  <button
                    type="button"
                    className="mt-1 inline-flex items-center gap-1 rounded text-[10px] focus-visible:ring-2 focus-visible:ring-ring"
                    aria-label={`${isExpanded ? "Collapse" : "Expand"} ${node.issue.title}`}
                    aria-expanded={isExpanded}
                    onClick={() => store.toggleExpanded(scope, node.id)}
                  >
                    {isExpanded ? (
                      <ChevronDown className="size-3" />
                    ) : (
                      <ChevronRight className="size-3" />
                    )}{" "}
                    {node.issue.childCount}
                  </button>
                ) : null}
              </div>
            );
          })}
        </div>
      </div>

      <aside
        className="min-h-0 overflow-y-auto border-t border-border @lg/workflow:border-t-0 @lg/workflow:border-s"
        aria-label="Workflow outline and details"
      >
        {selectionHidden ? (
          <div className="border-b border-border p-2 text-xs">
            <p>The selected work moved or is outside this focus.</p>
            <Button
              className="mt-1"
              size="xs"
              variant="outline"
              onClick={() => setRecoverSelection(true)}
            >
              Find current context
            </Button>
            {locateQuery.isPending ? (
              <p className="mt-1 text-muted-foreground">Finding current context…</p>
            ) : null}
            {locateQuery.error ? (
              <div className="mt-1 flex items-center gap-2 text-destructive">
                <span>{locateQuery.error}</span>
                <Button size="micro" variant="ghost" onClick={locateQuery.refresh}>
                  Retry
                </Button>
              </div>
            ) : null}
            {locateQuery.data ? (
              <Button
                className="mt-1"
                size="xs"
                variant="outline"
                onClick={() => props.onNavigateMatch(locateQuery.data!)}
              >
                Open current root #{(locateQuery.data.ancestry[0] ?? locateQuery.data.issue).number}
              </Button>
            ) : null}
          </div>
        ) : null}
        <nav className="border-b border-border p-2" aria-label="Workflow breadcrumbs">
          <ol className="flex flex-wrap items-center gap-1 text-[10px] text-muted-foreground">
            <li>{props.root.repository}</li>
            {(breadcrumbNodes.length > 0 ? breadcrumbNodes : map.nodes.slice(0, 1)).map((node) => (
              <li key={node.id} className="contents">
                <span aria-hidden="true">/</span>
                <button
                  type="button"
                  className="max-w-40 truncate rounded hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
                  aria-current={node.id === view.selectedId ? "location" : undefined}
                  onClick={() => selectIssue(node.issue)}
                >
                  #{node.issue.number} {node.issue.title}
                </button>
              </li>
            ))}
          </ol>
        </nav>
        <div className="p-2">
          <ul aria-label="Synchronized workflow outline" className="grid gap-1">
            {map.nodes.map((node) => (
              <li
                key={node.id}
                style={{ paddingInlineStart: `${node.depth * 12}px` }}
                className="flex items-center gap-1"
              >
                {node.issue.childCount > 0 && node.id !== rootId ? (
                  <button
                    type="button"
                    className="rounded p-1 focus-visible:ring-2 focus-visible:ring-ring"
                    aria-label={`${view.expanded.includes(node.id) ? "Collapse" : "Expand"} ${node.issue.title} in outline`}
                    onClick={() => store.toggleExpanded(scope, node.id)}
                  >
                    {view.expanded.includes(node.id) ? (
                      <ChevronDown className="size-3" />
                    ) : (
                      <ChevronRight className="size-3" />
                    )}
                  </button>
                ) : (
                  <span className="size-5" />
                )}
                <button
                  type="button"
                  className="min-w-0 flex-1 truncate rounded py-1 text-left text-xs focus-visible:ring-2 focus-visible:ring-ring"
                  aria-current={node.id === view.selectedId ? "true" : undefined}
                  onClick={() => selectIssue(node.issue)}
                >
                  #{node.issue.number} {node.issue.title}
                </button>
              </li>
            ))}
          </ul>
          {map.folds.map((fold) => (
            <Button
              key={fold.id}
              className="mt-1 w-full justify-start"
              size="xs"
              variant="ghost"
              aria-expanded={fold.open}
              onClick={() => store.toggleFold(scope, fold.id)}
            >
              {fold.open ? <ChevronDown /> : <ChevronRight />}{" "}
              {fold.group === "completed" ? "Completed — unverified" : "Cancelled / superseded"} (
              {fold.count})
            </Button>
          ))}
        </div>
        {selected ? (
          <WorkflowDetails
            environmentId={props.environmentId}
            projectId={props.projectId}
            rootNumber={props.root.number}
            issue={selected}
            refreshRequest={refreshRequest}
          />
        ) : (
          <p className="border-t border-border p-3 text-muted-foreground text-xs">
            Select work to inspect it without starting an agent.
          </p>
        )}
      </aside>
    </div>
  );
}
