import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type {
  EnvironmentId,
  WorkflowActiveWorkCursor,
  WorkflowActiveWorkEntry,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { Activity, ChevronDown } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

import { Button } from "~/components/ui/button";
import { Popover, PopoverPopup, PopoverTrigger } from "~/components/ui/popover";
import { randomUUID } from "~/lib/utils";
import { useRightPanelStore } from "~/rightPanelStore";
import { useThreadShells } from "~/state/entities";
import { useEnvironmentQuery } from "~/state/query";
import { workflowEnvironment } from "~/state/workflow";
import { buildThreadRouteParams } from "~/threadRoutes";
import { useWorkflowMapStore } from "~/workflowMapStore";

const activityLabel: Record<WorkflowActiveWorkEntry["activity"], string> = {
  running: "Running",
  waiting: "Waiting",
  attention: "Needs attention",
  unknown: "Unconfirmed",
  settled: "Settled",
};

function ActiveWorkPage(props: {
  environmentId: EnvironmentId;
  cursor?: WorkflowActiveWorkCursor;
  seenEntryIds?: readonly string[];
  refreshOnMount?: boolean;
  onOpen: (entry: WorkflowActiveWorkEntry) => void;
}) {
  const [showNext, setShowNext] = useState(false);
  const query = useEnvironmentQuery(
    workflowEnvironment.activeWork({
      environmentId: props.environmentId,
      input: props.cursor ? { cursor: props.cursor } : {},
    }),
  );
  const refresh = query.refresh;
  useEffect(() => {
    if (props.refreshOnMount) refresh();
  }, [props.refreshOnMount, refresh]);

  if (query.isPending && !query.data) {
    return <p className="px-2 py-3 text-muted-foreground text-xs">Loading active work…</p>;
  }
  if (query.error && !query.data) {
    return (
      <div className="grid gap-2 px-2 py-3 text-xs">
        <p className="font-medium">Active work unavailable</p>
        <p className="text-muted-foreground">
          This environment could not load Active work. It may need an updated T3 Code server.
        </p>
        <p className="break-words text-destructive" role="alert">
          {query.error}
        </p>
        <Button className="justify-self-start" size="xs" variant="outline" onClick={refresh}>
          Retry
        </Button>
      </div>
    );
  }
  if (!query.data) return null;
  const seenEntryIds = new Set(props.seenEntryIds ?? []);
  const entries = query.data.entries.filter((entry) => !seenEntryIds.has(entry.entryId));
  const nextSeenEntryIds = [...seenEntryIds, ...entries.map((entry) => entry.entryId)];

  return (
    <>
      {query.error ? (
        <div className="mx-1 mb-1 flex items-center gap-2 rounded-md bg-muted px-2 py-1.5 text-xs">
          <span className="min-w-0 flex-1">Active work may be stale. {query.error}</span>
          <Button size="micro" variant="ghost" onClick={refresh}>
            Retry
          </Button>
        </div>
      ) : null}
      {!props.cursor && !query.error ? (
        <p className="px-2 pb-1 text-[10px] text-muted-foreground">
          {query.data.entries.length}
          {query.data.nextCursor ? "+" : ""} active{" "}
          {query.data.entries.length === 1 ? "entry" : "entries"}
        </p>
      ) : null}
      {entries.length === 0 && !query.error && !props.cursor ? (
        <p className="px-2 py-3 text-muted-foreground text-xs">
          No active work in this environment.
        </p>
      ) : (
        <ul className="grid gap-1" aria-label={props.cursor ? "More active work" : "Active work"}>
          {entries.map((entry) => (
            <li key={entry.entryId}>
              <button
                type="button"
                className="grid w-full gap-0.5 rounded-md px-2 py-1.5 text-left hover:bg-accent focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-60"
                disabled={entry.navigationThreadId === null}
                onClick={() => props.onOpen(entry)}
              >
                <span className="flex min-w-0 items-center gap-1.5 text-xs">
                  <span className="truncate font-medium">
                    {entry.kind === "director"
                      ? (entry.title ?? `${entry.repository}#${entry.capabilityNumber}`)
                      : (entry.title ?? `${entry.kind} for #${entry.issueNumber}`)}
                  </span>
                  <span className="ml-auto shrink-0 text-muted-foreground">
                    {activityLabel[entry.activity]}
                  </span>
                </span>
                <span className="truncate text-[11px] text-muted-foreground">
                  {entry.projectTitle} · capability #{entry.capabilityNumber}
                  {entry.kind === "director" ? " · director" : ` · ticket #${entry.issueNumber}`}
                </span>
                {entry.providerThreadId ? (
                  <span className="truncate text-[10px] text-muted-foreground">
                    Provider child {entry.providerThreadId}
                  </span>
                ) : null}
                {entry.navigationThreadId === null ? (
                  <span className="text-[10px] text-destructive">
                    Recovery needs an available linked T3 thread.
                  </span>
                ) : null}
              </button>
            </li>
          ))}
        </ul>
      )}
      {query.data.nextCursor && !showNext ? (
        <Button className="mt-1 w-full" size="xs" variant="ghost" onClick={() => setShowNext(true)}>
          Load more active work
        </Button>
      ) : null}
      {query.data.nextCursor && showNext ? (
        <ActiveWorkPage
          environmentId={props.environmentId}
          cursor={query.data.nextCursor}
          seenEntryIds={nextSeenEntryIds}
          onOpen={props.onOpen}
        />
      ) : null}
    </>
  );
}

export function WorkflowActiveWork(props: {
  environmentId: EnvironmentId;
  environmentLabel: string;
}) {
  const navigate = useNavigate();
  const shells = useThreadShells();
  const [open, setOpen] = useState(false);
  const [refreshRevision, setRefreshRevision] = useState(0);
  const previousActivityRevision = useRef<string | null>(null);
  const previousEnvironmentId = useRef(props.environmentId);
  const refreshTimer = useRef<number | null>(null);
  const selectedEnvironmentRevision = useMemo(
    () =>
      shells
        .filter((thread) => thread.environmentId === props.environmentId)
        .map(
          (thread) =>
            `${thread.id}:${thread.updatedAt}:${thread.backgroundLiveness ?? ""}:${thread.latestTurn?.state ?? ""}:${thread.session?.status ?? ""}`,
        )
        .sort()
        .join("|"),
    [props.environmentId, shells],
  );

  useEffect(() => {
    if (previousEnvironmentId.current !== props.environmentId) {
      if (refreshTimer.current !== null) window.clearTimeout(refreshTimer.current);
      refreshTimer.current = null;
      previousEnvironmentId.current = props.environmentId;
      previousActivityRevision.current = selectedEnvironmentRevision;
      return;
    }
    if (!open) {
      if (refreshTimer.current !== null) window.clearTimeout(refreshTimer.current);
      refreshTimer.current = null;
      previousActivityRevision.current = selectedEnvironmentRevision;
      return;
    }
    if (previousActivityRevision.current === selectedEnvironmentRevision) return;
    previousActivityRevision.current = selectedEnvironmentRevision;
    if (refreshTimer.current !== null) return;
    refreshTimer.current = window.setTimeout(() => {
      refreshTimer.current = null;
      setRefreshRevision((value) => value + 1);
    }, 250);
  }, [open, props.environmentId, selectedEnvironmentRevision]);
  useEffect(
    () => () => {
      if (refreshTimer.current !== null) window.clearTimeout(refreshTimer.current);
    },
    [],
  );

  const openEntry = (entry: WorkflowActiveWorkEntry) => {
    if (!entry.navigationThreadId) return;
    const threadRef = scopeThreadRef(entry.environmentId, entry.navigationThreadId);
    const requestId = randomUUID();
    useWorkflowMapStore.getState().setNavigationTarget(threadRef, {
      requestId,
      projectId: entry.projectId,
      repository: entry.repository,
      rootNumber: entry.rootNumber,
      issueNumber: entry.issueNumber,
      activeWorkEntryId: entry.entryId,
      providerThreadId: entry.providerThreadId,
    });
    useRightPanelStore.getState().open(threadRef, "workflow");
    setOpen(false);
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(threadRef),
    });
  };

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        setOpen(nextOpen);
        if (nextOpen) setRefreshRevision((value) => value + 1);
      }}
    >
      <PopoverTrigger
        render={
          <Button size="xs" variant="outline">
            <Activity className="size-3.5" />
            Active work
            <ChevronDown className="size-3" />
          </Button>
        }
      />
      <PopoverPopup align="end" className="w-[min(26rem,var(--available-width))]" side="bottom">
        <div className="mb-2 px-2">
          <p className="font-medium text-sm">Active work</p>
          <p className="text-muted-foreground text-xs">{props.environmentLabel}</p>
        </div>
        {open ? (
          <ActiveWorkPage
            key={refreshRevision}
            environmentId={props.environmentId}
            refreshOnMount
            onOpen={openEntry}
          />
        ) : null}
      </PopoverPopup>
    </Popover>
  );
}
