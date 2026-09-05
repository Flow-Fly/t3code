import type {
  EnvironmentId,
  ProjectId,
  WorkflowIssueSummary,
  WorkflowRepository,
} from "@t3tools/contracts";
import { ChevronDown, ChevronRight, ExternalLink } from "lucide-react";
import { useMemo, useState } from "react";

import { useEnvironmentQuery } from "~/state/query";
import { workflowEnvironment } from "~/state/workflow";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { cn } from "~/lib/utils";

import {
  filterWorkflowRoots,
  resolveWorkflowRepository,
  workflowIssueBrief,
  workflowIssueStateLabel,
  workflowSourceLinks,
} from "./WorkflowPanel.logic";

interface WorkflowPanelProps {
  environmentId: EnvironmentId;
  environmentLabel: string;
  projectId: ProjectId;
  projectTitle: string;
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
  return (
    <label className="grid gap-1 text-xs">
      <span className="font-medium text-muted-foreground">Tracker repository</span>
      <select
        className="h-8 w-full rounded-md border border-input bg-background px-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
        value={props.value ?? ""}
        onChange={(event) => props.onChange(event.target.value || null)}
      >
        {props.repositories.length > 1 ? <option value="">Choose a repository</option> : null}
        {props.repositories.map((repository) => (
          <option key={repository.nameWithOwner} value={repository.nameWithOwner}>
            {repository.nameWithOwner} ({repository.remoteNames.join(", ")})
          </option>
        ))}
      </select>
    </label>
  );
}

function WorkflowTargetHeader(props: {
  environmentLabel: string;
  projectTitle: string;
  repositories: ReadonlyArray<WorkflowRepository>;
  repositoriesLoaded: boolean;
  repository: string | null;
  onRepositoryChange: (repository: string | null) => void;
}) {
  return (
    <header className="grid gap-3 border-b border-border px-4 py-3">
      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="min-w-0">
          <span className="block text-muted-foreground">Environment</span>
          <span className="block truncate font-medium">{props.environmentLabel}</span>
        </div>
        <div className="min-w-0">
          <span className="block text-muted-foreground">Project</span>
          <span className="block truncate font-medium">{props.projectTitle}</span>
        </div>
      </div>
      {props.repositoriesLoaded && props.repositories.length > 0 ? (
        <RepositoryPicker
          repositories={props.repositories}
          value={props.repository}
          onChange={props.onRepositoryChange}
        />
      ) : null}
    </header>
  );
}

function WorkflowTreeItem(props: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  repository: string;
  issue: WorkflowIssueSummary;
  selectedNumber: number | null;
  onSelect: (issue: WorkflowIssueSummary) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const childrenQuery = useEnvironmentQuery(
    expanded && props.issue.childCount > 0
      ? workflowEnvironment.children({
          environmentId: props.environmentId,
          input: {
            projectId: props.projectId,
            repository: props.repository,
            parentNumber: props.issue.number,
          },
        })
      : null,
  );
  const selected = props.selectedNumber === props.issue.number;

  return (
    <li>
      <div
        className={cn(
          "group flex min-h-8 items-center gap-1 rounded-md pe-2",
          selected ? "bg-accent text-foreground" : "text-muted-foreground hover:bg-accent/60",
        )}
      >
        {props.issue.childCount > 0 ? (
          <button
            type="button"
            className="flex size-7 shrink-0 items-center justify-center rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label={`${expanded ? "Collapse" : "Expand"} ${props.issue.title}`}
            aria-expanded={expanded}
            onClick={() => setExpanded((value) => !value)}
          >
            {expanded ? (
              <ChevronDown className="size-3.5" />
            ) : (
              <ChevronRight className="size-3.5" />
            )}
          </button>
        ) : (
          <span className="size-7 shrink-0" aria-hidden="true" />
        )}
        <button
          type="button"
          className="min-w-0 flex-1 py-1 text-left outline-none focus-visible:underline"
          onClick={() => props.onSelect(props.issue)}
        >
          <span className="block truncate text-xs text-foreground">
            <span className="text-muted-foreground">#{props.issue.number}</span> {props.issue.title}
          </span>
          <span className="block truncate text-[10px] capitalize">
            {props.issue.kind} · {workflowIssueStateLabel(props.issue)}
          </span>
        </button>
      </div>
      {expanded ? (
        childrenQuery.isPending && childrenQuery.data === null ? (
          <p className="py-2 ps-8 text-muted-foreground text-xs">Loading children…</p>
        ) : childrenQuery.error ? (
          <div className="flex items-center gap-2 py-2 ps-8 text-xs">
            <span className="text-destructive">{childrenQuery.error}</span>
            <Button size="micro" variant="ghost" onClick={childrenQuery.refresh}>
              Retry
            </Button>
          </div>
        ) : childrenQuery.data?.children.length === 0 ? (
          <p className="py-2 ps-8 text-muted-foreground text-xs">No visible children.</p>
        ) : (
          <ul className="ms-3 border-s border-border/70 ps-1">
            {childrenQuery.data?.children.map((child) => (
              <WorkflowTreeItem
                key={child.number}
                {...props}
                issue={child}
                selectedNumber={props.selectedNumber}
              />
            ))}
          </ul>
        )
      ) : null}
    </li>
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
  const [repositoryChoice, setRepositoryChoice] = useState<string | null>(null);
  const repositories = repositoriesQuery.data?.repositories ?? [];
  const repository = resolveWorkflowRepository(repositoryChoice, repositories);
  const [search, setSearch] = useState("");
  const [selection, setSelection] = useState<{
    context: string;
    issue: WorkflowIssueSummary;
  } | null>(null);
  const selectionContext = `${props.environmentId}:${props.projectId}:${repository ?? ""}`;
  const selectedIssue = selection?.context === selectionContext ? selection.issue : null;

  const rootsQuery = useEnvironmentQuery(
    repository
      ? workflowEnvironment.roots({
          environmentId: props.environmentId,
          input: { projectId: props.projectId, repository },
        })
      : null,
  );
  const visibleRoots = useMemo(
    () => filterWorkflowRoots(rootsQuery.data?.roots ?? [], search),
    [rootsQuery.data?.roots, search],
  );
  const detailQuery = useEnvironmentQuery(
    repository && selectedIssue
      ? workflowEnvironment.issueDetail({
          environmentId: props.environmentId,
          input: {
            projectId: props.projectId,
            repository,
            number: selectedIssue.number,
          },
        })
      : null,
  );

  const targetHeader = (
    <WorkflowTargetHeader
      environmentLabel={props.environmentLabel}
      projectTitle={props.projectTitle}
      repositories={repositories}
      repositoriesLoaded={repositoriesQuery.data !== null}
      repository={repository}
      onRepositoryChange={setRepositoryChoice}
    />
  );

  if (props.supported === null) {
    return (
      <section className="flex min-h-0 flex-1 flex-col" aria-label="Workflow">
        {targetHeader}
        <QueryMessage title="Loading Workflow…" description="Checking environment support." />
      </section>
    );
  }

  if (!props.supported) {
    return (
      <section className="flex min-h-0 flex-1 flex-col" aria-label="Workflow">
        {targetHeader}
        <QueryMessage
          title="Workflow unavailable"
          description="Update this environment's T3 Code server to browse GitHub workflow issues."
        />
      </section>
    );
  }

  return (
    <section className="flex min-h-0 flex-1 flex-col" aria-label="Workflow">
      {targetHeader}

      {repositoriesQuery.isPending && repositoriesQuery.data === null ? (
        <QueryMessage
          title="Loading repositories…"
          description="Checking this project's GitHub remotes."
        />
      ) : repositoriesQuery.error ? (
        <QueryMessage
          title="Could not load repositories"
          description={repositoriesQuery.error}
          retry={repositoriesQuery.refresh}
        />
      ) : repositoriesQuery.data && repositories.length === 0 ? (
        <QueryMessage
          title="No GitHub repository found"
          description="Add a GitHub remote to this project, then retry."
          retry={repositoriesQuery.refresh}
        />
      ) : !repository ? (
        <QueryMessage
          title="Choose the tracker repository"
          description="This project has multiple GitHub remotes. Select the repository that owns this workflow."
        />
      ) : rootsQuery.isPending && rootsQuery.data === null ? (
        <QueryMessage title="Loading workflow roots…" description={`Reading ${repository}.`} />
      ) : rootsQuery.error ? (
        <QueryMessage
          title="Could not load workflow roots"
          description={rootsQuery.error}
          retry={rootsQuery.refresh}
        />
      ) : rootsQuery.data?.roots.length === 0 ? (
        <QueryMessage
          title="No workflow roots"
          description="This repository has no top-level GitHub issues to browse."
          retry={rootsQuery.refresh}
        />
      ) : (
        <div className="grid min-h-0 flex-1 grid-cols-1 grid-rows-[minmax(12rem,0.8fr)_minmax(12rem,1.2fr)] xl:grid-cols-[minmax(14rem,0.85fr)_minmax(18rem,1.15fr)] xl:grid-rows-1">
          <div className="flex min-h-0 flex-col border-b border-border xl:border-e xl:border-b-0">
            <div className="border-b border-border p-3">
              <Input
                aria-label="Search workflow roots"
                placeholder="Search roots"
                size="sm"
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto p-2">
              {visibleRoots.length === 0 ? (
                <p className="px-2 py-6 text-center text-muted-foreground text-xs">
                  No roots match “{search}”.
                </p>
              ) : (
                <ul aria-label="Workflow outline" className="grid gap-0.5">
                  {visibleRoots.map((root) => (
                    <WorkflowTreeItem
                      key={root.number}
                      environmentId={props.environmentId}
                      projectId={props.projectId}
                      repository={repository}
                      issue={root}
                      selectedNumber={selectedIssue?.number ?? null}
                      onSelect={(issue) => setSelection({ context: selectionContext, issue })}
                    />
                  ))}
                </ul>
              )}
            </div>
          </div>

          <div className="min-h-0 overflow-y-auto">
            {!selectedIssue ? (
              <QueryMessage
                title="Select work to inspect"
                description="Selecting an issue opens its details without starting an agent."
              />
            ) : detailQuery.isPending && detailQuery.data === null ? (
              <QueryMessage
                title="Loading issue details…"
                description={`Reading #${selectedIssue.number}.`}
              />
            ) : detailQuery.error ? (
              <QueryMessage
                title="Could not load issue details"
                description={detailQuery.error}
                retry={detailQuery.refresh}
              />
            ) : detailQuery.data ? (
              <article className="grid gap-4 p-4">
                <div>
                  <p className="text-muted-foreground text-xs capitalize">
                    {detailQuery.data.kind} · {workflowIssueStateLabel(detailQuery.data)}
                  </p>
                  <h2 className="mt-1 font-semibold text-base leading-snug">
                    #{detailQuery.data.number} {detailQuery.data.title}
                  </h2>
                </div>
                <div>
                  <h3 className="font-medium text-xs">Brief</h3>
                  <p className="mt-1 whitespace-pre-wrap text-muted-foreground text-sm leading-relaxed">
                    {workflowIssueBrief(detailQuery.data) ?? "No description provided."}
                  </p>
                </div>
                <div>
                  <h3 className="font-medium text-xs">GitHub state</h3>
                  <dl className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
                    <dt className="text-muted-foreground">Raw state</dt>
                    <dd className="uppercase">{detailQuery.data.state}</dd>
                    <dt className="text-muted-foreground">State reason</dt>
                    <dd>{detailQuery.data.stateReason ?? "None"}</dd>
                    <dt className="text-muted-foreground">Labels</dt>
                    <dd>{detailQuery.data.labels.join(", ") || "None"}</dd>
                  </dl>
                </div>
                <div>
                  <h3 className="font-medium text-xs">Source</h3>
                  <ul className="mt-1 grid gap-1 text-xs">
                    <li>
                      <a
                        className="inline-flex items-center gap-1 text-info hover:underline"
                        href={detailQuery.data.url}
                        target="_blank"
                        rel="noreferrer noopener"
                      >
                        Open GitHub issue <ExternalLink className="size-3" />
                      </a>
                    </li>
                    {workflowSourceLinks(detailQuery.data.body).map((link) => (
                      <li key={link.url}>
                        <a
                          className="inline-flex items-center gap-1 text-info hover:underline"
                          href={link.url}
                          target="_blank"
                          rel="noreferrer noopener"
                        >
                          {link.label} <ExternalLink className="size-3" />
                        </a>
                      </li>
                    ))}
                  </ul>
                </div>
                {detailQuery.data.body.trim() ? (
                  <details className="rounded-md border border-border p-3">
                    <summary className="cursor-pointer font-medium text-xs">
                      Full issue description
                    </summary>
                    <pre className="mt-3 whitespace-pre-wrap font-sans text-muted-foreground text-xs leading-relaxed">
                      {detailQuery.data.body}
                    </pre>
                  </details>
                ) : null}
              </article>
            ) : null}
          </div>
        </div>
      )}
    </section>
  );
}
