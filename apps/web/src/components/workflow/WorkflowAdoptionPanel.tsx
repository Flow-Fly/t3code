import type {
  EnvironmentId,
  ProjectId,
  WorkflowAdoptionItem,
  WorkflowAdoptionPreview,
  WorkflowAdoptionRecord,
  WorkflowIssueKind,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";
import { useEffect, useRef, useState } from "react";

import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { useEnvironmentQuery, formatEnvironmentQueryError } from "~/state/query";
import { useAtomCommand } from "~/state/use-atom-command";
import { workflowEnvironment } from "~/state/workflow";

import { workflowAdoptionItemChanges } from "./WorkflowAdoption.logic";

const KINDS: ReadonlyArray<WorkflowIssueKind> = [
  "map",
  "decision",
  "capability",
  "container",
  "ticket",
  "task",
];

function commandError(result: { readonly cause: Cause.Cause<unknown> }) {
  return formatEnvironmentQueryError(result.cause);
}

export function WorkflowAdoptionPanel(props: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  repository: string;
  rootNumber: number;
  onClose: () => void;
  onChanged: () => void;
}) {
  const previewCommand = useAtomCommand(workflowEnvironment.adoptionPreview, {
    reportFailure: false,
  });
  const applyCommand = useAtomCommand(workflowEnvironment.adoptionApply, {
    reportFailure: false,
  });
  const undoCommand = useAtomCommand(workflowEnvironment.adoptionUndo, {
    reportFailure: false,
  });
  const recoverCommand = useAtomCommand(workflowEnvironment.adoptionRecover, {
    reportFailure: false,
  });
  const history = useEnvironmentQuery(
    workflowEnvironment.adoptionHistory({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        repository: props.repository,
        rootNumber: props.rootNumber,
      },
    }),
  );
  const [preview, setPreview] = useState<WorkflowAdoptionPreview | null>(null);
  const [items, setItems] = useState<ReadonlyArray<WorkflowAdoptionItem>>([]);
  const [result, setResult] = useState<WorkflowAdoptionRecord | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [undoReview, setUndoReview] = useState<WorkflowAdoptionRecord | null>(null);
  const requestId = useRef(0);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    closeButtonRef.current?.focus();
    return () => {
      requestId.current += 1;
    };
  }, []);

  const updateItem = (repository: string, number: number, patch: Partial<WorkflowAdoptionItem>) =>
    setItems((current) =>
      current.map((item) =>
        item.repository === repository && item.number === number ? { ...item, ...patch } : item,
      ),
    );

  const loadPreview = async () => {
    const currentRequest = ++requestId.current;
    setIsSubmitting(true);
    setError(null);
    const response = await previewCommand({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        repository: props.repository,
        rootNumber: props.rootNumber,
      },
    });
    if (currentRequest !== requestId.current) return;
    setIsSubmitting(false);
    if (AsyncResult.isSuccess(response)) {
      setPreview(response.value);
      setItems(response.value.items);
      setResult(null);
    } else if (AsyncResult.isFailure(response)) setError(commandError(response));
  };

  const apply = async () => {
    if (!preview || isSubmitting) return;
    const currentRequest = ++requestId.current;
    setIsSubmitting(true);
    setError(null);
    const response = await applyCommand({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        previewId: preview.previewId,
        repository: preview.repository,
        rootNumber: preview.rootNumber,
        items,
      },
    });
    if (currentRequest !== requestId.current) return;
    setIsSubmitting(false);
    if (AsyncResult.isSuccess(response)) {
      setResult(response.value);
      props.onChanged();
      history.refresh();
    } else if (AsyncResult.isFailure(response)) setError(commandError(response));
  };

  const undo = async (adoptionId: string) => {
    if (isSubmitting) return;
    const currentRequest = ++requestId.current;
    setIsSubmitting(true);
    setError(null);
    const response = await undoCommand({
      environmentId: props.environmentId,
      input: { projectId: props.projectId, adoptionId },
    });
    if (currentRequest !== requestId.current) return;
    setIsSubmitting(false);
    if (AsyncResult.isSuccess(response)) {
      setResult(response.value);
      setUndoReview(null);
      props.onChanged();
      history.refresh();
    } else if (AsyncResult.isFailure(response)) setError(commandError(response));
  };

  const recover = async (adoptionId: string) => {
    if (isSubmitting) return;
    const currentRequest = ++requestId.current;
    setIsSubmitting(true);
    setError(null);
    const response = await recoverCommand({
      environmentId: props.environmentId,
      input: {
        projectId: props.projectId,
        repository: props.repository,
        rootNumber: props.rootNumber,
        adoptionId,
      },
    });
    if (currentRequest !== requestId.current) return;
    setIsSubmitting(false);
    if (AsyncResult.isSuccess(response)) {
      setPreview(response.value.preview);
      setItems(response.value.preview.items);
      setResult(response.value.record);
      setUndoReview(null);
    } else if (AsyncResult.isFailure(response)) setError(commandError(response));
  };

  const parentConfirmationMissing = items.some(
    (item) =>
      item.included &&
      item.currentParentNumber !== item.proposedParentNumber &&
      !item.parentChangeConfirmed,
  );

  return (
    <section
      className="flex min-h-0 flex-1 flex-col bg-background"
      aria-labelledby="workflow-adoption-heading"
    >
      <header className="flex items-center justify-between border-b border-border px-3 py-2">
        <div>
          <h2 id="workflow-adoption-heading" className="font-medium text-sm">
            Adopt branch #{props.rootNumber}
          </h2>
          <p className="text-muted-foreground text-xs">
            Review classifications and exact GitHub changes. This does not start work or approve
            delivery.
          </p>
        </div>
        <Button ref={closeButtonRef} size="xs" variant="ghost" onClick={props.onClose}>
          Close
        </Button>
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto p-3">
        {!preview ? (
          <div className="grid gap-3 rounded-md border border-border p-3">
            <p className="text-xs">
              T3 will inspect the selected root and its current native descendants.
            </p>
            <Button size="sm" disabled={isSubmitting} onClick={() => void loadPreview()}>
              {isSubmitting ? "Inspecting…" : "Preview adoption"}
            </Button>
          </div>
        ) : (
          <div className="grid gap-3">
            <ul className="grid gap-2" aria-label="Adoption preview items">
              {items.map((item) => {
                const changes = workflowAdoptionItemChanges(item);
                const parentChanged = item.currentParentNumber !== item.proposedParentNumber;
                return (
                  <li
                    key={item.id}
                    className="grid gap-2 rounded-md border border-border p-3 text-xs"
                  >
                    <label className="flex items-start gap-2">
                      <input
                        type="checkbox"
                        checked={item.included}
                        onChange={(event) =>
                          updateItem(item.repository, item.number, {
                            included: event.target.checked,
                          })
                        }
                      />
                      <span>
                        <span className="font-medium">
                          {item.repository}#{item.number} {item.title}
                        </span>
                        <span className="block text-muted-foreground">
                          Current: {item.currentKind ?? "unclassified"}
                        </span>
                      </span>
                    </label>
                    {item.included ? (
                      <div className="grid gap-2 @md/workflow:grid-cols-2">
                        <label className="grid gap-1">
                          Classification
                          <select
                            className="h-8 rounded-md border border-input bg-background px-2"
                            value={item.proposedKind}
                            onChange={(event) =>
                              updateItem(item.repository, item.number, {
                                proposedKind: event.target.value as WorkflowIssueKind,
                              })
                            }
                          >
                            {KINDS.map((kind) => (
                              <option key={kind} value={kind}>
                                {kind}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label className="grid gap-1">
                          Native parent
                          <Input
                            size="sm"
                            inputMode="numeric"
                            placeholder="No parent"
                            value={item.proposedParentNumber ?? ""}
                            onChange={(event) => {
                              const value = event.target.value.trim();
                              updateItem(item.repository, item.number, {
                                proposedParentNumber: value ? Number(value) : null,
                                parentChangeConfirmed: value
                                  ? Number(value) === item.currentParentNumber
                                  : item.currentParentNumber === null,
                              });
                            }}
                          />
                        </label>
                        {parentChanged ? (
                          <label className="col-span-full flex items-center gap-2 rounded bg-amber-500/10 p-2">
                            <input
                              type="checkbox"
                              checked={item.parentChangeConfirmed}
                              onChange={(event) =>
                                updateItem(item.repository, item.number, {
                                  parentChangeConfirmed: event.target.checked,
                                })
                              }
                            />
                            Confirm native parent change
                          </label>
                        ) : null}
                        <div className="col-span-full">
                          <span className="font-medium">Exact changes</span>
                          {changes.length ? (
                            <ul className="list-disc pl-5">
                              {changes.map((change) => (
                                <li key={change}>{change}</li>
                              ))}
                            </ul>
                          ) : (
                            <p className="text-muted-foreground">No GitHub write needed.</p>
                          )}
                        </div>
                        {item.relationships.length ? (
                          <div className="col-span-full">
                            <span className="font-medium">Preserved sources</span>
                            <ul>
                              {item.relationships.map((relationship) => (
                                <li
                                  key={`${relationship.relationship}:${relationship.issueNumber}`}
                                >
                                  <a
                                    className="underline"
                                    href={relationship.source}
                                    target="_blank"
                                    rel="noreferrer"
                                  >
                                    {relationship.relationship} #{relationship.issueNumber}
                                  </a>
                                </li>
                              ))}
                            </ul>
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                  </li>
                );
              })}
            </ul>
            <Button
              disabled={isSubmitting || parentConfirmationMissing}
              onClick={() => void apply()}
            >
              {isSubmitting
                ? "Applying…"
                : result?.status === "partial"
                  ? "Retry incomplete changes"
                  : "Apply reviewed adoption"}
            </Button>
          </div>
        )}
        {error ? (
          <p
            className="mt-3 rounded-md bg-destructive/10 p-2 text-destructive text-xs"
            role="alert"
          >
            {error}
          </p>
        ) : null}
        {result ? (
          <div
            className="mt-3 rounded-md border border-border p-3 text-xs"
            aria-label="Adoption result"
          >
            <p className="font-medium">Result: {result.status}</p>
            <ul className="mt-1 list-disc pl-5">
              {result.operations.map((operation) => (
                <li
                  key={`${operation.repository}:${operation.issueId}:${operation.kind}:${operation.description}`}
                >
                  <a
                    className="underline"
                    href={`https://github.com/${operation.repository}/issues/${operation.issueNumber}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {operation.repository}#{operation.issueNumber}
                  </a>{" "}
                  {operation.description}: {operation.status}
                  {operation.owned ? " (adoption-owned)" : ""}
                  {operation.detail ? ` · ${operation.detail}` : ""}
                </li>
              ))}
            </ul>
          </div>
        ) : null}
        <section className="mt-4 border-t border-border pt-3" aria-label="Adoption history">
          <h3 className="font-medium text-xs">History</h3>
          {history.error ? <p className="text-destructive text-xs">{history.error}</p> : null}
          {history.data?.records.map((record) => (
            <div
              key={record.adoptionId}
              className="mt-2 flex items-center justify-between gap-2 rounded-md border border-border p-2 text-xs"
            >
              <span>
                {record.createdAt} · {record.status}
              </span>
              <span className="flex gap-1">
                {record.status === "partial" ? (
                  <Button
                    size="xs"
                    variant="outline"
                    disabled={isSubmitting}
                    onClick={() => void recover(record.adoptionId)}
                  >
                    Resume review
                  </Button>
                ) : null}
                <Button
                  size="xs"
                  variant="outline"
                  disabled={isSubmitting || record.status === "undone"}
                  onClick={() => setUndoReview(record)}
                >
                  Review undo
                </Button>
              </span>
            </div>
          ))}
          {undoReview ? (
            <div
              className="mt-3 rounded-md border border-border p-3 text-xs"
              aria-label="Review adoption undo"
            >
              <p className="font-medium">Review changes eligible for undo</p>
              <p className="mt-1 text-muted-foreground">
                The server will restore only adoption-owned values that still match. Later edits and
                unowned reconciliations are retained.
              </p>
              <ul className="mt-2 list-disc pl-5">
                {undoReview.operations.map((operation) => (
                  <li
                    key={`${operation.repository}:${operation.issueId}:${operation.kind}:${operation.description}`}
                  >
                    <a
                      className="underline"
                      href={`https://github.com/${operation.repository}/issues/${operation.issueNumber}`}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {operation.repository}#{operation.issueNumber}
                    </a>{" "}
                    {operation.description} ·{" "}
                    {operation.owned ? "eligible if unchanged" : "will be preserved"}
                    {operation.detail ? ` · ${operation.detail}` : ""}
                  </li>
                ))}
              </ul>
              <div className="mt-3 flex gap-2">
                <Button size="xs" variant="outline" onClick={() => setUndoReview(null)}>
                  Cancel
                </Button>
                <Button
                  size="xs"
                  disabled={isSubmitting}
                  onClick={() => void undo(undoReview.adoptionId)}
                >
                  Confirm selective undo
                </Button>
              </div>
            </div>
          ) : null}
        </section>
      </div>
    </section>
  );
}
