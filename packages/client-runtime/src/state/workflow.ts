import { WS_METHODS, type EnvironmentId, type WorkflowMonitorInput } from "@t3tools/contracts";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createAtomCommandScheduler,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

/** Environment-targeted GitHub browsing. Each query is executed by the selected server. */
export function createWorkflowEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const commandScheduler = createAtomCommandScheduler();
  const serialPerEnvironment = {
    mode: "serial" as const,
    key: ({ environmentId }: { readonly environmentId: string }) => environmentId,
  };
  const sync = createEnvironmentRpcSubscriptionAtomFamily(runtime, {
    label: "environment-data:workflow:sync",
    tag: WS_METHODS.workflowWatch,
    idleTtlMs: 0,
  });
  const syncTrigger = ({
    environmentId,
    input,
  }: {
    readonly environmentId: EnvironmentId;
    readonly input: WorkflowMonitorInput;
  }) =>
    sync({
      environmentId,
      input: { projectId: input.projectId, repository: input.repository },
    });
  return {
    sync,
    repositories: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:repositories",
      tag: WS_METHODS.workflowRepositories,
      staleTimeMs: 60_000,
    }),
    roots: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:roots",
      tag: WS_METHODS.workflowRoots,
      staleTimeMs: 30_000,
      idleTtlMs: 0,
      refreshTrigger: syncTrigger,
    }),
    children: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:children",
      tag: WS_METHODS.workflowChildren,
      staleTimeMs: 30_000,
      idleTtlMs: 0,
      refreshTrigger: syncTrigger,
    }),
    issueDetail: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:issue-detail",
      tag: WS_METHODS.workflowIssueDetail,
      staleTimeMs: 30_000,
      idleTtlMs: 0,
      refreshTrigger: syncTrigger,
    }),
    search: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:search",
      tag: WS_METHODS.workflowSearch,
      staleTimeMs: 30_000,
    }),
    locate: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:locate",
      tag: WS_METHODS.workflowLocate,
      staleTimeMs: 30_000,
    }),
    refresh: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:refresh",
      tag: WS_METHODS.workflowRefresh,
      scheduler: commandScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) =>
          JSON.stringify([environmentId, input.repository.toLowerCase()]),
      },
    }),
    start: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:start",
      tag: WS_METHODS.workflowStart,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    recovery: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:recovery",
      tag: WS_METHODS.workflowRecovery,
      staleTimeMs: 0,
    }),
    recover: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:recover",
      tag: WS_METHODS.workflowRecover,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    directorStart: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:director-start",
      tag: WS_METHODS.workflowDirectorStart,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    directorStatus: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:director-status",
      tag: WS_METHODS.workflowDirectorStatus,
      staleTimeMs: 0,
    }),
    directorReassessmentRetry: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:director-reassessment-retry",
      tag: WS_METHODS.workflowDirectorReassessmentRetry,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    directorResume: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:director-resume",
      tag: WS_METHODS.workflowDirectorResume,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    directorAdmit: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:director-admit",
      tag: WS_METHODS.workflowDirectorAdmit,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    adoptionPreview: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:adoption-preview",
      tag: WS_METHODS.workflowAdoptionPreview,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    adoptionApply: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:adoption-apply",
      tag: WS_METHODS.workflowAdoptionApply,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    adoptionHistory: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:adoption-history",
      tag: WS_METHODS.workflowAdoptionHistory,
      staleTimeMs: 0,
    }),
    adoptionRecover: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:adoption-recover",
      tag: WS_METHODS.workflowAdoptionRecover,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
    adoptionUndo: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workflow:adoption-undo",
      tag: WS_METHODS.workflowAdoptionUndo,
      scheduler: commandScheduler,
      concurrency: serialPerEnvironment,
    }),
  };
}
