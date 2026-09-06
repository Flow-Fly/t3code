import { WS_METHODS } from "@t3tools/contracts";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import { createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";
import { createAtomCommandScheduler, createEnvironmentRpcCommand } from "./runtime.ts";

/** Environment-targeted GitHub browsing. Each query is executed by the selected server. */
export function createWorkflowEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const commandScheduler = createAtomCommandScheduler();
  const serialPerEnvironment = {
    mode: "serial" as const,
    key: ({ environmentId }: { readonly environmentId: string }) => environmentId,
  };
  return {
    repositories: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:repositories",
      tag: WS_METHODS.workflowRepositories,
      staleTimeMs: 60_000,
    }),
    roots: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:roots",
      tag: WS_METHODS.workflowRoots,
      staleTimeMs: 30_000,
    }),
    children: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:children",
      tag: WS_METHODS.workflowChildren,
      staleTimeMs: 30_000,
    }),
    issueDetail: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:workflow:issue-detail",
      tag: WS_METHODS.workflowIssueDetail,
      staleTimeMs: 30_000,
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
