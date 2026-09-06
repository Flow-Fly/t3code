import * as Effect from "effect/Effect";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as WorkflowDirectorService from "../../../workflow/WorkflowDirectorService.ts";
import { WorkflowDirectorToolkit } from "./tools.ts";

export const workflowDirectorHandlers = {
  workflow_prepare_worker: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.prepareWorker(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_associate_worker: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.associateWorker(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_report_worker_handoff: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.reportWorkerHandoff(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
} satisfies Parameters<typeof WorkflowDirectorToolkit.toLayer>[0];

export const WorkflowDirectorToolkitHandlersLive =
  WorkflowDirectorToolkit.toLayer(workflowDirectorHandlers);
