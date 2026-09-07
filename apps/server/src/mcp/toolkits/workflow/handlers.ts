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
  workflow_prepare_ticket_review: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.prepareTicketReview(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_record_review_checks: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.recordReviewChecks(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_associate_ticket_review: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.associateTicketReview(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_report_ticket_review: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.reportTicketReview(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_record_review_dispositions: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.recordReviewDispositions(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
  workflow_resolve_ticket: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      return yield* workflow.resolveTicket(
        scope.environmentId,
        scope.threadId,
        scope.providerInstanceId,
        input,
      );
    }),
} satisfies Parameters<typeof WorkflowDirectorToolkit.toLayer>[0];

export const WorkflowDirectorToolkitHandlersLive =
  WorkflowDirectorToolkit.toLayer(workflowDirectorHandlers);
