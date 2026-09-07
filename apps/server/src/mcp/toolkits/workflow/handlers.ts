import * as Effect from "effect/Effect";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as WorkflowDirectorService from "../../../workflow/WorkflowDirectorService.ts";
import * as WorkflowMonitor from "../../../workflow/WorkflowMonitor.ts";
import { WorkflowDirectorToolkit } from "./tools.ts";

export const workflowDirectorHandlers = {
  workflow_prepare_worker: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .prepareWorker(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_associate_worker: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .associateWorker(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_report_worker_handoff: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .reportWorkerHandoff(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_prepare_ticket_review: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .prepareTicketReview(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_record_review_checks: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .recordReviewChecks(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_associate_ticket_review: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .associateTicketReview(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_report_ticket_review: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .reportTicketReview(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_record_review_dispositions: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .recordReviewDispositions(
          scope.environmentId,
          scope.threadId,
          scope.providerInstanceId,
          input,
        )
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
  workflow_resolve_ticket: (input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const workflow = yield* WorkflowDirectorService.WorkflowDirectorService;
      const notifier = yield* WorkflowMonitor.WorkflowRefreshNotifier;
      return yield* workflow
        .resolveTicket(scope.environmentId, scope.threadId, scope.providerInstanceId, input)
        .pipe(Effect.ensuring(notifier.directorChanged(scope).pipe(Effect.ignore)));
    }),
} satisfies Parameters<typeof WorkflowDirectorToolkit.toLayer>[0];

export const WorkflowDirectorToolkitHandlersLive =
  WorkflowDirectorToolkit.toLayer(workflowDirectorHandlers);
