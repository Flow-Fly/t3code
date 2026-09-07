import {
  WorkflowCapabilityCompleteInput,
  WorkflowCapabilityCompleteResult,
  WorkflowDirectorError,
  WorkflowDirectorHandoffPrepareInput,
  WorkflowDirectorHandoffReconcileInput,
  WorkflowDirectorHandoffStatus,
  WorkflowQueryError,
  WorkflowWorkerAssociateInput,
  WorkflowWorkerHandoffInput,
  WorkflowWorkerPrepareInput,
  WorkflowWorkerPrepareResult,
  WorkflowWorkerStatus,
  WorkflowReviewCheckReceiptInput,
  WorkflowReviewDispositionInput,
  WorkflowTicketResolveInput,
  WorkflowTicketResolveResult,
  WorkflowTicketReviewAssociateInput,
  WorkflowTicketReviewPrepareInput,
  WorkflowTicketReviewPrepareResult,
  WorkflowTicketReviewReportInput,
  WorkflowTicketReviewStatus,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as WorkflowDirectorService from "../../../workflow/WorkflowDirectorService.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  WorkflowDirectorService.WorkflowDirectorService,
];
const WorkflowWorkerFailure = Schema.Union([WorkflowDirectorError, WorkflowQueryError]);

export const WorkflowPrepareWorkerTool = Tool.make("workflow_prepare_worker", {
  description:
    "Admit one approved delivery ticket and reserve explicit write ownership before native Codex child dispatch. The authenticated director thread supplies capability, batch, project, repository, and environment identity.",
  parameters: WorkflowWorkerPrepareInput,
  success: WorkflowWorkerPrepareResult,
  failure: WorkflowWorkerFailure,
  dependencies,
});

export const WorkflowAssociateWorkerTool = Tool.make("workflow_associate_worker", {
  description:
    "Associate a prepared worker token with an exact child provider thread after that child has appeared in this director's native collaboration stream.",
  parameters: WorkflowWorkerAssociateInput,
  success: WorkflowWorkerStatus,
  failure: WorkflowDirectorError,
  dependencies,
});

export const WorkflowReportWorkerHandoffTool = Tool.make("workflow_report_worker_handoff", {
  description:
    "Record an associated worker's explicit implementation handoff, including commits and checks. Provider idle or turn completion is never treated as this report.",
  parameters: WorkflowWorkerHandoffInput,
  success: WorkflowWorkerStatus,
  failure: WorkflowDirectorError,
  dependencies,
});

export const WorkflowPrepareTicketReviewTool = Tool.make("workflow_prepare_ticket_review", {
  description:
    "Register agreed checks against an associated implementation handoff and, after exact provider command receipts verify them, prepare a fresh Astra/medium code-review coordinator.",
  parameters: WorkflowTicketReviewPrepareInput,
  success: WorkflowTicketReviewPrepareResult,
  failure: WorkflowWorkerFailure,
  dependencies,
});

export const WorkflowRecordReviewChecksTool = Tool.make("workflow_record_review_checks", {
  description:
    "Bind registered checks to exact native Codex command start/completion receipts. Commands must run through the normal provider approval path in the capability worktree.",
  parameters: WorkflowReviewCheckReceiptInput,
  success: WorkflowTicketReviewStatus,
  failure: WorkflowDirectorError,
  dependencies,
});

export const WorkflowAssociateTicketReviewTool = Tool.make("workflow_associate_ticket_review", {
  description:
    "Associate a prepared ticket review with one fresh exact native reviewer child under this director.",
  parameters: WorkflowTicketReviewAssociateInput,
  success: WorkflowTicketReviewStatus,
  failure: WorkflowDirectorError,
  dependencies,
});

export const WorkflowReportTicketReviewTool = Tool.make("workflow_report_ticket_review", {
  description:
    "Record one associated coordinator's independent Standards and Spec results using two fresh exact native child identities. This does not dispose findings.",
  parameters: WorkflowTicketReviewReportInput,
  success: WorkflowTicketReviewStatus,
  failure: WorkflowDirectorError,
  dependencies,
});

export const WorkflowRecordReviewDispositionsTool = Tool.make(
  "workflow_record_review_dispositions",
  {
    description:
      "Record the director's separate source-validated disposition and rationale for review findings.",
    parameters: WorkflowReviewDispositionInput,
    success: WorkflowTicketReviewStatus,
    failure: WorkflowDirectorError,
    dependencies,
  },
);

export const WorkflowResolveTicketTool = Tool.make("workflow_resolve_ticket", {
  description:
    "Resolve a delivery ticket only after committed clean work, provider-verified checks, settled independent review, dispositions, current scope, reconciled GitHub evidence, and a refreshed capability frontier.",
  parameters: WorkflowTicketResolveInput,
  success: WorkflowTicketResolveResult,
  failure: WorkflowWorkerFailure,
  dependencies,
});

export const WorkflowCompleteCapabilityTool = Tool.make("workflow_complete_capability", {
  description:
    "Register combined acceptance against one exact clean result head, bind exact native command receipts, reconcile durable evidence, and close the capability only while all approved work and native children remain settled.",
  parameters: WorkflowCapabilityCompleteInput,
  success: WorkflowCapabilityCompleteResult,
  failure: WorkflowWorkerFailure,
  dependencies,
});

export const WorkflowPrepareDirectorHandoffTool = Tool.make("workflow_prepare_director_handoff", {
  description:
    "Record the current ten-slot director's useful lessons and unresolved context. T3 computes the authoritative admissions, outcomes, links, worktree, and implementation head, then automatically starts a successor only after live authority and exact native settlement are verified.",
  parameters: WorkflowDirectorHandoffPrepareInput,
  success: WorkflowDirectorHandoffStatus,
  failure: WorkflowWorkerFailure,
  dependencies,
});

export const WorkflowReconcileDirectorHandoffTool = Tool.make(
  "workflow_reconcile_director_handoff",
  {
    description:
      "Acknowledge newly settled native activity on one exact predecessor handoff. This appends evidence for the current director without changing the original handoff or creating another director turn.",
    parameters: WorkflowDirectorHandoffReconcileInput,
    success: WorkflowDirectorHandoffStatus,
    failure: WorkflowWorkerFailure,
    dependencies,
  },
);

export const WorkflowDirectorToolkit = Toolkit.make(
  WorkflowPrepareWorkerTool,
  WorkflowAssociateWorkerTool,
  WorkflowReportWorkerHandoffTool,
  WorkflowPrepareTicketReviewTool,
  WorkflowRecordReviewChecksTool,
  WorkflowAssociateTicketReviewTool,
  WorkflowReportTicketReviewTool,
  WorkflowRecordReviewDispositionsTool,
  WorkflowResolveTicketTool,
  WorkflowCompleteCapabilityTool,
  WorkflowPrepareDirectorHandoffTool,
  WorkflowReconcileDirectorHandoffTool,
);
