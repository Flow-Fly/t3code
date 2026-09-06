import {
  WorkflowDirectorError,
  WorkflowQueryError,
  WorkflowWorkerAssociateInput,
  WorkflowWorkerHandoffInput,
  WorkflowWorkerPrepareInput,
  WorkflowWorkerPrepareResult,
  WorkflowWorkerStatus,
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

export const WorkflowDirectorToolkit = Toolkit.make(
  WorkflowPrepareWorkerTool,
  WorkflowAssociateWorkerTool,
  WorkflowReportWorkerHandoffTool,
);
