import { createWorkflowEnvironmentAtoms } from "@t3tools/client-runtime/state/workflow";

import { connectionAtomRuntime } from "../connection/runtime";

export const workflowEnvironment = createWorkflowEnvironmentAtoms(connectionAtomRuntime);
