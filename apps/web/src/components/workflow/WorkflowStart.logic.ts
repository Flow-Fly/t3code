import type { ModelSelection, ServerProvider } from "@t3tools/contracts";
import {
  buildProviderOptionSelectionsFromDescriptors,
  getModelSelectionStringOptionValue,
  getProviderOptionDescriptors,
} from "@t3tools/shared/model";

export type WorkflowStartSelection =
  | { readonly selection: ModelSelection; readonly message: null }
  | { readonly selection: null; readonly message: string };

export function resolveWorkflowStartSelection(
  providers: ReadonlyArray<ServerProvider>,
  projectDefault: ModelSelection | null | undefined,
): WorkflowStartSelection {
  if (!projectDefault) {
    return {
      selection: null,
      message: "Choose a default Codex model and reasoning effort in project settings.",
    };
  }
  const provider = providers.find(
    (candidate) => candidate.instanceId === projectDefault.instanceId,
  );
  if (
    !provider ||
    provider.driver !== "codex" ||
    !provider.enabled ||
    !provider.installed ||
    provider.status === "error" ||
    provider.status === "disabled"
  ) {
    return {
      selection: null,
      message: "The project default must use an enabled Codex provider.",
    };
  }
  const model = provider.models.find((candidate) => candidate.slug === projectDefault.model);
  if (!model?.capabilities) {
    return {
      selection: null,
      message: "The project default Codex model is unavailable.",
    };
  }
  const explicitEffort = getModelSelectionStringOptionValue(projectDefault, "reasoningEffort");
  const effortDescriptor = model.capabilities.optionDescriptors?.find(
    (descriptor) => descriptor.id === "reasoningEffort",
  );
  if (
    explicitEffort &&
    (effortDescriptor?.type !== "select" ||
      !effortDescriptor.options.some((option) => option.id === explicitEffort))
  ) {
    return {
      selection: null,
      message: `Reasoning effort '${explicitEffort}' is unavailable for the project default Codex model.`,
    };
  }
  const options = buildProviderOptionSelectionsFromDescriptors(
    getProviderOptionDescriptors({
      caps: model.capabilities,
      selections: projectDefault.options,
    }),
  );
  const selection = { ...projectDefault, ...(options ? { options } : {}) };
  if (!getModelSelectionStringOptionValue(selection, "reasoningEffort")) {
    return {
      selection: null,
      message: "Choose a reasoning effort for the project default Codex model.",
    };
  }
  return { selection, message: null };
}
