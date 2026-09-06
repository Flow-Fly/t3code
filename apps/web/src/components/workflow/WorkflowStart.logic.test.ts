import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { resolveWorkflowStartSelection } from "./WorkflowStart.logic";

function provider(driver = ProviderDriverKind.make("codex")): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make("workflow-provider"),
    driver,
    status: "ready",
    enabled: true,
    installed: true,
    auth: { status: "authenticated" },
    checkedAt: "2026-09-06T10:00:00.000Z",
    version: "1.0.0",
    models: [
      {
        slug: "gpt-6-astra",
        name: "GPT-6 Astra",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "reasoningEffort",
              label: "Reasoning effort",
              type: "select",
              currentValue: "high",
              options: [
                { id: "medium", label: "Medium" },
                { id: "high", label: "High" },
              ],
            },
          ],
        },
      },
    ],
    slashCommands: [],
    skills: [],
  };
}

describe("resolveWorkflowStartSelection", () => {
  it("materializes the configured model's reasoning effort without changing provider or model", () => {
    const codex = provider();
    expect(
      resolveWorkflowStartSelection([codex], {
        instanceId: codex.instanceId,
        model: "gpt-6-astra",
      }),
    ).toEqual({
      selection: {
        instanceId: codex.instanceId,
        model: "gpt-6-astra",
        options: [{ id: "reasoningEffort", value: "high" }],
      },
      message: null,
    });
  });

  it("does not substitute another provider for a non-Codex project default", () => {
    const claude = provider(ProviderDriverKind.make("claudeAgent"));
    const codex = { ...provider(), instanceId: ProviderInstanceId.make("codex-other") };
    expect(
      resolveWorkflowStartSelection([claude, codex], {
        instanceId: claude.instanceId,
        model: "gpt-6-astra",
      }),
    ).toEqual({
      selection: null,
      message: "The project default must use an enabled Codex provider.",
    });
  });

  it("rejects an unsupported explicit effort without replacing it with the provider default", () => {
    const codex = provider();
    expect(
      resolveWorkflowStartSelection([codex], {
        instanceId: codex.instanceId,
        model: "gpt-6-astra",
        options: [{ id: "reasoningEffort", value: "ultra" }],
      }),
    ).toEqual({
      selection: null,
      message: "Reasoning effort 'ultra' is unavailable for the project default Codex model.",
    });
  });
});
