#!/usr/bin/env node

import * as NodeFS from "node:fs";

import { runGitHubCommand } from "./fixture.mjs";

const statePath = process.env.T3_WORKFLOW_FIXTURE_STATE;
if (!statePath) {
  process.stderr.write("T3_WORKFLOW_FIXTURE_STATE must name an initialized fixture state file.\n");
  process.exit(2);
}

try {
  const args = process.argv.slice(2);
  const readsStdin =
    (args.includes("--input") && args[args.indexOf("--input") + 1] === "-") ||
    (args.includes("--body-file") && args[args.indexOf("--body-file") + 1] === "-");
  const stdin = readsStdin ? NodeFS.readFileSync(0, "utf8") : "";
  const result = runGitHubCommand(statePath, args, stdin);
  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
}
