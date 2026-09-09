#!/usr/bin/env node

import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import {
  addReassessment,
  initialize,
  inspectIssue,
  installLauncher,
  setFailure,
  setOffline,
} from "./fixture.mjs";

function argumentValue(args, flag) {
  const index = args.indexOf(flag);
  return index === -1 ? undefined : args[index + 1];
}

const args = process.argv.slice(2);
const command = args[0];
const statePath = argumentValue(args, "--state");
if (!statePath) {
  process.stderr.write("Pass --state <path>.\n");
  process.exit(2);
}

try {
  if (command === "init") {
    const binDirectory = argumentValue(args, "--bin");
    if (!binDirectory) throw new Error("Pass --bin <directory> when initializing the fixture.");
    const summary = initialize(statePath);
    installLauncher(
      binDirectory,
      statePath,
      NodePath.join(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "gh.mjs"),
    );
    process.stdout.write(`${JSON.stringify(summary)}\n`);
  } else if (command === "offline") {
    if (args[1] !== "on" && args[1] !== "off") throw new Error("Use offline on or offline off.");
    setOffline(statePath, args[1] === "on");
  } else if (command === "fail-next") {
    if (!args[1] || !args[2]) throw new Error("Use fail-next <before|after> <operation>.");
    setFailure(statePath, args[1], args[2]);
  } else if (command === "inspect") {
    if (!args[1] || !args[2]) throw new Error("Use inspect <repository> <issue-number>.");
    process.stdout.write(`${JSON.stringify(inspectIssue(statePath, args[1], Number(args[2])))}\n`);
  } else if (command === "reassessment") {
    if (!args[1] || !args[2] || !args[3] || !args[4]) {
      throw new Error(
        "Use reassessment <repository> <issue-number> <scope-change|cleared> <summary>.",
      );
    }
    addReassessment(statePath, args[1], Number(args[2]), args[3], args[4]);
  } else {
    throw new Error(`Unknown fixture control command ${command ?? "(missing)"}.`);
  }
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
}
