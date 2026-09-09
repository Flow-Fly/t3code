import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const LOCK_WAIT_MS = 2_000;
const STALE_LOCK_MS = 30_000;
const PAGE_SIZE = 100;

const now = "2026-09-09T12:00:00.000Z";
const owner = "workflow-fixture";

function resolutionBody(summary, outcome = "resolved") {
  return [
    "## Resolution",
    "<!-- t3-workflow:v1 resolution -->",
    `Outcome: ${outcome}`,
    "### Summary",
    summary,
    "### Evidence",
    "[Synthetic verification](https://example.invalid/workflow-fixture/evidence)",
  ].join("\n");
}

function reassessmentBody(outcome, summary, sourceUrl, supersededUrls) {
  return [
    "## Reassessment",
    "<!-- t3-workflow:v1 reassessment -->",
    `Trigger: [Synthetic owner evidence](${sourceUrl})`,
    `Outcome: ${outcome}`,
    ...supersededUrls.map((url) => `Supersedes: ${url}`),
    "### Changes",
    summary,
    "### Evidence",
    `[Synthetic owner evidence](${sourceUrl})`,
  ].join("\n");
}

function ticketBody({ repository, capabilityNumber, id, title, what, blocker = "None" }) {
  return [
    "## Parent",
    "",
    `[Capability](https://github.com/${repository}/issues/${capabilityNumber})`,
    "",
    `Approved slice: **${id}** ([ticket-breakdown approval](https://github.com/${repository}/issues/${capabilityNumber}#issuecomment-breakdown))`,
    "",
    "## What to build",
    "",
    what,
    "",
    "## Acceptance criteria",
    "",
    `- [ ] ${title} works in the synthetic workflow.`,
    "",
    "## Blocked by",
    "",
    blocker,
  ].join("\n");
}

function comment(id, body, createdAt = now) {
  return {
    id,
    url: `https://github.com/fixture/workflow-demo/issues/10#issuecomment-${id}`,
    body,
    createdAt,
    author: owner,
    authorAssociation: "OWNER",
  };
}

function commentOnIssue(repository, issueNumber, id, body, createdAt = now) {
  return {
    ...comment(id, body, createdAt),
    url: `https://github.com/${repository}/issues/${issueNumber}#issuecomment-${id}`,
  };
}

function issue(input) {
  return {
    databaseId: input.databaseId ?? 10_000 + input.number,
    id: input.id ?? `I_${input.repository.replaceAll("/", "_")}_${input.number}`,
    repository: input.repository,
    number: input.number,
    title: input.title,
    body: input.body ?? "",
    state: input.state ?? "OPEN",
    stateReason: input.stateReason ?? null,
    updatedAt: input.updatedAt ?? now,
    lastEditedAt: input.lastEditedAt ?? "2026-09-01T12:00:00.000Z",
    labels: input.labels ?? [],
    assignees: input.assignees ?? [],
    comments: input.comments ?? [],
    reopenedAt: input.reopenedAt ?? [],
    parent: input.parent ?? null,
    children: input.children ?? [],
    blockedBy: input.blockedBy ?? [],
  };
}

function approvalComments(repository, capabilityNumber, capabilityBody, slices) {
  const specificationSource = commentOnIssue(
    repository,
    capabilityNumber,
    `${repository.replaceAll("/", "-")}-specification-source`,
    "I approve this synthetic capability specification for the isolated Workflow exercise.",
    "2026-09-02T11:55:00.000Z",
  );
  const breakdownSource = commentOnIssue(
    repository,
    capabilityNumber,
    `${repository.replaceAll("/", "-")}-breakdown-source`,
    "I approve this synthetic ticket breakdown for the isolated Workflow exercise.",
    "2026-09-02T12:00:00.000Z",
  );
  const specification = comment(
    `${repository.replaceAll("/", "-")}-specification`,
    [
      "## Approval",
      "<!-- t3-workflow:v1 approval -->",
      "Kind: specification",
      `Approved by: ${owner}`,
      `Source: [Owner specification approval](${specificationSource.url})`,
      "### Approved content",
      capabilityBody,
    ].join("\n"),
    "2026-09-02T12:00:00.000Z",
  );
  const breakdown = comment(
    `${repository.replaceAll("/", "-")}-breakdown`,
    [
      "## Approval",
      "<!-- t3-workflow:v1 approval -->",
      "Kind: ticket-breakdown",
      `Approved by: ${owner}`,
      `Source: [Owner breakdown approval](${breakdownSource.url})`,
      "### Approved content",
      ...slices.flatMap((slice) => [
        "<details>",
        `<summary>${slice.id} — ${slice.title}</summary>`,
        slice.body,
        "</details>",
      ]),
    ].join("\n"),
    "2026-09-02T12:05:00.000Z",
  );
  specification.url = `https://github.com/${repository}/issues/${capabilityNumber}#issuecomment-specification`;
  breakdown.url = `https://github.com/${repository}/issues/${capabilityNumber}#issuecomment-breakdown`;
  return [specificationSource, breakdownSource, specification, breakdown];
}

function buildLargeRepository() {
  const repository = "fixture/workflow-demo";
  const capabilityBody =
    "## Summary\n\nExercise a large anonymous workflow.\n\n## Source map\n\nhttps://github.com/fixture/workflow-demo/issues/2";
  const issues = {};
  const sliceNumbers = Array.from({ length: 52 }, (_, index) => 101 + index);
  const slices = sliceNumbers.map((number, index) => {
    const id = `T${String(index + 1).padStart(2, "0")}`;
    const title = `Anonymous delivery slice ${String(index + 1).padStart(2, "0")}`;
    return {
      id,
      title,
      body: ticketBody({
        repository,
        capabilityNumber: 10,
        id,
        title,
        what: `Implement synthetic workflow behavior ${index + 1}.`,
        blocker:
          number === 152
            ? "[External preparation](https://github.com/fixture/workflow-dependency/issues/900)"
            : "None",
      }),
    };
  });

  issues[1] = issue({
    repository,
    number: 1,
    title: "Anonymous product initiative",
    labels: ["workflow:container"],
    children: [2, 10, 700, 800],
  });
  issues[2] = issue({
    repository,
    number: 2,
    title: "Anonymous nested decision map",
    labels: ["wayfinder:map"],
    parent: { repository, number: 1 },
    children: [3, 4],
    body: "## Destination\n\nChoose a safe synthetic path.\n\n## Remaining fog\n\nNone.",
  });
  issues[3] = issue({
    repository,
    number: 3,
    title: "Research a synthetic dependency",
    labels: ["wayfinder:research"],
    parent: { repository, number: 2 },
    state: "CLOSED",
    stateReason: "COMPLETED",
    comments: [comment("decision-3", resolutionBody("The dependency is available."))],
  });
  issues[4] = issue({
    repository,
    number: 4,
    title: "Prototype the alternate path",
    labels: ["wayfinder:prototype"],
    parent: { repository, number: 2 },
    state: "CLOSED",
    stateReason: "NOT_PLANNED",
    comments: [comment("decision-4", resolutionBody("The alternate path was superseded."))],
  });
  issues[10] = issue({
    repository,
    number: 10,
    title: "Anonymous large workflow capability",
    labels: ["workflow:capability"],
    parent: { repository, number: 1 },
    children: sliceNumbers,
    body: capabilityBody,
    comments: approvalComments(repository, 10, capabilityBody, slices),
  });

  for (const [index, number] of sliceNumbers.entries()) {
    const slice = slices[index];
    const isReady = number === 152;
    const hasLongLedger = number === 151;
    const nestedTasks = index % 8 === 0 ? [2_000 + index] : [];
    const comments = hasLongLedger
      ? Array.from({ length: 121 }, (_, ledgerIndex) =>
          comment(
            `ledger-${String(ledgerIndex + 1).padStart(3, "0")}`,
            resolutionBody(`Synthetic ledger entry ${ledgerIndex + 1}.`),
            `2026-09-${String(2 + Math.floor(ledgerIndex / 24)).padStart(2, "0")}T${String(ledgerIndex % 24).padStart(2, "0")}:00:00.000Z`,
          ),
        )
      : isReady
        ? []
        : [comment(`resolution-${number}`, resolutionBody(`Slice ${index + 1} is complete.`))];
    issues[number] = issue({
      repository,
      number,
      title: slice.title,
      body: slice.body,
      labels: ["workflow:ticket", ...(number === 149 ? ["workflow:superseded"] : [])],
      parent: { repository, number: 10 },
      children: nestedTasks,
      state: isReady ? "OPEN" : "CLOSED",
      stateReason: isReady ? null : number === 149 ? "NOT_PLANNED" : "COMPLETED",
      comments,
      blockedBy: number === 152 ? [{ repository: "fixture/workflow-dependency", number: 900 }] : [],
    });
    for (const taskNumber of nestedTasks) {
      issues[taskNumber] = issue({
        repository,
        number: taskNumber,
        title: `Nested task for slice ${index + 1}`,
        labels: ["workflow:task"],
        parent: { repository, number },
        state: "CLOSED",
        stateReason: "COMPLETED",
        comments: [comment(`nested-${taskNumber}`, resolutionBody("The nested task is complete."))],
      });
    }
  }

  issues[700] = issue({
    repository,
    number: 700,
    title: "Existing unlabeled delivery branch",
    parent: { repository, number: 1 },
    children: [701],
    body: "## Summary\n\nPreview this branch before adoption.",
  });
  issues[701] = issue({
    repository,
    number: 701,
    title: "Adoptable delivery slice",
    parent: { repository, number: 700 },
    labels: ["keep-this-label"],
    body: "## What to build\n\nAdopt this anonymous slice.",
    children: [702],
  });
  issues[702] = issue({
    repository,
    number: 702,
    title: "Adoptable nested task",
    parent: { repository, number: 701 },
    body: "## What to build\n\nRetain this nested task.",
  });
  issues[800] = issue({
    repository,
    number: 800,
    title: "Superseded workflow history",
    labels: ["workflow:container"],
    parent: { repository, number: 1 },
    children: [801],
  });
  issues[801] = issue({
    repository,
    number: 801,
    title: "Superseded delivery attempt",
    labels: ["workflow:ticket", "workflow:superseded"],
    parent: { repository, number: 800 },
    state: "CLOSED",
    stateReason: "NOT_PLANNED",
    comments: [comment("superseded-801", resolutionBody("A later slice replaced this attempt."))],
  });
  return { nameWithOwner: repository, defaultBranch: "main", issues };
}

function buildDependencyRepository() {
  const repository = "fixture/workflow-dependency";
  return {
    nameWithOwner: repository,
    defaultBranch: "main",
    issues: {
      900: issue({
        repository,
        number: 900,
        title: "External preparation",
        labels: ["workflow:task"],
        state: "CLOSED",
        stateReason: "COMPLETED",
        comments: [comment("external-900", resolutionBody("External preparation is complete."))],
      }),
    },
  };
}

function buildLiveRepository() {
  const repository = "fixture/workflow-live";
  const capabilityBody =
    "## Summary\n\nVerify one disposable implementation and review handoff.\n\n## Source map\n\nNone (standalone)";
  const body = ticketBody({
    repository,
    capabilityNumber: 1,
    id: "T01",
    title: "Format check summaries",
    what: "Implement the dependency-free formatCheckSummary function described in TASK.md. It returns `No checks ran.`, `2 checks passed.`, and `1 passed, 2 failed: lint, typecheck.` for the three fixed examples.",
  });
  const slices = [{ id: "T01", title: "Format check summaries", body }];
  return {
    nameWithOwner: repository,
    defaultBranch: "main",
    issues: {
      1: issue({
        repository,
        number: 1,
        title: "Tiny live verification capability",
        body: capabilityBody,
        labels: ["workflow:capability"],
        children: [2],
        comments: approvalComments(repository, 1, capabilityBody, slices),
      }),
      2: issue({
        repository,
        number: 2,
        title: "Format check summaries",
        body,
        labels: ["workflow:ticket", "ready-for-agent"],
        parent: { repository, number: 1 },
      }),
    },
  };
}

export function createInitialState() {
  const repositories = [buildLargeRepository(), buildDependencyRepository(), buildLiveRepository()];
  return {
    version: 1,
    owner,
    offline: false,
    failNext: null,
    nextCommentId: 10_000,
    repositories: Object.fromEntries(
      repositories.map((repository) => [repository.nameWithOwner, repository]),
    ),
  };
}

function sleep(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function acquireLock(statePath) {
  const lockPath = `${statePath}.lock`;
  const deadline = Date.now() + LOCK_WAIT_MS;
  while (true) {
    try {
      NodeFS.mkdirSync(lockPath);
      return lockPath;
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      let age;
      try {
        age = Date.now() - NodeFS.statSync(lockPath).mtimeMs;
      } catch (statError) {
        if (statError?.code === "ENOENT") continue;
        throw statError;
      }
      if (age > STALE_LOCK_MS) {
        throw new Error(
          `Fixture state lock is stale at ${lockPath}. Verify no fixture process is running, then remove that lock directory.`,
          { cause: error },
        );
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `Fixture state is busy at ${statePath}. Retry after the active command finishes.`,
          { cause: error },
        );
      }
      sleep(10);
    }
  }
}

function saveState(statePath, state) {
  const temporaryPath = `${statePath}.${process.pid}.${Date.now()}.tmp`;
  NodeFS.writeFileSync(temporaryPath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
  NodeFS.renameSync(temporaryPath, statePath);
}

export function readState(statePath) {
  return JSON.parse(NodeFS.readFileSync(statePath, "utf8"));
}

export function updateState(statePath, update) {
  const lockPath = acquireLock(statePath);
  try {
    const state = readState(statePath);
    const result = update(state);
    saveState(statePath, state);
    return result;
  } finally {
    NodeFS.rmdirSync(lockPath);
  }
}

export function initialize(statePath) {
  NodeFS.mkdirSync(NodePath.dirname(statePath), { recursive: true });
  saveState(statePath, createInitialState());
  return {
    repositories: {
      large: "fixture/workflow-demo",
      secondary: "fixture/workflow-dependency",
      live: "fixture/workflow-live",
    },
    largeSliceCount: 52,
  };
}

function quotePosixShell(value) {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

export function installLauncher(
  binDirectory,
  statePath,
  ghModulePath,
  realShellPath,
  nodePath = process.execPath,
) {
  NodeFS.mkdirSync(binDirectory, { recursive: true });
  const posixPath = NodePath.join(binDirectory, "gh");
  const quotedModule = quotePosixShell(ghModulePath);
  const quotedNode = quotePosixShell(nodePath);
  const quotedState = quotePosixShell(statePath);
  NodeFS.writeFileSync(
    posixPath,
    `#!/bin/sh\nexport T3_WORKFLOW_FIXTURE_STATE=${quotedState}\nexec ${quotedNode} ${quotedModule} "$@"\n`,
    { encoding: "utf8", mode: 0o755 },
  );
  const windowsPath = NodePath.join(binDirectory, "gh.cmd");
  NodeFS.writeFileSync(
    windowsPath,
    `@echo off\r\nset "T3_WORKFLOW_FIXTURE_STATE=${statePath}"\r\n"${nodePath}" "${ghModulePath}" %*\r\nexit /b %ERRORLEVEL%\r\n`,
    "utf8",
  );
  const loginShellPath = NodePath.join(binDirectory, "login-shell");
  const fixturePath = `${binDirectory}:${NodePath.dirname(nodePath)}`;
  NodeFS.writeFileSync(
    loginShellPath,
    `#!/bin/sh\nreal_shell=${quotePosixShell(realShellPath)}\nfixture_path=${quotePosixShell(fixturePath)}\ncase "$1" in\n  -ilc|-lc|-c)\n    mode=$1\n    command=$2\n    shift 2\n    wrapped_command='export PATH="$T3_WORKFLOW_FIXTURE_PATH:$PATH"; '"$command"\n    T3_WORKFLOW_FIXTURE_PATH="$fixture_path" exec "$real_shell" "$mode" "$wrapped_command" "$@"\n    ;;\n  *) exec "$real_shell" "$@" ;;\nesac\n`,
    { encoding: "utf8", mode: 0o755 },
  );
  return { posixPath, windowsPath, loginShellPath };
}

function repositoryFor(state, nameWithOwner) {
  const repository = state.repositories[nameWithOwner];
  if (!repository) throw new Error(`Fixture denies unsupported repository ${nameWithOwner}.`);
  return repository;
}

function issueFor(state, repositoryName, number) {
  const repository = repositoryFor(state, repositoryName);
  const selected = repository.issues[number];
  if (!selected) throw new Error(`Fixture issue ${repositoryName}#${number} was not found.`);
  return selected;
}

function repositoryFromUrl(url) {
  const match = /^repos\/([^/]+\/[^/]+)\/issues(?:\/|$)/u.exec(url);
  return match?.[1] ?? null;
}

function argumentValue(args, flag) {
  const index = args.indexOf(flag);
  return index === -1 ? undefined : args[index + 1];
}

function formValues(args) {
  const values = {};
  for (let index = 0; index < args.length - 1; index += 1) {
    if (args[index] !== "-f" && args[index] !== "-F") continue;
    const [key, ...rest] = args[index + 1].split("=");
    values[key] = rest.join("=");
  }
  return values;
}

function issueReference(state, selected, includeParent = true) {
  const parent = selected.parent
    ? issueReference(
        state,
        issueFor(state, selected.parent.repository, selected.parent.number),
        false,
      )
    : null;
  return {
    id: selected.id,
    number: selected.number,
    title: selected.title,
    url: `https://github.com/${selected.repository}/issues/${selected.number}`,
    state: selected.state,
    stateReason: selected.stateReason,
    updatedAt: selected.updatedAt,
    lastEditedAt: selected.lastEditedAt,
    repository: { nameWithOwner: selected.repository },
    labels: connection(
      selected.labels.map((name) => ({ name })),
      0,
      PAGE_SIZE,
    ),
    subIssuesSummary: { total: selected.children.length },
    ...(includeParent ? { parent } : {}),
  };
}

function rawComment(selected, value) {
  return {
    id: value.id,
    url: value.url.replace(
      "fixture/workflow-demo/issues/10",
      `${selected.repository}/issues/${selected.number}`,
    ),
    body: value.body,
    createdAt: value.createdAt,
    author: value.author ? { login: value.author } : null,
    authorAssociation: value.authorAssociation,
  };
}

function connection(items, offset, size, direction = "forward") {
  const start = direction === "backward" ? Math.max(0, items.length - size - offset) : offset;
  const end = direction === "backward" ? items.length - offset : offset + size;
  const nodes = items.slice(start, end);
  return {
    pageInfo: {
      hasNextPage: direction === "forward" && end < items.length,
      endCursor: direction === "forward" && end < items.length ? `offset:${end}` : null,
      hasPreviousPage: direction === "backward" && start > 0,
      startCursor: direction === "backward" && start > 0 ? `offset:${items.length - start}` : null,
    },
    nodes,
  };
}

function offsetFrom(cursor) {
  if (!cursor) return 0;
  const match = /^offset:(\d+)$/u.exec(cursor);
  if (!match) throw new Error(`Fixture cursor ${cursor} is invalid.`);
  return Number(match[1]);
}

function detailedIssue(state, selected, mode = "detail") {
  const reference = issueReference(state, selected);
  const comments = selected.comments.map((value) => rawComment(selected, value));
  const commentConnection =
    mode === "child" ? connection(comments, 0, 20, "backward") : connection(comments, 0, PAGE_SIZE);
  return {
    ...reference,
    body: selected.body,
    assignees: connection(
      selected.assignees.map((login) => ({ login })),
      0,
      PAGE_SIZE,
    ),
    comments: commentConnection,
    timelineItems: connection(
      selected.reopenedAt.map((createdAt) => ({ createdAt })),
      0,
      PAGE_SIZE,
    ),
    blockedBy: connection(
      selected.blockedBy.map((blocker) =>
        issueReference(state, issueFor(state, blocker.repository, blocker.number)),
      ),
      0,
      mode === "child" ? 20 : PAGE_SIZE,
    ),
  };
}

function graphql(state, values) {
  const query = values.query ?? "";
  const repositoryName = `${values.owner}/${values.name}`;
  const repository = repositoryFor(state, repositoryName);
  const number = values.number === undefined ? undefined : Number(values.number);
  const cursorOffset = offsetFrom(values.after ?? values.before);

  if (query.includes("query WorkflowRoots")) {
    const roots = Object.values(repository.issues)
      .filter((selected) => selected.parent === null)
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
      .map((selected) => issueReference(state, selected));
    return { data: { repository: { issues: connection(roots, cursorOffset, PAGE_SIZE) } } };
  }
  if (query.includes("query WorkflowChildren")) {
    const parent = issueFor(state, repositoryName, number);
    const children = parent.children.map((childNumber) =>
      detailedIssue(state, issueFor(state, repositoryName, childNumber), "child"),
    );
    return {
      data: {
        repository: {
          issue: {
            id: parent.id,
            body: parent.body,
            comments: connection(
              parent.comments.map((value) => rawComment(parent, value)),
              0,
              PAGE_SIZE,
            ),
            subIssues: connection(children, cursorOffset, PAGE_SIZE),
          },
        },
      },
    };
  }
  if (query.includes("query WorkflowDetail")) {
    return {
      data: {
        repository: { issue: detailedIssue(state, issueFor(state, repositoryName, number)) },
      },
    };
  }
  if (query.includes("query WorkflowIssueLookup")) {
    return {
      data: {
        repository: { issue: issueReference(state, issueFor(state, repositoryName, number)) },
      },
    };
  }
  if (query.includes("query WorkflowSearch")) {
    const searchText = (values.searchQuery ?? "")
      .replace(/^repo:\S+\s+is:issue\s+/u, "")
      .toLowerCase();
    const matches = Object.values(repository.issues)
      .filter(
        (selected) =>
          selected.title.toLowerCase().includes(searchText) ||
          selected.body.toLowerCase().includes(searchText),
      )
      .map((selected) => issueReference(state, selected));
    return { data: { search: connection(matches, cursorOffset, 20) } };
  }

  const selected = Object.values(state.repositories)
    .flatMap((candidate) => Object.values(candidate.issues))
    .find((candidate) => candidate.id === values.id);
  if (!selected) throw new Error(`Fixture node ${values.id} was not found.`);
  if (query.includes("query WorkflowLabels")) {
    return {
      data: {
        node: {
          labels: connection(
            selected.labels.map((name) => ({ name })),
            cursorOffset,
            PAGE_SIZE,
          ),
        },
      },
    };
  }
  if (query.includes("query WorkflowBlockedBy")) {
    return {
      data: {
        node: {
          blockedBy: connection(
            selected.blockedBy.map((blocker) =>
              issueReference(state, issueFor(state, blocker.repository, blocker.number)),
            ),
            cursorOffset,
            PAGE_SIZE,
          ),
        },
      },
    };
  }
  if (query.includes("query WorkflowCommentsBackward")) {
    return {
      data: {
        node: {
          comments: connection(
            selected.comments.map((value) => rawComment(selected, value)),
            cursorOffset,
            PAGE_SIZE,
            "backward",
          ),
        },
      },
    };
  }
  if (query.includes("query WorkflowComments")) {
    return {
      data: {
        node: {
          comments: connection(
            selected.comments.map((value) => rawComment(selected, value)),
            cursorOffset,
            PAGE_SIZE,
          ),
        },
      },
    };
  }
  if (query.includes("query WorkflowAssignees")) {
    return {
      data: {
        node: {
          assignees: connection(
            selected.assignees.map((login) => ({ login })),
            cursorOffset,
            PAGE_SIZE,
          ),
        },
      },
    };
  }
  if (query.includes("query WorkflowReopened")) {
    return {
      data: {
        node: {
          timelineItems: connection(
            selected.reopenedAt.map((createdAt) => ({ createdAt })),
            cursorOffset,
            PAGE_SIZE,
          ),
        },
      },
    };
  }
  if (query.includes("query WorkflowEvidenceLookup")) {
    return { data: { node: detailedIssue(state, selected) } };
  }
  throw new Error("Fixture denies unsupported GraphQL operation.");
}

function restIssue(selected) {
  return {
    id: selected.databaseId,
    node_id: selected.id,
    number: selected.number,
    title: selected.title,
    html_url: `https://github.com/${selected.repository}/issues/${selected.number}`,
    repository_url: `https://api.github.com/repos/${selected.repository}`,
    body: selected.body,
    updated_at: selected.updatedAt,
    labels: selected.labels.map((name) => ({ name })),
    parent_issue_url: selected.parent
      ? `https://api.github.com/repos/${selected.parent.repository}/issues/${selected.parent.number}`
      : null,
    sub_issues_summary: { total: selected.children.length, completed: 0 },
  };
}

function operationFor(args) {
  if (args[0] === "issue") return `issue-${args[1]}`;
  if (args[0] === "api" && args.includes("graphql")) return "graphql";
  if (args[0] === "api") {
    const endpoint = args.find((value) => value.startsWith("repos/"));
    if (endpoint?.includes("/labels"))
      return args.includes("POST") ? "adoption-label-add" : "adoption-label-remove";
    if (endpoint?.endsWith("/sub_issues")) return "adoption-parent-add";
    if (endpoint?.endsWith("/sub_issue")) return "adoption-parent-remove";
    return "rest-read";
  }
  if (args[0] === "repo" && args[1] === "view") return "repo-view";
  if (args[0] === "auth") return "auth-status";
  if (args[0] === "--version") return "version";
  return "unsupported";
}

function repositoryFromIssueArgs(args) {
  return argumentValue(args, "--repo");
}

function mutateIssueEdit(state, args) {
  const allowedFlags = new Set([
    "--repo",
    "--add-assignee",
    "--remove-assignee",
    "--add-label",
    "--remove-label",
  ]);
  for (let index = 3; index < args.length; index += 2) {
    const flag = args[index];
    if (!allowedFlags.has(flag) || args[index + 1] === undefined) {
      throw new Error(`Fixture denies unsupported issue edit argument ${flag ?? "(missing)"}.`);
    }
  }
  const repository = repositoryFromIssueArgs(args);
  const selected = issueFor(state, repository, Number(args[2]));
  const requestedAddAssignee = argumentValue(args, "--add-assignee");
  const requestedRemoveAssignee = argumentValue(args, "--remove-assignee");
  const addAssignee = requestedAddAssignee === "@me" ? state.owner : requestedAddAssignee;
  const removeAssignee = requestedRemoveAssignee === "@me" ? state.owner : requestedRemoveAssignee;
  const addLabel = argumentValue(args, "--add-label");
  const removeLabel = argumentValue(args, "--remove-label");
  if (addAssignee && !selected.assignees.includes(addAssignee))
    selected.assignees.push(addAssignee);
  if (removeAssignee)
    selected.assignees = selected.assignees.filter((value) => value !== removeAssignee);
  if (addLabel && !selected.labels.includes(addLabel)) selected.labels.push(addLabel);
  if (removeLabel) selected.labels = selected.labels.filter((value) => value !== removeLabel);
  if (!addAssignee && !removeAssignee && !addLabel && !removeLabel) {
    throw new Error("Fixture denies unsupported issue edit.");
  }
  return { stdout: "", stderr: "" };
}

function executeIssue(state, args, stdin) {
  const subcommand = args[1];
  const repository = repositoryFromIssueArgs(args);
  const selected = issueFor(state, repository, Number(args[2]));
  if (subcommand === "view") {
    if (argumentValue(args, "--json") !== "assignees") {
      throw new Error("Fixture denies unsupported issue view fields.");
    }
    return {
      stdout: `${selected.assignees.join("\n")}${selected.assignees.length ? "\n" : ""}`,
      stderr: "",
    };
  }
  if (subcommand === "edit") return mutateIssueEdit(state, args);
  if (subcommand === "comment") {
    const body =
      argumentValue(args, "--body") ??
      (argumentValue(args, "--body-file") === "-" ? stdin : undefined);
    if (body === undefined) throw new Error("Fixture requires an explicit issue comment body.");
    state.nextCommentId += 1;
    selected.comments.push(
      comment(`fixture-${state.nextCommentId}`, body, new Date().toISOString()),
    );
    selected.comments.at(-1).url =
      `https://github.com/${repository}/issues/${selected.number}#issuecomment-${state.nextCommentId}`;
    return { stdout: selected.comments.at(-1).url + "\n", stderr: "" };
  }
  if (subcommand === "close") {
    if (argumentValue(args, "--reason") !== "completed") {
      throw new Error("Fixture supports only completed issue closure.");
    }
    selected.state = "CLOSED";
    selected.stateReason = "COMPLETED";
    return {
      stdout: `Closed issue ${repository}#${selected.number} (${selected.title})\n`,
      stderr: "",
    };
  }
  if (subcommand === "reopen") {
    selected.state = "OPEN";
    selected.stateReason = "REOPENED";
    selected.reopenedAt.push(new Date().toISOString());
    return {
      stdout: `Reopened issue ${repository}#${selected.number} (${selected.title})\n`,
      stderr: "",
    };
  }
  throw new Error(`Fixture denies unsupported issue command ${subcommand}.`);
}

function executeApi(state, args, stdin) {
  if (args[1] === "user") return { stdout: `${state.owner}\n`, stderr: "" };
  if (args.includes("graphql"))
    return { stdout: `${JSON.stringify(graphql(state, formValues(args)))}\n`, stderr: "" };
  const endpoint = args.find((value) => value.startsWith("repos/"));
  const repositoryName = endpoint ? repositoryFromUrl(endpoint) : null;
  if (!endpoint || !repositoryName)
    throw new Error("Fixture denies unsupported GitHub API endpoint.");
  const repository = repositoryFor(state, repositoryName);
  const childList =
    /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/sub_issues\?per_page=100&page=(\d+)$/u.exec(endpoint);
  if (childList) {
    const selected = issueFor(state, repositoryName, Number(childList[1]));
    const page = Number(childList[2]);
    const children = selected.children
      .slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)
      .map((number) => restIssue(issueFor(state, repositoryName, number)));
    return { stdout: `${JSON.stringify(children)}\n`, stderr: "" };
  }
  const issueRead = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)$/u.exec(endpoint);
  if (issueRead && !args.includes("--method")) {
    return {
      stdout: `${JSON.stringify(restIssue(issueFor(state, repositoryName, Number(issueRead[1]))))}\n`,
      stderr: "",
    };
  }
  const labelAdd = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/labels$/u.exec(endpoint);
  if (labelAdd && args.includes("POST")) {
    const payload = JSON.parse(stdin || "null");
    if (!payload || !Array.isArray(payload.labels))
      throw new Error("Fixture requires a labels payload.");
    const selected = issueFor(state, repositoryName, Number(labelAdd[1]));
    for (const label of payload.labels)
      if (!selected.labels.includes(label)) selected.labels.push(label);
    return { stdout: "{}\n", stderr: "" };
  }
  const labelRemove = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/labels\/(.+)$/u.exec(endpoint);
  if (labelRemove && args.includes("DELETE")) {
    const selected = issueFor(state, repositoryName, Number(labelRemove[1]));
    selected.labels = selected.labels.filter(
      (label) => label !== decodeURIComponent(labelRemove[2]),
    );
    return { stdout: "{}\n", stderr: "" };
  }
  const parentAdd = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/sub_issues$/u.exec(endpoint);
  if (parentAdd && args.includes("POST")) {
    const childId = Number(formValues(args).sub_issue_id);
    const child = Object.values(repository.issues).find(
      (candidate) => candidate.databaseId === childId,
    );
    if (!child) throw new Error(`Fixture child database id ${childId} was not found.`);
    if (child.parent) {
      const oldParent = issueFor(state, child.parent.repository, child.parent.number);
      oldParent.children = oldParent.children.filter((number) => number !== child.number);
    }
    const newParent = issueFor(state, repositoryName, Number(parentAdd[1]));
    if (!newParent.children.includes(child.number)) newParent.children.push(child.number);
    child.parent = { repository: repositoryName, number: newParent.number };
    return { stdout: "{}\n", stderr: "" };
  }
  const parentRemove = /^repos\/[^/]+\/[^/]+\/issues\/(\d+)\/sub_issue$/u.exec(endpoint);
  if (parentRemove && args.includes("DELETE")) {
    const childId = Number(formValues(args).sub_issue_id);
    const child = Object.values(repository.issues).find(
      (candidate) => candidate.databaseId === childId,
    );
    if (!child) throw new Error(`Fixture child database id ${childId} was not found.`);
    const parent = issueFor(state, repositoryName, Number(parentRemove[1]));
    parent.children = parent.children.filter((number) => number !== child.number);
    child.parent = null;
    return { stdout: "{}\n", stderr: "" };
  }
  throw new Error(`Fixture denies unsupported GitHub API endpoint ${endpoint}.`);
}

function executeKnown(state, args, stdin) {
  if (args[0] === "--version")
    return { stdout: "gh version 2.83.0 (workflow fixture)\n", stderr: "" };
  if (args.join(" ") === "auth status --json hosts") {
    return {
      stdout: `${JSON.stringify({ hosts: { "github.com": [{ state: "success", active: true, host: "github.com", login: state.owner, tokenSource: "fixture", gitProtocol: "https" }] } })}\n`,
      stderr: "",
    };
  }
  if (args[0] === "repo" && args[1] === "view") {
    const repositoryName = args[2];
    const repository = repositoryFor(state, repositoryName);
    return {
      stdout: `${JSON.stringify({ nameWithOwner: repository.nameWithOwner, url: `https://github.com/${repository.nameWithOwner}`, sshUrl: `git@github.com:${repository.nameWithOwner}.git`, defaultBranchRef: { name: repository.defaultBranch } })}\n`,
      stderr: "",
    };
  }
  if (args[0] === "api") return executeApi(state, args, stdin);
  if (args[0] === "issue") return executeIssue(state, args, stdin);
  throw new Error(`Fixture denies unsupported gh command: ${args.join(" ")}.`);
}

export function runGitHubCommand(statePath, args, stdin) {
  const operation = operationFor(args);
  if (operation === "unsupported") {
    throw new Error(`Fixture denies unsupported gh command: ${args.join(" ")}.`);
  }
  let controlledFailure = null;
  const result = updateState(statePath, (state) => {
    if (state.offline && operation !== "version") {
      throw new Error("Synthetic GitHub outage: failed to connect to api.github.com.");
    }
    const failure = state.failNext?.operation === operation ? state.failNext : null;
    if (failure) state.failNext = null;
    if (failure?.timing === "before") {
      controlledFailure = `Synthetic failure before ${operation}.`;
      return { stdout: "", stderr: "" };
    }
    const commandResult = executeKnown(state, args, stdin);
    if (failure?.timing === "after")
      controlledFailure = `Synthetic response loss after ${operation}.`;
    return commandResult;
  });
  if (controlledFailure) throw new Error(controlledFailure);
  return result;
}

export function inspectIssue(statePath, repository, number) {
  return issueFor(readState(statePath), repository, number);
}

export function setOffline(statePath, offline) {
  updateState(statePath, (state) => {
    state.offline = offline;
  });
}

export function setFailure(statePath, timing, operation) {
  if (timing !== "before" && timing !== "after")
    throw new Error("Failure timing must be before or after.");
  updateState(statePath, (state) => {
    state.failNext = { timing, operation };
  });
}

export function addReassessment(statePath, repository, number, outcome, summary) {
  if (outcome !== "scope-change" && outcome !== "cleared") {
    throw new Error("Reassessment outcome must be scope-change or cleared.");
  }
  updateState(statePath, (state) => {
    const selected = issueFor(state, repository, number);
    const supersededUrls =
      outcome === "cleared"
        ? selected.comments
            .filter(
              (value) =>
                value.body.includes("<!-- t3-workflow:v1 reassessment -->") &&
                /^Outcome:\s*scope-change\s*$/imu.test(value.body),
            )
            .map((value) => value.url)
        : [];
    const latestCreatedAt = Math.max(
      Date.now(),
      ...selected.comments.map((value) => Date.parse(value.createdAt)),
    );
    state.nextCommentId += 1;
    const source = commentOnIssue(
      repository,
      number,
      `fixture-${state.nextCommentId}`,
      `Synthetic owner evidence: ${summary}`,
      new Date(latestCreatedAt + 1).toISOString(),
    );
    selected.comments.push(source);
    state.nextCommentId += 1;
    selected.comments.push(
      commentOnIssue(
        repository,
        number,
        `fixture-${state.nextCommentId}`,
        reassessmentBody(outcome, summary, source.url, supersededUrls),
        new Date(latestCreatedAt + 2).toISOString(),
      ),
    );
  });
}
