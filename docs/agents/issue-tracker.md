# Issue tracker: GitHub

Track this fork's work in `Flow-Fly/t3code`. Pass `--repo Flow-Fly/t3code` to
`gh issue` commands and use `repos/Flow-Fly/t3code` in API paths. The `origin`
remote still points at the upstream project; it is not this effort's tracker.

Read issue bodies, labels, assignees, state reasons and relevant comments before
acting. Use `--body-file` for multiline issue bodies and comments. Fetch every
page when listing children or dependencies. Verify relationships after writes.

## Wayfinding operations

- The map is one issue labelled `wayfinder:map`. Its body is the destination,
  notes, resolved-decision index, remaining fog and out-of-scope work.
- Decision tickets are native sub-issues of the map, labelled
  `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling` or
  `wayfinder:task`. Link children with `POST issues/{map}/sub_issues`, passing
  their numeric database ID as `sub_issue_id`.
- Read children with `GET issues/{map}/sub_issues`. Preserve their order.
- Read blocking edges with `GET issues/{ticket}/dependencies/blocked_by`.
  Add edges with `POST` to that endpoint, passing the blocker's numeric database
  ID as `issue_id`. Issue numbers and database IDs are different.
- The frontier contains open, unassigned children with satisfied dependencies
  and no pending reassessment. Use the completion rules below: a closed blocker
  alone is insufficient. An empty frontier can mean claimed work, blocked work,
  pending reassessment or remaining fog.
- Claim before work with `gh issue edit NUMBER --repo Flow-Fly/t3code
--add-assignee @me`. Re-read live state before choosing or claiming work;
  assignment alone is not an atomic lock between sessions.
- Resolve by posting a resolution record, closing the ticket as completed,
  then adding its linked title and a one-line gist to the map's Decisions so
  far. Read the latest map before editing it.
- Out-of-scope tickets belong in the map's Out of scope section, with their
  reason and linked title; they are not resolved decisions.

## Specifications and delivery

Publish specifications and delivery tickets as issues. Preserve the approval
steps in `$to-spec` and `$to-tickets`. An approved specification is not delivery
permission. Apply `ready-for-agent` only to approved delivery work whose blockers
are complete; planning tickets do not receive it.

For new work, the capability is the specification issue itself, labelled
`workflow:capability`.
Start its body with `## Summary` containing a short brief, then `## Source map`
with a named issue link, or `None (standalone)` when no map produced it. Preserve
the remaining `$to-spec` sections. A map may produce several capabilities;
decision tickets remain children of their map. For adopted capabilities with
separate specifications, follow Existing repositories below.

Delivery tickets carry `workflow:ticket` and are native sub-issues of their
capability. Preserve the `$to-tickets` template and its parent link; the native
parent is authoritative. Use native blocking for prerequisites. Artifact labels
identify kind, issue IDs identify work, and titles remain readable names. A
conflicting kind or parent needs classification before work starts.

Keep specification drafting and ticket slicing in the planning thread. Begin
implementation in a fresh director thread after the explicit Start action;
returning to ongoing work reuses its current thread.

## Director batches

Each director takes on at most 10 distinct delivery tickets. Count a ticket when
it enters the batch, including attempts that later fail or become blocked.
Retries and reviews of a ticket already in the batch use the same slot. In an
adopted slice/task hierarchy, count the delivery slice once; its smaller tasks
remain within that unit. Containers and decision maps do not consume delivery
slots. Re-read live readiness before claiming or dispatching work.

Before taking on another batch, stop admissions and settle or explicitly stop
the current workers and reviewers. Retain a handoff in durable workflow history
before starting the successor, and keep only one active director for the
capability. Reuse the capability worktree and preserve previous threads. If work
is complete, finish the capability; if it is blocked or awaiting approval, show
that state rather than spawning idle directors.

The handoff identifies its source thread, handled tickets and outcomes, and the
implementation head. Link existing specifications, issues, reviews and commits;
add only useful lessons, pitfalls or unresolved context, plus suggested skills.
The successor re-reads live approvals, ticket state and dependencies. A summary
or an untracked temporary file is insufficient as the durable work record.

## Workflow records

Keep records as issue comments with a visible heading and the exact marker
below. Use labelled fields and named links. Preserve prior records as history;
a newer record identifies which record or scope it supersedes.

| Record            | Marker                                 | Required content                                                                                               |
| ----------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `## Approval`     | `<!-- t3-workflow:v1 approval -->`     | `Kind: specification` or `Kind: ticket-breakdown`; `Approved by`; `Source`; `### Approved content`             |
| `## Resolution`   | `<!-- t3-workflow:v1 resolution -->`   | `Outcome: resolved`, `cancelled` or `out-of-scope`; `### Summary`; `### Evidence`                              |
| `## Reassessment` | `<!-- t3-workflow:v1 reassessment -->` | `Trigger` linking the change or reopening; `Outcome: cleared` or `scope-change`; `### Changes`; `### Evidence` |

Put both kinds of approval on the capability issue, in separate comments.
`Approved by` names the owner; `Source` points to their explicit approval in a
GitHub comment or a durable thread/message reference. Agents may record an
owner's approval only when that source is available. Retain the approved text
under `### Approved content`: the specification or the complete ticket breakdown
with scopes, acceptance criteria and dependencies. A link to mutable issue text
alone does not identify what was approved. Labels summarize records; applying a
label does not grant approval.

Put resolution and reassessment records on the affected issue. Evidence links
to the decision, prototype, research, implementation or verification that
supports the outcome. A heading or marker alone is not evidence. References
that cannot be checked remain unverified.

## Completion and reassessment

Resolve a delivery ticket after committed implementation, passing agreed checks
and fresh independent review. The director verifies findings against the fixed
base and final implementation head: fix confirmed blockers, settle owner
judgement calls, and record other dispositions. Attach the reviewed commit and
verification evidence to the resolution record.

Complete a capability when every required approved ticket is resolved and the
combined result passes its agreed acceptance checks. Record the outcome and
integrated evidence on the capability. Completion may span multiple director
batches; unchanged approved scope needs no extra final approval. Merge and
release remain separate actions.

- **Resolved:** closed as completed, with a resolution record whose resolved
  outcome and evidence cover the current scope and any subsequent reopening.
- **Closed — unverified:** closed as completed without a valid current
  resolution record. Reassess before treating it as a satisfied prerequisite.
- **Cancelled / out of scope:** closed as not planned, with the resolution
  record distinguishing the reason. This is not successful completion.
- **Needs reassessment:** an affected dependency was cancelled, became
  unverified or reopened, or approved scope changed. Apply
  `workflow:needs-reassessment`, remove stale `ready-for-agent`, and pause new
  starts of affected dependent work.

These rules apply to decision and delivery work. Open blockers remain blocking.
When a dependency no longer applies, record why and remove or replace its native
edge; retain the explanation in the reassessment record. A director can reassess
dependencies within approved scope. Scope changes require the owner's renewed
approval of the affected specification or breakdown. Clear the reassessment
label only after the record accounts for its triggers and the current scope is
approved where required; recompute readiness from live records and blockers.

Keep assignment and agent activity separate from completion: claimed, running,
waiting and interrupted work can still have an open issue. A finished turn does
not resolve the issue. Preserve old evidence when reopening work; an old
resolution does not satisfy the reopened prerequisite.

Check explicit prerequisites beyond native blockers, including human preparation,
resource availability and alternatives such as "A or B". Preserve the source of
each condition and record how it was satisfied in reassessment evidence. Hold
the affected action when a condition is unmet or unclear; show what needs review.
Keep references and queue order separate from dependencies. Human-in-the-loop
planning is intentional work, not itself an unmet prerequisite.

## Existing repositories

Adopt a selected branch from an issue link or discovered root. Preview its
existing graph, proposed classifications and exact tracker changes for owner
review. Let the owner correct or exclude candidates before applying changes.
Preserve issue IDs, names, existing labels and useful hierarchy; add the
canonical workflow metadata needed for the selected work. Classification,
adoption and readiness are separate: adoption does not approve or start work.

Recognize roles from repository conventions and issue evidence. A label or tree
depth alone may be ambiguous. Keep initiative and wave issues as containers;
they organize work rather than receiving implementation actions. A capability
can contain delivery slices and a decision map, with smaller tasks beneath its
slices. Only approved delivery tickets enter its director's batch. Keep native
parents, blockers, source links and supersession references distinct. Confirm
parent changes explicitly; body mentions alone do not establish ownership.

An adopted capability may retain a separate specification as a linked source.
Keep the capability's own scope and approval records explicit, including the
approved content. Approval of a broader source specification does not approve
every capability or its delivery breakdown. Show missing or uncertain source-map
metadata for correction. Use the in-issue specification convention for new work.

Re-read affected issues before applying the reviewed changes. Retain a record
of actual changes and partial failures. A reviewed undo restores only changes
made by adoption, after checking for subsequent edits, and preserves history.
Unselected work remains readable context; verify relevant prerequisites beyond
the adopted branch. Surface conflicts with repository execution policy during
preflight rather than importing historical director settings automatically.

Do not infer an approval or resolution when adding labels or relationships.
Review legacy evidence as needed for the requested next action. Propose a
normalized record with source links only when the evidence supplies the required
facts; otherwise show what is missing. Preserve partial approvals and later
supersession. An agent's assertion alone does not establish owner approval.
Use live issue state for current progress, with evidence verification separate;
retain old checklists and comments as history. A rejected prototype can resolve
its decision, while a superseded delivery slice is not successful delivery.

**PRs as a request surface: no.**
