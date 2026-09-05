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

A capability is the specification issue itself, labelled `workflow:capability`.
Start its body with `## Summary` containing a short brief, then `## Source map`
with a named issue link, or `None (standalone)` when no map produced it. Preserve
the remaining `$to-spec` sections. A map may produce several capabilities;
decision tickets remain children of their map.

Delivery tickets carry `workflow:ticket` and are native sub-issues of their
capability. Preserve the `$to-tickets` template and its parent link; the native
parent is authoritative. Use native blocking for prerequisites. Artifact labels
identify kind, issue IDs identify work, and titles remain readable names. A
conflicting kind or parent needs classification before work starts.

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

## Existing repositories

Offer a proposed classification and linking plan for owner confirmation before
adopting existing issues into this workflow. Keep missing metadata visible for
correction. After adoption, use the agreed conventions for new issues; titles
remain display names rather than machine identity.

Do not infer an approval or resolution when adding labels or relationships.
Adoption may add record markers to existing comments only when their content
already supplies the required facts; otherwise show the missing evidence.

**PRs as a request surface: no.**
