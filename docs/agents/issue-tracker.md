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
- The frontier contains open children with no open blockers, no pending
  dependency reassessment and no assignee. An empty frontier can mean claimed
  work, blocked work, pending reassessment or remaining fog.
- Claim before work with `gh issue edit NUMBER --repo Flow-Fly/t3code
--add-assignee @me`. Re-read live state before choosing or claiming work;
  assignment alone is not an atomic lock between sessions.
- Resolve by posting a resolution comment, closing the ticket with the
  appropriate reason, then adding its linked title and a one-line gist to the
  map's Decisions so far. Read the latest map before editing it.
- Out-of-scope tickets belong in the map's Out of scope section, with their
  reason and linked title; they are not resolved decisions.

## Specifications and delivery

Publish specifications and delivery tickets as issues. Preserve the approval
steps in `$to-spec` and `$to-tickets`. An approved specification is not delivery
permission. Apply `ready-for-agent` only to approved delivery work whose blockers
are complete; planning tickets do not receive it.

A capability is the specification issue itself. Put its brief and specification
in that issue, and make its delivery tickets native sub-issues. Link the
capability to its source Wayfinder map; a map may produce several capabilities.
Decision tickets remain children of their map.

## Cancelled prerequisites

Cancellation or out-of-scope closure is distinct from successful resolution.
Mark affected dependent work as needing reassessment and exclude it from ready
work until its dependency has been reassessed. This applies to decision and
delivery tickets, even when GitHub reports the blocker as closed.

## Existing repositories

Offer a proposed classification and linking plan for owner confirmation before
adopting existing issues into this workflow. Keep missing metadata visible for
correction. After adoption, use the agreed conventions for new issues; titles
remain display names rather than machine identity.

**PRs as a request surface: no.**
