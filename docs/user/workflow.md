# Workflow

Workflow lets you inspect a project's GitHub issue hierarchy from the right panel. Browsing does
not start an agent.

To start ready decision work, focus a workflow root, select the decision or prerequisite task,
and choose **Start**. The project must have an enabled Codex provider as its default, including a
model and reasoning effort, and the target workspace must expose the required Wayfinder skills.
T3 Code rechecks those conditions, claims the GitHub issue, creates a thread, submits the first
turn, and opens Workflow beside that thread.

When a map's in-scope decisions are resolved and its remaining unknowns are clear, open the map
from its planning thread and choose **Create capability**. T3 Code continues that thread with the
map context and the `to-spec` skill. Approve the resulting specification before choosing **Slice
tickets** on the capability. For a mapped capability created in T3 Code, ticket slicing returns to
the thread where **Create capability** ran. A standalone or externally authored capability uses
the current thread as its explicit planning target. A missing or ambiguous source, an association
from another project, or an unavailable planning thread prevents the phase from starting. Ticket
slicing first proposes a breakdown; publishing the delivery issues requires a separate owner
approval of that exact breakdown.

After the approved breakdown is published, choose **Start implementation** on the capability.
T3 Code verifies the current specification, every published delivery slice, Astra with high
reasoning effort, and the implementation and review skills in a dedicated capability worktree.
The director and its worktree survive reconnects. **Open director** returns to its thread,
**Resume** continues confirmed interrupted work, and **Retry setup** recovers setup that was
recorded but never submitted. Resume rechecks the current assignment, approval and readiness before
sending one continuation. A director admits up to ten delivery slices in one batch; blocked or
failed slices still count, while retries, review, and nested tasks reuse their slice's slot. At ten,
Workflow stops new admissions, settles the batch and saves a handoff before starting one successor
in the same worktree. Blocked or approval-waiting work waits without starting idle successors.

Use **Active work** to find ongoing capabilities and unresolved director, worker and review
activity in the selected environment. Opening an entry switches to its project and owning T3
thread, restores that root's Workflow view and shows the new target. If the destination is
unavailable, the entry stays visible so you can return after the project or thread is restored.

Workflow details show each admitted worker's ticket, write ownership, requested and observed
model settings, provider state, and reported implementation handoff. An unassociated child or
unknown/mismatched model remains visible for reconciliation. An idle child has only finished its
current turn; wait for an explicit handoff with commits and checks before treating its result as
ready for review. Write ownership remains reserved until the native worker and every observed
descendant close. An idle or finished turn, a handoff alone, or a child with an unknown outcome
keeps overlapping work held. Later execution or status activity makes stale close evidence
inapplicable; metadata-only updates preserve it.

Before a successor starts, Workflow checks the saved handoff against current tracker, thread and
child activity. If earlier work changed after settlement, open the named source thread and inspect
the new activity. An owner can acknowledge that exact settled state and retry the saved successor;
new activity makes the acknowledgement stale again.

After a successful implementation handoff, the director registers the agreed checks against the
exact committed head. Run those commands through Codex normally so any provider approval still
applies; Workflow accepts only the matching native command start and completion from the capability
worktree. It then prepares a fresh Astra/medium review with separate Standards and Spec reviewers.
Workflow keeps their exact identities, findings and the director's later dispositions beside the
worker history. A changed head, failed or unobserved check, idle child, live review descendant, or
undisposed finding prevents resolution.

Resolving a ticket writes a versioned GitHub evidence comment before closing it, confirms both from
fresh tracker data, and refreshes the capability frontier. If a write result is uncertain, the
resolution stays pending. Retry it to reconcile the same evidence record; Workflow does not infer
completion from a label or closed issue alone.

When every approved delivery slice is resolved, the director registers the combined acceptance
commands against the resulting head. Run them through Codex like the ticket checks. Failed checks,
reopened work, unsettled children or uncertain tracker writes keep the capability open. After the
matching commands pass, retry any pending evidence write so Workflow can confirm the evidence and
completed closure from fresh GitHub data.

If a scope or prerequisite change invalidates active work, Workflow holds new starts and asks the
director and its observed children to stop. Check each child outcome: a stopped director does not
prove its workers or reviewers stopped. Record a cleared reassessment after the prerequisite is
restored, or renew the affected approval when scope changed, then choose **Resume**. Refresh alone
never restarts reassessed work.

If the issue is already assigned, Start holds the new attempt. Arrange an explicit handoff before
trying again, or use **Take over here** after checking the other environment. Takeover changes the
GitHub assignment; it does not stop another agent or make the assignment an atomic lock.

Starting the same work again opens its original attempt. If T3 Code says the attempt is held, its
first submission or claim could not be confirmed. It will not send the first turn again
automatically; inspect the original thread and GitHub assignment before taking further action.

Linked attempts survive server restarts and client reconnects. **Open linked work** returns to the
preserved thread. **Resume** submits one continuation only when the original turn is durably
accepted, the linked work is interrupted and its GitHub assignment still matches. **Start fresh**
creates a new thread while retaining the previous attempt and thread history. Actions that start or
continue work refresh the target project, readiness, provider and required skills first.

Other providers can browse Workflow, but this launch flow currently uses Codex.
