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
recorded but never submitted. A director admits up to ten delivery slices in one batch; blocked
or failed slices still count, while retries, review, and nested tasks reuse their slice's slot.
At ten, finish or settle admitted work and wait for a successor before admitting more.

Workflow details show each admitted worker's ticket, write ownership, requested and observed
model settings, provider state, and reported implementation handoff. An unassociated child or
unknown/mismatched model remains visible for reconciliation. An idle child has only finished its
current turn; wait for an explicit handoff with commits and checks before treating its work as
settled. Write ownership remains reserved until T3 Code has verified settlement, so an overlapping
ticket stays held even after a worker reports a handoff.

If the issue is already assigned, Start holds the new attempt. Arrange an explicit handoff before
trying again, or use **Take over here** after checking the other environment. Takeover changes the
GitHub assignment; it does not stop another agent or make the assignment an atomic lock.

Starting the same work again opens its original attempt. If T3 Code says the attempt is held, its
first submission or claim could not be confirmed. It will not send the first turn again
automatically; inspect the original thread and GitHub assignment before taking further action.

Linked attempts survive server restarts and client reconnects. **Open linked work** returns to the
preserved thread. **Resume** submits one continuation only when the original turn is durably
accepted and the linked work is interrupted. **Start fresh** creates a new thread while retaining
the previous attempt and thread history. Actions that start or continue work refresh the target
project, readiness, provider and required skills first.

Other providers can browse Workflow, but this launch flow currently uses Codex.
