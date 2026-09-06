# Workflow

Workflow lets you inspect a project's GitHub issue hierarchy from the right panel. Browsing does
not start an agent.

To start ready decision work, focus a workflow root, select the decision or prerequisite task,
and choose **Start**. The project must have an enabled Codex provider as its default, including a
model and reasoning effort, and the target workspace must expose the required Wayfinder skills.
T3 Code rechecks those conditions, claims the GitHub issue, creates a thread, submits the first
turn, and opens Workflow beside that thread.

If the issue is already assigned, Start holds the new attempt. Arrange an explicit handoff before
trying again; T3 Code does not treat an assignment in another environment as its own claim.

Starting the same work again opens its original attempt. If T3 Code says the attempt is held, its
first submission or claim could not be confirmed. It will not send the first turn again
automatically; inspect the original thread and GitHub assignment before taking further action.

Other providers can browse Workflow, but this launch flow currently uses Codex.
