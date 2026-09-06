import {
  type CommandId,
  type OrchestrationCommand,
  type OrchestrationDispatchCommandError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

type TurnStartCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
type CreatedThreadTurnStartCommand = TurnStartCommand & {
  readonly bootstrap: {
    readonly createThread: NonNullable<NonNullable<TurnStartCommand["bootstrap"]>["createThread"]>;
  };
};

/** Runs the create-thread bootstrap fence immediately before its first turn. */
export function dispatchCreatedThreadTurnStart(input: {
  readonly command: CreatedThreadTurnStartCommand;
  readonly createCommandId: Effect.Effect<CommandId, OrchestrationDispatchCommandError>;
  readonly dispatch: (
    command: OrchestrationCommand,
  ) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;
  readonly drainThreadDeletionThrough: (sequence: number) => Effect.Effect<void>;
  readonly markThreadCreated?: Effect.Effect<void>;
}) {
  return Effect.gen(function* () {
    const { command } = input;
    const { bootstrap: _bootstrap, ...turnStart } = command;
    const created = yield* input.dispatch({
      type: "thread.create",
      commandId: yield* input.createCommandId,
      threadId: command.threadId,
      projectId: command.bootstrap.createThread.projectId,
      title: command.bootstrap.createThread.title,
      modelSelection: command.bootstrap.createThread.modelSelection,
      runtimeMode: command.bootstrap.createThread.runtimeMode,
      interactionMode: command.bootstrap.createThread.interactionMode,
      branch: command.bootstrap.createThread.branch,
      worktreePath: command.bootstrap.createThread.worktreePath,
      createdAt: command.bootstrap.createThread.createdAt,
    });
    yield* input.markThreadCreated ?? Effect.void;
    yield* input.drainThreadDeletionThrough(created.sequence);
    return yield* input.dispatch(turnStart);
  });
}
