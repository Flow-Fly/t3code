import {
  type CommandId,
  type OrchestrationCommand,
  OrchestrationDispatchCommandError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

type TurnStartCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;
type CreatedThreadTurnStartCommand = TurnStartCommand & {
  readonly bootstrap: {
    readonly createThread: NonNullable<NonNullable<TurnStartCommand["bootstrap"]>["createThread"]>;
  };
};

/** Runs the create-thread bootstrap fence immediately before its first turn. */
export const dispatchCreatedThreadTurnStart = Effect.fn("dispatchCreatedThreadTurnStart")(
  function* (input: {
    readonly command: CreatedThreadTurnStartCommand;
    readonly createCommandId: Effect.Effect<CommandId, OrchestrationDispatchCommandError>;
    readonly dispatch: (
      command: OrchestrationCommand,
    ) => Effect.Effect<{ readonly sequence: number }, OrchestrationDispatchCommandError>;
    readonly drainThreadDeletionThrough: (sequence: number) => Effect.Effect<void>;
    readonly markThreadCreated?: Effect.Effect<void>;
  }) {
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
  },
);

/** Classifies a failed first-turn dispatch before deciding whether its bootstrap is disposable. */
export const reconcileCreatedThreadTurnStartFailure = Effect.fn(
  "reconcileCreatedThreadTurnStartFailure",
)(function* (input: {
  readonly error: OrchestrationDispatchCommandError;
  readonly readTurnAcceptance: Effect.Effect<
    "accepted" | "not-accepted",
    OrchestrationDispatchCommandError
  >;
  readonly cleanupCreatedThread: Effect.Effect<boolean, OrchestrationDispatchCommandError>;
}) {
  return yield* input.readTurnAcceptance.pipe(
    Effect.matchEffect({
      onFailure: () => Effect.fail(input.error),
      onSuccess: (acceptance) => {
        if (acceptance === "accepted") {
          return Effect.fail(
            new OrchestrationDispatchCommandError({
              message: input.error.message,
              ...(input.error.cause !== undefined ? { cause: input.error.cause } : {}),
              bootstrapTurnDisposition: "accepted",
            }),
          );
        }
        return Effect.uninterruptible(input.cleanupCreatedThread).pipe(
          Effect.matchCauseEffect({
            onFailure: () =>
              Effect.fail(
                new OrchestrationDispatchCommandError({
                  message: input.error.message,
                  ...(input.error.cause !== undefined ? { cause: input.error.cause } : {}),
                  bootstrapTurnDisposition: "not-accepted",
                }),
              ),
            onSuccess: (threadDeleted) =>
              Effect.fail(
                new OrchestrationDispatchCommandError({
                  message: input.error.message,
                  ...(input.error.cause !== undefined ? { cause: input.error.cause } : {}),
                  ...(threadDeleted ? { bootstrapThreadDisposition: "deleted" as const } : {}),
                  bootstrapTurnDisposition: "not-accepted",
                }),
              ),
          }),
        );
      },
    }),
  );
});
