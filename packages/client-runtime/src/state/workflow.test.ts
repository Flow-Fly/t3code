import { EnvironmentId, ProjectId, WS_METHODS } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";
import { executeAtomQuery } from "./runtime.ts";
import { createWorkflowEnvironmentAtoms } from "./workflow.ts";

const LOCAL_ID = EnvironmentId.make("local-environment");
const REMOTE_ID = EnvironmentId.make("remote-environment");

function session(client: WsRpcProtocolClient): RpcSession {
  return {
    client,
    initialConfig: Effect.never,
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

const supervisor = Effect.fn("WorkflowEnvironmentTest.supervisor")(function* (
  environmentId: EnvironmentId,
  client: WsRpcProtocolClient,
) {
  const target =
    environmentId === REMOTE_ID
      ? new RelayConnectionTarget({ environmentId, label: "Remote environment" })
      : new PrimaryConnectionTarget({
          environmentId,
          label: "Local environment",
          httpBaseUrl: "https://local.example.test",
          wsBaseUrl: "wss://local.example.test",
        });
  const connectionState: SupervisorConnectionState = {
    ...AVAILABLE_CONNECTION_STATE,
    desired: true,
    network: "online",
    phase: "connected",
    attempt: 1,
    generation: 1,
  };
  return EnvironmentSupervisor.EnvironmentSupervisor.of({
    target,
    state: yield* SubscriptionRef.make(connectionState),
    session: yield* SubscriptionRef.make(Option.some(session(client))),
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
});

it.effect("runs Workflow queries on the selected remote environment", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const localClient = {
        [WS_METHODS.workflowRoots]: () => Effect.die("local environment must not be queried"),
      } as unknown as WsRpcProtocolClient;
      const remoteCalls = new Array<unknown>();
      const remoteClient = {
        [WS_METHODS.workflowRoots]: (input: unknown) =>
          Effect.sync(() => {
            remoteCalls.push(input);
            return { repository: "Flow-Fly/t3code", roots: [] };
          }),
        [WS_METHODS.workflowChildren]: (input: unknown) =>
          Effect.sync(() => {
            remoteCalls.push(input);
            return {
              parentNumber: 10,
              children: [],
              frontier: {
                status: "empty-claimed",
                message: "All otherwise available work is already claimed.",
                readyIssueIds: [],
              },
            };
          }),
      } as unknown as WsRpcProtocolClient;
      const local = yield* supervisor(LOCAL_ID, localClient);
      const remote = yield* supervisor(REMOTE_ID, remoteClient);
      const routedEnvironments = new Array<EnvironmentId>();
      const run: EnvironmentRegistry.EnvironmentRegistry["Service"]["run"] = (
        environmentId,
        effect,
      ) => {
        routedEnvironments.push(environmentId);
        return Effect.provideService(
          effect,
          EnvironmentSupervisor.EnvironmentSupervisor,
          environmentId === REMOTE_ID ? remote : local,
        );
      };
      const followStream: EnvironmentRegistry.EnvironmentRegistry["Service"]["followStream"] = (
        environmentId,
        stream,
      ) =>
        Stream.provideService(
          stream,
          EnvironmentSupervisor.EnvironmentSupervisor,
          environmentId === REMOTE_ID ? remote : local,
        );
      const environmentRegistry = EnvironmentRegistry.EnvironmentRegistry.of({
        run,
        followStream,
      } as unknown as EnvironmentRegistry.EnvironmentRegistry["Service"]);
      const runtime = Atom.runtime(
        Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry),
      );
      const atoms = createWorkflowEnvironmentAtoms(runtime);
      const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
        Effect.sync(() => registry.dispose()),
      );
      const input = {
        projectId: ProjectId.make("project-1"),
        repository: "Flow-Fly/t3code",
      } as const;
      const roots = atoms.roots({ environmentId: REMOTE_ID, input });
      const unmount = registry.mount(roots);
      yield* Effect.addFinalizer(() => Effect.sync(unmount));

      const result = yield* Effect.promise(() => executeAtomQuery(registry, roots));
      const children = atoms.children({
        environmentId: REMOTE_ID,
        input: { ...input, parentNumber: 10 },
      });
      const unmountChildren = registry.mount(children);
      yield* Effect.addFinalizer(() => Effect.sync(unmountChildren));
      const childResult = yield* Effect.promise(() => executeAtomQuery(registry, children));

      expect(AsyncResult.isSuccess(result)).toBe(true);
      expect(AsyncResult.isSuccess(childResult)).toBe(true);
      if (AsyncResult.isSuccess(childResult)) {
        expect(childResult.value.frontier?.status).toBe("empty-claimed");
      }
      expect(routedEnvironments).toEqual([REMOTE_ID, REMOTE_ID]);
      expect(remoteCalls).toEqual([input, { ...input, parentNumber: 10 }]);
    }),
  ),
);
