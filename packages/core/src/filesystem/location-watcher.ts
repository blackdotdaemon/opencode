export * as LocationWatcher from "./location-watcher.js"

import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Cause, Context, Effect, Exit, Layer, Scope, Semaphore, Stream } from "effect"
import { FileSystem } from "@opencode/schema/filesystem"
import path from "path"
import { Bus } from "../bus.js"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { Git } from "../git.js"
import { Location } from "../location.js"
import { Plugin } from "../plugin.js"
import { LocationWatcherPolicy } from "./location-watcher-policy.js"
import { Watcher } from "./watcher.js"

export interface Interface {}

export class Service extends Context.Service<Service, Interface>()("@opencode/LocationWatcher") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const location = yield* Location.Service
    const watcher = yield* Watcher.Service
    const bus = yield* Bus.Service
    const fs = yield* FSUtil.Service
    const git = yield* Git.Service
    const global = yield* Global.Service
    const policy = yield* LocationWatcherPolicy.Service
    const directory = path.resolve(location.directory)
    // The service also loads the home directory for global requests. Never watch it or a drive root recursively.
    const watchWorkspace =
      !location.workspaceID && directory !== path.parse(directory).root && !FSUtil.contains(directory, global.home)
    const internalDirectories = [global.data, global.cache, global.state].filter((entry) =>
      FSUtil.contains(directory, entry),
    )
    const start = Effect.fnUntraced(function* (input: Watcher.WatchInput) {
      const scope = yield* Scope.make()
      yield* Effect.gen(function* () {
        const updates = yield* watcher.subscribe(input)
        yield* Stream.runForEach(updates, (update) =>
          bus.publish(FileSystem.Event.Changed, {
            file: update.path,
            event: update.type === "create" ? "add" : update.type === "update" ? "change" : "unlink",
          }),
        )
      }).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          (cause) => Effect.logError("location watcher subscription failed", { path: input.path, cause }),
        ),
        Effect.forkIn(scope, { startImmediately: true }),
      )
      return scope
    })
    const target = yield* Effect.cached(
      Effect.gen(function* () {
        if (location.vcs?.type === "git") {
          const resolved = (yield* git.repo.discover(location.directory))?.gitDirectory
          const vcs = resolved ? yield* fs.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved)) : undefined
          if (vcs) return { path: path.join(vcs, "HEAD"), aliases: [".git", vcs, ...(resolved ? [resolved] : [])] }
        }
        if (location.vcs?.type === "hg") {
          const store = location.vcs.store
          const vcs = yield* fs.realPath(store).pipe(Effect.orElseSucceed(() => store))
          return { path: path.join(vcs, "branch"), aliases: [".hg", vcs] }
        }
      }).pipe(
        Effect.withSpan("LocationWatcher.target", { attributes: { directory: location.directory } }),
        Effect.catchCause((cause) =>
          Effect.logError("failed to resolve location watcher target", { cause }).pipe(Effect.as(undefined)),
        ),
      ),
    )
    const lock = Semaphore.makeUnsafe(1)
    let stopped = false
    let metadata: { path: string; scope: Scope.Closeable } | undefined
    let workspace: { key: string; scope: Scope.Closeable } | undefined
    const reconcile = () =>
      lock.withPermit(
        Effect.gen(function* () {
          if (stopped) return
          const resolved = yield* target
          const ignore = policy.current()
          const next = resolved && !resolved.aliases.some((alias) => ignore.includes(alias)) ? resolved.path : undefined
          if (metadata?.path !== next) {
            if (metadata) yield* Scope.close(metadata.scope, Exit.void)
            metadata = next ? { path: next, scope: yield* start({ path: next, type: "file" }) } : undefined
          }
          if (!watchWorkspace) return

          // Branch metadata has its own file subscription. App data and logs can change in response to these events.
          const ignored = [
            ...new Set([...FSUtil.ignoreGlobs, ".hg", "**/.hg/**", ...internalDirectories, ...ignore]),
          ].toSorted()
          const key = JSON.stringify(ignored)
          if (workspace?.key === key) return
          if (workspace) yield* Scope.close(workspace.scope, Exit.void)
          workspace = { key, scope: yield* start({ path: directory, type: "directory", ignore: ignored }) }
        }).pipe(Effect.withSpan("LocationWatcher.reconcile", { attributes: { directory: location.directory } })),
      )
    yield* Effect.addFinalizer(() =>
      lock.withPermit(
        Effect.gen(function* () {
          stopped = true
          if (metadata) yield* Scope.close(metadata.scope, Exit.void)
          if (workspace) yield* Scope.close(workspace.scope, Exit.void)
        }),
      ),
    )
    yield* policy.observe(reconcile)
    yield* Effect.gen(function* () {
      yield* Plugin.awaitActivation
      yield* reconcile()
    }).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterrupts(cause),
        (cause) => Effect.logError("failed to start location watcher", { cause }),
      ),
      Effect.forkScoped({ startImmediately: true }),
    )
    return Service.of({})
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [
    Watcher.node,
    FSUtil.node,
    Location.node,
    Git.node,
    Global.node,
    Bus.node,
    Plugin.node,
    LocationWatcherPolicy.node,
  ],
})
