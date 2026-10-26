# Changelog

## v0.2.0 (unreleased)

What a factory SDK needs: serving a module type's own operations, and calling
core.

- **`mgmt.Router`** serves the rpc_queue: a module type's operations
  (`HandlerFunc`, by envelope type) go to their handlers, everything else to
  the management `Server`, if any. A handler error made with
  `coreerrors.New(code, message)` replies with that code (e.g. `busy`).
  `mgmt.NewAMQPConsumer` now takes a `*Router` instead of a `*Server`.
- **`module.Config.Handlers`** adds operations to the rpc_queue, which is now
  served whenever there is management or a handler.
- **`lifecycle.CoreClient`** calls core over the current session (`Call`), for
  operations a module type sends core itself; `module.Config.Core` attaches it
  to every session. Between sessions calls fail with `ErrNotConnected`.
- **`mgmt.Server.Extend(Section)`** adds a section a module type's SDK serves
  itself to a module's management: its properties, actions (with their
  handlers) and status values join the module's manifest; its `Apply` runs
  after the module's `ApplyFunc` accepted a change and never on a refused one;
  its status values cover only its own keys. A section that would make the
  manifest invalid (a reused key or action name) is refused.
- **Action conditions.** Before it runs an action for the first time, the
  `Server` checks the action's `available_when` (protocol v0.6.0) against its
  current status and effective values, and refuses with `action_unavailable`
  and the reason. A retried invocation still gets its first answer.
- **Module icons.** `lifecycle.Identity.Icon` is an optional SVG icon sent in
  `module.register` (`icon`, now a reserved register field). `module.Run`
  refuses one that fails `protocol.ValidateIcon`; without one, core shows the
  module type's default.
- Requires `logos-golang-protocol` v0.6.0 (build operation types in tests,
  action conditions, icons).

## v0.1.2

- Requires `logos-golang-protocol` v0.5.0 (`protocol.ServiceFactory`).

## v0.1.1

- Docs and tests: the minion-builder module type is called `factory`
  (`logos.factory.rpc.<instance>`), no longer `minion-factory`. No code
  change: routing follows whatever module type a module registers with.

## v0.1.0

Extracted from `logos-golang-channel-core` v2.2.0, where these packages were
the control plane every module shares.

- **Packages** `lifecycle`, `module`, `mgmt`, `logstream`, `logging` and
  `errors` (only the shared codes: `ERR_INVALID_INPUT`, `ERR_NOT_IMPLEMENTED`,
  `ERR_INTERNAL`).
- **RPC routing per module type**: `mgmt.RPCExchange(moduleType)` is
  `logos.<module_type>.rpc`, and `module.RPCQueue(moduleType, instance)` is
  `logos.<module_type>.rpc.<instance>`. `mgmt.NewAMQPConsumer` takes the
  exchange. Channels keep `logos.channel.rpc` unchanged.
- **Registration extensions**: `lifecycle.WithRegisterField(key, value)` adds
  a top-level `module.register` field for what a module type declares, such
  as a channel's `connection`. The lifecycle contract's own keys are reserved.
  This replaces `lifecycle.WithConnection` and `module.Config.Connection`,
  which moved to channel-core.
- `module.Run` requires `Identity.ModuleType`. Management replies are stamped
  `{module_type, instance}` (`mgmt.Server.DefaultSource`) unless
  `mgmt.WithSource` was given. Before, they were stamped `channel` with no
  instance.
