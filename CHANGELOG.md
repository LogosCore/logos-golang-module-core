# Changelog

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
