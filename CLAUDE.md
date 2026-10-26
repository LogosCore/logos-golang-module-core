# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go SDK (`github.com/logoscore/logos-golang-module-core`) for the control plane shared by every Logos module type: registration and liveness, management, live log streaming and logging. Type-specific SDKs build on it: `logos-golang-channel-core` (channels) and `logos-golang-factory-core` (factories). Nothing here is specific to one module type; type-specific behaviour belongs in those SDKs.

Depends on `github.com/logoscore/logos-golang-protocol` for envelopes and the management manifest types.

## Build & Test

```bash
go build ./...
go test ./...
go vet ./...
```

No Makefile; standard Go tooling. Go 1.25.7.

## Packages

- **`pkg/module`**: `Run` supervises an instance's whole control-plane session. It connects, serves the rpc_queue (the `mgmt.Server`, `Config.Logs`, which declares the `logs` capability, and `Config.Handlers`, the operations a module type adds), registers, heartbeats, deregisters on shutdown, and reconnects with backoff on any failure. It attaches `Config.Core` to every session. The rpc_queue defaults to `RPCQueue(module_type, instance)`, and is served whenever there is management or a handler. Plumbing only: it never adds actions, properties or operations of its own.
- **`pkg/lifecycle`**: the register/heartbeat/deregister client. `WithManagement(s)` declares the manifest, applies `config.management` from every register ack, and reports the applied revision and status in heartbeats. `WithRegisterField(key, v)` adds a type-specific top-level register field (`ReservedRegisterFields` are refused). `Identity.Icon` is the module's optional SVG icon (`protocol.ValidateIcon`; `module.Run` refuses an invalid one). `CoreClient` calls core (`logos.core.rpc`) over whatever session is attached, failing with `ErrNotConnected` between sessions; an error reply comes back as a `coreerrors.Error` with core's code.
- **`pkg/mgmt`**: management `Server` built from a `protocol.ManagementManifest`. It handles `mgmt.config.apply`, `mgmt.action.invoke` and `mgmt.state.get`, and refuses an action whose `available_when` does not hold against its live status and effective values (`action_unavailable`; checked on a first invocation only, so a retry gets its first answer). `Extend(Section)` lets a type SDK add a section it serves itself (factory-core's builds): the section's `Apply` runs only after the module's own `ApplyFunc` accepted a change, so a refused change never half-applies, and its status values cover only its own keys. `Router` sends a module type's operations (`HandlerFunc` by envelope type; a `coreerrors.New(code, msg)` error replies with that code) to their handlers and everything else to the `Server`, if any. `AMQPConsumer` serves the `Router` on the rpc_queue, bound to `RPCExchange(module_type)` = `logos.<module_type>.rpc`. Start the consumer before registering.
- **`pkg/logstream`**: live log tail. `Hub` keeps a ring buffer; while core holds a leased stream it publishes batches to `logos.module.logs`. Records keep the module's attr structure.
- **`pkg/logging`**: `New(format, w, level, hub)` builds the module logger; `Headers(h)` logs HTTP headers as a group.
- **`pkg/errors`**: typed error (`Error` with `Code`, `Message`, wrapped `Err`); import it as `coreerrors`. Only shared codes live here. Type SDKs add their own codes on the same type.

## Conventions

- Packages live under `pkg/`; import paths are `github.com/logoscore/logos-golang-module-core/pkg/<name>`.
- Contracts are specified in the docs repo (`logos-docs`): `contracts/module-lifecycle.md`, `module-management.md`, `amqp-conventions.md`.
