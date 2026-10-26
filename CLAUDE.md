# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go SDK (`github.com/logoscore/logos-golang-module-core`) for the control plane shared by every Logos module type: registration and liveness, management, live log streaming and logging. Type-specific SDKs build on it: `logos-golang-channel-core` (channels) and, later, a factory SDK. Nothing here is specific to one module type; type-specific behaviour belongs in those SDKs.

Depends on `github.com/logoscore/logos-golang-protocol` for envelopes and the management manifest types.

## Build & Test

```bash
go build ./...
go test ./...
go vet ./...
```

No Makefile; standard Go tooling. Go 1.25.7.

## Packages

- **`pkg/module`**: `Run` supervises an instance's whole control-plane session. It connects, serves the `mgmt.Server` (and `Config.Logs`, which declares the `logs` capability) on the rpc_queue, registers, heartbeats, deregisters on shutdown, and reconnects with backoff on any failure. The rpc_queue defaults to `RPCQueue(module_type, instance)`. Plumbing only: it never adds actions or properties of its own.
- **`pkg/lifecycle`**: the register/heartbeat/deregister client. `WithManagement(s)` declares the manifest, applies `config.management` from every register ack, and reports the applied revision and status in heartbeats. `WithRegisterField(key, v)` adds a type-specific top-level register field (`ReservedRegisterFields` are refused).
- **`pkg/mgmt`**: management `Server` built from a `protocol.ManagementManifest`. It handles `mgmt.config.apply`, `mgmt.action.invoke` and `mgmt.state.get`. `AMQPConsumer` serves it on the rpc_queue, bound to `RPCExchange(module_type)` = `logos.<module_type>.rpc`. Start the consumer before registering.
- **`pkg/logstream`**: live log tail. `Hub` keeps a ring buffer; while core holds a leased stream it publishes batches to `logos.module.logs`. Records keep the module's attr structure.
- **`pkg/logging`**: `New(format, w, level, hub)` builds the module logger; `Headers(h)` logs HTTP headers as a group.
- **`pkg/errors`**: typed error (`Error` with `Code`, `Message`, wrapped `Err`); import it as `coreerrors`. Only shared codes live here. Type SDKs add their own codes on the same type.

## Conventions

- Packages live under `pkg/`; import paths are `github.com/logoscore/logos-golang-module-core/pkg/<name>`.
- Contracts are specified in the docs repo (`logos-docs`): `contracts/module-lifecycle.md`, `module-management.md`, `amqp-conventions.md`.
