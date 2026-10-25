# logos-golang-module-core

Go SDK for the control plane every Logos module shares with core, whatever its
type (channel, minion factory, ...): registration and liveness, management,
live log streaming and logging.

Type-specific SDKs build on it:

- [`logos-golang-channel-core`](https://github.com/logoscore/logos-golang-channel-core): channels.
- A minion-factory SDK will follow.

## Packages

| Package | What it does |
|---|---|
| `pkg/module` | `Run(ctx, Config)` runs one instance's whole control-plane session. It connects to RabbitMQ and serves management (and logs) on the rpc_queue. Then it registers, heartbeats, deregisters on shutdown, and reconnects with backoff. |
| `pkg/lifecycle` | `module.register` / `module.heartbeat` / `module.deregister` client. `WithManagement`, `WithStatus`, `WithMetrics`, and `WithRegisterField` for fields a module type declares at registration. |
| `pkg/mgmt` | Management `Server` built from a `protocol.ManagementManifest`: `mgmt.config.apply`, `mgmt.action.invoke`, `mgmt.state.get`, logs. `AMQPConsumer` serves it on the module's rpc_queue. |
| `pkg/logstream` | Live log tail for core: ring buffer, leased streams, batches to `logos.module.logs`. |
| `pkg/logging` | The module's slog logger (`json` / `text`), teed into a `logstream.Hub`. |
| `pkg/errors` | Typed error (`Code`, `Message`, wrapped cause) shared by all module SDKs. |

## RPC routing

Core calls a module back on the `rpc_queue` it sent at registration. By
convention that queue is `logos.<module_type>.rpc.<instance>`, bound to the
direct exchange `logos.<module_type>.rpc` with routing key = instance
(`module.RPCQueue`, `mgmt.RPCExchange`). Examples are
`logos.channel.rpc.http-1` and `logos.minion-factory.rpc.mf-1`.

## Usage

```go
srv, _ := mgmt.New(manifest)
hub := logstream.New("mf-1")
err := module.Run(ctx, module.Config{
	AMQPURL:    amqpURL,
	Identity:   lifecycle.Identity{ModuleType: "minion-factory", ModuleName: "go", Instance: "mf-1", Version: version},
	Management: srv,
	Logs:       hub,
	Options:    []lifecycle.Option{lifecycle.WithRegisterField("targets", targets)},
})
```

## Development

```bash
go build ./...
go test ./...
go vet ./...
```

Go 1.25.7. Depends on
[`logos-golang-protocol`](https://github.com/logoscore/logos-golang-protocol).
