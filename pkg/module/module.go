// Package module runs a module's whole control-plane session with core: it
// connects to RabbitMQ, serves the module's rpc_queue (management, if any, and
// the operations its module type adds), registers, heartbeats, and deregisters
// on shutdown — reconnecting with backoff whenever the broker connection or the
// initial register fails.
//
// It is plumbing only. What a module can be managed on (properties, actions,
// status values) is declared by the module through its mgmt.Server; this
// package never adds behavior of its own to that surface.
package module

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/logstream"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

// RPCQueue is the conventional rpc_queue of a moduleType instance: the queue
// core calls it back on, logos.<module_type>.rpc.<instance>, bound to
// mgmt.RPCExchange(moduleType).
func RPCQueue(moduleType, instance string) string {
	return mgmt.RPCExchange(moduleType) + "." + instance
}

// Config describes one module instance's control-plane session.
type Config struct {
	// AMQPURL is the RabbitMQ URL core's control plane runs on.
	AMQPURL string
	// Identity is sent at registration. An empty RPCQueue defaults to
	// RPCQueue(Identity.ModuleType, Identity.Instance).
	Identity lifecycle.Identity
	// Management, when set, is served on the rpc_queue and declared at
	// registration. Nil runs lifecycle only; core then cannot manage the
	// instance.
	Management *mgmt.Server
	// Logs, when set together with Management, lets core tail this module's
	// logs live: Run serves it on the rpc_queue (declaring the logs
	// capability) and attaches a publisher for each broker session.
	Logs *logstream.Hub
	// Handlers serve the operations a module type adds to the rpc_queue, by
	// envelope type (a factory's factory.build), next to management. With
	// handlers the rpc_queue is served even when Management is nil.
	Handlers map[string]mgmt.HandlerFunc
	// Core, when set, is attached to every session, so the module can call
	// core itself, e.g. to report a build's result.
	Core *lifecycle.CoreClient
	// Options are extra lifecycle options (e.g. WithStatus, WithMetrics, or
	// WithRegisterField for what a module type declares at registration).
	Options []lifecycle.Option
	// Logger receives session events: registered/deregistered at info,
	// failures, rejections and reconnects at warn. Defaults to slog.Default()
	// at the time of each event.
	Logger *slog.Logger
}

// Run keeps the instance registered until ctx is cancelled, then deregisters
// and returns nil. It returns an error only for an invalid Config.
//
// lifecycle.Client gives up on a failed initial register and does not recover
// a dropped broker connection, so Run supervises it: every failure reconnects
// with exponential backoff (1s doubling to 30s, reset after a session that
// stayed up longer than that).
//
// Core rejects sync traffic from unregistered instances, so stop Run only
// after the module has drained its in-flight requests: give it a context of
// its own rather than the one that starts the shutdown.
func Run(ctx context.Context, cfg Config) error {
	if cfg.AMQPURL == "" {
		return errors.New("module: AMQPURL is required")
	}
	if cfg.Identity.ModuleType == "" {
		return errors.New("module: Identity.ModuleType is required")
	}
	if cfg.Identity.RPCQueue == "" {
		cfg.Identity.RPCQueue = RPCQueue(cfg.Identity.ModuleType, cfg.Identity.Instance)
	}
	logger := func() *slog.Logger {
		if cfg.Logger != nil {
			return cfg.Logger
		}
		return slog.Default()
	}
	if cfg.Logs != nil {
		if cfg.Management == nil {
			return errors.New("module: Logs requires Management (it is served on the rpc_queue)")
		}
		cfg.Management.ServeLogs(cfg.Logs)
	}
	source := protocol.Source{Service: cfg.Identity.ModuleType, Instance: cfg.Identity.Instance}
	if cfg.Management != nil {
		cfg.Management.DefaultSource(source)
	}
	router, err := mgmt.NewRouter(source, cfg.Management, cfg.Handlers)
	if err != nil {
		return fmt.Errorf("module: %w", err)
	}

	backoff := minBackoff
	for {
		started := time.Now()
		err := runSession(ctx, cfg, router, logger)
		if ctx.Err() != nil {
			return nil
		}
		// A session that stayed up for a while was healthy; start over fast.
		if time.Since(started) > maxBackoff {
			backoff = minBackoff
		}
		logger().Warn("control plane: session failed; retrying", "err", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// runSession runs one broker connection's worth of lifecycle. It returns nil
// only on ctx cancellation (after deregistering).
func runSession(ctx context.Context, cfg Config, router *mgmt.Router, logger func() *slog.Logger) error {
	id := cfg.Identity
	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	pub, err := lifecycle.NewAMQPPublisher(conn)
	if err != nil {
		return fmt.Errorf("open lifecycle publisher: %w", err)
	}
	defer pub.Close()

	// Core may call the rpc_queue as soon as it acks the register, so the
	// consumer must be up first. It is closed after the deregister.
	opts := cfg.Options
	var rpcDone <-chan struct{}
	if cfg.Management != nil || len(cfg.Handlers) > 0 {
		consumer, err := mgmt.NewAMQPConsumer(conn, mgmt.RPCExchange(id.ModuleType), id.RPCQueue, id.Instance, router)
		if err != nil {
			return fmt.Errorf("serve rpc_queue %q: %w", id.RPCQueue, err)
		}
		defer consumer.Close()
		rpcDone = consumer.Done()
	}
	if cfg.Management != nil {
		if cfg.Logs != nil {
			lp, err := logstream.NewAMQPPublisher(conn)
			if err != nil {
				return fmt.Errorf("open log publisher: %w", err)
			}
			defer lp.Close()
			cfg.Logs.Attach(lp)
			// Streams die with the session; core's lease renewal restarts
			// them on the next one.
			defer cfg.Logs.Detach()
		}
		opts = append([]lifecycle.Option{lifecycle.WithManagement(cfg.Management)}, opts...)
	}
	if cfg.Core != nil {
		// Detached before the publisher closes: calls between sessions fail
		// fast instead of hanging on a dead channel.
		cfg.Core.Attach(pub, protocol.Source{Service: id.ModuleType, Instance: id.Instance})
		defer cfg.Core.Detach()
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	connLost := make(chan error, 1)
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case e := <-closed:
			connLost <- fmt.Errorf("RabbitMQ connection closed: %v", e)
			cancel()
		case <-rpcDone: // nil without a consumer: never ready
			connLost <- fmt.Errorf("rpc_queue consumer on %q stopped", id.RPCQueue)
			cancel()
		case <-sessionCtx.Done():
		}
	}()

	logged := loggingPublisher{pub: pub, instance: id.Instance, connClosed: conn.IsClosed, logger: logger}
	runErr := lifecycle.NewClient(logged, id, opts...).Run(sessionCtx)
	if ctx.Err() != nil && cfg.Logs != nil {
		// Shutting down: tell viewers before the publisher closes, whatever
		// the session's own outcome.
		cfg.Logs.Close()
	}
	if runErr != nil {
		return runErr
	}
	select {
	case err := <-connLost:
		return err
	default:
		return nil
	}
}

// loggingPublisher logs lifecycle outcomes. lifecycle.Client swallows heartbeat
// errors and exposes no hooks, so without this a rejected or unreachable
// instance would fail silently.
type loggingPublisher struct {
	pub      lifecycle.Publisher
	instance string
	// connClosed suppresses failures of the deregister lifecycle.Client
	// attempts after the connection is already gone; Run reports that loss.
	connClosed func() bool
	logger     func() *slog.Logger
}

func (p loggingPublisher) Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	reply, err := p.pub.Call(ctx, req)
	switch {
	case err != nil:
		if ctx.Err() == nil && !p.connClosed() {
			p.logger().Warn("control plane: call failed", "op", req.Type, "err", err)
		}
	case reply.Status == protocol.StatusError:
		code := "unknown"
		if reply.Error != nil {
			code = reply.Error.Code + ": " + reply.Error.Message
		}
		p.logger().Warn("control plane: rejected by core", "op", req.Type, "reason", code)
	case req.Type == lifecycle.TypeRegister:
		p.logger().Info("control plane: registered with core", "instance", p.instance)
	case req.Type == lifecycle.TypeDeregister:
		p.logger().Info("control plane: deregistered", "instance", p.instance)
	}
	return reply, err
}
