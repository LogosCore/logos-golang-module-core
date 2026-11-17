// Package lifecycle implements the module side of the module-registration
// (liveness) control-plane contract. A module is the RPC *client* for these
// operations and core is the RPC server: the module announces itself with
// module.register, proves liveness with module.heartbeat on an interval, and
// leaves cleanly with module.deregister on shutdown.
//
// The contract is specified in the docs repo under
// contracts/module-lifecycle.md and adr/0003-module-registration-lifecycle.md.
// Transport details (exchange logos.core.rpc, routing key core, direct reply-to)
// live in AMQPPublisher; the Client owns envelope construction and the liveness
// state machine and is transport-agnostic via the Publisher interface.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Operation type identifiers for the module lifecycle contract.
const (
	TypeRegister   = "module.register"
	TypeHeartbeat  = "module.heartbeat"
	TypeDeregister = "module.deregister"
)

// Status is a module's self-reported health, sent on each heartbeat: whether
// it can do its job. Module-specific states (e.g. an admin-paused channel)
// belong in its management status values, not here.
type Status string

const (
	StatusHealthy  Status = "healthy"
	StatusDegraded Status = "degraded"

	// Deprecated: heartbeat health is only healthy or degraded; core no longer
	// accepts "draining" as a meaning. Report a module-specific pause as a
	// management status value instead. Kept so v2 stays source-compatible.
	StatusDraining Status = "draining"
)

// Defaults applied when core's register ack omits or zeroes a liveness value.
const (
	defaultHeartbeatInterval = 30 * time.Second
	defaultGraceMisses       = 3
	defaultDeregisterTimeout = 5 * time.Second
)

// Identity is a module instance's static self-description, sent at registration.
// ModuleName is the hardcoded kind shared by every instance of a module (e.g.
// "http"); Instance is the unique id of one deployed instance and is the key
// core addresses for heartbeat, deregister, and management RPC.
type Identity struct {
	ModuleType  string // "channel" | "factory"
	ModuleName  string // hardcoded module kind, e.g. "http"
	Instance    string // unique deployed-instance id, self-assigned
	Version     string // module build/version
	RPCQueue    string // queue core uses to call back into this module
	Description string // optional free text, surfaced on the admin Modules page
	// Icon is an optional SVG icon core shows for the module, checked with
	// protocol.ValidateIcon; without one, core shows its module type's
	// default. Where the module keeps it (an embedded file, a constant,
	// something generated) is up to the module.
	Icon string
	// IconLight is an optional second icon, drawn instead of Icon on light
	// themes. The UI paints an icon in its text colour: light on dark themes,
	// dark on light ones. A drawing whose shape is the lit parts of a shaded
	// picture reads as a negative the other way round, so it needs one drawn
	// for light themes, with the shadows and lines as the shape. Checked like
	// Icon, and only with one.
	IconLight string
}

func (id Identity) validate() error {
	switch {
	case id.ModuleType == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "module_type is required")
	case id.ModuleName == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "module_name is required")
	case id.Instance == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "instance is required")
	case id.RPCQueue == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "rpc_queue is required")
	}
	if id.Icon != "" {
		if err := protocol.ValidateIcon(id.Icon); err != nil {
			return coreerrors.New(coreerrors.CodeInvalidInput, "icon: "+err.Error())
		}
	}
	if id.IconLight != "" {
		if id.Icon == "" {
			return coreerrors.New(coreerrors.CodeInvalidInput, "icon_light needs an icon")
		}
		if err := protocol.ValidateIcon(id.IconLight); err != nil {
			return coreerrors.New(coreerrors.CodeInvalidInput, "icon_light: "+err.Error())
		}
	}
	return nil
}

// BootstrapConfig is the registration ack: liveness parameters plus the opaque
// per-module bootstrap config core returns. With WithManagement, Management is
// the result of applying config.management from the ack (nil when core sent
// none).
type BootstrapConfig struct {
	HeartbeatInterval time.Duration
	GraceMisses       int
	Config            map[string]json.RawMessage
	Management        *protocol.ConfigApplyReply
}

// managementConfigKey is the register-reply config key holding the effective
// management config.
const managementConfigKey = "management"

// HeartbeatResult is the outcome of a single heartbeat. When ConfigChanged is
// true the module should re-register to pull updated bootstrap config.
type HeartbeatResult struct {
	ConfigChanged bool
}

// Publisher transmits a control-plane RPC request envelope to core and returns
// the reply. Implementations own AMQP transport; the Client owns envelope
// construction and payload semantics. Call must return once ctx ends: the
// Client gives every call a deadline, and a call core never answers must not
// outlive it. The Client serializes its own calls; a CoreClient sharing the
// Publisher calls concurrently with it (AMQPPublisher is safe for that).
type Publisher interface {
	Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error)
}

// MetricsFunc returns an optional lightweight liveness snapshot for a heartbeat.
type MetricsFunc func() map[string]any

// Client drives the module side of the lifecycle contract.
type Client struct {
	pub     Publisher
	id      Identity
	source  protocol.Source
	status  func() Status
	metrics MetricsFunc
	mgmt    *mgmt.Server
	// extensions are type-specific module.register fields, by key, in the
	// order first set.
	extensions    map[string]any
	extensionKeys []string

	deregisterTimeout time.Duration
	callTimeout       time.Duration
	logger            *slog.Logger
	// newTicker is injectable so the Run loop is deterministically testable.
	newTicker func(time.Duration) (<-chan time.Time, func())
}

// Option configures a Client.
type Option func(*Client)

// WithStatus sets the callback used to report heartbeat status. Without it the
// Client reports StatusHealthy.
func WithStatus(fn func() Status) Option {
	return func(c *Client) {
		if fn != nil {
			c.status = fn
		}
	}
}

// WithMetrics sets the callback supplying the optional heartbeat metrics
// snapshot.
func WithMetrics(fn MetricsFunc) Option {
	return func(c *Client) { c.metrics = fn }
}

// WithManagement declares s's manifest at registration and keeps s in sync with
// core: every register (including re-registers) applies the reply's
// config.management, and heartbeats report s's applied revision and status
// values (merged over WithMetrics, status keys winning). Serve s on the
// rpc_queue (mgmt.NewAMQPConsumer) before running the Client.
func WithManagement(s *mgmt.Server) Option {
	return func(c *Client) { c.mgmt = s }
}

// WithRegisterField adds a module-type-specific top-level field to every
// module.register payload (a channel's "connection", for example). The value
// is JSON-encoded as is; validating it is up to the SDK that declares it.
// Setting a key again replaces its value. Register refuses a key the
// lifecycle contract itself uses (ReservedRegisterFields).
func WithRegisterField(key string, value any) Option {
	return func(c *Client) {
		if c.extensions == nil {
			c.extensions = map[string]any{}
		}
		if _, ok := c.extensions[key]; !ok {
			c.extensionKeys = append(c.extensionKeys, key)
		}
		c.extensions[key] = value
	}
}

// ReservedRegisterFields are the module.register fields owned by the
// lifecycle contract; WithRegisterField cannot set them.
var ReservedRegisterFields = []string{
	"module_type", "module_name", "instance", "version", "rpc_queue",
	"description", "icon", "icon_light", "management", "applied_revision",
}

// WithLogger sets the logger Run reports with (failed re-registers, a block).
// Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.logger = l }
}

func (c *Client) log() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

// WithDeregisterTimeout overrides how long Run waits for the shutdown
// deregister RPC (default 5s).
func WithDeregisterTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.deregisterTimeout = d
		}
	}
}

// WithCallTimeout bounds how long each register, heartbeat and deregister
// waits for core's reply before it counts as lost (default
// DefaultCallTimeout).
func WithCallTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.callTimeout = d
		}
	}
}

// NewClient builds a lifecycle Client for id, transmitting over pub.
func NewClient(pub Publisher, id Identity, opts ...Option) *Client {
	c := &Client{
		pub:               pub,
		id:                id,
		source:            protocol.Source{Service: id.ModuleType, Instance: id.Instance},
		status:            func() Status { return StatusHealthy },
		deregisterTimeout: defaultDeregisterTimeout,
		callTimeout:       DefaultCallTimeout,
		newTicker: func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// --- wire payloads -------------------------------------------------------

type registerRequest struct {
	ModuleType  string `json:"module_type"`
	ModuleName  string `json:"module_name"`
	Instance    string `json:"instance"`
	Version     string `json:"version"`
	RPCQueue    string `json:"rpc_queue"`
	Description string `json:"description,omitempty"`
	Icon        string `json:"icon,omitempty"`
	IconLight   string `json:"icon_light,omitempty"`

	Management      *protocol.ManagementManifest `json:"management,omitempty"`
	AppliedRevision *int64                       `json:"applied_revision,omitempty"`

	// extensions are WithRegisterField values, merged in at the top level.
	extensions map[string]any
}

func (r registerRequest) MarshalJSON() ([]byte, error) {
	type plain registerRequest
	raw, err := json.Marshal(plain(r))
	if err != nil || len(r.extensions) == 0 {
		return raw, err
	}
	merged := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &merged); err != nil {
		return nil, err
	}
	for k, v := range r.extensions {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("register field %q: %w", k, err)
		}
		merged[k] = b
	}
	return json.Marshal(merged)
}

type registerReply struct {
	Instance              string                     `json:"instance"`
	Registered            bool                       `json:"registered"`
	HeartbeatIntervalSecs int                        `json:"heartbeat_interval_seconds"`
	HeartbeatGraceMisses  int                        `json:"heartbeat_grace_misses"`
	Config                map[string]json.RawMessage `json:"config"`
}

type heartbeatRequest struct {
	Instance string         `json:"instance"`
	Status   Status         `json:"status"`
	Metrics  map[string]any `json:"metrics,omitempty"`

	AppliedRevision *int64 `json:"applied_revision,omitempty"`
}

type heartbeatReply struct {
	Instance      string `json:"instance"`
	Ack           bool   `json:"ack"`
	ConfigChanged bool   `json:"config_changed"`
}

type deregisterRequest struct {
	Instance string `json:"instance"`
	Reason   string `json:"reason,omitempty"`
}

// --- operations ----------------------------------------------------------

// Register announces this module instance and returns the bootstrap config from
// core's ack. Re-registration is idempotent on the core side (takeover).
func (c *Client) Register(ctx context.Context) (BootstrapConfig, error) {
	if err := c.id.validate(); err != nil {
		return BootstrapConfig{}, err
	}
	for _, k := range c.extensionKeys {
		if isReservedRegisterField(k) {
			return BootstrapConfig{}, coreerrors.New(coreerrors.CodeInvalidInput,
				fmt.Sprintf("register field %q is reserved by the lifecycle contract", k))
		}
	}
	req := registerRequest{
		ModuleType:  c.id.ModuleType,
		ModuleName:  c.id.ModuleName,
		Instance:    c.id.Instance,
		Version:     c.id.Version,
		RPCQueue:    c.id.RPCQueue,
		Description: c.id.Description,
		Icon:        c.id.Icon,
		IconLight:   c.id.IconLight,
		extensions:  c.extensions,
	}
	if c.mgmt != nil {
		m := c.mgmt.Manifest()
		rev := c.mgmt.AppliedRevision()
		req.Management, req.AppliedRevision = &m, &rev
	}
	var rep registerReply
	if err := c.call(ctx, TypeRegister, req, &rep); err != nil {
		return BootstrapConfig{}, err
	}
	boot := bootstrapFrom(rep)
	if err := c.applyManagement(ctx, &boot); err != nil {
		return BootstrapConfig{}, err
	}
	return boot, nil
}

func isReservedRegisterField(key string) bool {
	for _, r := range ReservedRegisterFields {
		if key == r {
			return true
		}
	}
	return false
}

// applyManagement applies config.management from a register ack, the
// effective config the module must run with.
func (c *Client) applyManagement(ctx context.Context, boot *BootstrapConfig) error {
	raw, ok := boot.Config[managementConfigKey]
	if c.mgmt == nil || !ok {
		return nil
	}
	var cfg protocol.ManagementConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "decode config.management", err)
	}
	rep := c.mgmt.ApplyConfig(ctx, cfg)
	boot.Management = &rep
	return nil
}

// Heartbeat sends a single liveness beat and reports whether core signaled a
// config change.
func (c *Client) Heartbeat(ctx context.Context) (HeartbeatResult, error) {
	var metrics map[string]any
	if c.metrics != nil {
		metrics = c.metrics()
	}
	req := heartbeatRequest{Instance: c.id.Instance, Status: c.status()}
	if c.mgmt != nil {
		if status := c.mgmt.StatusSnapshot(); len(status) > 0 {
			merged := make(map[string]any, len(metrics)+len(status))
			for k, v := range metrics {
				merged[k] = v
			}
			for k, v := range status {
				merged[k] = v
			}
			metrics = merged
		}
		rev := c.mgmt.AppliedRevision()
		req.AppliedRevision = &rev
	}
	req.Metrics = metrics
	var rep heartbeatReply
	if err := c.call(ctx, TypeHeartbeat, req, &rep); err != nil {
		return HeartbeatResult{}, err
	}
	return HeartbeatResult{ConfigChanged: rep.ConfigChanged}, nil
}

// Deregister tells core to drop this instance from the active registry. reason
// is free-form (e.g. "shutdown", "redeploy").
func (c *Client) Deregister(ctx context.Context, reason string) error {
	return c.call(ctx, TypeDeregister, deregisterRequest{Instance: c.id.Instance, Reason: reason}, nil)
}

// Run registers, then heartbeats until ctx is cancelled, then deregisters
// gracefully. It re-registers when core reports the instance unknown
// (unknown_instance) or signals config_changed; each re-register re-applies
// the management config from the ack (WithManagement) and adopts a changed
// heartbeat interval. Run returns the error that ended it; a clean shutdown
// via ctx cancellation returns nil.
//
// Every call has a deadline (WithCallTimeout; a heartbeat's is at most the
// interval), so a request core never answers fails instead of holding the
// loop: core down, or its queue missing from a broker that came back without
// its state, where the request is dropped. A failed register ends Run, and so
// do as many heartbeats in a row with no answer as core's grace misses: by
// then core counts the instance dead, and the next session (module.Run)
// registers it again. A heartbeat core answered, even with an error, is no
// miss.
func (c *Client) Run(ctx context.Context) error {
	boot, err := c.Register(ctx)
	if err != nil {
		return err
	}
	c.reportApplied(ctx, boot)

	interval, grace := boot.HeartbeatInterval, boot.GraceMisses
	tick, stop := c.newTicker(interval)
	defer func() { stop() }()
	reregister := func() {
		boot, err := c.Register(ctx)
		if err != nil {
			// Asked again at the next heartbeat, which core answers with
			// unknown_instance until the instance is registered.
			if coreerrors.Code(err) == protocol.CodeInstanceBlocked {
				c.log().Warn("control plane: core has blocked this instance; asking again at the next heartbeat",
					"instance", c.id.Instance)
			} else {
				c.log().Warn("control plane: re-register failed; retrying at the next heartbeat",
					"instance", c.id.Instance, "err", err)
			}
			return
		}
		c.reportApplied(ctx, boot)
		grace = boot.GraceMisses
		if boot.HeartbeatInterval == interval {
			return
		}
		stop()
		interval = boot.HeartbeatInterval
		tick, stop = c.newTicker(interval)
	}

	misses := 0
	for {
		select {
		case <-ctx.Done():
			c.gracefulDeregister()
			return nil
		case <-tick:
			hctx, cancel := context.WithTimeout(ctx, interval)
			hb, err := c.Heartbeat(hctx)
			cancel()
			switch {
			case err == nil:
				misses = 0
				if hb.ConfigChanged {
					reregister()
				}
			case ctx.Err() != nil:
				// Shutting down: the next select deregisters.
			case coreerrors.Code(err) == protocol.CodeUnknownInstance:
				// Core no longer knows the instance (it counted it dead):
				// register again and resume.
				misses = 0
				reregister()
			case answered(err):
				misses = 0 // core is there; it refused this beat
			default:
				misses++
				if misses >= grace {
					return fmt.Errorf("no answer from core to %d heartbeats in a row: %w", misses, err)
				}
			}
		}
	}
}

// answered reports whether err is core's error reply, rather than a call that
// got none (no reply in time, no route, a closed channel).
func answered(err error) bool {
	var reply *coreerrors.Error
	return errors.As(err, &reply)
}

// reportApplied heartbeats right after a register whose reply carried config,
// so core sees the newly applied revision now rather than one interval later
// (register itself reports the revision from before that apply).
func (c *Client) reportApplied(ctx context.Context, boot BootstrapConfig) {
	if c.mgmt == nil || boot.Management == nil {
		return
	}
	_, _ = c.Heartbeat(ctx)
}

// gracefulDeregister deregisters with a fresh, bounded context since Run's ctx
// is already cancelled by the time this runs.
func (c *Client) gracefulDeregister() {
	ctx, cancel := context.WithTimeout(context.Background(), c.deregisterTimeout)
	defer cancel()
	_ = c.Deregister(ctx, "shutdown")
}

// --- internals -----------------------------------------------------------

// call builds a request envelope of the given type, transmits it via the
// Publisher, maps an error reply to a typed error, and decodes a success
// payload into out (when non-nil).
func (c *Client) call(ctx context.Context, typ string, payload, out any) error {
	env, err := c.newRequest(typ, payload)
	if err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "marshal "+typ+" request", err)
	}
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	reply, err := c.pub.Call(ctx, env)
	if err != nil {
		return err
	}
	if reply.Status == protocol.StatusError {
		return replyError(reply, "lifecycle rpc failed")
	}
	if out != nil {
		if err := json.Unmarshal(reply.Payload, out); err != nil {
			return coreerrors.Wrap(coreerrors.CodeInternal, "decode "+typ+" reply", err)
		}
	}
	return nil
}

func (c *Client) newRequest(typ string, payload any) (protocol.Envelope, error) {
	return newRequest(c.source, typ, payload)
}

// newRequest builds a control-plane request envelope. correlation_id equals
// message_id on an originating request, per the envelope contract.
func newRequest(source protocol.Source, typ string, payload any) (protocol.Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return protocol.Envelope{}, err
	}
	id := protocol.NewID()
	return protocol.Envelope{
		MessageID:     id,
		CorrelationID: id,
		Type:          typ,
		Version:       protocol.EnvelopeVersionV1,
		Timestamp:     protocol.NowTimestamp(),
		Source:        source,
		Payload:       raw,
	}, nil
}

// replyError maps an error reply to a typed error carrying core's code.
func replyError(reply protocol.ReplyEnvelope, fallback string) error {
	code, msg := protocol.CodeInternalError, fallback
	if reply.Error != nil {
		code, msg = reply.Error.Code, reply.Error.Message
	}
	return coreerrors.New(code, msg)
}

func bootstrapFrom(rep registerReply) BootstrapConfig {
	interval := defaultHeartbeatInterval
	if rep.HeartbeatIntervalSecs > 0 {
		interval = time.Duration(rep.HeartbeatIntervalSecs) * time.Second
	}
	grace := defaultGraceMisses
	if rep.HeartbeatGraceMisses > 0 {
		grace = rep.HeartbeatGraceMisses
	}
	return BootstrapConfig{HeartbeatInterval: interval, GraceMisses: grace, Config: rep.Config}
}
