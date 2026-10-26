// Package mgmt implements the module side of module management: a module
// declares a protocol.ManagementManifest (properties, actions, status values)
// and core applies config, invokes actions, and reads state over management
// RPC on the module's rpc_queue.
//
// Server is transport-agnostic: Handle dispatches any control-plane request
// envelope, and ApplyConfig is also fed the effective config core returns in
// the module.register reply (see lifecycle.WithManagement). AMQPConsumer serves
// the module's RabbitMQ rpc_queue through a Router, which hands management
// requests to the Server and the operations a module type adds (a factory's
// builds) to their own handlers.
//
// Values handed to callbacks are normalized by field type: int and bytes are
// int64, number is float64, string_list is []string, string_map is
// map[string]string, everything else is the decoded JSON value.
package mgmt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	// defaultActionTimeout bounds an action whose spec sets no timeout_seconds.
	defaultActionTimeout = 30 * time.Second
	// dedupeSize is how many recent invocation results are remembered.
	dedupeSize = 100
	// redacted replaces a set secret value in state.get replies.
	redacted = "***"
)

// ApplyFunc receives the full live effective property values (every declared
// property; restart-mode properties at the value the process started with).
// Returning an error rejects the whole change: the previous values stay
// effective.
type ApplyFunc func(ctx context.Context, live map[string]any) error

// ActionFunc runs an action with validated params. ctx is bounded by the
// action's timeout. The result is returned to core as the reply's result.
type ActionFunc func(ctx context.Context, params map[string]any, actor *protocol.Actor) (any, error)

// StatusFunc returns the current status values, keyed by StatusSpec.Key.
type StatusFunc func() map[string]any

// Option configures a Server.
type Option func(*Server)

// WithSource sets the source stamped on reply envelopes, normally
// {Service: module_type, Instance: instance}. module.Run sets that itself
// (DefaultSource) when it is not given.
func WithSource(src protocol.Source) Option {
	return func(s *Server) { s.source, s.sourceSet = src, true }
}

// DefaultSource sets the reply source unless WithSource gave one. Call it
// before serving.
func (s *Server) DefaultSource(src protocol.Source) {
	if !s.sourceSet {
		s.source = src
	}
}

// Server holds a module's management state. It is safe for concurrent use.
type Server struct {
	manifest protocol.ManagementManifest
	props    map[string]protocol.PropertySpec
	actSpecs map[string]protocol.ActionSpec
	startup  map[string]any // restart-mode values the process started with
	// booted is set by the first successful ApplyConfig. Until then the
	// process is still starting, so restart-mode values from core (normally the
	// register reply's config) are adopted as the startup values.
	booted    bool
	source    protocol.Source
	sourceSet bool

	applyMu sync.Mutex // serializes ApplyConfig, including the apply callback

	mu              sync.RWMutex
	applied         int64
	effective       map[string]any
	restartRequired []string
	onApply         ApplyFunc
	actions         map[string]ActionFunc
	status          StatusFunc
	logs            LogStreamer

	dedupeMu sync.Mutex
	dedupe   map[string]*invocation
	order    []string // dedupe keys, oldest first
}

// invocation is a remembered (possibly still running) action invocation.
type invocation struct {
	done   chan struct{}
	result json.RawMessage
	err    *protocol.EnvelopeError
}

// New builds a Server from manifest after validating it. Each property's
// Default is its initial effective value and, for restart-mode properties, the
// value the process started with.
func New(manifest protocol.ManagementManifest, opts ...Option) (*Server, error) {
	if err := protocol.ValidateManifest(manifest); err != nil {
		return nil, fmt.Errorf("invalid management manifest: %w", err)
	}
	s := &Server{
		manifest:  manifest,
		props:     map[string]protocol.PropertySpec{},
		actSpecs:  map[string]protocol.ActionSpec{},
		startup:   map[string]any{},
		effective: map[string]any{},
		actions:   map[string]ActionFunc{},
		dedupe:    map[string]*invocation{},
	}
	for _, p := range manifest.Properties {
		s.props[p.Key] = p
		v := normalize(p.FieldSpec, p.Default)
		s.effective[p.Key] = v
		if p.Apply == protocol.ApplyRestart {
			s.startup[p.Key] = v
		}
	}
	for _, a := range manifest.Actions {
		s.actSpecs[a.Name] = a
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// LogStreamer serves live log streaming (logstream.Hub implements it).
type LogStreamer interface {
	Start(req protocol.LogsStartRequest) (protocol.LogsStartReply, error)
	Stop(streamID string)
}

// ServeLogs serves mgmt.logs.start / mgmt.logs.stop with l and declares
// protocol.CapabilityLogs in the manifest. Call it before the server is
// registered with core.
func (s *Server) ServeLogs(l LogStreamer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = l
	if !s.manifest.Has(protocol.CapabilityLogs) {
		s.manifest.Capabilities = append(append([]string(nil), s.manifest.Capabilities...), protocol.CapabilityLogs)
	}
}

// OnApply sets the callback that applies live property values.
func (s *Server) OnApply(fn ApplyFunc) {
	s.mu.Lock()
	s.onApply = fn
	s.mu.Unlock()
}

// Action registers the handler for a declared action. It panics on an action
// the manifest does not declare, which is a programming error.
func (s *Server) Action(name string, fn ActionFunc) {
	if _, ok := s.actSpecs[name]; !ok {
		panic(fmt.Sprintf("mgmt: action %q is not declared in the manifest", name))
	}
	s.mu.Lock()
	s.actions[name] = fn
	s.mu.Unlock()
}

// Status sets the callback supplying status values.
func (s *Server) Status(fn StatusFunc) {
	s.mu.Lock()
	s.status = fn
	s.mu.Unlock()
}

// Manifest returns the declared manifest.
func (s *Server) Manifest() protocol.ManagementManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.manifest
}

// AppliedRevision returns the last config revision processed.
func (s *Server) AppliedRevision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// StatusSnapshot returns the current status values, restricted to the keys the
// manifest declares. It returns nil without a status callback.
func (s *Server) StatusSnapshot() map[string]any {
	s.mu.RLock()
	fn := s.status
	s.mu.RUnlock()
	if fn == nil {
		return nil
	}
	all := fn()
	out := make(map[string]any, len(s.manifest.Status))
	for _, st := range s.manifest.Status {
		if v, ok := all[st.Key]; ok {
			out[st.Key] = v
		}
	}
	return out
}

// ApplyConfig applies core's effective config. A revision older than the one
// already applied is ignored. For each property the value is cfg.Values[key],
// or the property's Default when absent; an invalid value is rejected and the
// previous value kept. Changed restart-mode values are reported, not applied,
// except on the first apply after start, which adopts them as startup values
// (the module calls ApplyConfig during startup with the register reply). The
// reply's AppliedRevision is cfg.Revision even when keys were rejected.
func (s *Server) ApplyConfig(ctx context.Context, cfg protocol.ManagementConfig) protocol.ConfigApplyReply {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	s.mu.RLock()
	current, prev, fn := s.applied, cloneMap(s.effective), s.onApply
	s.mu.RUnlock()
	if cfg.Revision < current {
		return s.currentReply()
	}

	next := cloneMap(prev)
	rejected := map[string]string{}
	var restart []string
	for _, p := range s.manifest.Properties {
		v, ok := cfg.Values[p.Key]
		if !ok {
			v = p.Default
		}
		if err := protocol.ValidateValue(p.FieldSpec, v); err != nil {
			rejected[p.Key] = err.Error()
			continue
		}
		v = normalize(p.FieldSpec, v)
		if p.Apply == protocol.ApplyRestart && s.booted {
			// The running value stays the startup one until a restart.
			if !reflect.DeepEqual(v, s.startup[p.Key]) {
				restart = append(restart, p.Key)
			}
			continue
		}
		next[p.Key] = v
	}

	applyFailed := false
	if fn != nil {
		if err := fn(ctx, cloneMap(next)); err != nil {
			applyFailed = true
			for k, v := range next {
				if !reflect.DeepEqual(v, prev[k]) {
					rejected[k] = "apply failed: " + err.Error()
				}
			}
			next = prev
		}
	}

	if !s.booted && !applyFailed {
		for _, p := range s.manifest.Properties {
			if p.Apply == protocol.ApplyRestart {
				s.startup[p.Key] = next[p.Key]
			}
		}
		s.booted = true
	}

	s.mu.Lock()
	s.applied = cfg.Revision
	s.effective = next
	s.restartRequired = restart
	s.mu.Unlock()

	rep := protocol.ConfigApplyReply{AppliedRevision: cfg.Revision, RestartRequired: restart}
	if len(rejected) > 0 {
		rep.Rejected = rejected
	}
	return rep
}

func (s *Server) currentReply() protocol.ConfigApplyReply {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return protocol.ConfigApplyReply{
		AppliedRevision: s.applied,
		RestartRequired: slices.Clone(s.restartRequired),
	}
}

// State returns the mgmt.state.get reply: effective values (set secrets as
// "***", unset secrets omitted) and the current status.
func (s *Server) State() protocol.StateGetReply {
	s.mu.RLock()
	values := make(map[string]any, len(s.effective))
	for _, p := range s.manifest.Properties {
		v := s.effective[p.Key]
		if p.Type == protocol.FieldSecret {
			if v == nil || v == "" {
				continue
			}
			v = redacted
		}
		values[p.Key] = v
	}
	rep := protocol.StateGetReply{
		AppliedRevision: s.applied,
		Values:          values,
		RestartRequired: slices.Clone(s.restartRequired),
	}
	s.mu.RUnlock()
	rep.Status = s.StatusSnapshot()
	return rep
}

// --- RPC dispatch --------------------------------------------------------

// Handle dispatches one management RPC request and returns its reply. It
// rejects an unknown envelope major version with unsupported_version and an
// unknown type or malformed payload with validation_failed.
func (s *Server) Handle(ctx context.Context, req protocol.Envelope) protocol.ReplyEnvelope {
	if major, ok := protocol.MajorVersion(req.Version); !ok || major != 1 {
		return s.errorReply(req, protocol.CodeUnsupportedVersion,
			fmt.Sprintf("unsupported envelope version %q", req.Version))
	}
	switch req.Type {
	case protocol.TypeMgmtConfigApply:
		var r protocol.ConfigApplyRequest
		if err := decode(req.Payload, &r); err != nil {
			return s.errorReply(req, protocol.CodeValidationFailed, "invalid payload: "+err.Error())
		}
		return s.reply(req, s.ApplyConfig(WithActor(ctx, r.Actor), r.ManagementConfig))
	case protocol.TypeMgmtActionInvoke:
		var r protocol.ActionInvokeRequest
		if err := decode(req.Payload, &r); err != nil {
			return s.errorReply(req, protocol.CodeValidationFailed, "invalid payload: "+err.Error())
		}
		return s.invoke(ctx, req, r)
	case protocol.TypeMgmtStateGet:
		return s.reply(req, s.State())
	case protocol.TypeMgmtLogsStart, protocol.TypeMgmtLogsStop:
		return s.handleLogs(req)
	default:
		return s.errorReply(req, protocol.CodeValidationFailed, fmt.Sprintf("unknown type %q", req.Type))
	}
}

// handleLogs serves the log streaming RPCs.
func (s *Server) handleLogs(req protocol.Envelope) protocol.ReplyEnvelope {
	s.mu.RLock()
	l := s.logs
	s.mu.RUnlock()
	if l == nil {
		return s.errorReply(req, protocol.CodeNotFound, "log streaming is not supported")
	}
	if req.Type == protocol.TypeMgmtLogsStop {
		var r protocol.LogsStopRequest
		if err := decode(req.Payload, &r); err != nil {
			return s.errorReply(req, protocol.CodeValidationFailed, "invalid payload: "+err.Error())
		}
		l.Stop(r.StreamID)
		return s.reply(req, r)
	}
	var r protocol.LogsStartRequest
	if err := decode(req.Payload, &r); err != nil {
		return s.errorReply(req, protocol.CodeValidationFailed, "invalid payload: "+err.Error())
	}
	if err := r.Validate(); err != nil {
		return s.errorReply(req, protocol.CodeValidationFailed, err.Error())
	}
	rep, err := l.Start(r)
	if err != nil {
		return s.errorReply(req, protocol.CodeInternalError, err.Error())
	}
	return s.reply(req, rep)
}

// invoke validates and runs an action, deduplicating on InvocationID.
func (s *Server) invoke(ctx context.Context, req protocol.Envelope, r protocol.ActionInvokeRequest) protocol.ReplyEnvelope {
	if r.InvocationID == "" {
		return s.errorReply(req, protocol.CodeValidationFailed, "invocation_id is required")
	}
	spec, ok := s.actSpecs[r.Action]
	s.mu.RLock()
	fn := s.actions[r.Action]
	s.mu.RUnlock()
	if !ok || fn == nil {
		return s.errorReply(req, protocol.CodeNotFound, fmt.Sprintf("unknown action %q", r.Action))
	}
	params, err := validateParams(spec, r.Params)
	if err != nil {
		return s.errorReply(req, protocol.CodeValidationFailed, err.Error())
	}

	inv, first := s.claim(r.Action, r.InvocationID)
	if first {
		// The action outlives the delivery's context: a consumer closed by a
		// reconnect must not cancel it, or core's retry of the same
		// invocation would get a false failure. Its own timeout bounds it.
		inv.result, inv.err = runAction(context.WithoutCancel(ctx), spec, fn, params, r.Actor)
		close(inv.done)
	} else {
		select {
		case <-inv.done:
		case <-ctx.Done():
			return s.errorReply(req, protocol.CodeActionFailed, "invocation still running")
		}
	}
	if inv.err != nil {
		return s.errorReply(req, inv.err.Code, inv.err.Message)
	}
	return s.reply(req, protocol.ActionInvokeReply{InvocationID: r.InvocationID, Result: inv.result})
}

// claim returns the remembered invocation of action with id, or registers a
// new one and reports that the caller must run it. Past dedupeSize the oldest
// finished invocation is forgotten; running ones are kept, so a retry never
// runs an action twice.
func (s *Server) claim(action, id string) (*invocation, bool) {
	key := action + "\x00" + id
	s.dedupeMu.Lock()
	defer s.dedupeMu.Unlock()
	if inv, ok := s.dedupe[key]; ok {
		return inv, false
	}
	inv := &invocation{done: make(chan struct{})}
	s.dedupe[key] = inv
	s.order = append(s.order, key)
	if len(s.order) > dedupeSize {
		for i, k := range s.order {
			select {
			case <-s.dedupe[k].done:
				delete(s.dedupe, k)
				s.order = append(s.order[:i:i], s.order[i+1:]...)
				return inv, true
			default:
			}
		}
	}
	return inv, true
}

// runAction runs fn bounded by the action timeout. A handler that ignores its
// context is abandoned at the deadline.
func runAction(ctx context.Context, spec protocol.ActionSpec, fn ActionFunc, params map[string]any, actor *protocol.Actor) (json.RawMessage, *protocol.EnvelopeError) {
	timeout := defaultActionTimeout
	if spec.TimeoutSeconds > 0 {
		timeout = time.Duration(spec.TimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(WithActor(ctx, actor), timeout)
	defer cancel()

	type outcome struct {
		result any
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- outcome{err: fmt.Errorf("panic: %v", p)}
			}
		}()
		res, err := fn(ctx, params, actor)
		done <- outcome{res, err}
	}()

	var o outcome
	select {
	case o = <-done:
	case <-ctx.Done():
		o.err = fmt.Errorf("action %q timed out after %s", spec.Name, timeout)
	}
	if o.err != nil {
		return nil, &protocol.EnvelopeError{Code: protocol.CodeActionFailed, Message: o.err.Error()}
	}
	if o.result == nil {
		return nil, nil
	}
	raw, err := json.Marshal(o.result)
	if err != nil {
		return nil, &protocol.EnvelopeError{Code: protocol.CodeActionFailed, Message: "encode result: " + err.Error()}
	}
	return raw, nil
}

// validateParams checks params against the action's declared params and
// returns them normalized. Undeclared params are rejected.
func validateParams(spec protocol.ActionSpec, in map[string]any) (map[string]any, error) {
	declared := map[string]bool{}
	out := make(map[string]any, len(spec.Params))
	for _, p := range spec.Params {
		declared[p.Key] = true
		v := in[p.Key]
		if err := protocol.ValidateValue(p, v); err != nil {
			return nil, fmt.Errorf("param %q: %w", p.Key, err)
		}
		if v != nil {
			out[p.Key] = normalize(p, v)
		}
	}
	for k := range in {
		if !declared[k] {
			return nil, fmt.Errorf("unknown param %q", k)
		}
	}
	return out, nil
}

func (s *Server) reply(req protocol.Envelope, payload any) protocol.ReplyEnvelope {
	rep, err := protocol.NewReply(req, s.source, payload)
	if err != nil {
		return s.errorReply(req, protocol.CodeInternalError, "encode reply: "+err.Error())
	}
	return rep
}

func (s *Server) errorReply(req protocol.Envelope, code, msg string) protocol.ReplyEnvelope {
	return protocol.NewErrorReply(req, s.source, code, msg)
}

// --- actor context -------------------------------------------------------

type actorKey struct{}

// WithActor returns ctx carrying actor.
func WithActor(ctx context.Context, actor *protocol.Actor) context.Context {
	if actor == nil {
		return ctx
	}
	return context.WithValue(ctx, actorKey{}, actor)
}

// ActorFromContext returns the actor that triggered the operation, if any. It
// is set on the ctx handed to ApplyFunc and ActionFunc for audit logging.
func ActorFromContext(ctx context.Context) *protocol.Actor {
	a, _ := ctx.Value(actorKey{}).(*protocol.Actor)
	return a
}

// --- helpers -------------------------------------------------------------

// decode unmarshals a payload keeping numbers exact (json.Number) so large
// integers survive validation and normalization.
func decode(raw json.RawMessage, out any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(out)
}

// normalize converts a value already valid for f to its canonical Go type.
func normalize(f protocol.FieldSpec, v any) any {
	if v == nil {
		return nil
	}
	switch f.Type {
	case protocol.FieldInt, protocol.FieldBytes:
		if n, ok := protocol.AsInt64(v); ok {
			return n
		}
	case protocol.FieldNumber:
		switch n := v.(type) {
		case json.Number:
			if x, err := n.Float64(); err == nil {
				return x
			}
		case float32:
			return float64(n)
		case int:
			return float64(n)
		case int32:
			return float64(n)
		case int64:
			return float64(n)
		}
	case protocol.FieldStringList:
		if items, ok := v.([]any); ok {
			out := make([]string, 0, len(items))
			for _, it := range items {
				s, _ := it.(string)
				out = append(out, s)
			}
			return out
		}
	case protocol.FieldStringMap:
		if entries, ok := v.(map[string]any); ok {
			out := make(map[string]string, len(entries))
			for k, it := range entries {
				s, _ := it.(string)
				out[k] = s
			}
			return out
		}
	}
	return v
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
