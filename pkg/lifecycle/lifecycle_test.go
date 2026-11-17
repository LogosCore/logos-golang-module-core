package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

func testIdentity() Identity {
	return Identity{
		ModuleType:  "channel",
		ModuleName:  "http",
		Instance:    "http-1",
		Version:     "1.2.0",
		RPCQueue:    "logos.channel.rpc.http-1",
		Description: "HTTP Logos channel",
	}
}

// fakePublisher records sent envelopes and replies via a scripted function.
// Requests silent picks get no reply: Call waits out their context, as
// AMQPPublisher does for a request core never answers.
type fakePublisher struct {
	mu      sync.Mutex
	sent    []protocol.Envelope
	replyFn func(req protocol.Envelope) (protocol.ReplyEnvelope, error)
	silent  func(req protocol.Envelope) bool
}

func (f *fakePublisher) Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	f.mu.Lock()
	f.sent = append(f.sent, req)
	fn, silent := f.replyFn, f.silent
	f.mu.Unlock()
	if silent != nil && silent(req) {
		<-ctx.Done()
		return protocol.ReplyEnvelope{}, fmt.Errorf("no reply from core: %w", ctx.Err())
	}
	return fn(req)
}

func (f *fakePublisher) calls() []protocol.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]protocol.Envelope, len(f.sent))
	copy(out, f.sent)
	return out
}

func (f *fakePublisher) hasType(t string) bool {
	for _, e := range f.calls() {
		if e.Type == t {
			return true
		}
	}
	return false
}

func okReply(t *testing.T, req protocol.Envelope, payload any) protocol.ReplyEnvelope {
	t.Helper()
	r, err := protocol.NewReply(req, protocol.Source{Service: "core", Instance: "core-1"}, payload)
	if err != nil {
		t.Fatalf("build reply: %v", err)
	}
	return r
}

func TestRegisterSuccessBuildsEnvelopeAndAppliesDefaults(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		// interval omitted -> client applies default
		return okReply(t, req, registerReply{Instance: "http-1", Registered: true}), nil
	}}
	c := NewClient(fp, testIdentity())

	boot, err := c.Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if boot.HeartbeatInterval != defaultHeartbeatInterval {
		t.Errorf("interval = %v, want default %v", boot.HeartbeatInterval, defaultHeartbeatInterval)
	}
	if boot.GraceMisses != defaultGraceMisses {
		t.Errorf("grace = %d, want %d", boot.GraceMisses, defaultGraceMisses)
	}

	sent := fp.calls()
	if len(sent) != 1 {
		t.Fatalf("sent %d envelopes, want 1", len(sent))
	}
	env := sent[0]
	if env.Type != TypeRegister {
		t.Errorf("type = %q, want %q", env.Type, TypeRegister)
	}
	if env.Version != protocol.EnvelopeVersionV1 {
		t.Errorf("version = %q, want %q", env.Version, protocol.EnvelopeVersionV1)
	}
	if env.MessageID == "" || env.CorrelationID != env.MessageID {
		t.Errorf("correlation_id %q must equal message_id %q", env.CorrelationID, env.MessageID)
	}
	if env.Source.Service != "channel" || env.Source.Instance != "http-1" {
		t.Errorf("source = %+v, want {channel http-1}", env.Source)
	}
	var rr registerRequest
	if err := json.Unmarshal(env.Payload, &rr); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if rr.ModuleName != "http" || rr.RPCQueue != "logos.channel.rpc.http-1" {
		t.Errorf("register payload = %+v", rr)
	}
}

func TestRegisterHonorsCoreProvidedInterval(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, registerReply{
			Instance: "http-1", Registered: true,
			HeartbeatIntervalSecs: 15, HeartbeatGraceMisses: 5,
		}), nil
	}}
	boot, err := NewClient(fp, testIdentity()).Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if boot.HeartbeatInterval != 15*time.Second {
		t.Errorf("interval = %v, want 15s", boot.HeartbeatInterval)
	}
	if boot.GraceMisses != 5 {
		t.Errorf("grace = %d, want 5", boot.GraceMisses)
	}
}

func TestRegisterValidationFailsBeforePublish(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		t.Fatal("publisher should not be called when identity is invalid")
		return protocol.ReplyEnvelope{}, nil
	}}
	id := testIdentity()
	id.Instance = ""
	if _, err := NewClient(fp, id).Register(context.Background()); err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if len(fp.calls()) != 0 {
		t.Fatalf("publisher called %d times, want 0", len(fp.calls()))
	}
}

func TestErrorReplyMapsToTypedCode(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return protocol.NewErrorReply(req, protocol.Source{Service: "core", Instance: "core-1"},
			protocol.CodeValidationFailed, "missing rpc_queue"), nil
	}}
	_, err := NewClient(fp, testIdentity()).Register(context.Background())
	if err == nil {
		t.Fatal("expected error reply to surface as error")
	}
	if got := coreerrors.Code(err); got != protocol.CodeValidationFailed {
		t.Errorf("code = %q, want %q", got, protocol.CodeValidationFailed)
	}
}

func TestHeartbeatSendsStatusAndMetrics(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, heartbeatReply{Instance: "http-1", Ack: true, ConfigChanged: true}), nil
	}}
	c := NewClient(fp, testIdentity(),
		WithStatus(func() Status { return StatusDegraded }),
		WithMetrics(func() map[string]any { return map[string]any{"profiles_enabled": 4} }),
	)

	res, err := c.Heartbeat(context.Background())
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !res.ConfigChanged {
		t.Error("ConfigChanged = false, want true")
	}
	var hr heartbeatRequest
	if err := json.Unmarshal(fp.calls()[0].Payload, &hr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hr.Status != StatusDegraded {
		t.Errorf("status = %q, want degraded", hr.Status)
	}
	if hr.Metrics["profiles_enabled"] != float64(4) {
		t.Errorf("metrics = %+v, want profiles_enabled=4", hr.Metrics)
	}
}

func TestDeregisterSendsReason(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, map[string]any{"instance": "http-1", "deregistered": true}), nil
	}}
	if err := NewClient(fp, testIdentity()).Deregister(context.Background(), "redeploy"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	var dr deregisterRequest
	if err := json.Unmarshal(fp.calls()[0].Payload, &dr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dr.Reason != "redeploy" || dr.Instance != "http-1" {
		t.Errorf("deregister payload = %+v", dr)
	}
}

func TestRunRegistersHeartbeatsAndDeregisters(t *testing.T) {
	hbSeen := make(chan struct{}, 1)
	fp := &fakePublisher{}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true, HeartbeatIntervalSecs: 1}), nil
		case TypeHeartbeat:
			select {
			case hbSeen <- struct{}{}:
			default:
			}
			return okReply(t, req, heartbeatReply{Instance: "http-1", Ack: true}), nil
		case TypeDeregister:
			return okReply(t, req, map[string]any{"deregistered": true}), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected type %q", req.Type)
	}

	c := NewClient(fp, testIdentity())
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	tickCh <- time.Time{} // drive exactly one heartbeat
	select {
	case <-hbSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat not observed")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	for _, typ := range []string{TypeRegister, TypeHeartbeat, TypeDeregister} {
		if !fp.hasType(typ) {
			t.Errorf("missing lifecycle op %q; sent=%v", typ, fp.calls())
		}
	}
}

func TestRunReregistersOnUnknownInstance(t *testing.T) {
	var mu sync.Mutex
	registers := 0
	reregistered := make(chan struct{}, 1)
	fp := &fakePublisher{}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			mu.Lock()
			registers++
			n := registers
			mu.Unlock()
			if n >= 2 {
				select {
				case reregistered <- struct{}{}:
				default:
				}
			}
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true, HeartbeatIntervalSecs: 1}), nil
		case TypeHeartbeat:
			// Core forgot us -> force a re-register.
			return protocol.NewErrorReply(req, protocol.Source{Service: "core", Instance: "core-1"},
				protocol.CodeUnknownInstance, "no active registration"), nil
		case TypeDeregister:
			return okReply(t, req, map[string]any{"deregistered": true}), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected %q", req.Type)
	}

	c := NewClient(fp, testIdentity())
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	tickCh <- time.Time{} // heartbeat -> unknown_instance -> re-register
	select {
	case <-reregistered:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not re-register after unknown_instance")
	}
	cancel()
	<-done
}

// fakeAMQPChannel captures published messages for AMQPPublisher transport
// tests; onPublish, when set, plays core's side (a reply, a return) once a
// request is out, as on the wire nothing can come back before it.
type fakeAMQPChannel struct {
	mu        sync.Mutex
	published []amqp.Publishing
	mandatory []bool
	pubErr    error
	onPublish func(amqp.Publishing)
}

func (f *fakeAMQPChannel) PublishWithContext(_ context.Context, _, _ string, mandatory, _ bool, msg amqp.Publishing) error {
	f.mu.Lock()
	if f.pubErr != nil {
		f.mu.Unlock()
		return f.pubErr
	}
	f.published = append(f.published, msg)
	f.mandatory = append(f.mandatory, mandatory)
	on := f.onPublish
	f.mu.Unlock()
	if on != nil {
		on(msg)
	}
	return nil
}

func (f *fakeAMQPChannel) Close() error { return nil }

func (f *fakeAMQPChannel) last() (amqp.Publishing, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.published[len(f.published)-1], f.mandatory[len(f.mandatory)-1]
}

func (f *fakeAMQPChannel) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func replyBody(t *testing.T, req protocol.Envelope) []byte {
	t.Helper()
	rep, err := protocol.NewReply(req, protocol.Source{Service: "core", Instance: "core-1"},
		registerReply{Instance: "http-1", Registered: true})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(rep)
	return b
}

func testRequest(id string) protocol.Envelope {
	return protocol.Envelope{
		MessageID:     id,
		CorrelationID: id,
		Type:          TypeRegister,
		Version:       protocol.EnvelopeVersionV1,
		Source:        protocol.Source{Service: "channel", Instance: "http-1"},
		Payload:       json.RawMessage(`{}`),
	}
}

func TestAMQPPublisherMatchesCorrelationAndSkipsStale(t *testing.T) {
	replies := make(chan amqp.Delivery, 2)
	req := testRequest("req-1")
	fch := &fakeAMQPChannel{onPublish: func(amqp.Publishing) {
		// A stale reply for a different correlation id, then the real one.
		replies <- amqp.Delivery{CorrelationId: "other", Body: []byte(`{"status":"ok"}`)}
		replies <- amqp.Delivery{CorrelationId: "req-1", Body: replyBody(t, req)}
	}}
	p := newPublisher(fch, replies, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := p.Call(ctx, req)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got.CorrelationID != "req-1" || got.Status != protocol.StatusOK {
		t.Errorf("reply = %+v, want correlation req-1 status ok", got)
	}

	pub, mandatory := fch.last()
	if pub.ReplyTo != directReplyTo {
		t.Errorf("reply_to = %q, want %q", pub.ReplyTo, directReplyTo)
	}
	if pub.CorrelationId != "req-1" || pub.Type != TypeRegister {
		t.Errorf("publishing props = {corr:%q type:%q}", pub.CorrelationId, pub.Type)
	}
	// Mandatory, so a request no queue takes comes back; and it expires
	// with the call, so core never acts on one its caller gave up on.
	if !mandatory {
		t.Error("request not published as mandatory")
	}
	if ms, err := strconv.Atoi(pub.Expiration); err != nil || ms < 1 || ms > 5000 {
		t.Errorf("expiration = %q, want the call's 5s deadline in ms", pub.Expiration)
	}
}

func TestAMQPPublisherHonorsContextCancel(t *testing.T) {
	p := newPublisher(&fakeAMQPChannel{}, make(chan amqp.Delivery), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Call(ctx, protocol.Envelope{CorrelationID: "x"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Call = %v, want context.Canceled", err)
	}
}

// A request core never answers (core down, its queue dropping nothing back)
// ends at the call's deadline, which also bounds the request's life in a
// queue.
func TestAMQPPublisherTimesOutWithoutAReply(t *testing.T) {
	fch := &fakeAMQPChannel{}
	p := newPublisher(fch, make(chan amqp.Delivery), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.Call(ctx, testRequest("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Call returned after %s", d)
	}
	if pub, _ := fch.last(); pub.Expiration == "" || pub.Expiration == "0" {
		t.Errorf("expiration = %q, want the 50ms deadline", pub.Expiration)
	}
}

func TestAMQPPublisherPublishError(t *testing.T) {
	p := newPublisher(&fakeAMQPChannel{pubErr: fmt.Errorf("broker down")}, make(chan amqp.Delivery), nil)
	if _, err := p.Call(context.Background(), protocol.Envelope{CorrelationID: "x"}); err == nil {
		t.Fatal("expected publish error, got nil")
	}
}

// A broker that came back without its state has no queue for core's RPC until
// core declares it again: the request comes back at once instead of being
// dropped while the call waits. Not a refusal: no coreerrors.Error, so
// callers retry it as a lost call.
func TestAMQPPublisherNoRoute(t *testing.T) {
	returns := make(chan amqp.Return, 1)
	fch := &fakeAMQPChannel{onPublish: func(m amqp.Publishing) {
		returns <- amqp.Return{CorrelationId: m.CorrelationId, ReplyCode: 312, ReplyText: "NO_ROUTE"}
	}}
	p := newPublisher(fch, make(chan amqp.Delivery), returns)
	done := make(chan error, 1)
	go func() {
		_, err := p.Call(context.Background(), testRequest("x"))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoRoute) {
			t.Fatalf("Call = %v, want ErrNoRoute", err)
		}
		var refusal *coreerrors.Error
		if errors.As(err, &refusal) {
			t.Fatalf("ErrNoRoute reads as core's refusal: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a returned request left its call waiting")
	}
}

// Calls share the publisher (the lifecycle loop, a factory's CoreClient): one
// core does not answer must not hold up another, and replies reach their
// calls in whatever order they come.
func TestAMQPPublisherCallsDoNotWaitForEachOther(t *testing.T) {
	replies := make(chan amqp.Delivery, 4)
	silentOut := make(chan struct{})
	fch := &fakeAMQPChannel{}
	fch.onPublish = func(m amqp.Publishing) {
		switch m.CorrelationId {
		case "silent":
			close(silentOut)
		case "answered":
			replies <- amqp.Delivery{CorrelationId: "answered", Body: replyBody(t, testRequest("answered"))}
		}
	}
	p := newPublisher(fch, replies, nil)

	sctx, scancel := context.WithCancel(context.Background())
	silentDone := make(chan error, 1)
	go func() {
		_, err := p.Call(sctx, testRequest("silent"))
		silentDone <- err
	}()
	<-silentOut

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := p.Call(ctx, testRequest("answered"))
	if err != nil {
		t.Fatalf("a call behind one core does not answer: %v", err)
	}
	if got.CorrelationID != "answered" {
		t.Errorf("reply = %+v, want the answered call's", got)
	}
	select {
	case err := <-silentDone:
		t.Fatalf("the silent call ended early: %v", err)
	default:
	}
	scancel()
	if err := <-silentDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("silent call = %v, want context.Canceled", err)
	}
}

// The channel closing (the session's end, or a channel error on a connection
// still up) fails the calls waiting and every later one, and closes Done,
// which module.Run watches to start a new session.
func TestAMQPPublisherClosedChannelFailsCalls(t *testing.T) {
	replies := make(chan amqp.Delivery)
	out := make(chan struct{})
	fch := &fakeAMQPChannel{onPublish: func(amqp.Publishing) { close(out) }}
	p := newPublisher(fch, replies, nil)
	done := make(chan error, 1)
	go func() {
		_, err := p.Call(context.Background(), testRequest("x"))
		done <- err
	}()
	<-out
	close(replies)
	select {
	case err := <-done:
		if !errors.Is(err, ErrPublisherClosed) {
			t.Fatalf("waiting call = %v, want ErrPublisherClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a closed channel left its call waiting")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done not closed")
	}
	if !errors.Is(p.Err(), ErrPublisherClosed) {
		t.Errorf("Err = %v", p.Err())
	}
	if _, err := p.Call(context.Background(), testRequest("y")); !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("call after close = %v, want ErrPublisherClosed", err)
	}
	if n := fch.count(); n != 1 {
		t.Errorf("published %d requests, want 1: none after the close", n)
	}
}

func TestAMQPPublisherCloseNilSafe(t *testing.T) {
	if err := (&AMQPPublisher{}).Close(); err != nil {
		t.Fatalf("Close on zero publisher: %v", err)
	}
	if err := newPublisher(&fakeAMQPChannel{}, make(chan amqp.Delivery), nil).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIdentityValidate(t *testing.T) {
	base := testIdentity()
	cases := map[string]func(*Identity){
		"missing module_type": func(id *Identity) { id.ModuleType = "" },
		"missing module_name": func(id *Identity) { id.ModuleName = "" },
		"missing instance":    func(id *Identity) { id.Instance = "" },
		"missing rpc_queue":   func(id *Identity) { id.RPCQueue = "" },
		"invalid icon":        func(id *Identity) { id.Icon = `<svg><script>alert(1)</script></svg>` },
		"invalid icon_light": func(id *Identity) {
			id.Icon, id.IconLight = testIcon, `<svg><script>alert(1)</script></svg>`
		},
		"icon_light without icon": func(id *Identity) { id.IconLight = testIcon },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			id := base
			mutate(&id)
			if err := id.validate(); err == nil {
				t.Errorf("%s: expected validation error", name)
			}
		})
	}
	if err := base.validate(); err != nil {
		t.Errorf("valid identity rejected: %v", err)
	}
}

func TestWithDeregisterTimeout(t *testing.T) {
	c := NewClient(&fakePublisher{}, testIdentity(), WithDeregisterTimeout(2*time.Second))
	if c.deregisterTimeout != 2*time.Second {
		t.Errorf("deregisterTimeout = %v, want 2s", c.deregisterTimeout)
	}
	// Non-positive is ignored.
	c2 := NewClient(&fakePublisher{}, testIdentity(), WithDeregisterTimeout(0))
	if c2.deregisterTimeout != defaultDeregisterTimeout {
		t.Errorf("deregisterTimeout = %v, want default", c2.deregisterTimeout)
	}
}

func testMgmtServer(t *testing.T) *mgmt.Server {
	t.Helper()
	s, err := mgmt.New(protocol.ManagementManifest{
		SchemaVersion: protocol.ManagementSchemaV1,
		Properties: []protocol.PropertySpec{{
			FieldSpec: protocol.FieldSpec{Key: "log_level", Type: protocol.FieldString, Default: "info"},
			Apply:     protocol.ApplyLive,
		}},
		Status: []protocol.StatusSpec{{Key: "requests_total", Type: protocol.FieldInt}},
	})
	if err != nil {
		t.Fatalf("mgmt.New: %v", err)
	}
	return s
}

// mgmtRegisterReply is a register ack carrying config.management.
func mgmtRegisterReply(t *testing.T, req protocol.Envelope, cfg protocol.ManagementConfig, interval int) protocol.ReplyEnvelope {
	t.Helper()
	raw, _ := json.Marshal(cfg)
	return okReply(t, req, registerReply{
		Instance: "http-1", Registered: true, HeartbeatIntervalSecs: interval,
		Config: map[string]json.RawMessage{"management": raw},
	})
}

func TestRegisterWithoutManagementOmitsFields(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		if req.Type == TypeRegister {
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true}), nil
		}
		return okReply(t, req, heartbeatReply{Ack: true}), nil
	}}
	c := NewClient(fp, testIdentity())
	boot, err := c.Register(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if boot.Management != nil {
		t.Errorf("Management = %+v, want nil", boot.Management)
	}
	if _, err := c.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, env := range fp.calls() {
		var raw map[string]any
		_ = json.Unmarshal(env.Payload, &raw)
		for _, k := range []string{"management", "applied_revision"} {
			if _, ok := raw[k]; ok {
				t.Errorf("%s payload has %q without management", env.Type, k)
			}
		}
	}
}

func TestRegisterSendsManifestAndAppliesConfig(t *testing.T) {
	s := testMgmtServer(t)
	var live map[string]any
	s.OnApply(func(_ context.Context, v map[string]any) error { live = v; return nil })
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return mgmtRegisterReply(t, req, protocol.ManagementConfig{Revision: 4, Values: map[string]any{"log_level": "debug"}}, 0), nil
	}}

	boot, err := NewClient(fp, testIdentity(), WithManagement(s)).Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if boot.Management == nil || boot.Management.AppliedRevision != 4 {
		t.Errorf("Management = %+v, want applied revision 4", boot.Management)
	}
	if live["log_level"] != "debug" || s.AppliedRevision() != 4 {
		t.Errorf("live = %+v revision = %d", live, s.AppliedRevision())
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(fp.calls()[0].Payload, &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["applied_revision"]) != "0" {
		t.Errorf("applied_revision = %s, want 0 before first apply", raw["applied_revision"])
	}
	var m protocol.ManagementManifest
	if err := json.Unmarshal(raw["management"], &m); err != nil || len(m.Properties) != 1 || m.Properties[0].Key != "log_level" {
		t.Errorf("management = %s (%v)", raw["management"], err)
	}
}

func TestRegisterSendsExtensionFields(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, registerReply{Instance: "http-1", Registered: true}), nil
	}}
	c := NewClient(fp, testIdentity(),
		WithRegisterField("connection", map[string]any{"version": 1}),
		WithRegisterField("targets", []string{"linux"}),
		WithRegisterField("connection", map[string]any{"version": 2}))
	if _, err := c.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(fp.calls()[0].Payload, &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["connection"]) != `{"version":2}` || string(raw["targets"]) != `["linux"]` {
		t.Errorf("extensions = %s, %s", raw["connection"], raw["targets"])
	}
	if string(raw["module_name"]) != `"http"` || string(raw["rpc_queue"]) == "" {
		t.Errorf("base fields lost: %s", fp.calls()[0].Payload)
	}
}

func TestRegisterRefusesReservedExtensionField(t *testing.T) {
	fp := &fakePublisher{}
	_, err := NewClient(fp, testIdentity(), WithRegisterField("instance", "other")).Register(context.Background())
	if coreerrors.Code(err) != coreerrors.CodeInvalidInput {
		t.Fatalf("err = %v, want invalid input", err)
	}
	if len(fp.calls()) != 0 {
		t.Error("register sent")
	}
}

func TestRegisterBadManagementConfigFails(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, registerReply{Registered: true,
			Config: map[string]json.RawMessage{"management": json.RawMessage(`"nope"`)}}), nil
	}}
	if _, err := NewClient(fp, testIdentity(), WithManagement(testMgmtServer(t))).Register(context.Background()); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestHeartbeatSendsRevisionAndMergesStatus(t *testing.T) {
	s := testMgmtServer(t)
	s.Status(func() map[string]any { return map[string]any{"requests_total": 9} })
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 2})
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, heartbeatReply{Ack: true}), nil
	}}
	c := NewClient(fp, testIdentity(), WithManagement(s),
		WithMetrics(func() map[string]any { return map[string]any{"requests_total": 1, "profiles": 2} }))
	if _, err := c.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	var hr heartbeatRequest
	if err := json.Unmarshal(fp.calls()[0].Payload, &hr); err != nil {
		t.Fatal(err)
	}
	if hr.AppliedRevision == nil || *hr.AppliedRevision != 2 {
		t.Errorf("applied_revision = %v, want 2", hr.AppliedRevision)
	}
	if hr.Metrics["requests_total"] != float64(9) || hr.Metrics["profiles"] != float64(2) {
		t.Errorf("metrics = %+v, want status to win and WithMetrics kept", hr.Metrics)
	}
}

func TestRunReappliesManagementOnConfigChanged(t *testing.T) {
	s := testMgmtServer(t)
	applied := make(chan string, 4)
	s.OnApply(func(_ context.Context, v map[string]any) error { applied <- v["log_level"].(string); return nil })

	var mu sync.Mutex
	registers := 0
	fp := &fakePublisher{}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			mu.Lock()
			registers++
			n := registers
			mu.Unlock()
			level, interval := "info", 1
			if n > 1 {
				level, interval = "debug", 5
			}
			return mgmtRegisterReply(t, req, protocol.ManagementConfig{Revision: int64(n), Values: map[string]any{"log_level": level}}, interval), nil
		case TypeHeartbeat:
			return okReply(t, req, heartbeatReply{Ack: true, ConfigChanged: true}), nil
		}
		return okReply(t, req, map[string]any{}), nil
	}

	c := NewClient(fp, testIdentity(), WithManagement(s))
	intervals := make(chan time.Duration, 4)
	tickCh := make(chan time.Time)
	c.newTicker = func(d time.Duration) (<-chan time.Time, func()) { intervals <- d; return tickCh, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	if got := <-applied; got != "info" {
		t.Fatalf("applied %q, want info", got)
	}
	tickCh <- time.Time{} // heartbeat -> config_changed -> re-register
	select {
	case got := <-applied:
		if got != "debug" {
			t.Fatalf("re-applied %q, want debug", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("config not re-applied after config_changed")
	}
	if <-intervals != time.Second {
		t.Error("first ticker interval != 1s")
	}
	select {
	case d := <-intervals:
		if d != 5*time.Second {
			t.Errorf("new interval = %v, want 5s", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ticker not reset to the new interval")
	}
	cancel()
	<-done
}

func TestRunHeartbeatsAppliedRevisionRightAfterRegister(t *testing.T) {
	s := testMgmtServer(t)
	fp := &fakePublisher{}
	beat := make(chan heartbeatRequest, 1)
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			return mgmtRegisterReply(t, req, protocol.ManagementConfig{Revision: 7, Values: map[string]any{}}, 30), nil
		case TypeHeartbeat:
			var hr heartbeatRequest
			_ = json.Unmarshal(req.Payload, &hr)
			select {
			case beat <- hr:
			default:
			}
			return okReply(t, req, heartbeatReply{Ack: true}), nil
		}
		return okReply(t, req, map[string]any{}), nil
	}

	c := NewClient(fp, testIdentity(), WithManagement(s))
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return make(chan time.Time), func() {} } // never ticks

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case hr := <-beat:
		if hr.AppliedRevision == nil || *hr.AppliedRevision != 7 {
			t.Fatalf("heartbeat applied_revision = %v, want 7", hr.AppliedRevision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat right after register")
	}
	cancel()
	<-done
}

const testIcon = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/></svg>`

const testIconLight = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9" fill="none" stroke="black"/></svg>`

func TestRegisterSendsTheIcon(t *testing.T) {
	for _, icons := range [][2]string{{testIcon, testIconLight}, {testIcon, ""}, {"", ""}} {
		fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true}), nil
		}}
		id := testIdentity()
		id.Icon, id.IconLight = icons[0], icons[1]
		if _, err := NewClient(fp, id).Register(context.Background()); err != nil {
			t.Fatal(err)
		}
		var raw map[string]any
		if err := json.Unmarshal(fp.calls()[0].Payload, &raw); err != nil {
			t.Fatal(err)
		}
		for i, key := range []string{"icon", "icon_light"} {
			got, sent := raw[key]
			if icons[i] == "" && sent {
				t.Errorf("an empty %s was sent: %v", key, got)
			}
			if icons[i] != "" && got != icons[i] {
				t.Errorf("%s = %v, want the identity's", key, got)
			}
		}
	}

	// A type SDK cannot set them as extensions.
	for _, key := range []string{"icon", "icon_light"} {
		fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true}), nil
		}}
		if _, err := NewClient(fp, testIdentity(), WithRegisterField(key, testIcon)).Register(context.Background()); err == nil {
			t.Errorf("%s taken as an extension field", key)
		}
	}
}

func TestRunLogsABlock(t *testing.T) {
	var mu sync.Mutex
	registers := 0
	refused := make(chan struct{}, 4)
	fp := &fakePublisher{}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		src := protocol.Source{Service: "core", Instance: "core-1"}
		switch req.Type {
		case TypeRegister:
			mu.Lock()
			registers++
			n := registers
			mu.Unlock()
			if n >= 2 { // blocked while running
				refused <- struct{}{}
				return protocol.NewErrorReply(req, src, protocol.CodeInstanceBlocked, "an operator blocked this instance"), nil
			}
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true, HeartbeatIntervalSecs: 1}), nil
		case TypeHeartbeat:
			return protocol.NewErrorReply(req, src, protocol.CodeUnknownInstance, "no active registration"), nil
		case TypeDeregister:
			return protocol.NewErrorReply(req, src, protocol.CodeUnknownInstance, "no active registration"), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected %q", req.Type)
	}

	var logs syncBuffer
	c := NewClient(fp, testIdentity(), WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	for range 2 { // still blocked at the next heartbeat: asked again
		tickCh <- time.Time{}
		select {
		case <-refused:
		case <-time.After(2 * time.Second):
			t.Fatal("client did not ask to register again")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := strings.Count(logs.String(), "core has blocked this instance"); n != 2 {
		t.Fatalf("logged the block %d times:\n%s", n, logs.String())
	}
}

// syncBuffer is a bytes.Buffer safe for a logger and a test reading it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A register core never answers (core down, or its queue missing from a
// broker that came back without it, which drops the request) fails at the call
// timeout: Run returns, and module.Run starts a new session. Before, Run
// waited for that reply forever and the instance never registered again.
func TestRunFailsARegisterCoreNeverAnswers(t *testing.T) {
	fp := &fakePublisher{silent: func(protocol.Envelope) bool { return true }}
	c := NewClient(fp, testIdentity(), WithCallTimeout(50*time.Millisecond))
	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run = %v, want the register's deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run still waiting for a register core never answers")
	}
}

// liveClient registers c with a fake core that answers registers (grace
// misses 2) and deregisters, runs it with a hand-driven ticker, and returns
// the ticker, a cancel and Run's result.
func liveClient(t *testing.T, fp *fakePublisher, heartbeat func(protocol.Envelope) (protocol.ReplyEnvelope, error),
	opts ...Option) (chan time.Time, context.CancelFunc, chan error) {
	t.Helper()
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true,
				HeartbeatIntervalSecs: 1, HeartbeatGraceMisses: 2}), nil
		case TypeHeartbeat:
			return heartbeat(req)
		case TypeDeregister:
			return okReply(t, req, map[string]any{"deregistered": true}), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected %q", req.Type)
	}
	c := NewClient(fp, testIdentity(), opts...)
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return tickCh, cancel, done
}

// As many heartbeats in a row with no answer as core's grace misses end Run
// with an error (no deregister: the session is broken, not shutting down), so
// the next session registers again once core is back.
func TestRunEndsAfterGraceMissedHeartbeats(t *testing.T) {
	fp := &fakePublisher{silent: func(req protocol.Envelope) bool { return req.Type == TypeHeartbeat }}
	tick, _, done := liveClient(t, fp, nil, WithCallTimeout(20*time.Millisecond))

	tick <- time.Time{} // first miss: still running
	select {
	case err := <-done:
		t.Fatalf("Run ended after one missed heartbeat: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	tick <- time.Time{} // second, the grace: the session ends
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "2 heartbeats in a row") || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run = %v, want no answer to 2 heartbeats", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run kept a session whose heartbeats core never answers")
	}
	if fp.hasType(TypeDeregister) {
		t.Error("deregistered on a broken session")
	}
}

// Only heartbeats with no answer count, and only in a row: one answered, even
// with an error reply (core is there), starts the count again.
func TestRunCountsOnlyUnansweredHeartbeatsInARow(t *testing.T) {
	var mu sync.Mutex
	script := []string{"silent", "ok", "silent", "refused", "silent", "ok"}
	next := func() string {
		mu.Lock()
		defer mu.Unlock()
		s := script[0]
		script = script[1:]
		return s
	}
	var cur string
	fp := &fakePublisher{}
	fp.silent = func(req protocol.Envelope) bool {
		if req.Type != TypeHeartbeat {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		return cur == "silent"
	}
	answered := make(chan struct{}, len(script))
	tick, cancel, done := liveClient(t, fp, func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		defer func() { answered <- struct{}{} }()
		mu.Lock()
		kind := cur
		mu.Unlock()
		if kind == "refused" {
			return protocol.NewErrorReply(req, protocol.Source{Service: "core", Instance: "core-1"},
				protocol.CodeInternalError, "store down"), nil
		}
		return okReply(t, req, heartbeatReply{Instance: "http-1", Ack: true}), nil
	}, WithCallTimeout(20*time.Millisecond))

	for range len(script) {
		kind := next()
		mu.Lock()
		cur = kind
		mu.Unlock()
		tick <- time.Time{}
		if kind == "silent" {
			time.Sleep(60 * time.Millisecond) // past the call timeout
		} else {
			<-answered
		}
		select {
		case err := <-done:
			t.Fatalf("Run ended after %q: %v", kind, err)
		default:
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil on shutdown", err)
	}
	if !fp.hasType(TypeDeregister) {
		t.Error("no deregister on shutdown")
	}
}

// Shutting down while a heartbeat waits is a clean shutdown, not a miss: Run
// deregisters and returns nil even with a grace of one.
func TestRunShutdownDuringAWaitingHeartbeat(t *testing.T) {
	waiting := make(chan struct{}, 1)
	fp := &fakePublisher{silent: func(req protocol.Envelope) bool {
		if req.Type != TypeHeartbeat {
			return false
		}
		waiting <- struct{}{}
		return true
	}}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true,
				HeartbeatIntervalSecs: 60, HeartbeatGraceMisses: 1}), nil
		case TypeDeregister:
			return okReply(t, req, map[string]any{"deregistered": true}), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected %q", req.Type)
	}
	c := NewClient(fp, testIdentity())
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	tickCh <- time.Time{}
	<-waiting
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
	if !fp.hasType(TypeDeregister) {
		t.Error("no deregister on shutdown")
	}
}

// A heartbeat waits at most its interval, even under a longer call timeout:
// beats never pile up behind one core does not answer.
func TestRunHeartbeatWaitsAtMostTheInterval(t *testing.T) {
	fp := &fakePublisher{silent: func(req protocol.Envelope) bool { return req.Type == TypeHeartbeat }}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, registerReply{Instance: "http-1", Registered: true, HeartbeatIntervalSecs: 1}), nil
	}
	c := NewClient(fp, testIdentity(), WithCallTimeout(time.Hour))
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	start := time.Now()
	tickCh <- time.Time{}
	select {
	case tickCh <- time.Time{}: // taken: the first beat ended
		if d := time.Since(start); d < 900*time.Millisecond || d > 3*time.Second {
			t.Fatalf("first heartbeat ended after %s, want about the 1s interval", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a heartbeat outlived its interval")
	}
}
