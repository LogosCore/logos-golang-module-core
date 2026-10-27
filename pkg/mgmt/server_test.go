package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

func prop(key string, typ protocol.FieldType, apply protocol.ApplyMode, def any) protocol.PropertySpec {
	return protocol.PropertySpec{FieldSpec: protocol.FieldSpec{Key: key, Type: typ, Default: def}, Apply: apply}
}

func testManifest() protocol.ManagementManifest {
	limit := prop("max_body", protocol.FieldBytes, protocol.ApplyLive, int64(1024))
	limit.Min, limit.Max = 17, 4096
	level := prop("log_level", protocol.FieldEnum, protocol.ApplyLive, "info")
	level.Options = []protocol.EnumOption{{Value: "info"}, {Value: "debug"}}
	return protocol.ManagementManifest{
		SchemaVersion: protocol.ManagementSchemaV1,
		Properties: []protocol.PropertySpec{
			limit,
			level,
			prop("base_url", protocol.FieldURL, protocol.ApplyRestart, "http://core:8002"),
			prop("token", protocol.FieldSecret, protocol.ApplyLive, nil),
		},
		Actions: []protocol.ActionSpec{
			{Name: "drain", Risk: protocol.RiskHigh},
			{Name: "slow", Risk: protocol.RiskLow, TimeoutSeconds: 1},
			{Name: "echo", Risk: protocol.RiskLow, Params: []protocol.FieldSpec{
				{Key: "count", Type: protocol.FieldInt, Required: true, Min: 1, Max: 10},
				{Key: "note", Type: protocol.FieldString},
			}},
		},
		Status: []protocol.StatusSpec{
			{Key: "requests_total", Type: protocol.FieldInt},
			{Key: "draining", Type: protocol.FieldBool},
		},
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	s, err := New(testManifest(), WithSource(protocol.Source{Service: "channel", Instance: "http-1"}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// request builds a v1 request envelope with a JSON payload.
func request(t *testing.T, typ string, payload any) protocol.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewID()
	return protocol.Envelope{
		MessageID: id, CorrelationID: id, Type: typ, Version: protocol.EnvelopeVersionV1,
		Source: protocol.Source{Service: "core", Instance: "core-1"}, Payload: raw,
	}
}

func decodeOK(t *testing.T, rep protocol.ReplyEnvelope, out any) {
	t.Helper()
	if rep.Status != protocol.StatusOK {
		t.Fatalf("status = %q, error = %+v, want ok", rep.Status, rep.Error)
	}
	if err := json.Unmarshal(rep.Payload, out); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
}

func wantError(t *testing.T, rep protocol.ReplyEnvelope, code string) {
	t.Helper()
	if rep.Status != protocol.StatusError || rep.Error == nil || rep.Error.Code != code {
		t.Fatalf("reply = %+v / %+v, want error %q", rep.Status, rep.Error, code)
	}
}

func TestNewRejectsInvalidManifest(t *testing.T) {
	m := testManifest()
	m.SchemaVersion = "9"
	if _, err := New(m); err == nil {
		t.Fatal("expected invalid manifest error")
	}
}

func TestActionPanicsOnUndeclared(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for undeclared action")
		}
	}()
	newTestServer(t).Action("nope", nil)
}

func TestApplyConfigLiveValuesAndDefaults(t *testing.T) {
	s := newTestServer(t)
	var got map[string]any
	s.OnApply(func(_ context.Context, live map[string]any) error { got = live; return nil })

	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{
		Revision: 3,
		Values:   map[string]any{"max_body": float64(2048), "log_level": "debug"},
	})
	if rep.AppliedRevision != 3 || len(rep.Rejected) != 0 || len(rep.RestartRequired) != 0 {
		t.Fatalf("reply = %+v", rep)
	}
	if got["max_body"] != int64(2048) || got["log_level"] != "debug" || got["base_url"] != "http://core:8002" {
		t.Fatalf("live = %+v", got)
	}
	if s.AppliedRevision() != 3 {
		t.Errorf("AppliedRevision = %d, want 3", s.AppliedRevision())
	}

	// Keys absent from Values revert to the module's own default.
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 4, Values: map[string]any{}})
	if got["max_body"] != int64(1024) || got["log_level"] != "info" {
		t.Fatalf("live after revert = %+v", got)
	}
}

func TestApplyConfigRejectsInvalidKeepsPrevious(t *testing.T) {
	s := newTestServer(t)
	var got map[string]any
	s.OnApply(func(_ context.Context, live map[string]any) error { got = live; return nil })
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 1, Values: map[string]any{"max_body": 2000}})

	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{
		Revision: 2,
		Values:   map[string]any{"max_body": 5, "log_level": "debug"},
	})
	if rep.AppliedRevision != 2 {
		t.Errorf("AppliedRevision = %d, want 2 even with rejected keys", rep.AppliedRevision)
	}
	if _, ok := rep.Rejected["max_body"]; !ok || len(rep.Rejected) != 1 {
		t.Fatalf("rejected = %+v, want only max_body", rep.Rejected)
	}
	if got["max_body"] != int64(2000) || got["log_level"] != "debug" {
		t.Fatalf("live = %+v, want previous max_body kept and log_level applied", got)
	}
}

func TestApplyConfigRestartRequired(t *testing.T) {
	s := newTestServer(t)
	var got map[string]any
	s.OnApply(func(_ context.Context, live map[string]any) error { got = live; return nil })
	boot(t, s)

	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{
		Revision: 1, Values: map[string]any{"base_url": "http://other:9000"},
	})
	if !slices.Equal(rep.RestartRequired, []string{"base_url"}) {
		t.Fatalf("restart_required = %v", rep.RestartRequired)
	}
	if got["base_url"] != "http://core:8002" {
		t.Errorf("restart-mode value applied live: %v", got["base_url"])
	}
	if st := s.State(); !slices.Equal(st.RestartRequired, []string{"base_url"}) || st.Values["base_url"] != "http://core:8002" {
		t.Errorf("state = %+v", st)
	}

	// Setting it back to the startup value clears the flag.
	rep = s.ApplyConfig(context.Background(), protocol.ManagementConfig{
		Revision: 2, Values: map[string]any{"base_url": "http://core:8002"},
	})
	if len(rep.RestartRequired) != 0 {
		t.Errorf("restart_required = %v, want empty", rep.RestartRequired)
	}
}

// boot performs the startup apply (as the register reply would) with no values.
func boot(t *testing.T, s *Server) {
	t.Helper()
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 0})
}

func TestApplyConfigFirstApplyAdoptsRestartValues(t *testing.T) {
	s := newTestServer(t)
	var got map[string]any
	s.OnApply(func(_ context.Context, live map[string]any) error { got = live; return nil })

	// The startup apply (register reply) is applied in full, restart-mode included.
	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{
		Revision: 3, Values: map[string]any{"base_url": "http://other:9000"},
	})
	if len(rep.RestartRequired) != 0 || got["base_url"] != "http://other:9000" {
		t.Fatalf("startup apply: restart_required = %v, live base_url = %v", rep.RestartRequired, got["base_url"])
	}

	// Later changes to it need a restart; reverting to the adopted value does not.
	rep = s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 4})
	if !slices.Equal(rep.RestartRequired, []string{"base_url"}) || got["base_url"] != "http://other:9000" {
		t.Fatalf("after boot: restart_required = %v, live base_url = %v", rep.RestartRequired, got["base_url"])
	}
}

func TestApplyConfigIgnoresStaleRevision(t *testing.T) {
	s := newTestServer(t)
	calls := 0
	s.OnApply(func(context.Context, map[string]any) error { calls++; return nil })
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 5, Values: map[string]any{"log_level": "debug"}})

	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 4, Values: map[string]any{}})
	if rep.AppliedRevision != 5 || calls != 1 {
		t.Fatalf("reply = %+v calls = %d, want revision 5 and no re-apply", rep, calls)
	}
	if s.State().Values["log_level"] != "debug" {
		t.Error("stale revision changed state")
	}
}

func TestApplyConfigCallbackErrorKeepsPrevious(t *testing.T) {
	s := newTestServer(t)
	s.OnApply(func(context.Context, map[string]any) error { return errors.New("boom") })
	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 1, Values: map[string]any{"log_level": "debug"}})
	if rep.AppliedRevision != 1 || rep.Rejected["log_level"] == "" {
		t.Fatalf("reply = %+v, want log_level rejected", rep)
	}
	if s.State().Values["log_level"] != "info" {
		t.Error("failed apply changed effective value")
	}
}

func TestHandleConfigApplyPassesActor(t *testing.T) {
	s := newTestServer(t)
	var actor *protocol.Actor
	s.OnApply(func(ctx context.Context, _ map[string]any) error { actor = ActorFromContext(ctx); return nil })
	rep := s.Handle(context.Background(), request(t, protocol.TypeMgmtConfigApply, protocol.ConfigApplyRequest{
		ManagementConfig: protocol.ManagementConfig{Revision: 7, Values: map[string]any{"max_body": 100}},
		Actor:            &protocol.Actor{Type: "user", ID: "u1"},
	}))
	var out protocol.ConfigApplyReply
	decodeOK(t, rep, &out)
	if out.AppliedRevision != 7 {
		t.Errorf("applied_revision = %d", out.AppliedRevision)
	}
	if actor == nil || actor.ID != "u1" {
		t.Errorf("actor = %+v, want u1", actor)
	}
	if rep.Source.Instance != "http-1" {
		t.Errorf("reply source = %+v", rep.Source)
	}
}

func TestStateGetRedactsSecrets(t *testing.T) {
	s := newTestServer(t)
	s.Status(func() map[string]any { return map[string]any{"requests_total": 3, "undeclared": 1} })

	var st protocol.StateGetReply
	decodeOK(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtStateGet, struct{}{})), &st)
	if _, ok := st.Values["token"]; ok {
		t.Errorf("unset secret present: %+v", st.Values)
	}
	if st.Status["requests_total"] != float64(3) {
		t.Errorf("status = %+v", st.Status)
	}
	if _, ok := st.Status["undeclared"]; ok {
		t.Error("undeclared status key reported")
	}

	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 1, Values: map[string]any{"token": "hunter2"}})
	decodeOK(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtStateGet, nil)), &st)
	if st.Values["token"] != "***" {
		t.Errorf("secret = %v, want ***", st.Values["token"])
	}
	if st.AppliedRevision != 1 {
		t.Errorf("applied_revision = %d", st.AppliedRevision)
	}
}

func TestHandleRejectsVersionAndType(t *testing.T) {
	s := newTestServer(t)
	req := request(t, protocol.TypeMgmtStateGet, nil)
	req.Version = "2.0"
	wantError(t, s.Handle(context.Background(), req), protocol.CodeUnsupportedVersion)

	req = request(t, "mgmt.bogus", nil)
	rep := s.Handle(context.Background(), req)
	wantError(t, rep, protocol.CodeValidationFailed)
	if rep.CorrelationID != req.CorrelationID {
		t.Errorf("correlation_id = %q, want %q", rep.CorrelationID, req.CorrelationID)
	}

	req = request(t, protocol.TypeMgmtConfigApply, nil)
	req.Payload = json.RawMessage(`{"revision":"x"}`)
	wantError(t, s.Handle(context.Background(), req), protocol.CodeValidationFailed)
}

func invoke(t *testing.T, s *Server, id, action string, params map[string]any) protocol.ReplyEnvelope {
	t.Helper()
	return s.Handle(context.Background(), request(t, protocol.TypeMgmtActionInvoke, protocol.ActionInvokeRequest{
		InvocationID: id, Action: action, Params: params, Actor: &protocol.Actor{Type: "user", ID: "u1"},
	}))
}

func TestActionInvokeValidatesParams(t *testing.T) {
	s := newTestServer(t)
	var got map[string]any
	s.Action("echo", func(_ context.Context, params map[string]any, actor *protocol.Actor) (any, error) {
		got = params
		return map[string]any{"by": actor.ID}, nil
	})

	wantError(t, invoke(t, s, "i1", "echo", nil), protocol.CodeValidationFailed)                                       // required
	wantError(t, invoke(t, s, "i2", "echo", map[string]any{"count": 11}), protocol.CodeValidationFailed)               // > max
	wantError(t, invoke(t, s, "i3", "echo", map[string]any{"count": 1, "extra": true}), protocol.CodeValidationFailed) // undeclared
	if got != nil {
		t.Fatal("handler ran on invalid params")
	}

	var out struct {
		InvocationID string         `json:"invocation_id"`
		Result       map[string]any `json:"result"`
	}
	decodeOK(t, invoke(t, s, "i4", "echo", map[string]any{"count": 2}), &out)
	if out.InvocationID != "i4" || out.Result["by"] != "u1" {
		t.Errorf("reply = %+v", out)
	}
	if got["count"] != int64(2) {
		t.Errorf("params = %+v, want normalized count=2", got)
	}
}

func TestActionInvokeNotFoundAndFailure(t *testing.T) {
	s := newTestServer(t)
	wantError(t, invoke(t, s, "i1", "missing", nil), protocol.CodeNotFound)
	wantError(t, invoke(t, s, "i2", "drain", nil), protocol.CodeNotFound) // declared, no handler

	s.Action("drain", func(context.Context, map[string]any, *protocol.Actor) (any, error) {
		return nil, errors.New("already draining")
	})
	rep := invoke(t, s, "i3", "drain", nil)
	wantError(t, rep, protocol.CodeActionFailed)
	if rep.Error.Message != "already draining" {
		t.Errorf("message = %q", rep.Error.Message)
	}
	wantError(t, invoke(t, s, "", "drain", nil), protocol.CodeValidationFailed)
}

func TestActionInvokeTimeout(t *testing.T) {
	s := newTestServer(t)
	s.Action("slow", func(ctx context.Context, _ map[string]any, _ *protocol.Actor) (any, error) {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // ignore the deadline briefly
		return "late", nil
	})
	start := time.Now()
	wantError(t, invoke(t, s, "i1", "slow", nil), protocol.CodeActionFailed)
	if d := time.Since(start); d < time.Second || d > 3*time.Second {
		t.Errorf("timed out after %v, want ~1s", d)
	}
}

func TestActionInvokeDedupe(t *testing.T) {
	s := newTestServer(t)
	var runs atomic.Int32
	s.Action("drain", func(context.Context, map[string]any, *protocol.Actor) (any, error) {
		return runs.Add(1), nil
	})

	first := invoke(t, s, "same", "drain", nil)
	second := invoke(t, s, "same", "drain", nil)
	if runs.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", runs.Load())
	}
	if string(first.Payload) != string(second.Payload) {
		t.Errorf("repeat payload %s != %s", second.Payload, first.Payload)
	}
	if first.CorrelationID == second.CorrelationID {
		t.Error("repeat reply must address the repeat request")
	}

	// The cache is bounded: the oldest invocation is forgotten.
	for i := range dedupeSize {
		invoke(t, s, "fill-"+string(rune('a'+i%26))+string(rune('a'+i/26)), "drain", nil)
	}
	invoke(t, s, "same", "drain", nil)
	if runs.Load() != int32(dedupeSize)+2 {
		t.Errorf("runs = %d, want evicted invocation to run again", runs.Load())
	}
}

func TestActionInvokeConcurrentDuplicateWaits(t *testing.T) {
	s := newTestServer(t)
	release := make(chan struct{})
	var runs atomic.Int32
	s.Action("drain", func(context.Context, map[string]any, *protocol.Actor) (any, error) {
		runs.Add(1)
		<-release
		return "ok", nil
	})
	var wg sync.WaitGroup
	reps := make([]protocol.ReplyEnvelope, 2)
	for i := range reps {
		wg.Add(1)
		go func() { defer wg.Done(); reps[i] = invoke(t, s, "dup", "drain", nil) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if runs.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", runs.Load())
	}
	for _, r := range reps {
		if r.Status != protocol.StatusOK {
			t.Errorf("reply = %+v", r.Error)
		}
	}
}

func TestActionPanicIsActionFailed(t *testing.T) {
	s := newTestServer(t)
	s.Action("drain", func(context.Context, map[string]any, *protocol.Actor) (any, error) { panic("oops") })
	wantError(t, invoke(t, s, "p", "drain", nil), protocol.CodeActionFailed)
}

func TestConcurrentUse(t *testing.T) {
	s := newTestServer(t)
	s.OnApply(func(context.Context, map[string]any) error { return nil })
	s.Status(func() map[string]any { return map[string]any{"requests_total": 1} })
	s.Action("drain", func(context.Context, map[string]any, *protocol.Actor) (any, error) { return nil, nil })
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: int64(i), Values: map[string]any{"max_body": 100 + i}})
		}()
		go func() { defer wg.Done(); _ = s.State() }()
		go func() { defer wg.Done(); invoke(t, s, "c", "drain", nil) }()
	}
	wg.Wait()
}

type fakeLogs struct {
	started []protocol.LogsStartRequest
	stopped []string
}

func (f *fakeLogs) Start(r protocol.LogsStartRequest) (protocol.LogsStartReply, error) {
	f.started = append(f.started, r)
	return protocol.LogsStartReply{StreamID: r.StreamID, LeaseSeconds: 30}, nil
}

func (f *fakeLogs) Stop(id string) { f.stopped = append(f.stopped, id) }

func TestServeLogs(t *testing.T) {
	s := newTestServer(t)
	wantError(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtLogsStart,
		protocol.LogsStartRequest{StreamID: "s1"})), protocol.CodeNotFound)
	if s.Manifest().Has(protocol.CapabilityLogs) {
		t.Fatal("logs capability declared before ServeLogs")
	}

	f := &fakeLogs{}
	s.ServeLogs(f)
	s.ServeLogs(f) // declares the capability once
	if caps := s.Manifest().Capabilities; len(caps) != 1 || caps[0] != protocol.CapabilityLogs {
		t.Fatalf("capabilities = %v", caps)
	}

	var rep protocol.LogsStartReply
	decodeOK(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtLogsStart,
		protocol.LogsStartRequest{StreamID: "s1", MinLevel: protocol.LogDebug})), &rep)
	if rep.StreamID != "s1" || rep.LeaseSeconds != 30 || f.started[0].MinLevel != protocol.LogDebug {
		t.Errorf("start reply = %+v, started = %+v", rep, f.started)
	}
	wantError(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtLogsStart,
		protocol.LogsStartRequest{StreamID: "bad id"})), protocol.CodeValidationFailed)

	decodeOK(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtLogsStop,
		protocol.LogsStopRequest{StreamID: "s1"})), &protocol.LogsStopRequest{})
	if len(f.stopped) != 1 || f.stopped[0] != "s1" {
		t.Errorf("stopped = %v", f.stopped)
	}
}

func TestActionSurvivesCancelledDelivery(t *testing.T) {
	s := newTestServer(t)
	ran := make(chan struct{})
	s.Action("slow", func(ctx context.Context, _ map[string]any, _ *protocol.Actor) (any, error) {
		select {
		case <-time.After(50 * time.Millisecond): // finishes despite the dead delivery
			close(ran)
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	// The consumer that delivered the request is already gone (reconnect).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep := s.Handle(ctx, request(t, protocol.TypeMgmtActionInvoke, protocol.ActionInvokeRequest{InvocationID: "i1", Action: "slow"}))
	if rep.Status != protocol.StatusOK {
		t.Fatalf("reply = %+v", rep.Error)
	}
	<-ran
	// Core's retry of the same invocation gets the real outcome.
	var out protocol.ActionInvokeReply
	decodeOK(t, invoke(t, s, "i1", "slow", nil), &out)
	if out.Result != "done" {
		t.Errorf("retry result = %v", out.Result)
	}
}

func TestDedupeIsPerAction(t *testing.T) {
	s := newTestServer(t)
	calls := map[string]int{}
	for _, name := range []string{"drain", "slow"} {
		s.Action(name, func(context.Context, map[string]any, *protocol.Actor) (any, error) {
			calls[name]++
			return name, nil
		})
	}
	decodeOK(t, invoke(t, s, "same-id", "drain", nil), &protocol.ActionInvokeReply{})
	decodeOK(t, invoke(t, s, "same-id", "slow", nil), &protocol.ActionInvokeReply{})
	decodeOK(t, invoke(t, s, "same-id", "drain", nil), &protocol.ActionInvokeReply{})
	if calls["drain"] != 1 || calls["slow"] != 1 {
		t.Errorf("calls = %v, want each action once", calls)
	}
}

func TestNormalizeStringMap(t *testing.T) {
	f := protocol.FieldSpec{Key: "headers", Type: protocol.FieldStringMap}
	got, ok := normalize(f, map[string]any{"X-A": "1"}).(map[string]string)
	if !ok || got["X-A"] != "1" {
		t.Errorf("normalize = %#v", got)
	}
}

func TestDefaultSource(t *testing.T) {
	s, err := New(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	s.DefaultSource(protocol.Source{Service: "factory", Instance: "bob-1"})
	if s.source.Service != "factory" || s.source.Instance != "bob-1" {
		t.Errorf("source = %+v", s.source)
	}
	s, _ = New(testManifest(), WithSource(protocol.Source{Service: "channel", Instance: "http-1"}))
	s.DefaultSource(protocol.Source{Service: "factory", Instance: "bob-1"})
	if s.source.Instance != "http-1" {
		t.Errorf("DefaultSource overrode WithSource: %+v", s.source)
	}
}

// buildsSection is a section the way a type SDK adds one.
func buildsSection(applied *[]map[string]any, max *int64) Section {
	limit := prop("max_builds", protocol.FieldInt, protocol.ApplyLive, int64(2))
	limit.Min, limit.Max = 1, 8
	return Section{
		Properties: []protocol.PropertySpec{limit},
		Actions:    []protocol.ActionSpec{{Name: "pause", Risk: protocol.RiskLow}},
		Status:     []protocol.StatusSpec{{Key: "builds_running", Type: protocol.FieldInt}},
		Apply: func(_ context.Context, live map[string]any) {
			*applied = append(*applied, live)
			*max = live["max_builds"].(int64)
		},
		Handlers: map[string]ActionFunc{
			"pause": func(context.Context, map[string]any, *protocol.Actor) (any, error) { return "paused", nil },
		},
		Values: func() map[string]any { return map[string]any{"builds_running": 1, "requests_total": 99} },
	}
}

func TestExtend(t *testing.T) {
	s := newTestServer(t)
	var own []string
	s.OnApply(func(_ context.Context, live map[string]any) error {
		own = append(own, live["log_level"].(string))
		if live["log_level"] == "debug" {
			return errors.New("no debug today")
		}
		return nil
	})
	s.Status(func() map[string]any { return map[string]any{"requests_total": 3, "builds_running": 7} })
	var applied []map[string]any
	var max int64
	if err := s.Extend(buildsSection(&applied, &max)); err != nil {
		t.Fatalf("Extend: %v", err)
	}

	m := s.Manifest()
	if len(m.Properties) != 5 || m.Properties[4].Key != "max_builds" || len(m.Actions) != 4 || len(m.Status) != 3 {
		t.Fatalf("manifest = %+v", m)
	}
	if st := s.State(); st.Values["max_builds"] != int64(2) {
		t.Errorf("max_builds before any apply = %v, want the default", st.Values["max_builds"])
	}

	// The section applies after the module, with every live value.
	rep := s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 1, Values: map[string]any{"max_builds": 5}})
	if len(rep.Rejected) != 0 || len(own) != 1 || len(applied) != 1 || max != 5 || applied[0]["log_level"] != "info" {
		t.Fatalf("reply = %+v, own = %v, section = %v", rep, own, applied)
	}
	// A change the module refuses never reaches the section.
	rep = s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 2, Values: map[string]any{"max_builds": 6, "log_level": "debug"}})
	if rep.Rejected["max_builds"] == "" || len(applied) != 1 || max != 5 || s.State().Values["max_builds"] != int64(5) {
		t.Fatalf("reply = %+v, section = %v, max = %d", rep, applied, max)
	}
	// Out of the section's bounds: refused like any property.
	rep = s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 3, Values: map[string]any{"max_builds": 9}})
	if rep.Rejected["max_builds"] == "" || len(applied) != 2 || max != 5 {
		t.Fatalf("reply = %+v, max = %d (an invalid value keeps the previous one)", rep, max)
	}

	var out protocol.ActionInvokeReply
	decodeOK(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtActionInvoke,
		protocol.ActionInvokeRequest{InvocationID: "i1", Action: "pause"})), &out)
	if out.Result != "paused" {
		t.Errorf("pause result = %v", out.Result)
	}

	// The section's status values join the module's, each side keeping to
	// its own keys.
	if got := s.StatusSnapshot(); got["requests_total"] != 3 || got["builds_running"] != 1 || len(got) != 2 {
		t.Errorf("status = %v", got)
	}
}

func TestExtendWithoutModuleCallbacks(t *testing.T) {
	s, err := New(protocol.ManagementManifest{SchemaVersion: protocol.ManagementSchemaV1})
	if err != nil {
		t.Fatal(err)
	}
	if s.StatusSnapshot() != nil {
		t.Fatal("status without any callback")
	}
	var applied []map[string]any
	var max int64
	if err := s.Extend(buildsSection(&applied, &max)); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 1, Values: map[string]any{"max_builds": 3}})
	if max != 3 {
		t.Errorf("max = %d, want 3", max)
	}
	if got := s.StatusSnapshot(); len(got) != 1 || got["builds_running"] != 1 {
		t.Errorf("status = %v", got)
	}

	// A key the section does not report stays unreported.
	t2, err := New(protocol.ManagementManifest{SchemaVersion: protocol.ManagementSchemaV1,
		Status: []protocol.StatusSpec{{Key: "requests_total", Type: protocol.FieldInt}}})
	if err != nil {
		t.Fatal(err)
	}
	t2.Status(func() map[string]any { return map[string]any{"requests_total": 1, "builds_running": 4} })
	sec := buildsSection(&applied, &max)
	sec.Values = func() map[string]any { return nil }
	if err := t2.Extend(sec); err != nil {
		t.Fatal(err)
	}
	if got := t2.StatusSnapshot(); len(got) != 1 || got["requests_total"] != 1 {
		t.Errorf("status = %v", got)
	}
}

func TestExtendRefusesInvalidSections(t *testing.T) {
	var applied []map[string]any
	var max int64
	for name, change := range map[string]func(*Section){
		"property clash": func(sec *Section) { sec.Properties[0].Key = "log_level" },
		"action clash": func(sec *Section) {
			sec.Actions[0].Name = "drain"
			sec.Handlers = map[string]ActionFunc{"drain": sec.Handlers["pause"]}
		},
		"status clash":         func(sec *Section) { sec.Status[0].Key = "draining" },
		"invalid spec":         func(sec *Section) { sec.Properties[0].Apply = "never" },
		"action no handler":    func(sec *Section) { sec.Handlers = nil },
		"handler no action":    func(sec *Section) { sec.Handlers["stop"] = sec.Handlers["pause"] },
		"too many status keys": func(sec *Section) { sec.Status = make([]protocol.StatusSpec, protocol.MaxManifestStatus) },
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t)
			before := s.Manifest()
			sec := buildsSection(&applied, &max)
			change(&sec)
			if err := s.Extend(sec); err == nil {
				t.Fatal("Extend accepted an invalid section")
			}
			if after := s.Manifest(); len(after.Properties) != len(before.Properties) ||
				len(after.Actions) != len(before.Actions) || len(after.Status) != len(before.Status) {
				t.Errorf("manifest changed: %+v", after)
			}
			if _, ok := s.State().Values["max_builds"]; ok {
				t.Error("a refused section's property took effect")
			}
			wantError(t, s.Handle(context.Background(), request(t, protocol.TypeMgmtActionInvoke,
				protocol.ActionInvokeRequest{InvocationID: "i1", Action: "pause"})), protocol.CodeNotFound)
		})
	}
}

// conditionalServer has drain and resume available by the draining status,
// and trace only at log level debug.
func conditionalServer(t *testing.T, draining *atomic.Bool, runs *atomic.Int32) *Server {
	t.Helper()
	level := prop("log_level", protocol.FieldEnum, protocol.ApplyLive, "info")
	level.Options = []protocol.EnumOption{{Value: "info"}, {Value: "debug"}}
	s, err := New(protocol.ManagementManifest{
		SchemaVersion: protocol.ManagementSchemaV1,
		Properties:    []protocol.PropertySpec{level},
		Actions: []protocol.ActionSpec{
			{Name: "drain", Risk: protocol.RiskHigh, AvailableWhen: []protocol.ActionCondition{
				{Status: "draining", Op: protocol.OpEq, Value: false, Message: "Already draining"}}},
			{Name: "resume", Risk: protocol.RiskLow, AvailableWhen: []protocol.ActionCondition{
				{Status: "draining", Op: protocol.OpEq, Value: true}}},
			{Name: "trace", Risk: protocol.RiskLow, AvailableWhen: []protocol.ActionCondition{
				{Property: "log_level", Op: protocol.OpEq, Value: "debug", Message: "Set the log level to debug first"}}},
		},
		Status: []protocol.StatusSpec{{Key: "draining", Type: protocol.FieldBool, Label: "Draining"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	set := func(on bool) ActionFunc {
		return func(context.Context, map[string]any, *protocol.Actor) (any, error) {
			runs.Add(1)
			draining.Store(on)
			return map[string]any{"draining": on}, nil
		}
	}
	s.Action("drain", set(true))
	s.Action("resume", set(false))
	s.Action("trace", func(context.Context, map[string]any, *protocol.Actor) (any, error) { runs.Add(1); return nil, nil })
	s.Status(func() map[string]any { return map[string]any{"draining": draining.Load()} })
	return s
}

func TestActionConditions(t *testing.T) {
	var draining atomic.Bool
	var runs atomic.Int32
	s := conditionalServer(t, &draining, &runs)

	// Resume is pointless while not draining.
	rep := invoke(t, s, "r1", "resume", nil)
	wantError(t, rep, protocol.CodeActionUnavailable)
	if rep.Error.Message != "Requires Draining: Yes" || runs.Load() != 0 {
		t.Fatalf("refusal %+v after %d runs", rep.Error, runs.Load())
	}

	var out protocol.ActionInvokeReply
	decodeOK(t, invoke(t, s, "d1", "drain", nil), &out)
	rep = invoke(t, s, "d2", "drain", nil)
	wantError(t, rep, protocol.CodeActionUnavailable)
	if rep.Error.Message != "Already draining" || runs.Load() != 1 {
		t.Fatalf("refusal %+v after %d runs", rep.Error, runs.Load())
	}
	// A retry of an invocation that ran gets its result, not a refusal.
	decodeOK(t, invoke(t, s, "d1", "drain", nil), &out)
	// And a refused one stays refused.
	wantError(t, invoke(t, s, "r1", "resume", nil), protocol.CodeActionUnavailable)

	decodeOK(t, invoke(t, s, "r2", "resume", nil), &out)
	if draining.Load() || runs.Load() != 2 {
		t.Fatalf("draining %v after %d runs", draining.Load(), runs.Load())
	}

	// A condition on a property follows its effective value.
	wantError(t, invoke(t, s, "t1", "trace", nil), protocol.CodeActionUnavailable)
	s.ApplyConfig(context.Background(), protocol.ManagementConfig{Revision: 1, Values: map[string]any{"log_level": "debug"}})
	decodeOK(t, invoke(t, s, "t2", "trace", nil), &out)
	if runs.Load() != 3 {
		t.Fatalf("%d runs", runs.Load())
	}
}

func TestActionConditionsOnAnUnreportedValue(t *testing.T) {
	var draining atomic.Bool
	var runs atomic.Int32
	s := conditionalServer(t, &draining, &runs)
	s.Status(func() map[string]any { return nil })
	var out protocol.ActionInvokeReply
	decodeOK(t, invoke(t, s, "r1", "resume", nil), &out)
	if runs.Load() != 1 {
		t.Fatalf("%d runs", runs.Load())
	}
}
