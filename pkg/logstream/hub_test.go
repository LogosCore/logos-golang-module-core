package logstream

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

type fakePub struct {
	mu      sync.Mutex
	batches []protocol.LogBatch
}

func (p *fakePub) PublishLogs(_ context.Context, b protocol.LogBatch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches = append(p.batches, b)
	return nil
}

func (p *fakePub) snapshot() []protocol.LogBatch {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]protocol.LogBatch(nil), p.batches...)
}

// waitFor polls until cond holds on the published batches.
func (p *fakePub) waitFor(t *testing.T, what string, cond func([]protocol.LogBatch) bool) []protocol.LogBatch {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b := p.snapshot(); cond(b) {
			return b
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; got %+v", what, p.snapshot())
	return nil
}

func messages(bs []protocol.LogBatch) []string {
	var out []string
	for _, b := range bs {
		for _, r := range b.Records {
			out = append(out, r.Message)
		}
	}
	return out
}

func ended(reason string) func([]protocol.LogBatch) bool {
	return func(bs []protocol.LogBatch) bool {
		return len(bs) > 0 && bs[len(bs)-1].End && bs[len(bs)-1].EndReason == reason
	}
}

func rec(level protocol.LogLevel, msg string) protocol.LogRecord {
	return protocol.LogRecord{Time: time.Now(), Level: level, Message: msg}
}

func newHub(t *testing.T, opts ...Option) (*Hub, *fakePub) {
	t.Helper()
	h := New("http-1", append([]Option{WithFlushInterval(5 * time.Millisecond)}, opts...)...)
	p := &fakePub{}
	h.Attach(p)
	return h, p
}

func TestBacklogThenLiveFilteredByLevel(t *testing.T) {
	h, p := newHub(t)
	h.Record(rec(protocol.LogInfo, "old-1"))
	h.Record(rec(protocol.LogDebug, "old-debug")) // below the stream's level
	h.Record(rec(protocol.LogWarn, "old-2"))

	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1", MinLevel: protocol.LogWarn, Backlog: 10}); err != nil {
		t.Fatal(err)
	}
	h.Record(rec(protocol.LogInfo, "live-info")) // below the stream's level
	h.Record(rec(protocol.LogError, "live-err"))
	h.Stop("s1")

	bs := p.waitFor(t, "stop", ended(protocol.LogEndStopped))
	if got := messages(bs); len(got) != 2 || got[0] != "old-2" || got[1] != "live-err" {
		t.Fatalf("messages = %v", got)
	}
	if !bs[0].Backlog || bs[0].Seq != 1 || bs[0].Instance != "http-1" || bs[0].StreamID != "s1" {
		t.Errorf("first batch = %+v", bs[0])
	}
	for i, b := range bs {
		if b.Seq != int64(i+1) {
			t.Errorf("batch %d seq = %d", i, b.Seq)
		}
	}
}

func TestDebugBacklogWhenModuleLogsDebug(t *testing.T) {
	h, p := newHub(t)
	// The module's own level is debug: records are built with no stream open.
	log := slog.New(h.Handler(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelDebug})))
	log.Debug("request served", "path", "/x")
	log.Info("startup")

	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1", MinLevel: protocol.LogDebug, Backlog: 10}); err != nil {
		t.Fatal(err)
	}
	h.Stop("s1")
	bs := p.waitFor(t, "stop", ended(protocol.LogEndStopped))
	if got := messages(bs); len(got) != 2 || got[0] != "request served" || got[1] != "startup" {
		t.Fatalf("backlog = %v, want the debug record too", got)
	}
}

func TestLeaseExpiresUnlessRenewed(t *testing.T) {
	h, p := newHub(t)
	var mu sync.Mutex
	now := time.Now()
	h.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }

	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1", LeaseSeconds: 10}); err != nil {
		t.Fatal(err)
	}
	advance(8 * time.Second)
	// Renewal: same id extends the lease and sends no second backlog.
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1", LeaseSeconds: 10, Backlog: 5}); err != nil {
		t.Fatal(err)
	}
	advance(8 * time.Second) // 16s after start, 8s after renewal
	time.Sleep(30 * time.Millisecond)
	if bs := p.snapshot(); len(bs) > 0 && bs[len(bs)-1].End {
		t.Fatalf("stream ended despite renewal: %+v", bs)
	}
	advance(3 * time.Second)
	p.waitFor(t, "lease expiry", ended(protocol.LogEndLeaseExpired))
}

func TestRateLimitCountsDropped(t *testing.T) {
	h, p := newHub(t, WithRate(5))
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1"}); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		h.Record(rec(protocol.LogInfo, "x"))
	}
	h.Stop("s1")
	bs := p.waitFor(t, "stop", ended(protocol.LogEndStopped))
	var dropped int64
	for _, b := range bs {
		dropped += b.Dropped
	}
	if n := len(messages(bs)); n != 5 || dropped != 15 {
		t.Errorf("delivered %d dropped %d, want 5 and 15", n, dropped)
	}
}

func TestDetachEndsSilentlyAndCloseSendsShutdown(t *testing.T) {
	h, p := newHub(t)
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1"}); err != nil {
		t.Fatal(err)
	}
	h.Detach()
	time.Sleep(30 * time.Millisecond)
	for _, b := range p.snapshot() {
		if b.End {
			t.Fatalf("detach published an end batch: %+v", b)
		}
	}
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s2"}); err == nil {
		t.Fatal("start without a publisher succeeded")
	}

	h.Attach(p)
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s2"}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if !ended(protocol.LogEndShutdown)(p.snapshot()) {
		t.Fatalf("Close did not publish shutdown: %+v", p.snapshot())
	}
}

func TestMaxStreamsEvictsOldest(t *testing.T) {
	h, p := newHub(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if _, err := h.Start(protocol.LogsStartRequest{StreamID: id}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond) // distinct creation times
	}
	p.waitFor(t, "eviction", func(bs []protocol.LogBatch) bool {
		for _, b := range bs {
			if b.StreamID == "a" && b.End && b.EndReason == protocol.LogEndReplaced {
				return true
			}
		}
		return false
	})
}

func TestSlogHandler(t *testing.T) {
	h, p := newHub(t)
	// The module's own output is at warn; the hub still gets info, and debug
	// only while a stream asks for it.
	next := slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	log := slog.New(h.Handler(next)).With("component", "http").WithGroup("req")

	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1", MinLevel: protocol.LogDebug}); err != nil {
		t.Fatal(err)
	}
	log.Debug("dbg", "path", "/x")
	log.Info("served", "status", 200, "err", errString("boom"), slog.Group("t", "ms", 12))
	h.Stop("s1")

	bs := p.waitFor(t, "stop", ended(protocol.LogEndStopped))
	var recs []protocol.LogRecord
	for _, b := range bs {
		recs = append(recs, b.Records...)
	}
	if len(recs) != 2 || recs[0].Level != protocol.LogDebug || recs[1].Level != protocol.LogInfo {
		t.Fatalf("records = %+v", recs)
	}
	// Groups stay nested, as the module's JSON output has them.
	a := recs[1].Attrs
	req, _ := a["req"].(map[string]any)
	tm, _ := req["t"].(map[string]any)
	if a["component"] != "http" || req["status"] != int64(200) || req["err"] != "boom" || tm["ms"] != int64(12) {
		t.Errorf("attrs = %+v", a)
	}
	if h.WantsDebug() {
		t.Error("WantsDebug after the debug stream stopped")
	}
}

type discard struct{}

func (discard) Write(b []byte) (int, error) { return len(b), nil }

type errString string

func (e errString) Error() string { return string(e) }

func TestSlogHandlerKeepsStructure(t *testing.T) {
	h, p := newHub(t)
	log := slog.New(h.Handler(slog.NewJSONHandler(discard{}, nil)))
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1"}); err != nil {
		t.Fatal(err)
	}
	type pair struct {
		A int `json:"a"`
	}
	log.Info("x",
		slog.Group("", "inlined", 1), // an empty group key inlines its members
		slog.Group("empty"),          // empty groups are left out
		"obj", pair{A: 2},            // JSON-marshalable values keep their shape
		"list", []string{"p", "q"},
	)
	h.Stop("s1")
	bs := p.waitFor(t, "stop", ended(protocol.LogEndStopped))
	var a map[string]any
	for _, b := range bs {
		for _, r := range b.Records {
			a = r.Attrs
		}
	}
	obj, _ := a["obj"].(json.RawMessage)
	list, _ := a["list"].(json.RawMessage)
	if a["inlined"] != int64(1) || string(obj) != `{"a":2}` || string(list) != `["p","q"]` {
		t.Errorf("attrs = %#v", a)
	}
	if _, ok := a["empty"]; ok {
		t.Error("empty group recorded")
	}
}

func TestValuesAreSnapshotsAndCapped(t *testing.T) {
	h, p := newHub(t)
	log := slog.New(h.Handler(slog.NewJSONHandler(discard{}, nil)))
	if _, err := h.Start(protocol.LogsStartRequest{StreamID: "s1"}); err != nil {
		t.Fatal(err)
	}
	m := map[string]int{"k": 1}
	log.Info("x", "m", m, "long", strings.Repeat("é", protocol.MaxLogValueBytes))
	m["k"] = 2 // the caller keeps mutating its map: the record must not see it
	h.Stop("s1")
	bs := p.waitFor(t, "stop", ended(protocol.LogEndStopped))
	a := bs[len(bs)-1].Records[0].Attrs
	if raw, _ := a["m"].(json.RawMessage); string(raw) != `{"k":1}` {
		t.Errorf("m = %s, want the value at log time", raw)
	}
	long, _ := a["long"].(string)
	if len(long) > protocol.MaxLogValueBytes+len("…") || !strings.HasSuffix(long, "…") || !utf8.ValidString(long) {
		t.Errorf("long value: %d bytes, valid UTF-8 %v", len(long), utf8.ValidString(long))
	}
}

func TestTruncate(t *testing.T) {
	if Truncate("abc", 5) != "abc" || Truncate("abcdef", 3) != "abc…" {
		t.Error("ascii")
	}
	if got := Truncate("aéb", 2); got != "a…" { // é is 2 bytes: never split it
		t.Errorf("Truncate = %q", got)
	}
}
