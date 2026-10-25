// Package logstream lets core tail a module's logs live. A Hub keeps a small
// ring buffer of recent records and, while core holds a leased stream open
// (mgmt.logs.start, renewed by repeating it), publishes the records in
// batches to protocol.LogsExchange. Nothing is persisted.
//
// Feed it through Handler (log/slog) or Record. Serve it with
// mgmt.Server.ServeLogs and give it a Publisher per broker session
// (module.Config.Logs does both).
package logstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Publisher sends one batch to core.
type Publisher interface {
	PublishLogs(ctx context.Context, b protocol.LogBatch) error
}

const (
	defaultRingSize      = 1000
	defaultRate          = 200 // records per second per stream
	defaultMaxStreams    = 4
	defaultFlushInterval = 250 * time.Millisecond
	streamBuffer         = 1024
	publishTimeout       = 5 * time.Second
	closeWait            = 2 * time.Second
)

// Option configures a Hub.
type Option func(*Hub)

// WithRingSize sets how many recent records are kept for backlogs.
func WithRingSize(n int) Option {
	return func(h *Hub) {
		if n > 0 {
			h.ring = make([]protocol.LogRecord, n)
		}
	}
}

// WithRate caps records per second per stream; the excess is counted as
// dropped.
func WithRate(perSecond int) Option {
	return func(h *Hub) {
		if perSecond > 0 {
			h.rate = float64(perSecond)
		}
	}
}

// WithFlushInterval sets how often a stream publishes a batch.
func WithFlushInterval(d time.Duration) Option {
	return func(h *Hub) {
		if d > 0 {
			h.flush = d
		}
	}
}

// Hub fans log records out to the ring buffer and the active streams.
type Hub struct {
	instance   string
	rate       float64
	flush      time.Duration
	maxStreams int
	now        func() time.Time

	mu      sync.Mutex
	ring    []protocol.LogRecord
	next    int
	full    bool
	streams map[string]*stream
	pub     Publisher
	closed  bool

	// debug counts streams at min level debug. Atomic so WantsDebug, which a
	// logger asks on every debug call, never takes mu.
	debug atomic.Int32

	wg sync.WaitGroup // stream goroutines
}

// New returns a Hub for instance.
func New(instance string, opts ...Option) *Hub {
	h := &Hub{
		instance:   instance,
		rate:       defaultRate,
		flush:      defaultFlushInterval,
		maxStreams: defaultMaxStreams,
		now:        time.Now,
		ring:       make([]protocol.LogRecord, defaultRingSize),
		streams:    map[string]*stream{},
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// WantsDebug reports whether a stream asked for debug records, so a logger
// can emit them even when its own level is higher.
func (h *Hub) WantsDebug() bool { return h.debug.Load() > 0 }

// Record adds r to the ring buffer and to every stream whose level admits it.
func (h *Hub) Record(r protocol.LogRecord) {
	r.Message = Truncate(r.Message, protocol.MaxLogMessageBytes)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	// The ring keeps whatever the logger produced: debug arrives only when
	// the module logs at debug or a stream asks for it (Handler.Enabled), and
	// new streams pick their level from it.
	h.ring[h.next] = r
	h.next = (h.next + 1) % len(h.ring)
	if h.next == 0 {
		h.full = true
	}
	now := h.now()
	for _, s := range h.streams {
		s.offer(r, now, h.rate)
	}
}

// Attach sets the publisher for the current broker session.
func (h *Hub) Attach(p Publisher) {
	h.mu.Lock()
	h.pub = p
	h.mu.Unlock()
}

// Detach drops the publisher and ends every stream without a final batch
// (there is nothing to publish it with). Core's lease renewal recreates them
// once the module is back.
func (h *Hub) Detach() {
	h.mu.Lock()
	h.pub = nil
	streams := h.takeStreams()
	h.mu.Unlock()
	for _, s := range streams {
		s.end("")
	}
}

// Close ends every stream with reason shutdown and stops recording. It waits
// up to closeWait for the final batches to be published.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	streams := h.takeStreams()
	h.mu.Unlock()
	for _, s := range streams {
		s.end(protocol.LogEndShutdown)
	}
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeWait):
	}
}

// takeStreams empties the stream table; h.mu must be held.
func (h *Hub) takeStreams() []*stream {
	out := make([]*stream, 0, len(h.streams))
	for id, s := range h.streams {
		out = append(out, s)
		delete(h.streams, id)
	}
	h.debug.Store(0)
	return out
}

// errNoPublisher rejects a start while no broker session is attached.
var errNoPublisher = errors.New("log streaming is not connected")

// Start opens a stream, or renews it when req.StreamID is already active.
func (h *Hub) Start(req protocol.LogsStartRequest) (protocol.LogsStartReply, error) {
	if err := req.Validate(); err != nil {
		return protocol.LogsStartReply{}, err
	}
	lease := req.LeaseSeconds
	if lease == 0 {
		lease = protocol.DefaultLogLeaseSeconds
	}
	minLevel := req.MinLevel
	if minLevel == "" {
		minLevel = protocol.LogInfo
	}
	reply := protocol.LogsStartReply{StreamID: req.StreamID, LeaseSeconds: lease}
	deadline := h.now().Add(time.Duration(lease) * time.Second)

	h.mu.Lock()
	if h.closed || h.pub == nil {
		h.mu.Unlock()
		return protocol.LogsStartReply{}, errNoPublisher
	}
	if s, ok := h.streams[req.StreamID]; ok {
		s.min = minLevel
		s.deadline = deadline
		h.recount()
		h.mu.Unlock()
		return reply, nil
	}
	var evicted *stream
	if len(h.streams) >= h.maxStreams {
		evicted = h.oldest()
		h.remove(evicted)
	}
	s := &stream{
		id:       req.StreamID,
		instance: h.instance,
		min:      minLevel,
		deadline: deadline,
		ch:       make(chan protocol.LogRecord, streamBuffer),
		stop:     make(chan string, 1),
		tokens:   h.rate,
		refilled: h.now(),
		created:  h.now(),
	}
	backlog := h.backlog(minLevel, req.Backlog)
	h.streams[s.id] = s
	h.recount()
	pub := h.pub
	// Added under mu, so a Close that takes this stream also waits for it.
	h.wg.Add(1)
	h.mu.Unlock()

	if evicted != nil {
		evicted.end(protocol.LogEndReplaced)
	}
	go func() {
		defer h.wg.Done()
		s.run(pub, backlog, h.flush, h.leaseCheck)
	}()
	return reply, nil
}

// Stop ends a stream with reason stopped. Unknown ids are ignored.
func (h *Hub) Stop(id string) {
	h.mu.Lock()
	s, ok := h.streams[id]
	if ok {
		h.remove(s)
	}
	h.mu.Unlock()
	if ok {
		s.end(protocol.LogEndStopped)
	}
}

// leaseCheck ends s if its lease ran out; called from the stream goroutine.
func (h *Hub) leaseCheck(s *stream) bool {
	h.mu.Lock()
	expired := h.streams[s.id] == s && !h.now().Before(s.deadline)
	if expired {
		h.remove(s)
	}
	h.mu.Unlock()
	return expired
}

// remove drops s from the table; h.mu must be held.
func (h *Hub) remove(s *stream) {
	delete(h.streams, s.id)
	h.recount()
}

// recount refreshes the debug-stream count; h.mu must be held.
func (h *Hub) recount() {
	var n int32
	for _, s := range h.streams {
		if s.min == protocol.LogDebug {
			n++
		}
	}
	h.debug.Store(n)
}

func (h *Hub) oldest() *stream {
	var o *stream
	for _, s := range h.streams {
		if o == nil || s.created.Before(o.created) {
			o = s
		}
	}
	return o
}

// backlog returns up to n ring records at or above minLevel, oldest first;
// h.mu must be held.
func (h *Hub) backlog(minLevel protocol.LogLevel, n int) []protocol.LogRecord {
	if n <= 0 {
		return nil
	}
	size := h.next
	if h.full {
		size = len(h.ring)
	}
	// Walk the ring newest first, in place.
	out := make([]protocol.LogRecord, 0, min(n, size))
	for k := 1; k <= size && len(out) < n; k++ {
		r := h.ring[(h.next-k+len(h.ring))%len(h.ring)]
		if r.Level.Rank() >= minLevel.Rank() {
			out = append(out, r)
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
