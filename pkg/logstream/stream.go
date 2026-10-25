package logstream

import (
	"context"
	"sync/atomic"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// stream is one leased log stream. min, deadline, tokens and refilled are
// guarded by the Hub's mutex; the goroutine in run owns publishing.
type stream struct {
	id       string
	instance string
	min      protocol.LogLevel
	deadline time.Time
	created  time.Time

	tokens   float64
	refilled time.Time

	ch      chan protocol.LogRecord
	stop    chan string // end reason; "" ends without a final batch
	dropped atomic.Int64
}

// offer queues r if the stream's level and rate admit it; h.mu must be held.
func (s *stream) offer(r protocol.LogRecord, now time.Time, rate float64) {
	if r.Level.Rank() < s.min.Rank() {
		return
	}
	s.tokens = min(rate, s.tokens+now.Sub(s.refilled).Seconds()*rate)
	s.refilled = now
	if s.tokens < 1 {
		s.dropped.Add(1)
		return
	}
	s.tokens--
	select {
	case s.ch <- r:
	default: // the publisher is behind
		s.dropped.Add(1)
	}
}

// end asks the stream goroutine to finish; only the first call counts.
func (s *stream) end(reason string) {
	select {
	case s.stop <- reason:
	default:
	}
}

// run publishes the backlog, then batches live records every flush until the
// stream is stopped or expired asks it to end.
func (s *stream) run(pub Publisher, backlog []protocol.LogRecord, flush time.Duration, expired func(*stream) bool) {
	var seq int64
	publish := func(b protocol.LogBatch) {
		seq++
		b.StreamID, b.Instance, b.Seq = s.id, s.instance, seq
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
		defer cancel()
		// A failed publish loses this batch; Seq lets core notice the gap.
		_ = pub.PublishLogs(ctx, b)
	}
	for len(backlog) > 0 {
		n := min(len(backlog), protocol.MaxLogBatchRecords)
		publish(protocol.LogBatch{Backlog: true, Records: backlog[:n]})
		backlog = backlog[n:]
	}

	var buf []protocol.LogRecord
	send := func(end bool, reason string) {
		dropped := s.dropped.Swap(0)
		if len(buf) == 0 && dropped == 0 && !end {
			return
		}
		publish(protocol.LogBatch{Records: buf, Dropped: dropped, End: end, EndReason: reason})
		buf = nil
	}
	drain := func() {
		for {
			select {
			case r := <-s.ch:
				buf = append(buf, r)
			default:
				return
			}
		}
	}

	t := time.NewTicker(flush)
	defer t.Stop()
	for {
		select {
		case r := <-s.ch:
			buf = append(buf, r)
			if len(buf) >= protocol.MaxLogBatchRecords {
				send(false, "")
			}
		case <-t.C:
			if expired(s) {
				drain()
				send(true, protocol.LogEndLeaseExpired)
				return
			}
			send(false, "")
		case reason := <-s.stop:
			if reason == "" {
				return
			}
			drain()
			send(true, reason)
			return
		}
	}
}
