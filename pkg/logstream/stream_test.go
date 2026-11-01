package logstream

import (
	"testing"
	"time"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

// A stream that ends with more records waiting than a batch holds sends
// them in batches of at most MaxLogBatchRecords, every record once, the
// last batch with the end.
func TestEndDrainsInBatchesWithinTheLimit(t *testing.T) {
	const waiting = 1200
	s := &stream{id: "s", instance: "i", ch: make(chan protocol.LogRecord, waiting), stop: make(chan string, 1)}
	for i := range waiting {
		s.ch <- rec(protocol.LogInfo, string(rune('a'+i%26)))
	}
	s.end(protocol.LogEndShutdown)
	pub := &fakePub{}
	s.run(pub, nil, time.Hour, func(*stream) bool { return false })

	batches := pub.snapshot()
	total := 0
	for i, b := range batches {
		if len(b.Records) > protocol.MaxLogBatchRecords {
			t.Fatalf("batch %d has %d records", i, len(b.Records))
		}
		if b.End != (i == len(batches)-1) {
			t.Fatalf("batch %d of %d: end %v", i, len(batches), b.End)
		}
		if b.Seq != int64(i+1) {
			t.Fatalf("batch %d: seq %d", i, b.Seq)
		}
		total += len(b.Records)
	}
	if total != waiting || batches[len(batches)-1].EndReason != protocol.LogEndShutdown {
		t.Fatalf("%d records in %d batches, last %+v", total, len(batches), batches[len(batches)-1])
	}
}
