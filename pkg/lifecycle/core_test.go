package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// recordingPublisher answers every call with reply and records the last
// request and whether its context had a deadline.
type recordingPublisher struct {
	reply       protocol.ReplyEnvelope
	got         protocol.Envelope
	hadDeadline bool
}

func (p *recordingPublisher) Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	p.got = req
	_, p.hadDeadline = ctx.Deadline()
	return p.reply, nil
}

func TestCoreClientNotConnected(t *testing.T) {
	c := NewCoreClient()
	if err := c.Call(context.Background(), protocol.TypeFactoryBuildResult, struct{}{}, nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("before Attach: %v", err)
	}
	c.Attach(&recordingPublisher{reply: protocol.ReplyEnvelope{Status: protocol.StatusOK, Payload: json.RawMessage(`{}`)}}, protocol.Source{})
	c.Detach()
	if err := c.Call(context.Background(), protocol.TypeFactoryBuildResult, struct{}{}, nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("after Detach: %v", err)
	}
}

func TestCoreClientCall(t *testing.T) {
	src := protocol.Source{Service: "factory", Instance: "bob-1"}
	pub := &recordingPublisher{reply: protocol.ReplyEnvelope{
		Status:  protocol.StatusOK,
		Payload: json.RawMessage(`{"minion_id":"b-1","recorded":true}`),
	}}
	c := NewCoreClient()
	c.Attach(pub, src)

	var out protocol.BuildResultReply
	err := c.Call(context.Background(), protocol.TypeFactoryBuildResult, protocol.BuildResult{MinionID: "b-1", Status: protocol.BuildSucceeded}, &out)
	if err != nil || !out.Recorded {
		t.Fatalf("Call = %v, reply %+v", err, out)
	}
	req := pub.got
	if req.Type != protocol.TypeFactoryBuildResult || req.Source != src || req.Version != protocol.EnvelopeVersionV1 ||
		req.MessageID == "" || req.CorrelationID != req.MessageID {
		t.Fatalf("request envelope: %+v", req)
	}
	var sent protocol.BuildResult
	if err := json.Unmarshal(req.Payload, &sent); err != nil || sent.MinionID != "b-1" {
		t.Fatalf("payload %s: %v", req.Payload, err)
	}
	if !pub.hadDeadline {
		t.Error("a context without a deadline got no DefaultCallTimeout")
	}
}

func TestCoreClientErrorReply(t *testing.T) {
	c := NewCoreClient()
	c.Attach(&recordingPublisher{reply: protocol.ReplyEnvelope{
		Status: protocol.StatusError,
		Error:  &protocol.EnvelopeError{Code: protocol.CodeNotFound, Message: "no such build"},
	}}, protocol.Source{})
	err := c.Call(context.Background(), protocol.TypeFactoryBuildProgress, struct{}{}, nil)
	if coreerrors.Code(err) != protocol.CodeNotFound {
		t.Fatalf("error reply: %v (code %q)", err, coreerrors.Code(err))
	}
}
