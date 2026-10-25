package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeChannel records declarations and publishes and feeds scripted deliveries.
type fakeChannel struct {
	mu         sync.Mutex
	exchanges  []string
	queue      string
	durable    bool
	autoDelete bool
	exclusive  bool
	bindKey    string
	bindEx     string
	published  []amqp.Publishing
	pubKeys    []string
	closed     bool
	cancelled  bool
	declareErr error
	deliveries chan amqp.Delivery
}

func (f *fakeChannel) ExchangeDeclare(name, kind string, durable, _, _, _ bool, _ amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exchanges = append(f.exchanges, name+"/"+kind)
	if !durable {
		return errors.New("exchange must be durable")
	}
	return nil
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, _ bool, _ amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue, f.durable, f.autoDelete, f.exclusive = name, durable, autoDelete, exclusive
	return amqp.Queue{Name: name}, f.declareErr
}

func (f *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bindKey, f.bindEx = key, exchange
	return nil
}

func (f *fakeChannel) Qos(int, int, bool) error { return nil }

func (f *fakeChannel) ConsumeWithContext(context.Context, string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return f.deliveries, nil
}

func (f *fakeChannel) PublishWithContext(_ context.Context, _, key string, _, _ bool, msg amqp.Publishing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, msg)
	f.pubKeys = append(f.pubKeys, key)
	return nil
}

func (f *fakeChannel) Cancel(string, bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cancelled {
		f.cancelled = true
		close(f.deliveries) // the broker closes the stream on cancel-ok
	}
	return nil
}

func (f *fakeChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeChannel) publishes() ([]amqp.Publishing, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]amqp.Publishing(nil), f.published...), append([]string(nil), f.pubKeys...)
}

// fakeAck counts acks.
type fakeAck struct {
	mu   sync.Mutex
	acks int
}

func (a *fakeAck) Ack(uint64, bool) error        { a.mu.Lock(); a.acks++; a.mu.Unlock(); return nil }
func (a *fakeAck) Nack(uint64, bool, bool) error { return nil }
func (a *fakeAck) Reject(uint64, bool) error     { return nil }
func (a *fakeAck) count() int                    { a.mu.Lock(); defer a.mu.Unlock(); return a.acks }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAMQPConsumerDeclaresAndServes(t *testing.T) {
	fch := &fakeChannel{deliveries: make(chan amqp.Delivery, 4)}
	s := newTestServer(t)
	c, err := startConsumer(fch, RPCExchange("channel"), "logos.channel.rpc.http-1", "http-1", s)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if fch.queue != "logos.channel.rpc.http-1" || fch.durable || !fch.autoDelete || !fch.exclusive {
		t.Errorf("queue declare = %q durable=%v autoDelete=%v exclusive=%v", fch.queue, fch.durable, fch.autoDelete, fch.exclusive)
	}
	if fch.bindEx != "logos.channel.rpc" || fch.bindKey != "http-1" {
		t.Errorf("bind = %q/%q", fch.bindEx, fch.bindKey)
	}
	if len(fch.exchanges) != 1 || fch.exchanges[0] != "logos.channel.rpc"+"/direct" {
		t.Errorf("exchanges = %v", fch.exchanges)
	}

	ack := &fakeAck{}
	req := request(t, protocol.TypeMgmtStateGet, nil)
	body, _ := json.Marshal(req)
	fch.deliveries <- amqp.Delivery{Acknowledger: ack, Body: body, CorrelationId: req.CorrelationID,
		ReplyTo: "amq.rabbitmq.reply-to.abc", Type: req.Type}
	// Malformed body still gets an error reply and an ack.
	fch.deliveries <- amqp.Delivery{Acknowledger: ack, Body: []byte("not json"), CorrelationId: "bad-1",
		ReplyTo: "amq.rabbitmq.reply-to.abc", Type: protocol.TypeMgmtStateGet}
	// No reply_to: acked, nothing published.
	fch.deliveries <- amqp.Delivery{Acknowledger: ack, Body: body}

	waitFor(t, func() bool { return ack.count() == 3 })
	pubs, keys := fch.publishes()
	if len(pubs) != 2 {
		t.Fatalf("published %d replies, want 2", len(pubs))
	}
	byCorr := map[string]protocol.ReplyEnvelope{}
	for i, p := range pubs {
		if keys[i] != "amq.rabbitmq.reply-to.abc" {
			t.Errorf("reply routed to %q", keys[i])
		}
		var rep protocol.ReplyEnvelope
		if err := json.Unmarshal(p.Body, &rep); err != nil {
			t.Fatalf("decode reply: %v", err)
		}
		byCorr[p.CorrelationId] = rep
	}
	if rep := byCorr[req.CorrelationID]; rep.Status != protocol.StatusOK || rep.CorrelationID != req.CorrelationID {
		t.Errorf("state reply = %+v", rep)
	}
	if rep := byCorr["bad-1"]; rep.Status != protocol.StatusError || rep.Error.Code != protocol.CodeValidationFailed {
		t.Errorf("malformed reply = %+v", rep)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-c.Done():
	default:
		t.Error("Done not closed after Close")
	}
	if !fch.closed {
		t.Error("channel not closed")
	}
	_ = c.Close() // idempotent
}

func TestAMQPConsumerDoneOnDeliveryClose(t *testing.T) {
	fch := &fakeChannel{deliveries: make(chan amqp.Delivery)}
	c, err := startConsumer(fch, "x", "q", "i", newTestServer(t))
	if err != nil {
		t.Fatal(err)
	}
	fch.Cancel("", false) // broker-side stream close
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done not closed when deliveries closed")
	}
	_ = c.Close()
}

func TestAMQPConsumerDeclareError(t *testing.T) {
	fch := &fakeChannel{declareErr: errors.New("RESOURCE_LOCKED")}
	if _, err := startConsumer(fch, "x", "q", "i", newTestServer(t)); err == nil {
		t.Fatal("expected declare error")
	}
}
