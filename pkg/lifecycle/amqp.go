package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Control-plane transport constants for module → core lifecycle RPC. See the
// docs repo: contracts/amqp-conventions.md.
const (
	// CoreRPCExchange is the direct exchange for module → core lifecycle RPC.
	CoreRPCExchange = "logos.core.rpc"
	// CoreRPCRoutingKey routes a lifecycle request to core's shared queue.
	CoreRPCRoutingKey = "core"
	// directReplyTo is RabbitMQ's pseudo-queue for direct reply-to: the client
	// consumes it and sets it as the request reply_to; no per-call queue is
	// declared.
	directReplyTo = "amq.rabbitmq.reply-to"
)

// ErrNoRoute is a request the broker handed back: no queue takes core's RPC,
// so core has not started on this broker yet. A broker that came back without
// its state (a RabbitMQ container recreated) has it again only once core
// declares it. A lost call like any other, retried; it only comes at once
// instead of at the deadline. It is not a coreerrors.Error: core did not
// refuse anything.
var ErrNoRoute = errors.New("lifecycle: the broker has no queue for core's RPC: core has not started on it yet")

// ErrPublisherClosed is a call on a publisher whose AMQP channel closed: its
// session is over.
var ErrPublisherClosed = errors.New("lifecycle: the control-plane channel closed")

// publishChannel is the subset of *amqp.Channel that AMQPPublisher needs,
// extracted for testability.
type publishChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// AMQPPublisher is a Publisher backed by a RabbitMQ connection. It publishes
// lifecycle requests to the core RPC exchange and reads replies over RabbitMQ
// direct reply-to, handing each to the call waiting for it by correlation_id,
// so calls run concurrently and one that core never answers holds up no
// other. It is safe for concurrent use.
//
// Requests are mandatory, so one no queue takes comes back as ErrNoRoute at
// once, and expire with their call's deadline, so a request whose caller gave
// up waiting is not left for core to act on when it starts consuming.
type AMQPPublisher struct {
	ch publishChannel

	mu      sync.Mutex
	waiting map[string]chan<- callResult // by correlation_id
	done    chan struct{}
	err     error // why done closed
}

type callResult struct {
	reply protocol.ReplyEnvelope
	err   error
}

// NewAMQPPublisher opens a channel on conn, idempotently declares the core RPC
// exchange, and begins consuming the direct reply-to pseudo-queue so replies
// can be received. Release it with Close; Done is closed once its channel is.
func NewAMQPPublisher(conn *amqp.Connection) (*AMQPPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open amqp channel: %w", err)
	}
	// Durable direct exchange; idempotent if core already declared it.
	if err := ch.ExchangeDeclare(CoreRPCExchange, "direct", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("declare exchange %q: %w", CoreRPCExchange, err)
	}
	// Mandatory requests that no queue takes come back here. The library
	// hands them over from its reader goroutine, so dispatch always reads.
	returns := ch.NotifyReturn(make(chan amqp.Return, 16))
	// Direct reply-to requires consuming the pseudo-queue (auto-ack) before
	// publishing any request that uses it.
	replies, err := ch.Consume(directReplyTo, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("consume direct reply-to: %w", err)
	}
	return newPublisher(ch, replies, returns), nil
}

// newPublisher starts handing replies and returned requests to their calls. It
// is done when replies closes: the channel closed.
func newPublisher(ch publishChannel, replies <-chan amqp.Delivery, returns <-chan amqp.Return) *AMQPPublisher {
	p := &AMQPPublisher{ch: ch, waiting: map[string]chan<- callResult{}, done: make(chan struct{})}
	go p.dispatch(replies, returns)
	return p
}

// dispatch reads until both replies and returns are closed, which the library
// does, one after the other, when the channel closes: it hands a return over
// from its reader goroutine, so nothing may stop reading returns before then.
func (p *AMQPPublisher) dispatch(replies <-chan amqp.Delivery, returns <-chan amqp.Return) {
	for replies != nil || returns != nil {
		select {
		case d, ok := <-replies:
			if !ok {
				replies = nil
				p.finish(ErrPublisherClosed)
				continue
			}
			var res callResult
			if err := json.Unmarshal(d.Body, &res.reply); err != nil {
				res.err = fmt.Errorf("decode reply envelope: %w", err)
			}
			p.deliver(d.CorrelationId, res) // nobody waiting: a stale reply
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			p.deliver(r.CorrelationId, callResult{err: fmt.Errorf("%w (%d %s)", ErrNoRoute, r.ReplyCode, r.ReplyText)})
		}
	}
}

func (p *AMQPPublisher) deliver(correlationID string, res callResult) {
	p.mu.Lock()
	w, ok := p.waiting[correlationID]
	delete(p.waiting, correlationID)
	p.mu.Unlock()
	if ok {
		w <- res // buffered: never blocks
	}
}

// finish closes Done and fails every waiting call with err.
func (p *AMQPPublisher) finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return
	}
	p.err = err
	close(p.done)
	for id, w := range p.waiting {
		w <- callResult{err: err}
		delete(p.waiting, id)
	}
}

// Done is closed once the publisher's channel is: every call fails from then
// on, and the session it served is over.
func (p *AMQPPublisher) Done() <-chan struct{} { return p.done }

// Err is why Done closed, nil before.
func (p *AMQPPublisher) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Close releases the underlying channel.
func (p *AMQPPublisher) Close() error {
	if p.ch == nil {
		return nil
	}
	return p.ch.Close()
}

// Call publishes req to the core RPC exchange and waits for the reply whose
// correlation_id matches req's, until ctx ends: give ctx a deadline, or a
// request core never answers waits forever. A request no queue takes fails
// with ErrNoRoute; one on a closed channel with ErrPublisherClosed.
func (p *AMQPPublisher) Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return protocol.ReplyEnvelope{}, fmt.Errorf("marshal request envelope: %w", err)
	}
	wait := make(chan callResult, 1)
	p.mu.Lock()
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		return protocol.ReplyEnvelope{}, err
	}
	p.waiting[req.CorrelationID] = wait
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.waiting, req.CorrelationID)
		p.mu.Unlock()
	}()

	msg := amqp.Publishing{
		ContentType:   "application/json",
		Type:          req.Type,
		MessageId:     req.MessageID,
		CorrelationId: req.CorrelationID,
		ReplyTo:       directReplyTo,
		Body:          body,
	}
	if deadline, ok := ctx.Deadline(); ok {
		// A per-message TTL in milliseconds; at least 1, as 0 would
		// expire it at once on a queue nobody consumes.
		msg.Expiration = strconv.FormatInt(max(time.Until(deadline).Milliseconds(), 1), 10)
	}
	if err := p.ch.PublishWithContext(ctx, CoreRPCExchange, CoreRPCRoutingKey, true, false, msg); err != nil {
		return protocol.ReplyEnvelope{}, fmt.Errorf("publish lifecycle request: %w", err)
	}

	select {
	case res := <-wait:
		return res.reply, res.err
	case <-ctx.Done():
		return protocol.ReplyEnvelope{}, fmt.Errorf("no reply from core: %w", ctx.Err())
	}
}
