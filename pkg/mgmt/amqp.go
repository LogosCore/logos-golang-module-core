package mgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RPCExchange is the direct exchange for core → module RPC of moduleType:
// logos.<module_type>.rpc (logos.channel.rpc, logos.minion-factory.rpc). A
// module's rpc_queue is bound to it with routing key = instance; core itself
// publishes to the default exchange with routing key = rpc_queue. See the docs
// repo: contracts/amqp-conventions.md.
func RPCExchange(moduleType string) string { return "logos." + moduleType + ".rpc" }

// maxInFlight bounds concurrently handled requests (and the prefetch), so a
// long-running action does not block state reads.
const maxInFlight = 8

// consumeChannel is the subset of *amqp.Channel that AMQPConsumer needs,
// extracted for testability.
type consumeChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Cancel(consumer string, noWait bool) error
	Close() error
}

// AMQPConsumer serves a Server on a module's rpc_queue. Requests are envelope
// JSON bodies; each reply is published to the request's reply_to over the
// default exchange, echoing correlation_id. Every delivery is acked.
type AMQPConsumer struct {
	ch     consumeChannel
	srv    *Server
	tag    string
	ctx    context.Context // handler context, cancelled by Close
	cancel context.CancelFunc
	done   chan struct{}

	pubMu    sync.Mutex // serializes publish and ack on the shared channel
	inflight sync.WaitGroup
	closeErr error
	once     sync.Once
}

// NewAMQPConsumer opens a channel on conn, declares queue (non-durable,
// exclusive, auto-delete, so stale requests die with the connection), binds it
// to exchange (normally RPCExchange(module type)) with routing key instance,
// and starts consuming. It returns once consuming has begun, so a module can
// register right after. Release it with Close.
func NewAMQPConsumer(conn *amqp.Connection, exchange, queue, instance string, s *Server) (*AMQPConsumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open amqp channel: %w", err)
	}
	c, err := startConsumer(ch, exchange, queue, instance, s)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}
	return c, nil
}

func startConsumer(ch consumeChannel, exchange, queue, instance string, s *Server) (*AMQPConsumer, error) {
	// Durable direct exchange; idempotent if core already declared it.
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare exchange %q: %w", exchange, err)
	}
	if _, err := ch.QueueDeclare(queue, false, true, true, false, nil); err != nil {
		return nil, fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := ch.QueueBind(queue, instance, exchange, false, nil); err != nil {
		return nil, fmt.Errorf("bind queue %q: %w", queue, err)
	}
	if err := ch.Qos(maxInFlight, 0, false); err != nil {
		return nil, fmt.Errorf("set qos: %w", err)
	}
	// Stopped by an explicit Cancel in Close: cancelling via the consume
	// context races amqp091's own cancel against Channel.Close and can hang.
	tag := "logos-mgmt-" + protocol.NewULID()
	deliveries, err := ch.ConsumeWithContext(context.Background(), queue, tag, false, true, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume %q: %w", queue, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &AMQPConsumer{ch: ch, srv: s, tag: tag, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go c.loop(deliveries)
	return c, nil
}

// Done is closed when the consumer stops: after Close, or when the broker
// closes the delivery stream (channel or connection loss).
func (c *AMQPConsumer) Done() <-chan struct{} { return c.done }

// Close stops consuming, cancels and waits for in-flight requests, and
// releases the channel.
func (c *AMQPConsumer) Close() error {
	c.once.Do(func() {
		c.cancel()
		// Closes the delivery stream; fails harmlessly on a dead channel.
		_ = c.ch.Cancel(c.tag, false)
		<-c.done
		c.closeErr = c.ch.Close()
	})
	return c.closeErr
}

// loop serves deliveries until the stream closes (Cancel, channel or
// connection loss). Deliveries arriving after Close are acked unanswered.
func (c *AMQPConsumer) loop(deliveries <-chan amqp.Delivery) {
	defer close(c.done)
	defer c.inflight.Wait()
	defer c.cancel()
	sem := make(chan struct{}, maxInFlight)
	for d := range deliveries {
		if c.ctx.Err() != nil {
			_ = d.Ack(false)
			continue
		}
		sem <- struct{}{}
		c.inflight.Add(1)
		go func() {
			defer func() { <-sem; c.inflight.Done() }()
			c.handle(c.ctx, d)
		}()
	}
}

// handle serves one delivery: decode, dispatch, reply, ack.
func (c *AMQPConsumer) handle(ctx context.Context, d amqp.Delivery) {
	var req protocol.Envelope
	var reply protocol.ReplyEnvelope
	if err := json.Unmarshal(d.Body, &req); err != nil {
		req = protocol.Envelope{CorrelationID: d.CorrelationId, Type: d.Type, Version: protocol.EnvelopeVersionV1}
		reply = c.srv.errorReply(req, protocol.CodeValidationFailed, "invalid envelope: "+err.Error())
	} else {
		if req.CorrelationID == "" {
			req.CorrelationID = d.CorrelationId
		}
		reply = c.srv.Handle(ctx, req)
	}

	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	if d.ReplyTo != "" {
		if body, err := json.Marshal(reply); err == nil {
			corr := d.CorrelationId
			if corr == "" {
				corr = reply.CorrelationID
			}
			// A failed publish leaves core to time out; nothing to retry.
			_ = c.ch.PublishWithContext(context.Background(), "", d.ReplyTo, false, false, amqp.Publishing{
				ContentType:   "application/json",
				Type:          reply.Type,
				MessageId:     reply.MessageID,
				CorrelationId: corr,
				Body:          body,
			})
		}
	}
	_ = d.Ack(false)
}
