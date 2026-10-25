package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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

// publishChannel is the subset of *amqp.Channel that AMQPPublisher needs,
// extracted for testability.
type publishChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// AMQPPublisher is a Publisher backed by a RabbitMQ connection. It publishes
// lifecycle requests to the core RPC exchange and reads replies over RabbitMQ
// direct reply-to. Call is serialized with a mutex; it is intended to be driven
// by a single Client loop.
type AMQPPublisher struct {
	ch      publishChannel
	replies <-chan amqp.Delivery
	mu      sync.Mutex
}

// NewAMQPPublisher opens a channel on conn, idempotently declares the core RPC
// exchange, and begins consuming the direct reply-to pseudo-queue so replies
// can be received. Release it with Close.
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
	// Direct reply-to requires consuming the pseudo-queue (auto-ack) before
	// publishing any request that uses it.
	replies, err := ch.Consume(directReplyTo, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("consume direct reply-to: %w", err)
	}
	return &AMQPPublisher{ch: ch, replies: replies}, nil
}

// Close releases the underlying channel.
func (p *AMQPPublisher) Close() error {
	if p.ch == nil {
		return nil
	}
	return p.ch.Close()
}

// Call publishes req to the core RPC exchange and waits for the reply whose
// correlation_id matches req's. Unrelated deliveries are skipped. It honors ctx
// cancellation.
func (p *AMQPPublisher) Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	body, err := json.Marshal(req)
	if err != nil {
		return protocol.ReplyEnvelope{}, fmt.Errorf("marshal request envelope: %w", err)
	}
	if err := p.ch.PublishWithContext(ctx, CoreRPCExchange, CoreRPCRoutingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		Type:          req.Type,
		MessageId:     req.MessageID,
		CorrelationId: req.CorrelationID,
		ReplyTo:       directReplyTo,
		Body:          body,
	}); err != nil {
		return protocol.ReplyEnvelope{}, fmt.Errorf("publish lifecycle request: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return protocol.ReplyEnvelope{}, ctx.Err()
		case d, ok := <-p.replies:
			if !ok {
				return protocol.ReplyEnvelope{}, fmt.Errorf("reply channel closed")
			}
			if d.CorrelationId != req.CorrelationID {
				continue // stale or unrelated reply
			}
			var reply protocol.ReplyEnvelope
			if err := json.Unmarshal(d.Body, &reply); err != nil {
				return protocol.ReplyEnvelope{}, fmt.Errorf("decode reply envelope: %w", err)
			}
			return reply, nil
		}
	}
}
