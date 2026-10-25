package logstream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPPublisher publishes batches to protocol.LogsExchange with routing key =
// stream id, on a channel of its own. Batches are transient: nobody replays
// them, and a batch no queue is bound for is dropped by the broker.
type AMQPPublisher struct {
	mu sync.Mutex
	ch *amqp.Channel
}

// NewAMQPPublisher opens a channel on conn and declares the exchange.
func NewAMQPPublisher(conn *amqp.Connection) (*AMQPPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	// Durable direct exchange; idempotent if core already declared it.
	if err := ch.ExchangeDeclare(protocol.LogsExchange, "direct", true, false, false, false, nil); err != nil {
		ch.Close()
		return nil, fmt.Errorf("declare exchange %q: %w", protocol.LogsExchange, err)
	}
	return &AMQPPublisher{ch: ch}, nil
}

// PublishLogs implements Publisher.
func (p *AMQPPublisher) PublishLogs(ctx context.Context, b protocol.LogBatch) error {
	body, err := json.Marshal(b)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch.PublishWithContext(ctx, protocol.LogsExchange, b.StreamID, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Transient,
		Body:         body,
	})
}

// Close closes the channel.
func (p *AMQPPublisher) Close() error { return p.ch.Close() }
