package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// ErrNotConnected is returned by CoreClient.Call while no session is
// attached: the module is between broker connections. Callers retry.
var ErrNotConnected = errors.New("lifecycle: not connected to core")

// DefaultCallTimeout bounds a CoreClient.Call whose context has no deadline.
const DefaultCallTimeout = 10 * time.Second

// CoreClient calls core's RPC (logos.core.rpc) over the module's current
// session: the operations a module type sends core itself, such as a
// factory's build results. module.Run attaches it to every session and
// detaches it when the session ends. It is safe for concurrent use.
type CoreClient struct {
	mu     sync.RWMutex
	pub    Publisher
	source protocol.Source
}

// NewCoreClient returns a CoreClient with no session attached.
func NewCoreClient() *CoreClient { return &CoreClient{} }

// Attach makes calls go through pub, stamped with source. pub must be safe
// for concurrent use, since the lifecycle Client shares it (AMQPPublisher is).
func (c *CoreClient) Attach(pub Publisher, source protocol.Source) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pub, c.source = pub, source
}

// Detach ends the session: calls fail with ErrNotConnected until the next
// Attach.
func (c *CoreClient) Detach() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pub = nil
}

// Call sends one operation to core and decodes the reply payload into reply
// (when non-nil). An error reply comes back as a coreerrors.Error with core's
// code (coreerrors.Code), so a caller can tell a refusal from a lost call. A
// ctx without a deadline gets DefaultCallTimeout.
func (c *CoreClient) Call(ctx context.Context, typ string, payload, reply any) error {
	c.mu.RLock()
	pub, source := c.pub, c.source
	c.mu.RUnlock()
	if pub == nil {
		return ErrNotConnected
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultCallTimeout)
		defer cancel()
	}
	req, err := newRequest(source, typ, payload)
	if err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "marshal "+typ+" request", err)
	}
	rep, err := pub.Call(ctx, req)
	if err != nil {
		return err
	}
	if rep.Status == protocol.StatusError {
		return replyError(rep, "core rejected "+typ)
	}
	if reply != nil {
		if err := json.Unmarshal(rep.Payload, reply); err != nil {
			return coreerrors.Wrap(coreerrors.CodeInternal, "decode "+typ+" reply", err)
		}
	}
	return nil
}
