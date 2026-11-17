package module

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

func TestRPCQueue(t *testing.T) {
	if got := RPCQueue("channel", "http-1"); got != "logos.channel.rpc.http-1" {
		t.Errorf("RPCQueue = %q", got)
	}
	if got := RPCQueue("factory", "bob-1"); got != "logos.factory.rpc.bob-1" {
		t.Errorf("RPCQueue = %q", got)
	}
}

func TestRun_RequiresModuleType(t *testing.T) {
	err := Run(context.Background(), Config{AMQPURL: "amqp://x/", Identity: lifecycle.Identity{ModuleName: "http", Instance: "test"}})
	if err == nil {
		t.Fatal("Run without ModuleType returned nil")
	}
}

func TestRun_RefusesManagementHandler(t *testing.T) {
	err := Run(context.Background(), Config{
		AMQPURL:  "amqp://x/",
		Identity: lifecycle.Identity{ModuleType: "factory", ModuleName: "bob", Instance: "test"},
		Handlers: map[string]mgmt.HandlerFunc{protocol.TypeMgmtStateGet: func(context.Context, json.RawMessage) (any, error) {
			return nil, nil
		}},
	})
	if err == nil {
		t.Fatal("Run took a handler for a management operation")
	}
}

func TestRun_RefusesAnInvalidIcon(t *testing.T) {
	err := Run(context.Background(), Config{
		AMQPURL: "amqp://x/",
		Identity: lifecycle.Identity{ModuleType: "channel", ModuleName: "http", Instance: "test",
			Icon: `<svg viewBox="0 0 24 24"/>`}, // no SVG namespace
	})
	if err == nil || !strings.Contains(err.Error(), "Identity.Icon") {
		t.Fatalf("Run with an invalid icon: %v", err)
	}
}

func TestRun_RefusesAnInvalidLightIcon(t *testing.T) {
	const icon = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/></svg>`
	for name, id := range map[string]lifecycle.Identity{
		"invalid":         {ModuleType: "channel", ModuleName: "http", Instance: "test", Icon: icon, IconLight: `<svg viewBox="0 0 24 24"/>`},
		"without an icon": {ModuleType: "channel", ModuleName: "http", Instance: "test", IconLight: icon},
	} {
		err := Run(context.Background(), Config{AMQPURL: "amqp://x/", Identity: id})
		if err == nil || !strings.Contains(err.Error(), "Identity.IconLight") {
			t.Errorf("Run with a light icon %s: %v", name, err)
		}
	}
}

func TestRun_RequiresAMQPURL(t *testing.T) {
	if err := Run(context.Background(), Config{}); err == nil {
		t.Fatal("Run with empty AMQPURL returned nil")
	}
}

func TestRun_StopsOnCancelWhileBrokerUnreachable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Port 1 refuses connections, so Run sits in its retry backoff.
		done <- Run(ctx, Config{
			AMQPURL:  "amqp://guest:guest@127.0.0.1:1/",
			Identity: lifecycle.Identity{ModuleType: "channel", ModuleName: "http", Instance: "test"},
		})
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRetryAfter(t *testing.T) {
	blocked := fmt.Errorf("register: %w", coreerrors.New(protocol.CodeInstanceBlocked, "an operator blocked this instance"))
	if wait, msg := retryAfter(blocked, minBackoff); wait != maxBackoff || !strings.Contains(msg, "blocked") {
		t.Errorf("blocked: %s %q, want %s", wait, msg, maxBackoff)
	}
	if wait, msg := retryAfter(errors.New("connection refused"), 4*time.Second); wait != 4*time.Second || strings.Contains(msg, "blocked") {
		t.Errorf("other error: %s %q", wait, msg)
	}
	// Core starting on a broker that came back without its state: soon,
	// whatever the backoff had grown to.
	noRoute := fmt.Errorf("no answer from core to 3 heartbeats in a row: %w", fmt.Errorf("%w (312 NO_ROUTE)", lifecycle.ErrNoRoute))
	if wait, msg := retryAfter(noRoute, maxBackoff); wait != noRouteBackoff || !strings.Contains(msg, "not started") {
		t.Errorf("no route: %s %q, want %s", wait, msg, noRouteBackoff)
	}
}

type errPublisher struct{ err error }

func (p errPublisher) Call(context.Context, protocol.Envelope) (protocol.ReplyEnvelope, error) {
	return protocol.ReplyEnvelope{}, p.err
}

// A call core did not answer in time, or that came back with no route, is
// logged: before, a timeout read as the session ending and went unlogged. A
// call cut short by the session ending, or on a connection already gone, is
// not.
func TestLoggingPublisherLogsLostCalls(t *testing.T) {
	cases := []struct {
		err        error
		connClosed bool
		logged     bool
	}{
		{fmt.Errorf("no reply from core: %w", context.DeadlineExceeded), false, true},
		{fmt.Errorf("%w (312 NO_ROUTE)", lifecycle.ErrNoRoute), false, true},
		{fmt.Errorf("no reply from core: %w", context.Canceled), false, false},
		{lifecycle.ErrPublisherClosed, true, false},
	}
	for _, tc := range cases {
		var buf strings.Builder
		p := loggingPublisher{pub: errPublisher{tc.err}, instance: "http-1",
			connClosed: func() bool { return tc.connClosed },
			logger:     func() *slog.Logger { return slog.New(slog.NewTextHandler(&buf, nil)) }}
		_, _ = p.Call(context.Background(), protocol.Envelope{Type: lifecycle.TypeHeartbeat})
		if got := strings.Contains(buf.String(), "control plane: call failed"); got != tc.logged {
			t.Errorf("%v (conn closed %v): logged %v, want %v", tc.err, tc.connClosed, got, tc.logged)
		}
	}
}
