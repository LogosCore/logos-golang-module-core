package module

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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
