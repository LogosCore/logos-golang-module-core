package module

import (
	"context"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
)

func TestRPCQueue(t *testing.T) {
	if got := RPCQueue("channel", "http-1"); got != "logos.channel.rpc.http-1" {
		t.Errorf("RPCQueue = %q", got)
	}
	if got := RPCQueue("minion-factory", "mf-1"); got != "logos.minion-factory.rpc.mf-1" {
		t.Errorf("RPCQueue = %q", got)
	}
}

func TestRun_RequiresModuleType(t *testing.T) {
	err := Run(context.Background(), Config{AMQPURL: "amqp://x/", Identity: lifecycle.Identity{ModuleName: "http", Instance: "test"}})
	if err == nil {
		t.Fatal("Run without ModuleType returned nil")
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
