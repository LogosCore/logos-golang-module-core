package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

var factorySource = protocol.Source{Service: "factory", Instance: "bob-1"}

// buildHandler replies by minion id: accepted, busy, or a plain error.
func buildHandler(_ context.Context, payload json.RawMessage) (any, error) {
	var p protocol.BuildReply
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, coreerrors.New(protocol.CodeValidationFailed, err.Error())
	}
	switch p.MinionID {
	case "busy":
		return nil, coreerrors.New(protocol.CodeBusy, "two builds running")
	case "boom":
		return nil, errors.New("disk full")
	}
	return protocol.BuildReply{MinionID: p.MinionID, Accepted: true}, nil
}

func TestRouterServesHandlers(t *testing.T) {
	r, err := NewRouter(factorySource, nil, map[string]HandlerFunc{protocol.TypeFactoryBuild: buildHandler})
	if err != nil {
		t.Fatal(err)
	}
	call := func(minionID string) protocol.ReplyEnvelope {
		return r.Handle(context.Background(), request(t, protocol.TypeFactoryBuild, map[string]string{"minion_id": minionID}))
	}

	rep := call("b-1")
	var ok protocol.BuildReply
	if rep.Status != protocol.StatusOK || rep.Source != factorySource || json.Unmarshal(rep.Payload, &ok) != nil || !ok.Accepted {
		t.Fatalf("accepted: %+v", rep)
	}
	if rep := call("busy"); rep.Status != protocol.StatusError || rep.Error.Code != protocol.CodeBusy || rep.Error.Message != "two builds running" {
		t.Errorf("busy: %+v", rep.Error)
	}
	if rep := call("boom"); rep.Error == nil || rep.Error.Code != protocol.CodeInternalError || !strings.Contains(rep.Error.Message, "disk full") {
		t.Errorf("plain error: %+v", rep.Error)
	}

	unknown := r.Handle(context.Background(), request(t, protocol.TypeMgmtStateGet, struct{}{}))
	if unknown.Error == nil || unknown.Error.Code != protocol.CodeValidationFailed {
		t.Errorf("management request without management: %+v", unknown.Error)
	}
	v2 := request(t, protocol.TypeFactoryBuild, map[string]string{"minion_id": "b-1"})
	v2.Version = "2.0"
	if rep := r.Handle(context.Background(), v2); rep.Error == nil || rep.Error.Code != protocol.CodeUnsupportedVersion {
		t.Errorf("v2: %+v", rep.Error)
	}
}

func TestRouterFallsBackToManagement(t *testing.T) {
	s := newTestServer(t)
	r, err := NewRouter(factorySource, s, map[string]HandlerFunc{protocol.TypeFactoryBuild: buildHandler})
	if err != nil {
		t.Fatal(err)
	}
	rep := r.Handle(context.Background(), request(t, protocol.TypeMgmtStateGet, struct{}{}))
	var state protocol.StateGetReply
	if rep.Status != protocol.StatusOK || json.Unmarshal(rep.Payload, &state) != nil || state.Values == nil {
		t.Fatalf("mgmt.state.get through the router: %+v", rep)
	}
	if rep := r.Handle(context.Background(), request(t, protocol.TypeFactoryBuild, map[string]string{"minion_id": "b-2"})); rep.Status != protocol.StatusOK {
		t.Fatalf("handler next to management: %+v", rep.Error)
	}
}

func TestNewRouterRefuses(t *testing.T) {
	for name, handlers := range map[string]map[string]HandlerFunc{
		"management type": {protocol.TypeMgmtStateGet: buildHandler},
		"empty type":      {"": buildHandler},
		"nil handler":     {protocol.TypeFactoryBuild: nil},
	} {
		if _, err := NewRouter(factorySource, nil, handlers); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
