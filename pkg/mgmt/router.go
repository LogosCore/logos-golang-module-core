package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// HandlerFunc serves one operation a module type adds to its rpc_queue, such
// as a factory's factory.build. It returns the reply payload. An error made
// with coreerrors.New(code, message) and a control-plane code
// (protocol.CodeBusy, protocol.CodeValidationFailed, ...) becomes an error
// reply with that code; any other error becomes internal_error.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) (any, error)

// Router serves a module's rpc_queue: the operations its module type adds, by
// envelope type, and every other request by the management Server, when the
// module has one. AMQPConsumer serves a Router.
type Router struct {
	source   protocol.Source
	mgmt     *Server
	handlers map[string]HandlerFunc
}

// NewRouter routes handlers by envelope type and everything else to mgmt, or
// refuses it when mgmt is nil (the module is not managed). source is stamped
// on the handlers' replies. Handlers may not take a management operation
// (mgmt.*).
func NewRouter(source protocol.Source, mgmt *Server, handlers map[string]HandlerFunc) (*Router, error) {
	for typ, h := range handlers {
		if typ == "" || strings.HasPrefix(typ, "mgmt.") || h == nil {
			return nil, fmt.Errorf("mgmt: cannot route %q to a handler", typ)
		}
	}
	return &Router{source: source, mgmt: mgmt, handlers: handlers}, nil
}

// Handle dispatches one request and returns its reply.
func (r *Router) Handle(ctx context.Context, req protocol.Envelope) protocol.ReplyEnvelope {
	h, ok := r.handlers[req.Type]
	if !ok {
		if r.mgmt != nil {
			return r.mgmt.Handle(ctx, req)
		}
		return r.errorReply(req, protocol.CodeValidationFailed, fmt.Sprintf("unknown type %q", req.Type))
	}
	if major, ok := protocol.MajorVersion(req.Version); !ok || major != 1 {
		return r.errorReply(req, protocol.CodeUnsupportedVersion,
			fmt.Sprintf("unsupported envelope version %q", req.Version))
	}
	payload, err := h(ctx, req.Payload)
	if err != nil {
		var typed *coreerrors.Error
		if errors.As(err, &typed) {
			return r.errorReply(req, typed.Code, typed.Message)
		}
		return r.errorReply(req, protocol.CodeInternalError, err.Error())
	}
	rep, err := protocol.NewReply(req, r.source, payload)
	if err != nil {
		return r.errorReply(req, protocol.CodeInternalError, "encode reply: "+err.Error())
	}
	return rep
}

func (r *Router) errorReply(req protocol.Envelope, code, msg string) protocol.ReplyEnvelope {
	return protocol.NewErrorReply(req, r.source, code, msg)
}
