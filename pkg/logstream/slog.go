package logstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Handler wraps next so every record also reaches the Hub. Records below
// next's level are still built while a stream asks for debug, and go to the
// Hub only; next's output is unchanged.
func (h *Hub) Handler(next slog.Handler) slog.Handler {
	return &handler{hub: h, next: next}
}

type handler struct {
	hub    *Hub
	next   slog.Handler
	attrs  []groupedAttr // from WithAttrs, with the groups open at the time
	groups []string      // groups opened by WithGroup
}

type groupedAttr struct {
	groups []string
	attr   slog.Attr
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l) || l >= slog.LevelInfo || h.hub.WantsDebug()
}

// Handle passes r on and records it for the hub with the module's own
// structure: groups stay nested objects, exactly as a JSON handler would
// write them.
func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	var err error
	if h.next.Enabled(ctx, r.Level) {
		err = h.next.Handle(ctx, r)
	}
	rec := protocol.LogRecord{Time: r.Time, Level: level(r.Level), Message: r.Message}
	if rec.Time.IsZero() {
		rec.Time = time.Now()
	}
	b := &attrBuilder{root: map[string]any{}, budget: protocol.MaxLogAttrs}
	for _, ga := range h.attrs {
		b.add(ga.groups, ga.attr)
	}
	r.Attrs(func(a slog.Attr) bool {
		b.add(h.groups, a)
		return b.budget > 0
	})
	if len(b.root) > 0 {
		rec.Attrs = b.root
	}
	h.hub.Record(rec)
	return err
}

func (h *handler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(as)
	c.attrs = append([]groupedAttr(nil), h.attrs...)
	for _, a := range as {
		c.attrs = append(c.attrs, groupedAttr{groups: h.groups, attr: a})
	}
	return &c
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.next = h.next.WithGroup(name)
	c.groups = append(append([]string(nil), h.groups...), name)
	return &c
}

// attrBuilder assembles nested attrs, keeping at most budget leaf values.
type attrBuilder struct {
	root   map[string]any
	budget int
}

// add puts a under the given groups, following slog's rules: an empty key on
// a group inlines its members, empty groups are left out.
func (b *attrBuilder) add(groups []string, a slog.Attr) {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		members := v.Group()
		if len(members) == 0 {
			return
		}
		if a.Key != "" {
			groups = append(append([]string(nil), groups...), a.Key)
		}
		for _, m := range members {
			b.add(groups, m)
		}
		return
	}
	if a.Key == "" || b.budget <= 0 {
		return
	}
	node := b.root
	for _, g := range groups {
		child, ok := node[g].(map[string]any)
		if !ok {
			child = map[string]any{}
			node[g] = child
		}
		node = child
	}
	node[a.Key] = value(v)
	b.budget--
}

// value renders v as the JSON handler would: scalars as they are, errors as
// their message, other values as their JSON. Structured values are marshalled
// here, once, so the record never shares a map or slice the caller may go on
// mutating. Strings and JSON longer than MaxLogValueBytes are truncated.
func value(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return Truncate(v.String(), protocol.MaxLogValueBytes)
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return int64(v.Duration()) // as slog's JSON handler: nanoseconds
	case slog.KindTime:
		return v.Time().Format(time.RFC3339Nano)
	}
	x := v.Any()
	if err, ok := x.(error); ok {
		return Truncate(err.Error(), protocol.MaxLogValueBytes)
	}
	raw, err := json.Marshal(x)
	if err != nil {
		return Truncate(fmt.Sprint(x), protocol.MaxLogValueBytes)
	}
	if len(raw) > protocol.MaxLogValueBytes {
		// Cut JSON is no longer JSON; keep it readable as a string.
		return Truncate(string(raw), protocol.MaxLogValueBytes)
	}
	return json.RawMessage(raw)
}

// Truncate shortens s to at most n bytes, on a UTF-8 boundary, marking the cut
// with "…".
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func level(l slog.Level) protocol.LogLevel {
	switch {
	case l < slog.LevelInfo:
		return protocol.LogDebug
	case l < slog.LevelWarn:
		return protocol.LogInfo
	case l < slog.LevelError:
		return protocol.LogWarn
	default:
		return protocol.LogError
	}
}
