// Package logging sets up a module's log/slog logger the same way in every
// module: JSON or text on one writer at a managed level, teed into a
// logstream.Hub for core's live log tail, plus helpers for common attributes.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/logoscore/logos-golang-module-core/pkg/logstream"
)

// Output formats.
const (
	FormatJSON = "json" // one JSON object per line; for log aggregators
	FormatText = "text" // logfmt key=value; for reading in a terminal
)

// New returns a logger writing format to w at level. With a non-nil hub,
// every record also reaches the live log tail (which may ask for records
// below level; w never gets those). An empty format means JSON.
func New(format string, w io.Writer, level slog.Leveler, hub *logstream.Hub) (*slog.Logger, error) {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", FormatJSON:
		h = slog.NewJSONHandler(w, opts)
	case FormatText:
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q (want %q or %q)", format, FormatJSON, FormatText)
	}
	if hub != nil {
		h = hub.Handler(h)
	}
	return slog.New(h), nil
}

// Headers returns every header in h as a "headers" group, keyed by the
// lowercased header name, multiple values joined with ", ". Values are logged
// as received: nothing is redacted.
func Headers(h http.Header) slog.Attr {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	attrs := make([]any, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, slog.String(strings.ToLower(k), strings.Join(h[k], ", ")))
	}
	return slog.Group("headers", attrs...)
}
