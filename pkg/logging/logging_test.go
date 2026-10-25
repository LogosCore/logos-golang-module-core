package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

func TestNewFormats(t *testing.T) {
	var buf bytes.Buffer
	log, err := New("", &buf, slog.LevelInfo, nil)
	if err != nil {
		t.Fatal(err)
	}
	log.Debug("hidden")
	log.Info("served", "status", 200)
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("default format is not JSON: %q (%v)", buf.String(), err)
	}
	if rec["msg"] != "served" || rec["status"] != float64(200) {
		t.Errorf("record = %v", rec)
	}

	buf.Reset()
	log, err = New("TEXT", &buf, slog.LevelInfo, nil)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("served", "status", 200)
	if !strings.Contains(buf.String(), "msg=served status=200") {
		t.Errorf("text output = %q", buf.String())
	}

	if _, err := New("xml", &buf, slog.LevelInfo, nil); err == nil {
		t.Error("unknown format accepted")
	}
}

func TestHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("User-Agent", "curl/8")
	h.Add("Accept", "a/b")
	h.Add("Accept", "c/d")
	h.Set("Authorization", "Bearer secret")

	var buf bytes.Buffer
	log, _ := New(FormatJSON, &buf, slog.LevelInfo, nil)
	log.Info("req", Headers(h))
	var rec struct {
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"user-agent": "curl/8", "accept": "a/b, c/d", "authorization": "Bearer secret"}
	if len(rec.Headers) != len(want) {
		t.Fatalf("headers = %v", rec.Headers)
	}
	for k, v := range want {
		if rec.Headers[k] != v {
			t.Errorf("%s = %q, want %q", k, rec.Headers[k], v)
		}
	}
}
