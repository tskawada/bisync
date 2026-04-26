package notify

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tskawada/bisync/internal/config"
)

// --- helpers ---

func newCfg(handlers ...config.NotifyHandlerConfig) *config.Config {
	return &config.Config{
		Node:   config.NodeConfig{Name: "albus"},
		Peer:   config.PeerConfig{Name: "tina"},
		Notify: config.NotifyConfig{Handlers: handlers},
	}
}

// --- Notifier / dispatcher ---

func TestNotifier_routesOnlyMatchingEvents(t *testing.T) {
	received := make(chan *Payload, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p Payload
		json.NewDecoder(r.Body).Decode(&p)
		received <- &p
	}))
	defer srv.Close()

	cfg := newCfg(config.NotifyHandlerConfig{
		Type:   "webhook",
		URL:    srv.URL,
		Events: []string{"conflict"},
	})
	n := New(cfg)

	n.Error("media", "a.txt", "boom")   // should NOT reach webhook
	n.Conflict("media", "b.txt", "c!", nil) // should reach webhook

	if len(received) != 1 {
		t.Fatalf("expected 1 webhook call, got %d", len(received))
	}
	p := <-received
	if p.Event != EventConflict {
		t.Errorf("expected conflict event, got %s", p.Event)
	}
	if p.Node != "albus" || p.Peer != "tina" {
		t.Errorf("unexpected node/peer: %s/%s", p.Node, p.Peer)
	}
}

func TestNotifier_multipleHandlers(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
	}))
	defer srv.Close()

	cfg := newCfg(
		config.NotifyHandlerConfig{Type: "webhook", URL: srv.URL, Events: []string{"error"}},
		config.NotifyHandlerConfig{Type: "webhook", URL: srv.URL, Events: []string{"error", "conflict"}},
	)
	n := New(cfg)
	n.Error("", "", "oops")
	if calls != 2 {
		t.Errorf("expected 2 handler calls, got %d", calls)
	}
}

func TestNotifier_noHandlersDoesNotPanic(t *testing.T) {
	n := New(newCfg())
	n.Conflict("media", "x.txt", "msg", nil)
	n.Error("media", "x.txt", "err")
	n.TransferFailure("media", "x.txt", "fail")
	n.PeerUnreachable("down")
	n.RecoveryComplete("ok")
}

// --- webhook ---

func TestWebhook_sendsJSON(t *testing.T) {
	var captured Payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected application/json, got %s", r.Header.Get("Content-Type"))
		}
		json.NewDecoder(r.Body).Decode(&captured)
	}))
	defer srv.Close()

	wh := newWebhook(config.NotifyHandlerConfig{
		URL:    srv.URL,
		Events: []string{"conflict"},
	})
	p := &Payload{
		Event:    EventConflict,
		Node:     "albus",
		SyncPair: "media",
		Path:     "photos/a.jpg",
		Message:  "test",
	}
	if err := wh.Send(p); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if captured.Path != "photos/a.jpg" {
		t.Errorf("unexpected path: %q", captured.Path)
	}
}

func TestWebhook_returnsErrorOn4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	wh := newWebhook(config.NotifyHandlerConfig{URL: srv.URL, Events: []string{"error"}})
	if err := wh.Send(&Payload{Event: EventError}); err == nil {
		t.Error("expected error for 5xx response")
	}
}

func TestWebhook_handlesEvent(t *testing.T) {
	wh := newWebhook(config.NotifyHandlerConfig{Events: []string{"conflict", "error"}})
	if !wh.HandlesEvent(EventConflict) {
		t.Error("expected conflict to be handled")
	}
	if wh.HandlesEvent(EventTransferFailure) {
		t.Error("expected transfer_failure to be unhandled")
	}
}

// --- command ---

func TestCommand_executesWithEnvVars(t *testing.T) {
	// Write a helper script that captures env vars into a file.
	dir := t.TempDir()
	outFile := filepath.Join(dir, "env.txt")
	script := filepath.Join(dir, "notify.sh")
	scriptContent := "#!/bin/sh\necho \"$BISYNC_EVENT $BISYNC_NODE $BISYNC_PATH\" > " + outFile + "\n"
	if err := os.WriteFile(script, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	cmd := newCommand(config.NotifyHandlerConfig{
		Command: script,
		Events:  []string{"conflict"},
	})
	p := &Payload{
		Event: EventConflict,
		Node:  "albus",
		Path:  "photos/img.jpg",
	}
	if err := cmd.Send(p); err != nil {
		t.Fatalf("Send: %v", err)
	}
	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	out := strings.TrimSpace(string(raw))
	if !strings.Contains(out, "conflict") || !strings.Contains(out, "albus") {
		t.Errorf("unexpected output: %q", out)
	}
}

func TestCommand_returnsErrorOnNonZeroExit(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fail.sh")
	os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0755)

	cmd := newCommand(config.NotifyHandlerConfig{Command: script, Events: []string{"error"}})
	if err := cmd.Send(&Payload{Event: EventError}); err == nil {
		t.Error("expected error for non-zero exit")
	}
}

func TestCommand_handlesEvent(t *testing.T) {
	cmd := newCommand(config.NotifyHandlerConfig{Events: []string{"transfer_failure"}})
	if !cmd.HandlesEvent(EventTransferFailure) {
		t.Error("expected transfer_failure to be handled")
	}
	if cmd.HandlesEvent(EventConflict) {
		t.Error("expected conflict to be unhandled")
	}
}
