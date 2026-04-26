package watcher

import (
	"testing"
	"time"
)

func TestFilter_excluded(t *testing.T) {
	f, err := NewFilter([]string{"*.tmp", ".Trash-*", "@eaDir/**"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		path     string
		expected bool
	}{
		{"photos/img.jpg", false},
		{"photos/img.tmp", true},
		{".Trash-1000/file", true},
		{"@eaDir/sub/thumb", true},
		{"docs/report.pdf", false},
	}
	for _, c := range cases {
		if got := f.Excluded(c.path); got != c.expected {
			t.Errorf("Excluded(%q) = %v, want %v", c.path, got, c.expected)
		}
	}
}

func TestDebouncer_coalesces(t *testing.T) {
	db := NewDebouncer(50 * time.Millisecond)
	ev := &RawEvent{SyncPair: "media", Path: "a.jpg", EventType: "modify"}

	// Push the same path three times rapidly.
	db.Push(ev)
	db.Push(ev)
	db.Push(ev)

	// Should coalesce into one event after the debounce window.
	select {
	case got := <-db.Events():
		if got.Path != "a.jpg" {
			t.Errorf("unexpected path %q", got.Path)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for debounced event")
	}

	// No second event should arrive.
	select {
	case <-db.Events():
		t.Fatal("unexpected second event")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDebouncer_flush(t *testing.T) {
	db := NewDebouncer(10 * time.Second) // long window
	db.Push(&RawEvent{SyncPair: "media", Path: "b.jpg", EventType: "create"})
	db.Push(&RawEvent{SyncPair: "media", Path: "c.jpg", EventType: "create"})

	db.Flush()

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-db.Events():
			got[ev.Path] = true
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timed out after %d events", i)
		}
	}
	if !got["b.jpg"] || !got["c.jpg"] {
		t.Errorf("missing events: %v", got)
	}
}
