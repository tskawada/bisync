package changelog

import (
	"context"
	"testing"
	"time"
)

func TestUnsyncedEntries_filtersBySyncPair(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	s.Append(ctx, &Entry{SyncPair: "media", Path: "a.jpg", EventType: "modify", NodeName: "albus"})
	s.Append(ctx, &Entry{SyncPair: "docs", Path: "b.pdf", EventType: "create", NodeName: "albus"})

	entries, err := s.UnsyncedEntries(ctx, "media")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].SyncPair != "media" {
		t.Errorf("expected 1 media entry, got %+v", entries)
	}
}

func TestMarkPeerAck(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	id, _ := s.Append(ctx, &Entry{SyncPair: "media", Path: "x.jpg", EventType: "modify", NodeName: "albus"})
	s.MarkSynced(ctx, []int64{id})
	if err := s.MarkPeerAck(ctx, []int64{id}); err != nil {
		t.Fatalf("MarkPeerAck: %v", err)
	}
	var peerAck int
	s.db.QueryRowContext(ctx, `SELECT peer_ack FROM changelog WHERE id = ?`, id).Scan(&peerAck)
	if peerAck != 1 {
		t.Error("expected peer_ack=1 after MarkPeerAck")
	}
}

func TestMarkSynced_emptyList(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	if err := s.MarkSynced(ctx, nil); err != nil {
		t.Errorf("MarkSynced with empty list should not error: %v", err)
	}
}

func TestHasUnsyncedForPath(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	s.Append(ctx, &Entry{SyncPair: "media", Path: "check.jpg", EventType: "modify", NodeName: "albus"})

	has, err := s.HasUnsyncedForPath(ctx, "media", "check.jpg")
	if err != nil || !has {
		t.Errorf("expected true, got has=%v err=%v", has, err)
	}

	has2, _ := s.HasUnsyncedForPath(ctx, "media", "other.jpg")
	if has2 {
		t.Error("expected false for path with no entry")
	}
}

func TestEntriesSinceHLC(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	s.Append(ctx, &Entry{SyncPair: "media", Path: "old.jpg", EventType: "modify", NodeName: "albus"})
	barrier := HLCNow()
	time.Sleep(time.Millisecond)
	s.Append(ctx, &Entry{SyncPair: "media", Path: "new.jpg", EventType: "modify", NodeName: "albus"})

	entries, err := s.EntriesSinceHLC(ctx, barrier)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Path == "old.jpg" {
			t.Error("old.jpg should be excluded by SinceHLC")
		}
	}
	found := false
	for _, e := range entries {
		if e.Path == "new.jpg" {
			found = true
		}
	}
	if !found {
		t.Error("new.jpg should be included after barrier HLC")
	}
}

func TestCurrentHLC_emptyStore(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	hlc, err := s.CurrentHLC(ctx)
	if err != nil {
		t.Fatalf("CurrentHLC: %v", err)
	}
	if hlc == "" {
		t.Error("expected non-empty HLC even for empty store")
	}
}

func TestCurrentHLC_returnsMaxHLC(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	s.Append(ctx, &Entry{SyncPair: "media", Path: "a.jpg", EventType: "modify", NodeName: "albus"})
	time.Sleep(time.Millisecond)
	id, _ := s.Append(ctx, &Entry{SyncPair: "media", Path: "b.jpg", EventType: "modify", NodeName: "albus"})

	var hlcB string
	s.db.QueryRowContext(ctx, `SELECT hlc_timestamp FROM changelog WHERE id = ?`, id).Scan(&hlcB)

	maxHLC, err := s.CurrentHLC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if CompareHLC(maxHLC, hlcB) < 0 {
		t.Errorf("CurrentHLC %s should be >= most recent entry %s", maxHLC, hlcB)
	}
}

func TestAppend_setsHLCAutomatically(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	e := &Entry{SyncPair: "media", Path: "auto.jpg", EventType: "create", NodeName: "albus"}
	s.Append(ctx, e)
	if e.HLCTimestamp == "" {
		t.Error("Append should set HLCTimestamp on the entry")
	}
	_, _, err := ParseHLC(e.HLCTimestamp)
	if err != nil {
		t.Errorf("HLCTimestamp is not valid HLC: %v", err)
	}
}

func TestAppendWithHLC_usesProvidedHLC(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	given := FormatHLC(time.Now().UTC(), 0x1234)
	e := &Entry{
		SyncPair: "media", Path: "manual.jpg", EventType: "modify",
		NodeName: "albus", HLCTimestamp: given,
	}
	s.AppendWithHLC(ctx, e)

	entries, _ := s.UnsyncedEntries(ctx, "media")
	if len(entries) == 0 || entries[0].HLCTimestamp != given {
		t.Errorf("expected HLC %s, got %v", given, entries)
	}
}
