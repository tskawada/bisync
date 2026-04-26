package changelog

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenance_deletesOldSyncedEntries(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "maint.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Insert an entry and immediately mark it synced+peer_acked.
	id, _ := s.Append(ctx, &Entry{SyncPair: "media", Path: "a.txt", EventType: "modify", NodeName: "albus"})
	s.MarkSynced(ctx, []int64{id})
	s.MarkPeerAck(ctx, []int64{id})

	// Backdate the created_at so it falls before the retention window.
	_, err = s.db.ExecContext(ctx,
		`UPDATE changelog SET created_at = ? WHERE id = ?`,
		time.Now().UTC().AddDate(0, 0, -100).Format(time.RFC3339Nano), id,
	)
	if err != nil {
		t.Fatal(err)
	}

	m := NewMaintenance(s, 90, 30)
	m.deleteOldSynced(ctx)

	entries, _ := s.UnsyncedEntries(ctx, "")
	for _, e := range entries {
		if e.ID == id {
			t.Error("old synced entry should have been deleted")
		}
	}
	// Verify via direct count.
	var n int
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM changelog WHERE id = ?`, id).Scan(&n)
	if n != 0 {
		t.Error("expected 0 rows after maintenance, got non-zero")
	}
}

func TestMaintenance_keepsRecentSyncedEntries(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "maint2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, _ := s.Append(ctx, &Entry{SyncPair: "media", Path: "b.txt", EventType: "create", NodeName: "albus"})
	s.MarkSynced(ctx, []int64{id})
	s.MarkPeerAck(ctx, []int64{id})
	// created_at is now (default) — within the 90-day window.

	m := NewMaintenance(s, 90, 30)
	m.deleteOldSynced(ctx)

	var n int
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM changelog WHERE id = ?`, id).Scan(&n)
	if n != 1 {
		t.Error("recent synced entry should be retained")
	}
}

func TestMaintenance_deletesStaleTombstones(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "tomb.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, _ := s.Append(ctx, &Entry{SyncPair: "media", Path: "c.txt", EventType: "delete", NodeName: "albus"})
	_, err = s.db.ExecContext(ctx,
		`UPDATE changelog SET created_at = ? WHERE id = ?`,
		time.Now().UTC().AddDate(0, 0, -40).Format(time.RFC3339Nano), id,
	)
	if err != nil {
		t.Fatal(err)
	}

	m := NewMaintenance(s, 90, 30)
	m.deleteStaleTombstones(ctx)

	var n int
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM changelog WHERE id = ?`, id).Scan(&n)
	if n != 0 {
		t.Error("stale tombstone should have been deleted")
	}
}

func TestMaintenance_keepsRecentTombstones(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "tomb2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, _ := s.Append(ctx, &Entry{SyncPair: "media", Path: "d.txt", EventType: "delete", NodeName: "albus"})

	m := NewMaintenance(s, 90, 30)
	m.deleteStaleTombstones(ctx)

	var n int
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM changelog WHERE id = ?`, id).Scan(&n)
	if n != 1 {
		t.Error("recent tombstone should be retained")
	}
}
