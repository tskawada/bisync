package changelog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS changelog (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_pair     TEXT    NOT NULL,
    path          TEXT    NOT NULL,
    event_type    TEXT    NOT NULL,
    rename_to     TEXT,
    file_size     INTEGER,
    file_mtime    INTEGER,
    file_mode     INTEGER,
    is_dir        INTEGER NOT NULL DEFAULT 0,
    hlc_timestamp TEXT    NOT NULL,
    node_name     TEXT    NOT NULL,
    synced        INTEGER NOT NULL DEFAULT 0,
    peer_ack      INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_changelog_unsynced  ON changelog(synced) WHERE synced = 0;
CREATE INDEX IF NOT EXISTS idx_changelog_pair_path ON changelog(sync_pair, path);
CREATE INDEX IF NOT EXISTS idx_changelog_hlc       ON changelog(hlc_timestamp);
CREATE INDEX IF NOT EXISTS idx_changelog_created   ON changelog(created_at);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`

// Entry is a single changelog record.
type Entry struct {
	ID           int64
	SyncPair     string
	Path         string
	EventType    string
	RenameTo     string
	FileSize     int64
	FileMtime    int64
	FileMode     uint32
	IsDir        bool
	HLCTimestamp string
	NodeName     string
	Synced       bool
	PeerAck      bool
	CreatedAt    time.Time
}

// Store wraps the SQLite changelog database.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the changelog database at dbPath.
func Open(dbPath string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db %q: %w", dbPath, err)
	}
	// SQLite allows only one concurrent writer; serialise writes via single connection.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Append writes a new entry, setting HLCTimestamp automatically.
func (s *Store) Append(ctx context.Context, e *Entry) (int64, error) {
	e.HLCTimestamp = HLCNow()
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO changelog
		    (sync_pair, path, event_type, rename_to, file_size, file_mtime,
		     file_mode, is_dir, hlc_timestamp, node_name)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		e.SyncPair, e.Path, e.EventType, nullStr(e.RenameTo),
		e.FileSize, e.FileMtime, e.FileMode, boolInt(e.IsDir),
		e.HLCTimestamp, e.NodeName,
	)
	if err != nil {
		return 0, fmt.Errorf("append changelog: %w", err)
	}
	return res.LastInsertId()
}

// AppendWithHLC writes an entry using the provided HLC (used by recovery).
func (s *Store) AppendWithHLC(ctx context.Context, e *Entry) (int64, error) {
	if e.HLCTimestamp == "" {
		e.HLCTimestamp = HLCNow()
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO changelog
		    (sync_pair, path, event_type, rename_to, file_size, file_mtime,
		     file_mode, is_dir, hlc_timestamp, node_name)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		e.SyncPair, e.Path, e.EventType, nullStr(e.RenameTo),
		e.FileSize, e.FileMtime, e.FileMode, boolInt(e.IsDir),
		e.HLCTimestamp, e.NodeName,
	)
	if err != nil {
		return 0, fmt.Errorf("append changelog with HLC: %w", err)
	}
	return res.LastInsertId()
}

// UnsyncedEntries returns entries with synced=0 for the given sync pair
// (empty string means all pairs), ordered by HLC.
func (s *Store) UnsyncedEntries(ctx context.Context, syncPair string) ([]*Entry, error) {
	q := baseSelect + ` WHERE synced = 0`
	var args []any
	if syncPair != "" {
		q += ` AND sync_pair = ?`
		args = append(args, syncPair)
	}
	q += ` ORDER BY hlc_timestamp ASC`
	return s.query(ctx, q, args...)
}

// EntriesSinceHLC returns unsynced entries whose HLC is after sinceHLC.
func (s *Store) EntriesSinceHLC(ctx context.Context, sinceHLC string) ([]*Entry, error) {
	q := baseSelect + ` WHERE synced = 0`
	var args []any
	if sinceHLC != "" {
		q += ` AND hlc_timestamp > ?`
		args = append(args, sinceHLC)
	}
	q += ` ORDER BY hlc_timestamp ASC`
	return s.query(ctx, q, args...)
}

// MarkSynced sets synced=1 for the given IDs.
func (s *Store) MarkSynced(ctx context.Context, ids []int64) error {
	return s.updateFlag(ctx, "synced", ids)
}

// MarkPeerAck sets peer_ack=1 for the given IDs.
func (s *Store) MarkPeerAck(ctx context.Context, ids []int64) error {
	return s.updateFlag(ctx, "peer_ack", ids)
}

func (s *Store) updateFlag(ctx context.Context, col string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `UPDATE changelog SET `+col+` = 1 WHERE id = ?`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, id); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// GetMetadata reads a metadata value by key.
func (s *Store) GetMetadata(ctx context.Context, key string) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return v, err == nil, err
}

// SetMetadata upserts a metadata key/value.
func (s *Store) SetMetadata(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO metadata(key,value) VALUES(?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, value,
	)
	return err
}

// CurrentHLC returns the highest HLC timestamp recorded in the changelog.
func (s *Store) CurrentHLC(ctx context.Context) (string, error) {
	var v sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(hlc_timestamp) FROM changelog`).Scan(&v); err != nil {
		return "", err
	}
	if !v.Valid {
		return HLCNow(), nil
	}
	return v.String, nil
}

// HasUnsyncedForPath reports whether there is any unsynced entry for path.
func (s *Store) HasUnsyncedForPath(ctx context.Context, syncPair, path string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM changelog WHERE sync_pair=? AND path=? AND synced=0`,
		syncPair, path,
	).Scan(&n)
	return n > 0, err
}

const baseSelect = `
	SELECT id, sync_pair, path, event_type, COALESCE(rename_to,''),
	       COALESCE(file_size,0), COALESCE(file_mtime,0), COALESCE(file_mode,0),
	       is_dir, hlc_timestamp, node_name, synced, peer_ack,
	       created_at
	FROM changelog`

func (s *Store) query(ctx context.Context, q string, args ...any) ([]*Entry, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Entry
	for rows.Next() {
		e := &Entry{}
		var isDir, synced, peerAck int
		var createdAt string
		if err := rows.Scan(
			&e.ID, &e.SyncPair, &e.Path, &e.EventType, &e.RenameTo,
			&e.FileSize, &e.FileMtime, &e.FileMode, &isDir,
			&e.HLCTimestamp, &e.NodeName, &synced, &peerAck, &createdAt,
		); err != nil {
			return nil, err
		}
		e.IsDir = isDir != 0
		e.Synced = synced != 0
		e.PeerAck = peerAck != 0
		e.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		out = append(out, e)
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
