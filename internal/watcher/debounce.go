package watcher

import (
	"sync"
	"time"
)

const maxDebounceEntries = 100_000

// RawEvent is an event from fanotify before debouncing.
type RawEvent struct {
	SyncPair  string
	Path      string // relative to sync pair root
	EventType string // create, modify, delete, rename, attrib
	RenameTo  string // only for rename events
	IsDir     bool
	Pid       int32
}

// debouncedEvent is the final coalesced event written to changelog.
type debouncedEvent struct {
	SyncPair  string
	Path      string
	EventType string
	RenameTo  string
	IsDir     bool
}

// Debouncer coalesces rapid events for the same path into a single entry.
type Debouncer struct {
	mu       sync.Mutex
	pending  map[string]*pendingEntry
	duration time.Duration
	out      chan debouncedEvent
}

type pendingEntry struct {
	event *RawEvent
	timer *time.Timer
}

// NewDebouncer creates a Debouncer with the given window duration.
func NewDebouncer(d time.Duration) *Debouncer {
	return &Debouncer{
		pending:  make(map[string]*pendingEntry),
		duration: d,
		out:      make(chan debouncedEvent, 1024),
	}
}

// Push feeds a new raw event into the debouncer.
// If the same (syncPair, path) already has a pending event, its timer resets.
func (db *Debouncer) Push(ev *RawEvent) {
	key := ev.SyncPair + "\x00" + ev.Path

	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.pending) >= maxDebounceEntries {
		db.flushLocked()
	}

	if pe, ok := db.pending[key]; ok {
		pe.event = ev
		pe.timer.Reset(db.duration)
		return
	}

	pe := &pendingEntry{event: ev}
	pe.timer = time.AfterFunc(db.duration, func() {
		db.mu.Lock()
		e, ok := db.pending[key]
		if ok {
			delete(db.pending, key)
		}
		db.mu.Unlock()

		if ok {
			db.out <- debouncedEvent{
				SyncPair:  e.event.SyncPair,
				Path:      e.event.Path,
				EventType: e.event.EventType,
				RenameTo:  e.event.RenameTo,
				IsDir:     e.event.IsDir,
			}
		}
	})
	db.pending[key] = pe
}

// Flush immediately emits all pending events and resets internal state.
func (db *Debouncer) Flush() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.flushLocked()
}

func (db *Debouncer) flushLocked() {
	for key, pe := range db.pending {
		pe.timer.Stop()
		db.out <- debouncedEvent{
			SyncPair:  pe.event.SyncPair,
			Path:      pe.event.Path,
			EventType: pe.event.EventType,
			RenameTo:  pe.event.RenameTo,
			IsDir:     pe.event.IsDir,
		}
		delete(db.pending, key)
	}
}

// Events returns the channel of coalesced events.
func (db *Debouncer) Events() <-chan debouncedEvent {
	return db.out
}
