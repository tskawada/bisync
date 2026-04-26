package reconciler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	grpcclient "github.com/tskawada/bisync/internal/grpc"
)

// Transferrer abstracts the file transfer layer so reconciler is testable.
type Transferrer interface {
	Send(ctx context.Context, sp config.SyncPairConfig, relPaths []string) error
}

// Notifier abstracts the notification layer.
type Notifier interface {
	conflictNotifier
	Error(syncPair, path, message string)
}

// Reconciler orchestrates changelog exchange and conflict resolution.
type Reconciler struct {
	cfg      *config.Config
	store    *changelog.Store
	client   *grpcclient.Client
	transfer Transferrer
	notify   Notifier
	resolver *conflictResolver
	mu       sync.Mutex // ensures one reconcile cycle at a time
	// blockedPaths contains paths blocked by "manual" conflict policy.
	blockedPaths sync.Map
}

// New creates a Reconciler.
func New(
	cfg *config.Config,
	store *changelog.Store,
	client *grpcclient.Client,
	transfer Transferrer,
	notify Notifier,
) *Reconciler {
	r := &Reconciler{
		cfg:      cfg,
		store:    store,
		client:   client,
		transfer: transfer,
		notify:   notify,
	}
	r.resolver = newConflictResolver(cfg, store, notify)
	return r
}

// Run executes one reconciliation cycle.
// If another cycle is already running it returns immediately.
func (r *Reconciler) Run(ctx context.Context) error {
	if !r.mu.TryLock() {
		return nil
	}
	defer r.mu.Unlock()

	log.Debug().Msg("reconcile: start")

	// 1. Fetch peer's unsynced entries.
	resp, err := r.client.ExchangeChangelog(ctx, &grpcclient.ChangelogRequest{})
	if err != nil {
		return fmt.Errorf("exchange changelog: %w", err)
	}

	// 2. Fetch local unsynced entries.
	localEntries, err := r.store.UnsyncedEntries(ctx, "")
	if err != nil {
		return fmt.Errorf("read local changelog: %w", err)
	}

	// 3. Build maps keyed by (syncPair, path).
	localMap := groupEntries(localEntries)
	remoteMap := groupProtoEntries(resp.Entries)

	// 4. Collect all unique (syncPair, path) keys.
	allKeys := mergeKeys(localMap, remoteMap)

	var (
		localSyncedIDs  []int64
		remoteSyncedIDs []int64
	)

	// 5. Process each path.
	for key, sp := range allKeys {
		localEntry := localMap[key]
		remoteEntry := remoteMap[key]

		// Skip paths blocked by manual conflict.
		if _, blocked := r.blockedPaths.Load(key); blocked {
			// Unblock if one side has been deleted.
			if localEntry != nil && localEntry.EventType == "delete" &&
				(remoteEntry == nil || remoteEntry.EventType == "delete") {
				r.blockedPaths.Delete(key)
				log.Info().Str("path", key).Msg("manual conflict unblocked (file deleted)")
			}
			continue
		}

		spCfg, ok := r.cfg.SyncPairByName(sp)
		if !ok {
			continue
		}

		switch {
		case localEntry != nil && remoteEntry == nil:
			// Local-only change: push to remote.
			if err := r.pushToRemote(ctx, spCfg, localEntry); err != nil {
				log.Error().Err(err).Str("path", localEntry.Path).Msg("push failed")
				r.notify.Error(sp, localEntry.Path, err.Error())
				continue
			}
			localSyncedIDs = append(localSyncedIDs, localEntry.ID)

		case localEntry == nil && remoteEntry != nil:
			// Remote-only change: pull from remote.
			if err := r.pullFromRemote(ctx, spCfg, remoteEntry); err != nil {
				log.Error().Err(err).Str("path", remoteEntry.Path).Msg("pull failed")
				r.notify.Error(sp, remoteEntry.Path, err.Error())
				continue
			}
			remoteSyncedIDs = append(remoteSyncedIDs, remoteEntry.ID)

		case localEntry != nil && remoteEntry != nil:
			// Both modified: conflict.
			if localEntry.EventType == "delete" && remoteEntry.EventType == "delete" {
				// Both deleted — nothing to do.
				localSyncedIDs = append(localSyncedIDs, localEntry.ID)
				remoteSyncedIDs = append(remoteSyncedIDs, remoteEntry.ID)
				continue
			}

			// Handle rename specially.
			if localEntry.EventType == "rename" {
				if err := r.applyRename(ctx, spCfg, localEntry); err != nil {
					log.Error().Err(err).Str("path", localEntry.Path).Msg("rename failed")
					continue
				}
				localSyncedIDs = append(localSyncedIDs, localEntry.ID)
				continue
			}

			winner, result, err := r.resolver.resolve(ctx, spCfg, localEntry, protoToEntry(remoteEntry))
			if err != nil {
				log.Error().Err(err).Str("path", localEntry.Path).Msg("conflict resolution failed")
				continue
			}
			if result == conflictBlocked {
				r.blockedPaths.Store(key, true)
				continue
			}

			if winner == localEntry {
				if err := r.pushToRemote(ctx, spCfg, localEntry); err != nil {
					log.Error().Err(err).Str("path", localEntry.Path).Msg("push winner failed")
					continue
				}
			} else {
				if err := r.pullFromRemote(ctx, spCfg, remoteEntry); err != nil {
					log.Error().Err(err).Str("path", remoteEntry.Path).Msg("pull winner failed")
					continue
				}
			}
			localSyncedIDs = append(localSyncedIDs, localEntry.ID)
			remoteSyncedIDs = append(remoteSyncedIDs, remoteEntry.ID)
		}
	}

	// 6. Mark local entries as synced.
	if err := r.store.MarkSynced(ctx, localSyncedIDs); err != nil {
		log.Error().Err(err).Msg("mark local synced failed")
	}

	// 7. Acknowledge remote entries.
	if len(remoteSyncedIDs) > 0 {
		if _, err := r.client.AckTransfer(ctx, &grpcclient.AckRequest{
			EntryIDs: remoteSyncedIDs,
			NodeName: r.cfg.Node.Name,
		}); err != nil {
			log.Error().Err(err).Msg("ack remote entries failed")
		}
	}

	log.Debug().
		Int("local_synced", len(localSyncedIDs)).
		Int("remote_acked", len(remoteSyncedIDs)).
		Msg("reconcile: done")
	return nil
}

func (r *Reconciler) pushToRemote(ctx context.Context, sp config.SyncPairConfig, e *changelog.Entry) error {
	switch e.EventType {
	case "delete":
		resp, err := r.client.DeleteFile(ctx, &grpcclient.DeleteRequest{
			SyncPair: sp.Name,
			Path:     e.Path,
			NodeName: r.cfg.Node.Name,
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("remote delete rejected: %s", resp.Error)
		}
		return nil
	case "rename":
		return r.applyRename(ctx, sp, e)
	default:
		// Skip transfer if the file no longer exists locally — a subsequent
		// delete entry will clean up the peer side.
		fullPath := filepath.Join(sp.LocalPath, e.Path)
		if _, err := os.Lstat(fullPath); os.IsNotExist(err) {
			log.Warn().Str("path", e.Path).Str("type", e.EventType).
				Msg("reconciler: file gone before transfer; skipping (delete entry will follow)")
			return nil
		}
		return r.transfer.Send(ctx, sp, []string{e.Path})
	}
}

func (r *Reconciler) pullFromRemote(ctx context.Context, sp config.SyncPairConfig, e *grpcclient.ChangelogEntry) error {
	local := protoToEntry(e)
	switch e.EventType {
	case "delete":
		resp, err := r.client.DeleteFile(ctx, &grpcclient.DeleteRequest{
			SyncPair: sp.Name,
			Path:     e.Path,
			NodeName: r.cfg.Node.Name,
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("local delete via peer rejected: %s", resp.Error)
		}
		return nil
	case "rename":
		return r.applyRename(ctx, sp, local)
	default:
		// create/modify: the originating node's pushToRemote handles the actual
		// rsync transfer. We just ack the entry here so the originator knows we
		// received it. Calling Send in the wrong direction (local→peer) was causing
		// unnecessary rsync failures and multi-minute delays.
		return nil
	}
}

func (r *Reconciler) applyRename(ctx context.Context, sp config.SyncPairConfig, e *changelog.Entry) error {
	if e.RenameTo == "" {
		// Stale entry from a previous version — mark synced and skip.
		log.Warn().Str("path", e.Path).Msg("rename entry has no RenameTo; skipping")
		return nil
	}
	resp, err := r.client.RenameFile(ctx, &grpcclient.RenameRequest{
		SyncPair: sp.Name,
		FromPath: e.Path,
		ToPath:   e.RenameTo,
		NodeName: r.cfg.Node.Name,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("remote rename rejected: %s", resp.Error)
	}
	return nil
}

type pairKey = string // "syncPair\x00path"

func groupEntries(entries []*changelog.Entry) map[pairKey]*changelog.Entry {
	m := make(map[pairKey]*changelog.Entry, len(entries))
	for _, e := range entries {
		k := e.SyncPair + "\x00" + e.Path
		if prev, ok := m[k]; !ok || changelog.CompareHLC(e.HLCTimestamp, prev.HLCTimestamp) > 0 {
			m[k] = e
		}
	}
	return m
}

func groupProtoEntries(entries []*grpcclient.ChangelogEntry) map[pairKey]*grpcclient.ChangelogEntry {
	m := make(map[pairKey]*grpcclient.ChangelogEntry, len(entries))
	for _, e := range entries {
		k := e.SyncPair + "\x00" + e.Path
		if prev, ok := m[k]; !ok || changelog.CompareHLC(e.HLCTimestamp, prev.HLCTimestamp) > 0 {
			m[k] = e
		}
	}
	return m
}

// mergeKeys returns a map from pairKey to sync pair name.
func mergeKeys(local map[pairKey]*changelog.Entry, remote map[pairKey]*grpcclient.ChangelogEntry) map[pairKey]string {
	m := make(map[pairKey]string)
	for k, e := range local {
		m[k] = e.SyncPair
	}
	for k, e := range remote {
		m[k] = e.SyncPair
	}
	return m
}

func protoToEntry(e *grpcclient.ChangelogEntry) *changelog.Entry {
	return &changelog.Entry{
		ID:           e.ID,
		SyncPair:     e.SyncPair,
		Path:         e.Path,
		EventType:    e.EventType,
		RenameTo:     e.RenameTo,
		FileSize:     e.FileSize,
		FileMtime:    e.FileMtime,
		FileMode:     e.FileMode,
		IsDir:        e.IsDir,
		HLCTimestamp: e.HLCTimestamp,
		NodeName:     e.NodeName,
	}
}
