package recovery

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
)

const metaKeyLastShutdown = "last_shutdown"

// Manager handles catchup scan on daemon restart.
type Manager struct {
	cfg      *config.Config
	store    *changelog.Store
	nodeName string
}

// New creates a recovery Manager.
func New(cfg *config.Config, store *changelog.Store) *Manager {
	return &Manager{
		cfg:      cfg,
		store:    store,
		nodeName: cfg.Node.Name,
	}
}

// RecordShutdown records the current timestamp as last_shutdown in metadata.
func (m *Manager) RecordShutdown(ctx context.Context) error {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	if err := m.store.SetMetadata(ctx, metaKeyLastShutdown, ts); err != nil {
		return fmt.Errorf("record shutdown: %w", err)
	}
	return nil
}

// CatchupScan scans local sync pair directories for files modified since the
// last shutdown and appends them to the changelog. If there is no last_shutdown
// record (first boot), it logs a warning and returns without scanning.
func (m *Manager) CatchupScan(ctx context.Context) error {
	lastShutdownStr, ok, err := m.store.GetMetadata(ctx, metaKeyLastShutdown)
	if err != nil {
		return fmt.Errorf("get last_shutdown: %w", err)
	}
	if !ok {
		log.Warn().Msg("recovery: no last_shutdown recorded — first boot, skipping catchup scan")
		log.Warn().Msg("recovery: existing files will NOT be synced; run rsync manually if needed")
		return nil
	}

	lastShutdown, err := time.Parse(time.RFC3339Nano, lastShutdownStr)
	if err != nil {
		return fmt.Errorf("parse last_shutdown %q: %w", lastShutdownStr, err)
	}
	log.Info().Time("last_shutdown", lastShutdown).Msg("recovery: starting catchup scan")

	workers := m.cfg.Recovery.CatchupWorkers
	if workers <= 0 {
		workers = 4
	}

	for _, sp := range m.cfg.SyncPairs {
		if err := m.scanPair(ctx, sp, lastShutdown, workers); err != nil {
			log.Error().Err(err).Str("pair", sp.Name).Msg("recovery: scan failed")
		}
	}
	log.Info().Msg("recovery: catchup scan complete")
	return nil
}

func (m *Manager) scanPair(ctx context.Context, sp config.SyncPairConfig, since time.Time, workers int) error {
	type fileInfo struct {
		rel  string
		info fs.FileInfo
	}

	fileCh := make(chan fileInfo, 128)
	var walkErr error

	go func() {
		defer close(fileCh)
		err := filepath.WalkDir(sp.LocalPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil || ctx.Err() != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(sp.LocalPath, path)
			if err != nil {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return nil
			}
			if info.ModTime().After(since) {
				fileCh <- fileInfo{rel: rel, info: info}
			}
			return nil
		})
		if err != nil {
			walkErr = err
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range fileCh {
				e := &changelog.Entry{
					SyncPair:     sp.Name,
					Path:         fi.rel,
					EventType:    "modify",
					FileSize:     fi.info.Size(),
					FileMtime:    fi.info.ModTime().Unix(),
					FileMode:     uint32(fi.info.Mode()),
					HLCTimestamp: changelog.HLCNow(),
					NodeName:     m.nodeName,
				}
				if _, err := m.store.AppendWithHLC(ctx, e); err != nil {
					log.Error().Err(err).Str("path", fi.rel).Msg("recovery: append failed")
				}
			}
		}()
	}
	wg.Wait()
	return walkErr
}
