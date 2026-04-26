package changelog

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// Maintenance performs periodic changelog cleanup.
type Maintenance struct {
	store         *Store
	retentionDays int
	tombstoneDays int
}

func NewMaintenance(store *Store, retentionDays, tombstoneDays int) *Maintenance {
	return &Maintenance{
		store:         store,
		retentionDays: retentionDays,
		tombstoneDays: tombstoneDays,
	}
}

// Run executes one maintenance pass.
func (m *Maintenance) Run(ctx context.Context) {
	if err := m.deleteOldSynced(ctx); err != nil {
		log.Error().Err(err).Msg("maintenance: delete old synced entries")
	}
	if err := m.deleteStaleTombstones(ctx); err != nil {
		log.Error().Err(err).Msg("maintenance: delete stale tombstones")
	}
}

func (m *Maintenance) deleteOldSynced(ctx context.Context) error {
	cutoff := time.Now().UTC().AddDate(0, 0, -m.retentionDays).Format(time.RFC3339Nano)
	res, err := m.store.db.ExecContext(ctx,
		`DELETE FROM changelog WHERE synced=1 AND peer_ack=1 AND created_at < ?`, cutoff,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Info().Int64("count", n).Msg("maintenance: removed old synced entries")
	}
	return nil
}

func (m *Maintenance) deleteStaleTombstones(ctx context.Context) error {
	cutoff := time.Now().UTC().AddDate(0, 0, -m.tombstoneDays).Format(time.RFC3339Nano)
	res, err := m.store.db.ExecContext(ctx,
		`DELETE FROM changelog WHERE event_type='delete' AND created_at < ?`, cutoff,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Info().Int64("count", n).Msg("maintenance: removed stale tombstones")
	}
	return nil
}

// RunScheduled blocks until ctx is cancelled, running maintenance once per day.
func (m *Maintenance) RunScheduled(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Run(ctx)
		}
	}
}
