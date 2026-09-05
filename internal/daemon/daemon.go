package daemon

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	bisyncgrpc "github.com/tskawada/bisync/internal/grpc"
	"github.com/tskawada/bisync/internal/notify"
	"github.com/tskawada/bisync/internal/reconciler"
	"github.com/tskawada/bisync/internal/recovery"
	"github.com/tskawada/bisync/internal/transfer"
	"github.com/tskawada/bisync/internal/watcher"
)

const (
	shutdownTimeout  = 60 * time.Second
	reconcileChanCap = 8
)

// Daemon is the top-level bisync daemon.
type Daemon struct {
	cfg      *config.Config
	store    *changelog.Store
	notifier *notify.Notifier
	dryRun   bool
	bgWg     sync.WaitGroup // tracks background goroutines that access store
}

// New creates a Daemon from the given config.
func New(cfg *config.Config, dryRun bool) (*Daemon, error) {
	setupLogging(cfg)

	store, err := changelog.Open(cfg.Changelog.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open changelog db: %w", err)
	}

	notifier := notify.New(cfg)

	return &Daemon{
		cfg:      cfg,
		store:    store,
		notifier: notifier,
		dryRun:   dryRun,
	}, nil
}

// Run starts all components and blocks until a signal is received.
func (d *Daemon) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Graceful shutdown on signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case sig := <-sigCh:
			log.Info().Str("signal", sig.String()).Msg("daemon: shutdown signal received")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Recovery: catchup scan for changes missed while stopped.
	// Runs in the background so the watcher and reconciler start immediately.
	if d.cfg.Recovery.CatchupScan {
		d.bgWg.Add(1)
		go func() {
			defer d.bgWg.Done()
			rec := recovery.New(d.cfg, d.store)
			if err := rec.CatchupScan(ctx); err != nil && ctx.Err() == nil {
				log.Error().Err(err).Msg("daemon: catchup scan error")
			}
		}()
	}

	// Channel that signals the reconciler when new changelog entries arrive.
	notifyCh := make(chan struct{}, reconcileChanCap)

	// Transfer manager.
	var transferMgr *transfer.Manager
	var w *watcher.Watcher

	notifyCh2 := make(chan struct{}, reconcileChanCap)

	// Watcher.
	var watchErr error
	w, watchErr = watcher.NewWatcher(d.cfg, d.store, notifyCh2)
	if watchErr != nil {
		return fmt.Errorf("create watcher: %w", watchErr)
	}
	_ = notifyCh

	transferMgr = transfer.NewManager(d.cfg, w)
	defer transferMgr.Close()

	// gRPC peer client.
	peerAddr := fmt.Sprintf("%s:%d", d.cfg.Peer.Address, d.cfg.Peer.GRPCPort)
	client, err := bisyncgrpc.NewClient(peerAddr, d.cfg.Peer.SharedSecret)
	if err != nil {
		return fmt.Errorf("create grpc client: %w", err)
	}
	defer client.Close()

	// gRPC server (serves peer requests).
	grpcSrv := bisyncgrpc.NewServer(d.cfg, d.store)

	// Reconciler.
	rec := reconciler.New(d.cfg, d.store, client, transferMgr, d.notifier)

	// Maintenance.
	maint := changelog.NewMaintenance(d.store,
		d.cfg.Changelog.RetentionDays,
		d.cfg.Conflict.TombstoneRetentionDays,
	)

	// Start components.
	go maint.RunScheduled(ctx)
	go grpcSrv.Listen(ctx) //nolint:errcheck

	// Watcher goroutine.
	go func() {
		if err := w.Start(ctx); err != nil {
			log.Error().Err(err).Msg("watcher: fatal error — stopping daemon")
			cancel()
		}
	}()

	// Reconcile loop: run whenever notified or on a 30s ticker as fallback.
	go d.reconcileLoop(ctx, rec, notifyCh2)

	log.Info().
		Str("node", d.cfg.Node.Name).
		Str("peer", d.cfg.Peer.Name).
		Msg("daemon: running")

	// Wait for context cancellation (from signal handler).
	<-ctx.Done()

	log.Info().Msg("daemon: shutting down")
	return d.shutdown(w, transferMgr)
}

func (d *Daemon) reconcileLoop(ctx context.Context, rec *reconciler.Reconciler, notifyCh <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-notifyCh:
			// Drain extra notifications.
			for len(notifyCh) > 0 {
				<-notifyCh
			}
			if err := rec.Run(ctx); err != nil {
				log.Error().Err(err).Msg("reconcile error")
			}
		case <-ticker.C:
			if err := rec.Run(ctx); err != nil {
				log.Error().Err(err).Msg("reconcile tick error")
			}
		}
	}
}

func (d *Daemon) shutdown(w *watcher.Watcher, tm *transfer.Manager) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// 1. Flush debounce buffers.
	log.Info().Msg("daemon: flushing debounce buffers")
	w.FlushAll(ctx)

	// 2. Record shutdown time for recovery.
	rec := recovery.New(d.cfg, d.store)
	if err := rec.RecordShutdown(ctx); err != nil {
		log.Error().Err(err).Msg("daemon: record shutdown failed")
	}

	// 3. Close transfer pool (waits for in-progress transfers).
	tm.Close()

	// 4. Wait for background goroutines (e.g. catchup scan) to exit before
	// closing the DB they may still be writing to.
	d.bgWg.Wait()

	// 5. Close changelog DB.
	if err := d.store.Close(); err != nil {
		log.Error().Err(err).Msg("daemon: close db failed")
	}

	log.Info().Msg("daemon: shutdown complete")
	return nil
}

func setupLogging(cfg *config.Config) {
	level, err := zerolog.ParseLevel(cfg.Logging.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	if cfg.Logging.Output == "stdout" || cfg.Logging.Output == "" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	}
}
