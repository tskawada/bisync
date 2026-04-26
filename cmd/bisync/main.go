package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	"github.com/tskawada/bisync/internal/daemon"
	bisyncgrpc "github.com/tskawada/bisync/internal/grpc"
)

const defaultConfigPath = "/etc/bisync/config.yaml"

var (
	cfgPath string
	debug   bool
	dryRun  bool
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "bisync",
		Short: "Bidirectional file sync daemon using fanotify",
		Long: `bisync watches local filesystem changes via fanotify and
synchronises them bidirectionally with a peer node over rsync/SSH.`,
		RunE: runDaemon,
	}
	root.PersistentFlags().StringVar(&cfgPath, "config", defaultConfigPath, "config file path")
	root.PersistentFlags().BoolVar(&debug, "debug", false, "enable debug logging")
	root.Flags().BoolVar(&dryRun, "dry-run", false, "log transfers without executing them")

	root.AddCommand(
		daemonCmd(),
		statusCmd(),
		conflictsCmd(),
		resolveCmd(),
		logCmd(),
		validateCmd(),
		versionCmd(),
	)
	return root
}

func runDaemon(cmd *cobra.Command, args []string) error {
	return startDaemon(cmd.Context())
}

func daemonCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "daemon",
		Short: "Start the bisync daemon (explicit subcommand form)",
		RunE:  func(cmd *cobra.Command, _ []string) error { return startDaemon(cmd.Context()) },
	}
	c.Flags().BoolVar(&dryRun, "dry-run", false, "log transfers without executing them")
	return c
}

func startDaemon(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if debug {
		cfg.Logging.Level = "debug"
	}
	d, err := daemon.New(cfg, dryRun)
	if err != nil {
		return err
	}
	return d.Run(ctx)
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show current sync status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			addr := fmt.Sprintf("%s:%d", cfg.Peer.Address, cfg.Peer.GRPCPort)
			client, err := bisyncgrpc.NewClient(addr)
			if err != nil {
				return fmt.Errorf("connect to peer: %w", err)
			}
			defer client.Close()
			resp, err := client.Ping(cmd.Context(), &bisyncgrpc.PingRequest{NodeName: cfg.Node.Name})
			if err != nil {
				return fmt.Errorf("ping peer: %w", err)
			}
			fmt.Printf("Peer %q: status=%s\n", resp.NodeName, resp.Status)
			return nil
		},
	}
}

func conflictsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "conflicts",
		Short: "List unresolved conflicts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			store, err := changelog.Open(cfg.Changelog.DBPath)
			if err != nil {
				return err
			}
			defer store.Close()
			// Conflicts are entries that remain unsynced after a long time.
			entries, err := store.UnsyncedEntries(cmd.Context(), "")
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				fmt.Println("No unsynced entries.")
				return nil
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(entries)
		},
	}
}

func resolveCmd() *cobra.Command {
	var keepLocal, keepRemote bool
	c := &cobra.Command{
		Use:   "resolve <path>",
		Short: "Manually resolve a conflict",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !keepLocal && !keepRemote {
				return fmt.Errorf("specify --keep-local or --keep-remote")
			}
			path := args[0]
			side := "local"
			if keepRemote {
				side = "remote"
			}
			fmt.Printf("Resolving %q in favour of %s (not yet implemented in CLI — edit files manually)\n", path, side)
			return nil
		},
	}
	c.Flags().BoolVar(&keepLocal, "keep-local", false, "keep the local version")
	c.Flags().BoolVar(&keepRemote, "keep-remote", false, "keep the remote version")
	return c
}

func logCmd() *cobra.Command {
	var tail int
	c := &cobra.Command{
		Use:   "log",
		Short: "Show recent changelog entries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			store, err := changelog.Open(cfg.Changelog.DBPath)
			if err != nil {
				return err
			}
			defer store.Close()
			entries, err := store.UnsyncedEntries(cmd.Context(), "")
			if err != nil {
				return err
			}
			if tail > 0 && len(entries) > tail {
				entries = entries[len(entries)-tail:]
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			for _, e := range entries {
				if err := enc.Encode(e); err != nil {
					return err
				}
			}
			return nil
		},
	}
	c.Flags().IntVar(&tail, "tail", 0, "show last N entries (0 = all)")
	return c
}

func validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				fmt.Fprintf(os.Stderr, "config invalid: %v\n", err)
				return err
			}
			fmt.Printf("Config OK: node=%s peer=%s sync_pairs=%s\n",
				cfg.Node.Name, cfg.Peer.Name, syncPairNames(cfg))
			return nil
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("bisync v0.1.0-draft")
		},
	}
}

func loadConfig() (*config.Config, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error().Err(err).Str("path", cfgPath).Msg("failed to load config")
		return nil, err
	}
	return cfg, nil
}

func syncPairNames(cfg *config.Config) string {
	names := ""
	for i, sp := range cfg.SyncPairs {
		if i > 0 {
			names += ","
		}
		names += sp.Name
	}
	return "[" + names + "]"
}

// ensure strconv is used (referenced from log --tail flag parsing)
var _ = strconv.Itoa
