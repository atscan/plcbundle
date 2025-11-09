package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
)

func NewSyncCommand() *cobra.Command {
	var (
		plcURL     string
		continuous bool
		interval   time.Duration
		maxBundles int
	)

	cmd := &cobra.Command{
		Use:     "sync",
		Aliases: []string{"fetch"},
		Short:   "Fetch new bundles from PLC directory",
		Long: `Fetch new bundles from PLC directory

Download new operations from the PLC directory and create bundles.
Similar to 'git fetch' - updates your local repository with new data.

By default, fetches until caught up. Use --continuous to run as a daemon
that continuously syncs at regular intervals.`,

		Example: `  # Fetch new bundles once
  plcbundle sync

  # Fetch from specific directory
  plcbundle sync --dir ./my-bundles

  # Run continuously (daemon mode)
  plcbundle sync --continuous

  # Custom sync interval
  plcbundle sync --continuous --interval 30s

  # Fetch maximum 10 bundles then stop
  plcbundle sync --max-bundles 10

  # Continuous with limit
  plcbundle sync --continuous --max-bundles 100 --interval 1m

  # Verbose output
  plcbundle sync --continuous -v`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")
			quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")

			// ✨ Sync creates repository if missing
			mgr, dir, err := getManager(&ManagerOptions{
				Cmd:      cmd,
				PLCURL:   plcURL,
				AutoInit: true,
			})
			if err != nil {
				return err
			}
			defer mgr.Close()

			if !quiet {
				fmt.Printf("Syncing from: %s\n", dir)
				fmt.Printf("PLC Directory: %s\n", plcURL)
				if continuous {
					fmt.Printf("Mode: continuous (interval: %s)\n", interval)
					if maxBundles > 0 {
						fmt.Printf("Max bundles: %d\n", maxBundles)
					}
					fmt.Printf("(Press Ctrl+C to stop)\n")
				} else {
					if maxBundles > 0 {
						fmt.Printf("Max bundles: %d\n", maxBundles)
					}
				}
				fmt.Printf("\n")
			}

			if continuous {
				return runContinuousSync(mgr, interval, maxBundles, verbose, quiet)
			}

			return runSingleSync(cmd.Context(), mgr, maxBundles, verbose, quiet)
		},
	}

	cmd.Flags().StringVar(&plcURL, "plc", "https://plc.directory", "PLC directory URL")
	cmd.Flags().BoolVar(&continuous, "continuous", false, "Keep syncing (run as daemon)")
	cmd.Flags().DurationVar(&interval, "interval", 1*time.Minute, "Sync interval for continuous mode")
	cmd.Flags().IntVar(&maxBundles, "max-bundles", 0, "Maximum bundles to fetch (0 = all)")

	return cmd
}

func runSingleSync(ctx context.Context, mgr *bundle.Manager, maxBundles int, verbose bool, quiet bool) error {
	logger := &commandLogger{quiet: quiet}

	config := &internalsync.SyncLoopConfig{
		MaxBundles: maxBundles,
		Verbose:    verbose,
		Logger:     logger,
	}

	// Call manager method (not internal directly)
	synced, err := mgr.RunSyncOnce(ctx, config, verbose)
	if err != nil {
		return err
	}

	if !quiet {
		if synced == 0 {
			fmt.Fprintf(os.Stderr, "\n✓ Already up to date\n")
		} else {
			fmt.Fprintf(os.Stderr, "\n✓ Sync complete: %d bundle(s) fetched\n", synced)
		}
	}

	return nil
}

func runContinuousSync(mgr *bundle.Manager, interval time.Duration, maxBundles int, verbose bool, quiet bool) error {
	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		if !quiet {
			fmt.Fprintf(os.Stderr, "\n\n⚠️  Shutdown signal received...\n")
			fmt.Fprintf(os.Stderr, "  Saving mempool...\n")
		}

		if err := mgr.GetMempool().Save(); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ Failed to save mempool: %v\n", err)
		} else if !quiet {
			fmt.Fprintf(os.Stderr, "  ✓ Mempool saved\n")
		}

		if !quiet {
			fmt.Fprintf(os.Stderr, "  Closing DID index...\n")
		}
		if err := mgr.GetDIDIndex().Close(); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ Failed to close index: %v\n", err)
		} else if !quiet {
			fmt.Fprintf(os.Stderr, "  ✓ Index closed\n")
		}

		if !quiet {
			fmt.Fprintf(os.Stderr, "  ✓ Shutdown complete\n")
		}

		cancel()
		os.Exit(0)
	}()

	logger := &commandLogger{quiet: quiet}

	config := &internalsync.SyncLoopConfig{
		Interval:   interval,
		MaxBundles: maxBundles,
		Verbose:    verbose,
		Logger:     logger,
	}

	// Call manager method (not internal directly)
	return mgr.RunSyncLoop(ctx, config)
}
