package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
)

func NewStreamCommand() *cobra.Command {
	var (
		all      bool
		rangeStr string
		sync     bool
		plcURL   string
	)

	cmd := &cobra.Command{
		Use:     "stream [flags]",
		Aliases: []string{"backfill"},
		Short:   "Stream operations to stdout (JSONL)",
		Long: `Stream operations to stdout in JSONL format

Outputs PLC operations as newline-delimited JSON to stdout.
Progress and status messages go to stderr.

By default, streams only existing bundles. Use --sync to also fetch
new bundles from PLC directory until caught up.`,

		Example: `  # Stream all existing bundles
  plcbundle stream --all

  # Stream and sync new bundles
  plcbundle stream --all --sync

  # Stream specific range (existing only)
  plcbundle stream --range 1-100

  # Stream from specific PLC directory
  plcbundle stream --all --sync --plc https://plc.directory

  # Pipe to file
  plcbundle stream --all > operations.jsonl

  # Process with jq
  plcbundle stream --all | jq -r .did | sort | uniq -c

  # Sync and filter with detector
  plcbundle stream --all --sync | plcbundle detector filter spam`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")
			quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")

			// Setup PLC client only if syncing
			var effectivePLCURL string
			if sync {
				effectivePLCURL = plcURL
			}

			mgr, dir, err := getManagerFromCommand(cmd, effectivePLCURL)
			if err != nil {
				return err
			}
			defer mgr.Close()

			if !quiet {
				fmt.Fprintf(os.Stderr, "Streaming from: %s\n", dir)
			}

			// Determine bundle range
			var start, end int

			if all {
				index := mgr.GetIndex()
				bundles := index.GetBundles()

				if len(bundles) == 0 {
					if sync {
						// No bundles but sync enabled - start from 1
						start = 1
						end = 0 // Will be updated after sync
					} else {
						if !quiet {
							fmt.Fprintf(os.Stderr, "No bundles available (use --sync to fetch)\n")
						}
						return nil
					}
				} else {
					start = bundles[0].BundleNumber
					end = bundles[len(bundles)-1].BundleNumber
				}

			} else if rangeStr != "" {
				var err error
				start, end, err = parseBundleRange(rangeStr)
				if err != nil {
					return err
				}

			} else {
				return fmt.Errorf("either --all or --range required")
			}

			if !quiet {
				if sync {
					fmt.Fprintf(os.Stderr, "Mode: stream existing + sync new bundles\n")
				} else {
					fmt.Fprintf(os.Stderr, "Mode: stream existing only\n")
				}
				fmt.Fprintf(os.Stderr, "\n")
			}

			return streamBundles(cmd.Context(), mgr, start, end, sync, verbose, quiet)
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "Stream all bundles")
	cmd.Flags().StringVarP(&rangeStr, "range", "r", "", "Stream bundle range (e.g., '1-100')")
	cmd.Flags().BoolVar(&sync, "sync", false, "Also fetch new bundles from PLC (until caught up)")
	cmd.Flags().StringVar(&plcURL, "plc", "https://plc.directory", "PLC directory URL (for --sync)")

	return cmd
}

func streamBundles(ctx context.Context, mgr BundleManager, start, end int, doSync bool, verbose bool, quiet bool) error {
	operationCount := 0

	// Phase 1: Stream existing bundles
	existingCount := 0
	if end > 0 {
		existingCount = streamExistingBundles(ctx, mgr, start, end, &operationCount, verbose, quiet)
	}

	// Phase 2: Sync and stream new bundles (if enabled)
	fetchedCount := 0
	if doSync {
		if !quiet {
			fmt.Fprintf(os.Stderr, "\nSyncing new bundles from PLC...\n")
		}

		logger := &streamLogger{quiet: quiet}
		config := &internalsync.SyncLoopConfig{
			MaxBundles: 0,
			Verbose:    verbose,
			Logger:     logger,
			OnBundleSynced: func(bundleNum, synced, mempoolCount int, duration, indexTime time.Duration) {
				// Stream the bundle we just created
				if bundle, err := mgr.LoadBundle(ctx, bundleNum); err == nil {
					for _, op := range bundle.Operations {
						if len(op.RawJSON) > 0 {
							fmt.Println(string(op.RawJSON))
						} else {
							data, _ := json.Marshal(op)
							fmt.Println(string(data))
						}
					}
					operationCount += len(bundle.Operations)
				}
			},
		}

		var err error
		fetchedCount, err = mgr.RunSyncOnce(ctx, config, verbose)
		if err != nil {
			return err
		}
	}

	// Summary
	if !quiet {
		fmt.Fprintf(os.Stderr, "\n✓ Stream complete\n")

		if doSync && fetchedCount > 0 {
			fmt.Fprintf(os.Stderr, "  Bundles: %d (%d existing + %d synced)\n",
				existingCount+fetchedCount, existingCount, fetchedCount)
		} else if doSync {
			fmt.Fprintf(os.Stderr, "  Bundles: %d (already up to date)\n", existingCount)
		} else {
			fmt.Fprintf(os.Stderr, "  Bundles: %d\n", existingCount)
		}

		fmt.Fprintf(os.Stderr, "  Operations: %d\n", operationCount)
	}

	return nil
}

func streamExistingBundles(ctx context.Context, mgr BundleManager, start, end int, operationCount *int, verbose bool, quiet bool) int {
	processedCount := 0

	for bundleNum := start; bundleNum <= end; bundleNum++ {
		select {
		case <-ctx.Done():
			return processedCount
		default:
		}

		bundle, err := mgr.LoadBundle(ctx, bundleNum)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "Bundle %06d: not found (skipped)\n", bundleNum)
			}
			continue
		}

		// Output operations to stdout (JSONL)
		for _, op := range bundle.Operations {
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			} else {
				data, _ := json.Marshal(op)
				fmt.Println(string(data))
			}
		}

		*operationCount += len(bundle.Operations)
		processedCount++

		if verbose {
			fmt.Fprintf(os.Stderr, "Bundle %06d: ✓ (%d ops)\n", bundleNum, len(bundle.Operations))
		} else if !quiet && processedCount%100 == 0 {
			fmt.Fprintf(os.Stderr, "Streamed: %d bundles, %d ops\r", processedCount, *operationCount)
		}
	}

	if !quiet && !verbose && processedCount > 0 {
		fmt.Fprintf(os.Stderr, "Existing: %d bundles, %d ops\n", processedCount, *operationCount)
	}

	return processedCount
}

type streamLogger struct {
	quiet   bool
	verbose bool
}

func (l *streamLogger) Printf(format string, v ...interface{}) {
	if !l.quiet {
		fmt.Fprintf(os.Stderr, format+"\n", v...)
	}
}

func (l *streamLogger) Println(v ...interface{}) {
	if !l.quiet {
		fmt.Fprintln(os.Stderr, v...)
	}
}
