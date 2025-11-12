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

func NewExportCommand() *cobra.Command {
	var (
		all      bool
		rangeStr string
		sync     bool
		plcURL   string
		count    int
		after    string
	)

	cmd := &cobra.Command{
		Use:     "export [flags]",
		Aliases: []string{"stream", "backfill"},
		Short:   "Export operations to stdout (JSONL)",
		Long: `Export operations to stdout in JSONL format

Outputs PLC operations as newline-delimited JSON to stdout.
Progress and status messages go to stderr.

By default, exports only existing bundles. Use --sync to also fetch
new bundles from PLC directory until caught up.

Supports filtering by count and timestamp for selective exports.`,

		Example: `  # Export all existing bundles
  plcbundle export --all

  # Export and sync new bundles
  plcbundle export --all --sync

  # Export specific range (existing only)
  plcbundle export --range 1-100

  # Export with limit
  plcbundle export --all --count 50000

  # Export after timestamp
  plcbundle export --all --after 2024-01-01T00:00:00Z

  # Combine filters
  plcbundle export --range 1-100 --count 10000 --after 2024-01-01T00:00:00Z

  # Export from specific PLC directory
  plcbundle export --all --sync --plc https://plc.directory

  # Pipe to file
  plcbundle export --all > operations.jsonl

  # Process with jq
  plcbundle export --all | jq -r .did | sort | uniq -c

  # Sync and filter with detector
  plcbundle export --all --sync | plcbundle detector filter spam

  # Using aliases (backwards compatible)
  plcbundle stream --all --sync
  plcbundle backfill --all`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")
			quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")

			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd, PLCURL: plcURL})
			if err != nil {
				return err
			}
			defer mgr.Close()

			if !quiet {
				fmt.Fprintf(os.Stderr, "Exporting from: %s\n", dir)
			}

			// Parse after timestamp if provided
			var afterTime time.Time
			if after != "" {
				afterTime, err = time.Parse(time.RFC3339, after)
				if err != nil {
					return fmt.Errorf("invalid --after timestamp (use RFC3339 format): %w", err)
				}
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
					fmt.Fprintf(os.Stderr, "Mode: export existing + sync new bundles\n")
				} else {
					fmt.Fprintf(os.Stderr, "Mode: export existing only\n")
				}

				if count > 0 {
					fmt.Fprintf(os.Stderr, "Limit: %d operations\n", count)
				}
				if after != "" {
					fmt.Fprintf(os.Stderr, "After: %s\n", after)
				}
				fmt.Fprintf(os.Stderr, "\n")
			}

			return exportBundles(cmd.Context(), mgr, exportOptions{
				start:     start,
				end:       end,
				sync:      sync,
				count:     count,
				afterTime: afterTime,
				verbose:   verbose,
				quiet:     quiet,
			})
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "Export all bundles")
	cmd.Flags().StringVarP(&rangeStr, "range", "r", "", "Export bundle range (e.g., '1-100')")
	cmd.Flags().BoolVar(&sync, "sync", false, "Also fetch new bundles from PLC (until caught up)")
	cmd.Flags().StringVar(&plcURL, "plc", "https://plc.directory", "PLC directory URL (for --sync)")
	cmd.Flags().IntVarP(&count, "count", "n", 0, "Limit number of operations (0 = unlimited)")
	cmd.Flags().StringVar(&after, "after", "", "Only export operations after this timestamp (RFC3339)")

	return cmd
}

type exportOptions struct {
	start     int
	end       int
	sync      bool
	count     int
	afterTime time.Time
	verbose   bool
	quiet     bool
}

func exportBundles(ctx context.Context, mgr BundleManager, opts exportOptions) error {
	operationCount := 0
	exported := 0

	// Phase 1: Export existing bundles
	existingCount := 0
	if opts.end > 0 {
		existingCount, exported = exportExistingBundles(
			ctx, mgr, opts.start, opts.end,
			&operationCount, opts.count, opts.afterTime,
			opts.verbose, opts.quiet,
		)
	}

	// Check if we hit the count limit
	if opts.count > 0 && exported >= opts.count {
		if !opts.quiet {
			fmt.Fprintf(os.Stderr, "\n✓ Export complete (limit reached)\n")
			fmt.Fprintf(os.Stderr, "  Bundles: %d\n", existingCount)
			fmt.Fprintf(os.Stderr, "  Operations: %d\n", exported)
		}
		return nil
	}

	// Phase 2: Sync and export new bundles (if enabled and not at limit)
	fetchedCount := 0
	if opts.sync && (opts.count == 0 || exported < opts.count) {
		if !opts.quiet {
			fmt.Fprintf(os.Stderr, "\nSyncing new bundles from PLC...\n")
		}

		logger := &exportLogger{quiet: opts.quiet}
		config := &internalsync.SyncLoopConfig{
			MaxBundles: 0,
			Verbose:    opts.verbose,
			Logger:     logger,
			OnBundleSynced: func(bundleNum, synced, mempoolCount int, duration, indexTime time.Duration) {
				// Stream the bundle we just created
				if bundle, err := mgr.LoadBundle(ctx, bundleNum); err == nil {
					for _, op := range bundle.Operations {
						// Apply filters
						if !opts.afterTime.IsZero() && op.CreatedAt.Before(opts.afterTime) {
							continue
						}

						if opts.count > 0 && exported >= opts.count {
							return // Stop when limit reached
						}

						// Output operation
						if len(op.RawJSON) > 0 {
							fmt.Println(string(op.RawJSON))
						} else {
							data, _ := json.Marshal(op)
							fmt.Println(string(data))
						}
						exported++
						operationCount++
					}
				}
			},
		}

		var err error
		fetchedCount, err = mgr.RunSyncOnce(ctx, config)
		if err != nil {
			return err
		}
	}

	// Summary
	if !opts.quiet {
		fmt.Fprintf(os.Stderr, "\n✓ Export complete\n")

		if opts.sync && fetchedCount > 0 {
			fmt.Fprintf(os.Stderr, "  Bundles: %d (%d existing + %d synced)\n",
				existingCount+fetchedCount, existingCount, fetchedCount)
		} else if opts.sync {
			fmt.Fprintf(os.Stderr, "  Bundles: %d (already up to date)\n", existingCount)
		} else {
			fmt.Fprintf(os.Stderr, "  Bundles: %d\n", existingCount)
		}

		fmt.Fprintf(os.Stderr, "  Operations: %d", exported)
		if opts.count > 0 {
			fmt.Fprintf(os.Stderr, " (limit: %d)", opts.count)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}

	return nil
}

func exportExistingBundles(
	ctx context.Context,
	mgr BundleManager,
	start, end int,
	operationCount *int,
	limit int,
	afterTime time.Time,
	verbose bool,
	quiet bool,
) (bundleCount int, exported int) {

	processedCount := 0
	exportedOps := 0

	for bundleNum := start; bundleNum <= end; bundleNum++ {
		select {
		case <-ctx.Done():
			return processedCount, exportedOps
		default:
		}

		bundle, err := mgr.LoadBundle(ctx, bundleNum)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "Bundle %06d: not found (skipped)\n", bundleNum)
			}
			continue
		}

		// Export operations with filters
		for _, op := range bundle.Operations {
			// Filter by timestamp
			if !afterTime.IsZero() && op.CreatedAt.Before(afterTime) {
				continue
			}

			// Check count limit
			if limit > 0 && exportedOps >= limit {
				if verbose {
					fmt.Fprintf(os.Stderr, "Bundle %06d: limit reached, stopping\n", bundleNum)
				}
				return processedCount, exportedOps
			}

			// Output operation to stdout (JSONL)
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			} else {
				data, _ := json.Marshal(op)
				fmt.Println(string(data))
			}
			exportedOps++
		}

		*operationCount += len(bundle.Operations)
		processedCount++

		if verbose {
			fmt.Fprintf(os.Stderr, "Bundle %06d: ✓ (%d ops, %d exported)\n",
				bundleNum, len(bundle.Operations), exportedOps)
		} else if !quiet && processedCount%100 == 0 {
			fmt.Fprintf(os.Stderr, "Exported: %d bundles, %d ops\r", processedCount, exportedOps)
		}
	}

	if !quiet && !verbose && processedCount > 0 {
		fmt.Fprintf(os.Stderr, "Existing: %d bundles, %d ops\n", processedCount, exportedOps)
	}

	return processedCount, exportedOps
}

type exportLogger struct {
	quiet bool
}

func (l *exportLogger) Printf(format string, v ...interface{}) {
	if !l.quiet {
		fmt.Fprintf(os.Stderr, format+"\n", v...)
	}
}

func (l *exportLogger) Println(v ...interface{}) {
	if !l.quiet {
		fmt.Fprintln(os.Stderr, v...)
	}
}
