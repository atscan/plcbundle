package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
)

func NewIndexCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "DID index management",
		Long: `DID index management operations

Manage the DID position index which maps DIDs to their bundle locations.
This index enables fast O(1) DID lookups and is required for DID
resolution and query operations.`,

		Example: `  # Build DID position index
  plcbundle index build

  # Repair DID index (rebuild from bundles)
  plcbundle index repair

  # Show DID index statistics
  plcbundle index stats

  # Verify DID index integrity
  plcbundle index verify`,
	}

	cmd.AddCommand(newIndexBuildCommand())
	cmd.AddCommand(newIndexRepairCommand())
	cmd.AddCommand(newIndexStatsCommand())
	cmd.AddCommand(newIndexVerifyCommand())

	return cmd
}

// ============================================================================
// INDEX BUILD - Build DID position index
// ============================================================================

func newIndexBuildCommand() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build DID position index",
		Long: `Build DID position index from bundles

Creates a sharded index mapping each DID to its bundle locations,
enabling fast O(1) DID lookups. Required for DID resolution.

The index is built incrementally and auto-updates as new bundles
are added. Use --force to rebuild from scratch.`,

		Example: `  # Build index
  plcbundle index build

  # Force rebuild from scratch
  plcbundle index build --force`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			stats := mgr.GetDIDIndexStats()
			if stats["exists"].(bool) && !force {
				fmt.Printf("DID index already exists (use --force to rebuild)\n")
				fmt.Printf("Directory: %s\n", dir)
				fmt.Printf("Total DIDs: %s\n", formatNumber(int(stats["total_dids"].(int64))))
				return nil
			}

			index := mgr.GetIndex()
			bundleCount := index.Count()

			if bundleCount == 0 {
				fmt.Printf("No bundles to index\n")
				return nil
			}

			fmt.Printf("Building DID index in: %s\n", dir)
			fmt.Printf("Indexing %d bundles...\n\n", bundleCount)

			progress := ui.NewProgressBar(bundleCount)
			start := time.Now()
			ctx := context.Background()

			err = mgr.BuildDIDIndex(ctx, func(current, total int) {
				progress.Set(current)
			})

			progress.Finish()

			if err != nil {
				return fmt.Errorf("build failed: %w", err)
			}

			elapsed := time.Since(start)
			stats = mgr.GetDIDIndexStats()

			fmt.Printf("\n✓ DID index built in %s\n", elapsed.Round(time.Millisecond))
			fmt.Printf("  Total DIDs: %s\n", formatNumber(int(stats["total_dids"].(int64))))
			fmt.Printf("  Shards: %d\n", stats["shard_count"])
			fmt.Printf("  Location: %s/.plcbundle/\n", dir)

			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Rebuild even if index exists")

	return cmd
}

// ============================================================================
// INDEX REPAIR - Repair/rebuild DID index
// ============================================================================

func newIndexRepairCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "repair",
		Aliases: []string{"rebuild"},
		Short:   "Repair DID index",
		Long: `Repair DID index by rebuilding from bundles

Rebuilds the DID index from scratch and verifies consistency.
Use this when:
  • DID index is corrupted
  • Index is out of sync with bundles
  • After manual bundle operations
  • Upgrade to new index version`,

		Example: `  # Repair DID index
  plcbundle index repair

  # Verbose output
  plcbundle index repair -v`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			stats := mgr.GetDIDIndexStats()
			if !stats["exists"].(bool) {
				fmt.Printf("DID index does not exist\n")
				fmt.Printf("Use: plcbundle index build\n")
				return nil
			}

			fmt.Printf("Repairing DID index in: %s\n\n", dir)

			index := mgr.GetIndex()
			bundleCount := index.Count()

			if bundleCount == 0 {
				fmt.Printf("No bundles to index\n")
				return nil
			}

			fmt.Printf("Rebuilding index from %d bundles...\n\n", bundleCount)

			var progress *ui.ProgressBar
			if !verbose {
				progress = ui.NewProgressBar(bundleCount)
			}

			start := time.Now()
			ctx := context.Background()

			err = mgr.BuildDIDIndex(ctx, func(current, total int) {
				if progress != nil {
					progress.Set(current)
				} else if current%100 == 0 || current == total {
					fmt.Printf("Progress: %d/%d (%.1f%%)   \r",
						current, total, float64(current)/float64(total)*100)
				}
			})

			if progress != nil {
				progress.Finish()
			}

			if err != nil {
				return fmt.Errorf("repair failed: %w", err)
			}

			// Verify consistency
			fmt.Printf("\nVerifying consistency...\n")
			if err := mgr.GetDIDIndex().VerifyAndRepairIndex(ctx, mgr); err != nil {
				return fmt.Errorf("verification failed: %w", err)
			}

			elapsed := time.Since(start)
			stats = mgr.GetDIDIndexStats()

			fmt.Printf("\n✓ DID index repaired in %s\n", elapsed.Round(time.Millisecond))
			fmt.Printf("  Total DIDs: %s\n", formatNumber(int(stats["total_dids"].(int64))))
			fmt.Printf("  Shards: %d\n", stats["shard_count"])
			fmt.Printf("  Last bundle: %06d\n", stats["last_bundle"])

			return nil
		},
	}

	return cmd
}

// ============================================================================
// INDEX STATS - Show DID index statistics
// ============================================================================

func newIndexStatsCommand() *cobra.Command {
	var showJSON bool

	cmd := &cobra.Command{
		Use:     "stats",
		Aliases: []string{"info"},
		Short:   "Show DID index statistics",
		Long: `Show DID index statistics

Displays DID index information including total DIDs indexed,
shard distribution, cache statistics, and coverage.`,

		Example: `  # Show statistics
  plcbundle index stats

  # JSON output
  plcbundle index stats --json`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			stats := mgr.GetDIDIndexStats()

			if showJSON {
				data, _ := json.MarshalIndent(stats, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if !stats["exists"].(bool) {
				fmt.Printf("DID index does not exist\n")
				fmt.Printf("Run: plcbundle index build\n")
				return nil
			}

			indexedDIDs := stats["indexed_dids"].(int64)
			mempoolDIDs := stats["mempool_dids"].(int64)
			totalDIDs := stats["total_dids"].(int64)

			fmt.Printf("\nDID Index Statistics\n")
			fmt.Printf("════════════════════\n\n")
			fmt.Printf("  Location:      %s/.plcbundle/\n", dir)

			if mempoolDIDs > 0 {
				fmt.Printf("  Indexed DIDs:  %s (in bundles)\n", formatNumber(int(indexedDIDs)))
				fmt.Printf("  Mempool DIDs:  %s (not yet bundled)\n", formatNumber(int(mempoolDIDs)))
				fmt.Printf("  Total DIDs:    %s\n", formatNumber(int(totalDIDs)))
			} else {
				fmt.Printf("  Total DIDs:    %s\n", formatNumber(int(totalDIDs)))
			}

			fmt.Printf("  Shard count:   %d\n", stats["shard_count"])
			fmt.Printf("  Last bundle:   %06d\n", stats["last_bundle"])
			fmt.Printf("  Updated:       %s\n\n", stats["updated_at"].(time.Time).Format("2006-01-02 15:04:05"))

			fmt.Printf("  Cached shards: %d / %d\n", stats["cached_shards"], stats["cache_limit"])

			if cachedList, ok := stats["cache_order"].([]int); ok && len(cachedList) > 0 {
				fmt.Printf("  Hot shards:    ")
				for i, shard := range cachedList {
					if i > 0 {
						fmt.Printf(", ")
					}
					if i >= 10 {
						fmt.Printf("... (+%d more)", len(cachedList)-10)
						break
					}
					fmt.Printf("%02x", shard)
				}
				fmt.Printf("\n")
			}

			fmt.Printf("\n")
			return nil
		},
	}

	cmd.Flags().BoolVar(&showJSON, "json", false, "Output as JSON")

	return cmd
}

// ============================================================================
// INDEX VERIFY - Verify DID index integrity
// ============================================================================

func newIndexVerifyCommand() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:     "verify",
		Aliases: []string{"check"},
		Short:   "Verify DID index integrity",
		Long: `Verify DID index integrity and consistency

Checks the DID index for consistency with bundles:
  • Index version is current
  • All bundles are indexed
  • Shard files are valid
  • No corruption detected

Automatically repairs minor issues.`,

		Example: `  # Verify DID index
  plcbundle index verify

  # Verbose output
  plcbundle index verify -v`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			stats := mgr.GetDIDIndexStats()

			if !stats["exists"].(bool) {
				fmt.Printf("DID index does not exist\n")
				fmt.Printf("Run: plcbundle index build\n")
				return nil
			}

			fmt.Printf("Verifying DID index in: %s\n\n", dir)

			ctx := context.Background()

			if verbose {
				fmt.Printf("Index version: %d\n", mgr.GetDIDIndex().GetConfig().Version)
				fmt.Printf("Total DIDs:    %s\n", formatNumber(int(stats["total_dids"].(int64))))
				fmt.Printf("Shards:        %d\n", stats["shard_count"])
				fmt.Printf("Last bundle:   %06d\n\n", stats["last_bundle"])
			}

			fmt.Printf("Checking consistency with bundles...\n")

			if err := mgr.GetDIDIndex().VerifyAndRepairIndex(ctx, mgr); err != nil {
				fmt.Printf("\n✗ DID index verification failed\n")
				fmt.Printf("  Error: %v\n", err)
				return fmt.Errorf("verification failed: %w", err)
			}

			stats = mgr.GetDIDIndexStats()

			fmt.Printf("\n✓ DID index is valid\n")
			fmt.Printf("  Total DIDs:  %s\n", formatNumber(int(stats["total_dids"].(int64))))
			fmt.Printf("  Shards:      %d\n", stats["shard_count"])
			fmt.Printf("  Last bundle: %06d\n", stats["last_bundle"])

			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")

	return cmd
}
