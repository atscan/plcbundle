package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

func NewStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"info"},
		Short:   "Show repository status",
		Long: `Show repository status and statistics

Displays overview of the bundle repository including bundles, 
storage, timeline, mempool, and DID index status.`,

		Example: `  # Show status
  plcbundle status

  # Using alias
  plcbundle info`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return showStatus(mgr, dir)
		},
	}

	return cmd
}

func showStatus(mgr *bundle.Manager, dir string) error {
	index := mgr.GetIndex()
	stats := index.GetStats()
	bundleCount := stats["bundle_count"].(int)

	// Header
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("               plcbundle Repository Status\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	// Location & Origin
	fmt.Printf("📁 %s\n", dir)
	if origin := index.Origin; origin != "" {
		fmt.Printf("🌐 %s\n", origin)
	}
	fmt.Printf("\n")

	// Empty repository
	if bundleCount == 0 {
		fmt.Printf("⚠️  Empty repository (no bundles)\n\n")
		fmt.Printf("Get started:\n")
		fmt.Printf("  plcbundle clone <url>    Clone from remote\n")
		fmt.Printf("  plcbundle sync           Fetch from PLC directory\n\n")
		return nil
	}

	// Extract stats
	firstBundle := stats["first_bundle"].(int)
	lastBundle := stats["last_bundle"].(int)
	totalCompressedSize := stats["total_size"].(int64)
	totalUncompressedSize := stats["total_uncompressed_size"].(int64)
	startTime := stats["start_time"].(time.Time)
	endTime := stats["end_time"].(time.Time)
	updatedAt := stats["updated_at"].(time.Time)

	// Bundles summary
	ratio := float64(totalUncompressedSize) / float64(totalCompressedSize)
	avgSize := totalCompressedSize / int64(bundleCount)
	fmt.Printf("📦 Bundles\n")
	fmt.Printf("   Count:         %s  (%06d → %06d)\n",
		formatNumber(bundleCount), firstBundle, lastBundle)
	fmt.Printf("   Compressed:    %s  (avg %s/bundle)\n",
		formatBytes(totalCompressedSize), formatBytes(avgSize))
	fmt.Printf("   Uncompressed:  %s  (%.2fx compression)\n\n",
		formatBytes(totalUncompressedSize), ratio)

	// Timeline
	duration := endTime.Sub(startTime)
	age := time.Since(updatedAt)
	fmt.Printf("📅 Timeline\n")
	fmt.Printf("   Coverage:      %s → %s\n",
		startTime.Format("2006-01-02 15:04"), endTime.Format("2006-01-02 15:04"))
	fmt.Printf("   Timespan:      %s\n", formatDuration(duration))
	fmt.Printf("   Last Updated:  %s ago\n\n", formatDuration(age))

	// Operations
	mempoolStats := mgr.GetMempoolStats()
	mempoolCount := mempoolStats["count"].(int)
	bundleOpsCount := bundleCount * types.BUNDLE_SIZE
	totalOps := bundleOpsCount + mempoolCount

	fmt.Printf("🔢 Operations\n")
	if mempoolCount > 0 {
		fmt.Printf("   Total:         %s records (%s bundled + %s mempool)\n",
			formatNumber(totalOps), formatNumber(bundleOpsCount), formatNumber(mempoolCount))
	} else {
		fmt.Printf("   Total:         %s records\n", formatNumber(bundleOpsCount))
	}

	if duration.Hours() > 0 {
		opsPerHour := float64(bundleOpsCount) / duration.Hours()
		fmt.Printf("   Rate:          %.0f ops/hour\n", opsPerHour)
	}
	fmt.Printf("\n")

	// Mempool
	if mempoolCount > 0 {
		targetBundle := mempoolStats["target_bundle"].(int)
		canCreate := mempoolStats["can_create_bundle"].(bool)
		progress := float64(mempoolCount) / float64(types.BUNDLE_SIZE) * 100

		fmt.Printf("🔄 Mempool (next bundle: %06d)\n", targetBundle)
		fmt.Printf("   Operations:    %s / %s  (%.1f%%)\n",
			formatNumber(mempoolCount), formatNumber(types.BUNDLE_SIZE), progress)

		// Progress bar
		barWidth := 40
		filled := int(float64(barWidth) * progress / 100)
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		fmt.Printf("   [%s]\n", bar)

		if canCreate {
			fmt.Printf("   ✓ Ready to create bundle\n")
		} else {
			remaining := types.BUNDLE_SIZE - mempoolCount
			fmt.Printf("   Need %s more operations\n", formatNumber(remaining))
		}
		fmt.Printf("\n")
	}

	// DID Index
	didStats := mgr.GetDIDIndexStats()
	if didStats["exists"].(bool) {
		indexedDIDs := didStats["indexed_dids"].(int64)
		mempoolDIDs := didStats["mempool_dids"].(int64)
		totalDIDs := didStats["total_dids"].(int64)
		lastIndexedBundle := didStats["last_bundle"].(int)
		shardCount := didStats["shard_count"].(int)
		cachedShards := didStats["cached_shards"].(int)

		fmt.Printf("🔍 DID Index\n")
		if mempoolDIDs > 0 {
			fmt.Printf("   DIDs:          %s  (%s indexed + %s mempool)\n",
				formatNumber(int(totalDIDs)),
				formatNumber(int(indexedDIDs)),
				formatNumber(int(mempoolDIDs)))
		} else {
			fmt.Printf("   DIDs:          %s\n", formatNumber(int(totalDIDs)))
		}

		fmt.Printf("   Shards:        %d  (%d cached)\n", shardCount, cachedShards)

		// Index health
		if lastIndexedBundle < lastBundle {
			behind := lastBundle - lastIndexedBundle
			fmt.Printf("   Status:        ⚠️  Behind by %d bundle", behind)
			if behind > 1 {
				fmt.Printf("s")
			}
			fmt.Printf(" (at %06d, need %06d)\n", lastIndexedBundle, lastBundle)
		} else {
			fmt.Printf("   Status:        ✓ Up to date (bundle %06d)\n", lastIndexedBundle)
		}
		fmt.Printf("\n")
	}

	// Gaps
	gaps := index.FindGaps()
	if len(gaps) > 0 {
		fmt.Printf("⚠️  Missing Bundles: %d gap", len(gaps))
		if len(gaps) > 1 {
			fmt.Printf("s")
		}
		fmt.Printf("\n")

		// Show gaps compactly
		displayCount := len(gaps)
		if displayCount > 15 {
			displayCount = 15
		}

		fmt.Printf("   ")
		for i := 0; i < displayCount; i++ {
			if i > 0 {
				fmt.Printf(", ")
			}
			if i > 0 && i%8 == 0 {
				fmt.Printf("\n   ")
			}
			fmt.Printf("%06d", gaps[i])
		}
		if len(gaps) > displayCount {
			fmt.Printf(", ... +%d more", len(gaps)-displayCount)
		}
		fmt.Printf("\n\n")
	}

	// Chain Hashes
	firstMeta, err := index.GetBundle(firstBundle)
	if err == nil {
		fmt.Printf("🔐 Chain Hashes\n")
		fmt.Printf("   Root (bundle %06d):\n", firstBundle)
		fmt.Printf("     %s\n", firstMeta.Hash)

		lastMeta, err := index.GetBundle(lastBundle)
		if err == nil {
			fmt.Printf("   Head (bundle %06d):\n", lastBundle)
			fmt.Printf("     %s\n", lastMeta.Hash)
		}
		fmt.Printf("\n")
	}

	return nil
}
