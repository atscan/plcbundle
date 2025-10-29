package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
)

func showGeneralInfo(mgr *bundle.Manager, dir string, verbose bool, showBundles bool, verify bool, showTimeline bool) {
	index := mgr.GetIndex()
	info := mgr.GetInfo()
	stats := index.GetStats()
	bundleCount := stats["bundle_count"].(int)

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("              PLC Bundle Repository Overview\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Location
	fmt.Printf("📁 Location\n")
	fmt.Printf("   Directory:  %s\n", dir)
	fmt.Printf("   Index:      %s\n", filepath.Base(info["index_path"].(string)))
	fmt.Printf("\n")

	if bundleCount == 0 {
		fmt.Printf("⚠️  No bundles found\n")
		fmt.Printf("\n")
		fmt.Printf("Get started:\n")
		fmt.Printf("  plcbundle fetch          # Fetch bundles from PLC\n")
		fmt.Printf("  plcbundle rebuild        # Rebuild index from existing files\n")
		fmt.Printf("\n")
		return
	}

	firstBundle := stats["first_bundle"].(int)
	lastBundle := stats["last_bundle"].(int)
	totalCompressedSize := stats["total_size"].(int64)
	startTime := stats["start_time"].(time.Time)
	endTime := stats["end_time"].(time.Time)
	updatedAt := stats["updated_at"].(time.Time)

	// Calculate total uncompressed size
	bundles := index.GetBundles()
	var totalUncompressedSize int64
	for _, meta := range bundles {
		totalUncompressedSize += meta.UncompressedSize
	}

	// Summary
	fmt.Printf("📊 Summary\n")
	fmt.Printf("   Bundles:       %s\n", formatNumber(bundleCount))
	fmt.Printf("   Range:         %06d → %06d\n", firstBundle, lastBundle)
	fmt.Printf("   Compressed:    %s\n", formatBytes(totalCompressedSize))
	fmt.Printf("   Uncompressed:  %s\n", formatBytes(totalUncompressedSize))
	if totalUncompressedSize > 0 {
		ratio := float64(totalUncompressedSize) / float64(totalCompressedSize)
		fmt.Printf("   Ratio:         %.2fx compression\n", ratio)
	}
	fmt.Printf("   Avg/Bundle:    %s\n", formatBytes(totalCompressedSize/int64(bundleCount)))
	fmt.Printf("\n")

	// Timeline
	duration := endTime.Sub(startTime)
	fmt.Printf("📅 Timeline\n")
	fmt.Printf("   First Op:      %s\n", startTime.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Last Op:       %s\n", endTime.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Timespan:      %s\n", formatDuration(duration))
	fmt.Printf("   Last Updated:  %s\n", updatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Age:           %s ago\n", formatDuration(time.Since(updatedAt)))
	fmt.Printf("\n")

	// Operations count (exact calculation)
	mempoolStats := mgr.GetMempoolStats()
	mempoolCount := mempoolStats["count"].(int)
	bundleOpsCount := bundleCount * bundle.BUNDLE_SIZE
	totalOps := bundleOpsCount + mempoolCount

	fmt.Printf("🔢 Operations\n")
	fmt.Printf("   Bundles:       %s records\n", formatNumber(bundleOpsCount))
	if mempoolCount > 0 {
		fmt.Printf("   Mempool:       %s records\n", formatNumber(mempoolCount))
	}
	fmt.Printf("   Total:         %s records\n", formatNumber(totalOps))
	if duration.Hours() > 0 {
		opsPerHour := float64(bundleOpsCount) / duration.Hours()
		fmt.Printf("   Rate:          %.0f ops/hour (bundles)\n", opsPerHour)
	}
	fmt.Printf("\n")

	// Hashes (full, not trimmed)
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

	// Gaps
	gaps := index.FindGaps()
	if len(gaps) > 0 {
		fmt.Printf("⚠️  Missing Bundles: %d\n", len(gaps))
		if len(gaps) <= 10 || verbose {
			fmt.Printf("   ")
			for i, gap := range gaps {
				if i > 0 {
					fmt.Printf(", ")
				}
				if i > 0 && i%10 == 0 {
					fmt.Printf("\n   ")
				}
				fmt.Printf("%06d", gap)
			}
			fmt.Printf("\n")
		} else {
			fmt.Printf("   First few: ")
			for i := 0; i < 5; i++ {
				if i > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%06d", gaps[i])
			}
			fmt.Printf(" ... (use -v to show all)\n")
		}
		fmt.Printf("\n")
	}

	// Mempool
	if mempoolCount > 0 {
		targetBundle := mempoolStats["target_bundle"].(int)
		canCreate := mempoolStats["can_create_bundle"].(bool)
		progress := float64(mempoolCount) / float64(bundle.BUNDLE_SIZE) * 100

		fmt.Printf("🔄 Mempool (next bundle: %06d)\n", targetBundle)
		fmt.Printf("   Operations:    %s / %s\n", formatNumber(mempoolCount), formatNumber(bundle.BUNDLE_SIZE))
		fmt.Printf("   Progress:      %.1f%%\n", progress)

		// Progress bar
		barWidth := 40
		filled := int(float64(barWidth) * float64(mempoolCount) / float64(bundle.BUNDLE_SIZE))
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		fmt.Printf("   [%s]\n", bar)

		if canCreate {
			fmt.Printf("   ✓ Ready to create bundle\n")
		} else {
			remaining := bundle.BUNDLE_SIZE - mempoolCount
			fmt.Printf("   Need %s more operations\n", formatNumber(remaining))
		}
		fmt.Printf("\n")
	}

	// Chain verification
	if verify {
		fmt.Printf("🔐 Chain Verification\n")
		fmt.Printf("   Verifying %d bundles...\n", bundleCount)

		ctx := context.Background()
		result, err := mgr.VerifyChain(ctx)
		if err != nil {
			fmt.Printf("   ✗ Verification failed: %v\n", err)
		} else if result.Valid {
			fmt.Printf("   ✓ Chain is valid\n")
			fmt.Printf("   ✓ All %d bundles verified\n", len(result.VerifiedBundles))

			// Show head hash (full)
			lastMeta, _ := index.GetBundle(lastBundle)
			if lastMeta != nil {
				fmt.Printf("   Head: %s\n", lastMeta.Hash)
			}
		} else {
			fmt.Printf("   ✗ Chain is broken\n")
			fmt.Printf("   Verified: %d/%d bundles\n", len(result.VerifiedBundles), bundleCount)
			fmt.Printf("   Broken at: %06d\n", result.BrokenAt)
			fmt.Printf("   Error: %s\n", result.Error)
		}
		fmt.Printf("\n")
	}

	// Timeline visualization
	if showTimeline {
		fmt.Printf("📈 Timeline Visualization\n")
		visualizeTimeline(index, verbose)
		fmt.Printf("\n")
	}

	// Bundle list
	if showBundles {
		bundles := index.GetBundles()
		fmt.Printf("📚 Bundle List (%d total)\n", len(bundles))
		fmt.Printf("\n")
		fmt.Printf("   Number   | Start Time          | End Time            | Ops    | DIDs   | Size\n")
		fmt.Printf("   ---------|---------------------|---------------------|--------|--------|--------\n")

		for _, meta := range bundles {
			fmt.Printf("   %06d   | %s | %s | %6d | %6d | %7s\n",
				meta.BundleNumber,
				meta.StartTime.Format("2006-01-02 15:04"),
				meta.EndTime.Format("2006-01-02 15:04"),
				meta.OperationCount,
				meta.DIDCount,
				formatBytes(meta.CompressedSize))
		}
		fmt.Printf("\n")
	} else if bundleCount > 0 {
		fmt.Printf("💡 Tip: Use --bundles to see detailed bundle list\n")
		fmt.Printf("        Use --timeline to see timeline visualization\n")
		fmt.Printf("        Use --verify to verify chain integrity\n")
		fmt.Printf("\n")
	}

	// File system stats (verbose)
	if verbose {
		fmt.Printf("💾 File System\n")

		// Calculate average compression ratio
		if totalCompressedSize > 0 && totalUncompressedSize > 0 {
			avgRatio := float64(totalUncompressedSize) / float64(totalCompressedSize)
			savings := (1 - float64(totalCompressedSize)/float64(totalUncompressedSize)) * 100
			fmt.Printf("   Compression:   %.2fx average ratio\n", avgRatio)
			fmt.Printf("   Space Saved:   %.1f%% (%s)\n", savings, formatBytes(totalUncompressedSize-totalCompressedSize))
		}

		// Index size
		indexPath := info["index_path"].(string)
		if indexInfo, err := os.Stat(indexPath); err == nil {
			fmt.Printf("   Index Size:    %s\n", formatBytes(indexInfo.Size()))
		}

		fmt.Printf("\n")
	}
}

func visualizeTimeline(index *bundle.Index, verbose bool) {
	bundles := index.GetBundles()
	if len(bundles) == 0 {
		return
	}

	// Group bundles by date
	type dateGroup struct {
		date  string
		count int
		first int
		last  int
	}

	dateMap := make(map[string]*dateGroup)
	for _, meta := range bundles {
		dateStr := meta.StartTime.Format("2006-01-02")
		if group, exists := dateMap[dateStr]; exists {
			group.count++
			group.last = meta.BundleNumber
		} else {
			dateMap[dateStr] = &dateGroup{
				date:  dateStr,
				count: 1,
				first: meta.BundleNumber,
				last:  meta.BundleNumber,
			}
		}
	}

	// Sort dates
	var dates []string
	for date := range dateMap {
		dates = append(dates, date)
	}
	sort.Strings(dates)

	// Find max count for scaling
	maxCount := 0
	for _, group := range dateMap {
		if group.count > maxCount {
			maxCount = group.count
		}
	}

	// Display
	fmt.Printf("\n")
	barWidth := 40
	for _, date := range dates {
		group := dateMap[date]
		barLen := int(float64(barWidth) * float64(group.count) / float64(maxCount))
		if barLen == 0 && group.count > 0 {
			barLen = 1
		}

		bar := strings.Repeat("█", barLen)
		fmt.Printf("   %s | %-40s | %3d bundles", group.date, bar, group.count)
		if verbose {
			fmt.Printf(" (%06d-%06d)", group.first, group.last)
		}
		fmt.Printf("\n")
	}
}

// Helper formatting functions

func formatNumber(n int) string {
	s := fmt.Sprintf("%d", n)
	// Add thousand separators
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func formatBytes(bytes int64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0f seconds", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1f minutes", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	days := d.Hours() / 24
	if days < 30 {
		return fmt.Sprintf("%.1f days", days)
	}
	if days < 365 {
		return fmt.Sprintf("%.1f months", days/30)
	}
	return fmt.Sprintf("%.1f years", days/365)
}
