package commands

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	flag "github.com/spf13/pflag"

	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

// InfoCommand handles the info subcommand
func InfoCommand(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	bundleNum := fs.Int("bundle", 0, "specific bundle info (0 = general info)")
	verbose := fs.Bool("v", false, "verbose output")
	showBundles := fs.Bool("bundles", false, "show bundle list")
	verify := fs.Bool("verify", false, "verify chain integrity")
	showTimeline := fs.Bool("timeline", false, "show timeline visualization")

	if err := fs.Parse(args); err != nil {
		return err
	}

	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	if *bundleNum > 0 {
		return showBundleInfo(mgr, dir, *bundleNum, *verbose)
	}

	return showGeneralInfo(mgr, dir, *verbose, *showBundles, *verify, *showTimeline)
}

func showGeneralInfo(mgr *bundle.Manager, dir string, verbose, showBundles, verify, showTimeline bool) error {
	index := mgr.GetIndex()
	info := mgr.GetInfo()
	stats := index.GetStats()
	bundleCount := stats["bundle_count"].(int)

	fmt.Printf("\n═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("              PLC Bundle Repository Overview\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	fmt.Printf("📁 Location\n")
	fmt.Printf("   Directory:  %s\n", dir)
	fmt.Printf("   Index:      %s\n\n", filepath.Base(info["index_path"].(string)))

	fmt.Printf("🌐 Origin\n")
	fmt.Printf("   Source:     %s\n\n", index.Origin)

	if bundleCount == 0 {
		fmt.Printf("⚠️  No bundles found\n\n")
		fmt.Printf("Get started:\n")
		fmt.Printf("  plcbundle fetch          # Fetch bundles from PLC\n")
		fmt.Printf("  plcbundle rebuild        # Rebuild index from existing files\n\n")
		return nil
	}

	firstBundle := stats["first_bundle"].(int)
	lastBundle := stats["last_bundle"].(int)
	totalCompressedSize := stats["total_size"].(int64)
	totalUncompressedSize := stats["total_uncompressed_size"].(int64)
	startTime := stats["start_time"].(time.Time)
	endTime := stats["end_time"].(time.Time)
	updatedAt := stats["updated_at"].(time.Time)

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
	fmt.Printf("   Avg/Bundle:    %s\n\n", formatBytes(totalCompressedSize/int64(bundleCount)))

	// Timeline
	duration := endTime.Sub(startTime)
	fmt.Printf("📅 Timeline\n")
	fmt.Printf("   First Op:      %s\n", startTime.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Last Op:       %s\n", endTime.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Timespan:      %s\n", formatDuration(duration))
	fmt.Printf("   Last Updated:  %s\n", updatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("   Age:           %s ago\n\n", formatDuration(time.Since(updatedAt)))

	// Operations
	mempoolStats := mgr.GetMempoolStats()
	mempoolCount := mempoolStats["count"].(int)
	bundleOpsCount := bundleCount * types.BUNDLE_SIZE
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

	// Hashes
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
		progress := float64(mempoolCount) / float64(types.BUNDLE_SIZE) * 100

		fmt.Printf("🔄 Mempool (next bundle: %06d)\n", targetBundle)
		fmt.Printf("   Operations:    %s / %s\n", formatNumber(mempoolCount), formatNumber(types.BUNDLE_SIZE))
		fmt.Printf("   Progress:      %.1f%%\n", progress)

		barWidth := 40
		filled := int(float64(barWidth) * float64(mempoolCount) / float64(types.BUNDLE_SIZE))
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

	// Timeline
	if showTimeline {
		fmt.Printf("📈 Timeline Visualization\n")
		visualizeTimeline(index, verbose)
		fmt.Printf("\n")
	}

	// Bundle list
	if showBundles {
		bundles := index.GetBundles()
		fmt.Printf("📚 Bundle List (%d total)\n\n", len(bundles))
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
		fmt.Printf("        Use --verify to verify chain integrity\n\n")
	}

	return nil
}

func showBundleInfo(mgr *bundle.Manager, dir string, bundleNum int, verbose bool) error {
	ctx := context.Background()
	b, err := mgr.LoadBundle(ctx, bundleNum)
	if err != nil {
		return err
	}

	fmt.Printf("\n═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    Bundle %06d\n", b.BundleNumber)
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	fmt.Printf("📁 Location\n")
	fmt.Printf("   Directory:  %s\n", dir)
	fmt.Printf("   File:       %06d.jsonl.zst\n\n", b.BundleNumber)

	duration := b.EndTime.Sub(b.StartTime)
	fmt.Printf("📅 Time Range\n")
	fmt.Printf("   Start:      %s\n", b.StartTime.Format("2006-01-02 15:04:05.000 MST"))
	fmt.Printf("   End:        %s\n", b.EndTime.Format("2006-01-02 15:04:05.000 MST"))
	fmt.Printf("   Duration:   %s\n", formatDuration(duration))
	fmt.Printf("   Created:    %s\n\n", b.CreatedAt.Format("2006-01-02 15:04:05 MST"))

	fmt.Printf("📊 Content\n")
	fmt.Printf("   Operations:  %s\n", formatNumber(len(b.Operations)))
	fmt.Printf("   Unique DIDs: %s\n", formatNumber(b.DIDCount))
	if len(b.Operations) > 0 && b.DIDCount > 0 {
		avgOpsPerDID := float64(len(b.Operations)) / float64(b.DIDCount)
		fmt.Printf("   Avg ops/DID: %.2f\n", avgOpsPerDID)
	}
	fmt.Printf("\n")

	fmt.Printf("💾 Size\n")
	fmt.Printf("   Compressed:   %s\n", formatBytes(b.CompressedSize))
	fmt.Printf("   Uncompressed: %s\n", formatBytes(b.UncompressedSize))
	fmt.Printf("   Ratio:        %.2fx\n", b.CompressionRatio())
	fmt.Printf("   Efficiency:   %.1f%% savings\n\n", (1-float64(b.CompressedSize)/float64(b.UncompressedSize))*100)

	fmt.Printf("🔐 Cryptographic Hashes\n")
	fmt.Printf("   Chain Hash:\n     %s\n", b.Hash)
	fmt.Printf("   Content Hash:\n     %s\n", b.ContentHash)
	fmt.Printf("   Compressed:\n     %s\n", b.CompressedHash)
	if b.Parent != "" {
		fmt.Printf("   Parent Chain Hash:\n     %s\n", b.Parent)
	}
	fmt.Printf("\n")

	if verbose && len(b.Operations) > 0 {
		showBundleSamples(b)
		showBundleDIDStats(b)
	}

	return nil
}

func showBundleSamples(b *bundle.Bundle) {
	fmt.Printf("📝 Sample Operations (first 5)\n")
	showCount := 5
	if len(b.Operations) < showCount {
		showCount = len(b.Operations)
	}

	for i := 0; i < showCount; i++ {
		op := b.Operations[i]
		fmt.Printf("   %d. %s\n", i+1, op.DID)
		fmt.Printf("      CID: %s\n", op.CID)
		fmt.Printf("      Time: %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000"))
		if op.IsNullified() {
			fmt.Printf("      ⚠️  Nullified: %s\n", op.GetNullifyingCID())
		}
	}
	fmt.Printf("\n")
}

func showBundleDIDStats(b *bundle.Bundle) {
	didOps := make(map[string]int)
	for _, op := range b.Operations {
		didOps[op.DID]++
	}

	type didCount struct {
		did   string
		count int
	}

	var counts []didCount
	for did, count := range didOps {
		counts = append(counts, didCount{did, count})
	}

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})

	fmt.Printf("🏆 Most Active DIDs\n")
	showCount := 5
	if len(counts) < showCount {
		showCount = len(counts)
	}

	for i := 0; i < showCount; i++ {
		fmt.Printf("   %d. %s (%d ops)\n", i+1, counts[i].did, counts[i].count)
	}
	fmt.Printf("\n")
}

func visualizeTimeline(index *bundleindex.Index, verbose bool) {
	bundles := index.GetBundles()
	if len(bundles) == 0 {
		return
	}

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

	var dates []string
	for date := range dateMap {
		dates = append(dates, date)
	}
	sort.Strings(dates)

	maxCount := 0
	for _, group := range dateMap {
		if group.count > maxCount {
			maxCount = group.count
		}
	}

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
