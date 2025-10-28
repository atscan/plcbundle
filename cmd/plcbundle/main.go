package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/atscan/plcbundle/bundle"
	"github.com/atscan/plcbundle/plc"
)

// Version information (injected at build time via ldflags or read from build info)
var (
	version   = "dev"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func init() {
	// Try to get version from build info (works with go install)
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}

		// Extract git commit and build time from build settings
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if setting.Value != "" {
					gitCommit = setting.Value
					if len(gitCommit) > 7 {
						gitCommit = gitCommit[:7] // Short hash
					}
				}
			case "vcs.time":
				if setting.Value != "" {
					buildDate = setting.Value
				}
			}
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "fetch":
		cmdFetch()
	case "rebuild":
		cmdRebuild()
	case "verify":
		cmdVerify()
	case "info":
		cmdInfo()
	case "export":
		cmdExport()
	case "backfill":
		cmdBackfill()
	case "mempool":
		cmdMempool()
	case "serve":
		cmdServe()
	case "compare":
		cmdCompare()
	case "version":
		fmt.Printf("plcbundle version %s\n", version)
		fmt.Printf("  commit: %s\n", gitCommit)
		fmt.Printf("  built:  %s\n", buildDate)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`plcbundle %s - DID PLC Bundle Management Tool

Usage:
  plcbundle <command> [options]

Commands:
  fetch      Fetch next bundle from PLC directory
  rebuild    Rebuild index from existing bundle files
  verify     Verify bundle integrity
  info       Show bundle information
  export     Export operations from bundles
  backfill   Fetch/load all bundles and stream to stdout
  mempool    Show mempool status and operations
  serve      Start HTTP server to serve bundle data
  compare    Compare local index with target index
  version    Show version

Security Model:
  Bundles are cryptographically chained but require external verification:
  - Verify against original PLC directory
  - Compare with multiple independent mirrors
  - Check published root and head hashes
  - Anyone can reproduce bundles from PLC directory`, version)
}

// getManager creates or opens a bundle manager in the detected directory
func getManager(plcURL string) (*bundle.Manager, string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, "", err
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create directory: %w", err)
	}

	config := bundle.DefaultConfig(dir)

	var client *plc.Client
	if plcURL != "" {
		client = plc.NewClient(plcURL)
	}

	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		return nil, "", err
	}

	return mgr, dir, nil
}

func cmdFetch() {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL")
	count := fs.Int("count", 0, "number of bundles to fetch (0 = fetch all available)")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager(*plcURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)

	ctx := context.Background()

	// Get starting bundle info
	index := mgr.GetIndex()
	lastBundle := index.GetLastBundle()
	startBundle := 1
	if lastBundle != nil {
		startBundle = lastBundle.BundleNumber + 1
	}

	fmt.Printf("Starting from bundle %06d\n", startBundle)

	if *count > 0 {
		fmt.Printf("Fetching %d bundles...\n", *count)
	} else {
		fmt.Printf("Fetching all available bundles...\n")
	}

	fetchedCount := 0
	consecutiveErrors := 0
	maxConsecutiveErrors := 3

	for {
		// Check if we've reached the requested count
		if *count > 0 && fetchedCount >= *count {
			break
		}

		currentBundle := startBundle + fetchedCount

		if *count > 0 {
			fmt.Printf("Fetching bundle %d/%d (bundle %06d)...\n", fetchedCount+1, *count, currentBundle)
		} else {
			fmt.Printf("Fetching bundle %06d...\n", currentBundle)
		}

		b, err := mgr.FetchNextBundle(ctx)
		if err != nil {
			// Check if we've reached the end (insufficient operations)
			if isEndOfDataError(err) {
				fmt.Printf("\n✓ Caught up! No more complete bundles available.\n")
				fmt.Printf("  Last bundle: %06d\n", currentBundle-1)
				break
			}

			// Handle other errors
			consecutiveErrors++
			fmt.Fprintf(os.Stderr, "Error fetching bundle %06d: %v\n", currentBundle, err)

			if consecutiveErrors >= maxConsecutiveErrors {
				fmt.Fprintf(os.Stderr, "Too many consecutive errors, stopping.\n")
				os.Exit(1)
			}

			// Wait a bit before retrying
			fmt.Printf("Waiting 5 seconds before retry...\n")
			time.Sleep(5 * time.Second)
			continue
		}

		// Reset error counter on success
		consecutiveErrors = 0

		if err := mgr.SaveBundle(ctx, b); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving bundle %06d: %v\n", b.BundleNumber, err)
			os.Exit(1)
		}

		fetchedCount++
		fmt.Printf("✓ Saved bundle %06d (%d operations, %d DIDs)\n",
			b.BundleNumber, len(b.Operations), b.DIDCount)
	}

	if fetchedCount > 0 {
		fmt.Printf("\n✓ Fetch complete: %d bundles retrieved\n", fetchedCount)
		fmt.Printf("  Current range: %06d - %06d\n", startBundle, startBundle+fetchedCount-1)
	} else {
		fmt.Printf("\n✓ Already up to date!\n")
	}
}

// isEndOfDataError checks if the error indicates we've reached the end of available data
func isEndOfDataError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Check for insufficient operations error
	if strings.Contains(errMsg, "insufficient operations") {
		return true
	}

	// Check for "no more operations available"
	if strings.Contains(errMsg, "no more operations available") {
		return true
	}

	// Check for "reached latest data"
	if strings.Contains(errMsg, "reached latest data") {
		return true
	}

	return false
}

func cmdRebuild() {
	fs := flag.NewFlagSet("rebuild", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose output")
	workers := fs.Int("workers", 4, "number of parallel workers (0 = CPU count)")
	noProgress := fs.Bool("no-progress", false, "disable progress bar")
	fs.Parse(os.Args[2:])

	// Auto-detect CPU count
	if *workers == 0 {
		*workers = runtime.NumCPU()
	}

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Rebuilding index from: %s\n", dir)
	fmt.Printf("Using %d workers\n", *workers)

	// Find all bundle files
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl.zst"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error scanning directory: %v\n", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Println("No bundle files found")
		return
	}

	fmt.Printf("Found %d bundle files\n", len(files))
	fmt.Printf("\n")

	start := time.Now()

	// Create progress bar
	var progress *ProgressBar
	var progressCallback func(int, int)

	if !*noProgress {
		progress = NewProgressBar(len(files))
		progressCallback = func(current, total int) {
			progress.Set(current)
		}
		fmt.Println("Processing bundles:")
	}

	// Use parallel scan
	result, err := mgr.ScanDirectoryParallel(*workers, progressCallback)
	if err != nil {
		if progress != nil {
			progress.Finish()
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Finish progress bar
	if progress != nil {
		progress.Finish()
	}

	elapsed := time.Since(start)

	fmt.Printf("\n")
	fmt.Printf("✓ Index rebuilt in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total bundles:      %d\n", result.BundleCount)
	fmt.Printf("  Compressed size:    %s\n", formatBytes(result.TotalSize))
	fmt.Printf("  Uncompressed size:  %s\n", formatBytes(result.TotalUncompressed))

	// Calculate compression ratio
	if result.TotalUncompressed > 0 {
		ratio := float64(result.TotalUncompressed) / float64(result.TotalSize)
		fmt.Printf("  Compression ratio:  %.2fx\n", ratio)
	}

	fmt.Printf("  Average speed:      %.1f bundles/sec\n", float64(result.BundleCount)/elapsed.Seconds())

	if elapsed.Seconds() > 0 {
		compressedThroughput := float64(result.TotalSize) / elapsed.Seconds() / (1024 * 1024)
		uncompressedThroughput := float64(result.TotalUncompressed) / elapsed.Seconds() / (1024 * 1024)
		fmt.Printf("  Throughput (compressed):   %.1f MB/s\n", compressedThroughput)
		fmt.Printf("  Throughput (uncompressed): %.1f MB/s\n", uncompressedThroughput)
	}

	fmt.Printf("  Index file:         %s\n", filepath.Join(dir, bundle.INDEX_FILE))

	if len(result.MissingGaps) > 0 {
		fmt.Printf("  ⚠️  Missing gaps:     %d bundles\n", len(result.MissingGaps))
	}

	// Verify chain if requested
	if *verbose {
		fmt.Printf("\n")
		fmt.Printf("Verifying chain integrity...\n")

		ctx := context.Background()
		verifyResult, err := mgr.VerifyChain(ctx)
		if err != nil {
			fmt.Printf("  ⚠️  Verification error: %v\n", err)
		} else if verifyResult.Valid {
			fmt.Printf("  ✓ Chain is valid (%d bundles verified)\n", len(verifyResult.VerifiedBundles))

			// Show head hash
			index := mgr.GetIndex()
			if lastMeta := index.GetLastBundle(); lastMeta != nil {
				fmt.Printf("  Chain head: %s...\n", lastMeta.ChainHash[:16])
			}
		} else {
			fmt.Printf("  ✗ Chain verification failed\n")
			fmt.Printf("  Broken at: bundle %06d\n", verifyResult.BrokenAt)
			fmt.Printf("  Error: %s\n", verifyResult.Error)
		}
	}
}

func cmdVerify() {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	bundleNum := fs.Int("bundle", 0, "specific bundle to verify (0 = verify chain)")
	verbose := fs.Bool("v", false, "verbose output")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)

	ctx := context.Background()

	if *bundleNum > 0 {
		// Verify specific bundle
		fmt.Printf("Verifying bundle %06d...\n", *bundleNum)

		result, err := mgr.VerifyBundle(ctx, *bundleNum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Verification failed: %v\n", err)
			os.Exit(1)
		}

		if result.Valid {
			fmt.Printf("✓ Bundle %06d is valid\n", *bundleNum)
			if *verbose {
				fmt.Printf("  File exists: %v\n", result.FileExists)
				fmt.Printf("  Hash match: %v\n", result.HashMatch)
				fmt.Printf("  Hash: %s\n", result.LocalHash[:16]+"...")
			}
		} else {
			fmt.Printf("✗ Bundle %06d is invalid\n", *bundleNum)
			if result.Error != nil {
				fmt.Printf("  Error: %v\n", result.Error)
			}
			if !result.FileExists {
				fmt.Printf("  File not found\n")
			}
			if !result.HashMatch && result.FileExists {
				fmt.Printf("  Expected hash: %s...\n", result.ExpectedHash[:16])
				fmt.Printf("  Actual hash:   %s...\n", result.LocalHash[:16])
			}
			os.Exit(1)
		}
	} else {
		// Verify entire chain
		index := mgr.GetIndex()
		bundles := index.GetBundles()

		if len(bundles) == 0 {
			fmt.Println("No bundles to verify")
			return
		}

		fmt.Printf("Verifying chain of %d bundles...\n", len(bundles))
		fmt.Println()

		verifiedCount := 0
		errorCount := 0
		lastPercent := -1

		for i, meta := range bundles {
			bundleNum := meta.BundleNumber

			// Show progress
			percent := (i * 100) / len(bundles)
			if percent != lastPercent || *verbose {
				if *verbose {
					fmt.Printf("  [%3d%%] Verifying bundle %06d...", percent, bundleNum)
				} else if percent%10 == 0 && percent != lastPercent {
					fmt.Printf("  [%3d%%] Verified %d/%d bundles...\n", percent, i, len(bundles))
				}
				lastPercent = percent
			}

			// Verify file hash
			result, err := mgr.VerifyBundle(ctx, bundleNum)
			if err != nil {
				if *verbose {
					fmt.Printf(" ERROR\n")
				}
				fmt.Printf("\n✗ Failed to verify bundle %06d: %v\n", bundleNum, err)
				errorCount++
				continue
			}

			if !result.Valid {
				if *verbose {
					fmt.Printf(" INVALID\n")
				}
				fmt.Printf("\n✗ Bundle %06d hash verification failed\n", bundleNum)
				if result.Error != nil {
					fmt.Printf("  Error: %v\n", result.Error)
				}
				errorCount++
				continue
			}

			// Verify chain link (prev_bundle_hash)
			if i > 0 {
				prevMeta := bundles[i-1]
				if meta.PrevBundleHash != prevMeta.Hash {
					if *verbose {
						fmt.Printf(" CHAIN BROKEN\n")
					}
					fmt.Printf("\n✗ Chain broken at bundle %06d\n", bundleNum)
					fmt.Printf("  Expected prev_hash: %s...\n", prevMeta.Hash[:16])
					fmt.Printf("  Actual prev_hash:   %s...\n", meta.PrevBundleHash[:16])
					errorCount++
					continue
				}
			}

			if *verbose {
				fmt.Printf(" ✓\n")
			}
			verifiedCount++
		}

		// Final summary
		fmt.Println()
		if errorCount == 0 {
			fmt.Printf("✓ Chain is valid (%d bundles verified)\n", verifiedCount)
			fmt.Printf("  First bundle: %06d\n", bundles[0].BundleNumber)
			fmt.Printf("  Last bundle:  %06d\n", bundles[len(bundles)-1].BundleNumber)
			fmt.Printf("  Chain head:   %s...\n", bundles[len(bundles)-1].Hash[:16])
		} else {
			fmt.Printf("✗ Chain verification failed\n")
			fmt.Printf("  Verified: %d/%d bundles\n", verifiedCount, len(bundles))
			fmt.Printf("  Errors: %d\n", errorCount)
			os.Exit(1)
		}
	}
}

func cmdInfo() {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	bundleNum := fs.Int("bundle", 0, "specific bundle info (0 = general info)")
	verbose := fs.Bool("v", false, "verbose output")
	showBundles := fs.Bool("bundles", false, "show bundle list")
	verify := fs.Bool("verify", false, "verify chain integrity")
	showTimeline := fs.Bool("timeline", false, "show timeline visualization")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	if *bundleNum > 0 {
		showBundleInfo(mgr, dir, *bundleNum, *verbose)
	} else {
		showGeneralInfo(mgr, dir, *verbose, *showBundles, *verify, *showTimeline)
	}
}

func showBundleInfo(mgr *bundle.Manager, dir string, bundleNum int, verbose bool) {
	ctx := context.Background()
	b, err := mgr.LoadBundle(ctx, bundleNum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    Bundle %06d\n", b.BundleNumber)
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Location
	fmt.Printf("📁 Location\n")
	fmt.Printf("   Directory:  %s\n", dir)
	fmt.Printf("   File:       %06d.jsonl.zst\n", b.BundleNumber)
	fmt.Printf("\n")

	// Time Range
	duration := b.EndTime.Sub(b.StartTime)
	fmt.Printf("📅 Time Range\n")
	fmt.Printf("   Start:      %s\n", b.StartTime.Format("2006-01-02 15:04:05.000 MST"))
	fmt.Printf("   End:        %s\n", b.EndTime.Format("2006-01-02 15:04:05.000 MST"))
	fmt.Printf("   Duration:   %s\n", formatDuration(duration))
	fmt.Printf("   Created:    %s\n", b.CreatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("\n")

	// Content
	fmt.Printf("📊 Content\n")
	fmt.Printf("   Operations: %s\n", formatNumber(len(b.Operations)))
	fmt.Printf("   Unique DIDs: %s\n", formatNumber(b.DIDCount))
	if len(b.Operations) > 0 {
		avgOpsPerDID := float64(len(b.Operations)) / float64(b.DIDCount)
		fmt.Printf("   Avg ops/DID: %.2f\n", avgOpsPerDID)
	}
	fmt.Printf("\n")

	// Size
	fmt.Printf("💾 Size\n")
	fmt.Printf("   Compressed:   %s\n", formatBytes(b.CompressedSize))
	fmt.Printf("   Uncompressed: %s\n", formatBytes(b.UncompressedSize))
	fmt.Printf("   Ratio:        %.2fx\n", b.CompressionRatio())
	fmt.Printf("   Efficiency:   %.1f%% savings\n", (1-float64(b.CompressedSize)/float64(b.UncompressedSize))*100)
	fmt.Printf("\n")

	// Hashes
	fmt.Printf("🔐 Cryptographic Hashes\n")
	fmt.Printf("   Content (SHA-256):\n")
	fmt.Printf("     %s\n", b.Hash)
	fmt.Printf("   Compressed:\n")
	fmt.Printf("     %s\n", b.CompressedHash)
	if b.PrevBundleHash != "" {
		fmt.Printf("   Previous Bundle:\n")
		fmt.Printf("     %s\n", b.PrevBundleHash)
	}
	fmt.Printf("\n")

	// Chain
	if b.PrevBundleHash != "" || b.Cursor != "" {
		fmt.Printf("🔗 Chain Information\n")
		if b.Cursor != "" {
			fmt.Printf("   Cursor:     %s\n", b.Cursor)
		}
		if b.PrevBundleHash != "" {
			fmt.Printf("   Links to:   Bundle %06d\n", bundleNum-1)
		}
		if len(b.BoundaryCIDs) > 0 {
			fmt.Printf("   Boundary:   %d CIDs at same timestamp\n", len(b.BoundaryCIDs))
		}
		fmt.Printf("\n")
	}

	// Verbose: Show sample operations
	if verbose && len(b.Operations) > 0 {
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

	// Verbose: Show DID statistics
	if verbose && len(b.Operations) > 0 {
		didOps := make(map[string]int)
		for _, op := range b.Operations {
			didOps[op.DID]++
		}

		// Find most active DIDs
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
}

func cmdExport() {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	count := fs.Int("count", 1000, "number of operations to export")
	after := fs.String("after", "", "timestamp to start after (RFC3339)")
	fs.Parse(os.Args[2:])

	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	// Parse after time
	var afterTime time.Time
	if *after != "" {
		afterTime, err = time.Parse(time.RFC3339, *after)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid after time: %v\n", err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	ops, err := mgr.ExportOperations(ctx, afterTime, *count)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Export failed: %v\n", err)
		os.Exit(1)
	}

	// Output as JSONL
	for _, op := range ops {
		if len(op.RawJSON) > 0 {
			fmt.Println(string(op.RawJSON))
		}
	}

	fmt.Fprintf(os.Stderr, "Exported %d operations\n", len(ops))
}

func cmdBackfill() {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL")
	startFrom := fs.Int("start", 1, "bundle number to start from")
	endAt := fs.Int("end", 0, "bundle number to end at (0 = until caught up)")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager(*plcURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Fprintf(os.Stderr, "Starting backfill from: %s\n", dir)
	fmt.Fprintf(os.Stderr, "Starting from bundle: %06d\n", *startFrom)
	if *endAt > 0 {
		fmt.Fprintf(os.Stderr, "Ending at bundle: %06d\n", *endAt)
	} else {
		fmt.Fprintf(os.Stderr, "Ending: when caught up\n")
	}
	fmt.Fprintf(os.Stderr, "\n")

	ctx := context.Background()

	currentBundle := *startFrom
	processedCount := 0
	fetchedCount := 0
	loadedCount := 0
	operationCount := 0

	for {
		// Check if we've reached the end bundle
		if *endAt > 0 && currentBundle > *endAt {
			break
		}

		fmt.Fprintf(os.Stderr, "Processing bundle %06d... ", currentBundle)

		// Try to load from disk first
		bundle, err := mgr.LoadBundle(ctx, currentBundle)

		if err != nil {
			// Bundle doesn't exist, try to fetch it
			fmt.Fprintf(os.Stderr, "fetching... ")

			bundle, err = mgr.FetchNextBundle(ctx)
			if err != nil {
				if isEndOfDataError(err) {
					fmt.Fprintf(os.Stderr, "\n✓ Caught up! No more complete bundles available.\n")
					break
				}
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)

				// If we can't fetch, we're done
				break
			}

			// Save the fetched bundle
			if err := mgr.SaveBundle(ctx, bundle); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR saving: %v\n", err)
				os.Exit(1)
			}

			fetchedCount++
			fmt.Fprintf(os.Stderr, "saved... ")
		} else {
			loadedCount++
		}

		// Output operations to stdout (JSONL)
		for _, op := range bundle.Operations {
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			}
		}

		operationCount += len(bundle.Operations)
		processedCount++

		fmt.Fprintf(os.Stderr, "✓ (%d ops, %d DIDs)\n", len(bundle.Operations), bundle.DIDCount)

		currentBundle++

		// Show progress summary every 100 bundles
		if processedCount%100 == 0 {
			fmt.Fprintf(os.Stderr, "\n--- Progress: %d bundles processed (%d fetched, %d loaded) ---\n",
				processedCount, fetchedCount, loadedCount)
			fmt.Fprintf(os.Stderr, "    Total operations: %d\n\n", operationCount)
		}
	}

	// Final summary
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "✓ Backfill complete\n")
	fmt.Fprintf(os.Stderr, "  Bundles processed: %d\n", processedCount)
	fmt.Fprintf(os.Stderr, "  Newly fetched: %d\n", fetchedCount)
	fmt.Fprintf(os.Stderr, "  Loaded from disk: %d\n", loadedCount)
	fmt.Fprintf(os.Stderr, "  Total operations: %d\n", operationCount)
	fmt.Fprintf(os.Stderr, "  Range: %06d - %06d\n", *startFrom, currentBundle-1)
}

func cmdMempool() {
	fs := flag.NewFlagSet("mempool", flag.ExitOnError)
	clear := fs.Bool("clear", false, "clear the mempool")
	export := fs.Bool("export", false, "export mempool operations as JSONL to stdout")
	refresh := fs.Bool("refresh", false, "reload mempool from disk")
	validate := fs.Bool("validate", false, "validate chronological order")
	verbose := fs.Bool("v", false, "verbose output")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)
	fmt.Println()

	// Handle validate
	if *validate {
		fmt.Printf("Validating mempool chronological order...\n")
		if err := mgr.ValidateMempool(); err != nil {
			fmt.Fprintf(os.Stderr, "✗ Validation failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✓ Mempool validation passed\n")
		return
	}

	// Handle refresh
	if *refresh {
		fmt.Printf("Refreshing mempool from disk...\n")
		if err := mgr.RefreshMempool(); err != nil {
			fmt.Fprintf(os.Stderr, "Error refreshing mempool: %v\n", err)
			os.Exit(1)
		}

		// Validate after refresh
		if err := mgr.ValidateMempool(); err != nil {
			fmt.Fprintf(os.Stderr, "⚠ Warning: mempool validation failed after refresh: %v\n", err)
		} else {
			fmt.Printf("✓ Mempool refreshed and validated\n\n")
		}
	}

	// Handle clear
	if *clear {
		stats := mgr.GetMempoolStats()
		count := stats["count"].(int)

		if count == 0 {
			fmt.Println("Mempool is already empty")
			return
		}

		fmt.Printf("⚠ This will clear %d operations from the mempool.\n", count)
		fmt.Printf("Are you sure? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(strings.TrimSpace(response)) != "y" {
			fmt.Println("Cancelled")
			return
		}

		if err := mgr.ClearMempool(); err != nil {
			fmt.Fprintf(os.Stderr, "Error clearing mempool: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✓ Mempool cleared (%d operations removed)\n", count)
		return
	}

	// Handle export
	if *export {
		ops, err := mgr.GetMempoolOperations()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting mempool operations: %v\n", err)
			os.Exit(1)
		}

		if len(ops) == 0 {
			fmt.Fprintf(os.Stderr, "Mempool is empty\n")
			return
		}

		// Output as JSONL to stdout
		for _, op := range ops {
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			}
		}

		fmt.Fprintf(os.Stderr, "Exported %d operations from mempool\n", len(ops))
		return
	}

	// Default: Show mempool stats
	stats := mgr.GetMempoolStats()
	count := stats["count"].(int)
	canCreate := stats["can_create_bundle"].(bool)
	targetBundle := stats["target_bundle"].(int)
	minTimestamp := stats["min_timestamp"].(time.Time)
	validated := stats["validated"].(bool)

	fmt.Printf("Mempool Status:\n")
	fmt.Printf("  Target bundle: %06d\n", targetBundle)
	fmt.Printf("  Operations: %d\n", count)
	fmt.Printf("  Can create bundle: %v (need %d)\n", canCreate, bundle.BUNDLE_SIZE)
	fmt.Printf("  Min timestamp: %s\n", minTimestamp.Format("2006-01-02 15:04:05"))

	validationIcon := "✓"
	if !validated {
		validationIcon = "⚠"
	}
	fmt.Printf("  Validated: %s %v\n", validationIcon, validated)

	if count > 0 {
		if sizeBytes, ok := stats["size_bytes"].(int); ok {
			fmt.Printf("  Size: %.2f KB\n", float64(sizeBytes)/1024)
		}

		if firstTime, ok := stats["first_time"].(time.Time); ok {
			fmt.Printf("  First operation: %s\n", firstTime.Format("2006-01-02 15:04:05"))
		}

		if lastTime, ok := stats["last_time"].(time.Time); ok {
			fmt.Printf("  Last operation: %s\n", lastTime.Format("2006-01-02 15:04:05"))
		}

		progress := float64(count) / float64(bundle.BUNDLE_SIZE) * 100
		fmt.Printf("  Progress: %.1f%% (%d/%d)\n", progress, count, bundle.BUNDLE_SIZE)

		// Show progress bar
		barWidth := 40
		filled := int(float64(barWidth) * float64(count) / float64(bundle.BUNDLE_SIZE))
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		fmt.Printf("  [%s]\n", bar)
	} else {
		fmt.Printf("  (empty)\n")
	}

	// Verbose: Show sample operations
	if *verbose && count > 0 {
		fmt.Println()
		fmt.Printf("Sample operations (showing up to 10):\n")

		ops, err := mgr.GetMempoolOperations()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting operations: %v\n", err)
			os.Exit(1)
		}

		showCount := 10
		if len(ops) < showCount {
			showCount = len(ops)
		}

		for i := 0; i < showCount; i++ {
			op := ops[i]
			fmt.Printf("  %d. DID: %s\n", i+1, op.DID)
			fmt.Printf("     CID: %s\n", op.CID)
			fmt.Printf("     Created: %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000"))
		}

		if len(ops) > showCount {
			fmt.Printf("  ... and %d more\n", len(ops)-showCount)
		}
	}

	fmt.Println()

	// Show mempool file
	mempoolFilename := fmt.Sprintf("plc_mempool_%06d.jsonl", targetBundle)
	fmt.Printf("File: %s\n", filepath.Join(dir, mempoolFilename))
}

func cmdServe() {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.String("port", "8080", "HTTP server port")
	host := fs.String("host", "127.0.0.1", "HTTP server host")
	sync := fs.Bool("sync", false, "enable sync mode (auto-sync from PLC)")
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL (for sync mode)")
	syncInterval := fs.Duration("sync-interval", 5*time.Minute, "sync interval for sync mode")
	workers := fs.Int("workers", 4, "number of workers for auto-rebuild (0 = CPU count)")
	fs.Parse(os.Args[2:])

	// Auto-detect CPU count
	if *workers == 0 {
		*workers = runtime.NumCPU()
	}

	// Create manager with PLC client if sync mode is enabled
	var plcURLForManager string
	if *sync {
		plcURLForManager = *plcURL
	}

	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Check if index exists
	indexPath := filepath.Join(dir, bundle.INDEX_FILE)
	indexExists := fileExists(indexPath)

	// Check if bundle files exist
	bundleFiles, _ := filepath.Glob(filepath.Join(dir, "*.jsonl.zst"))
	hasBundles := len(bundleFiles) > 0

	// Auto-rebuild if no index but bundles exist
	if !indexExists && hasBundles {
		fmt.Printf("📦 No index found, but %d bundle files detected\n", len(bundleFiles))
		fmt.Printf("🔨 Starting automatic rebuild in background...\n")
		fmt.Printf("\n")

		// Start rebuild in background
		go func() {
			// Create a temporary manager for rebuild
			config := bundle.DefaultConfig(dir)
			rebuildMgr, err := bundle.NewManager(config, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Rebuild] Error: %v\n", err)
				return
			}
			defer rebuildMgr.Close()

			fmt.Printf("[Rebuild] Rebuilding index from %d bundles...\n", len(bundleFiles))
			start := time.Now()

			result, err := rebuildMgr.ScanDirectoryParallel(*workers, func(current, total int) {
				if current%100 == 0 || current == total {
					fmt.Printf("[Rebuild] Progress: %d/%d bundles (%.1f%%)\n",
						current, total, float64(current)/float64(total)*100)
				}
			})

			if err != nil {
				fmt.Fprintf(os.Stderr, "[Rebuild] Error: %v\n", err)
				return
			}

			elapsed := time.Since(start)
			fmt.Printf("[Rebuild] ✓ Index rebuilt in %s\n", elapsed.Round(time.Millisecond))
			fmt.Printf("[Rebuild]   Total bundles: %d\n", result.BundleCount)
			fmt.Printf("[Rebuild]   Speed: %.1f bundles/sec\n", float64(result.BundleCount)/elapsed.Seconds())
			fmt.Printf("[Rebuild]   Server will now serve all bundles\n")
			fmt.Printf("\n")
		}()

		// Give rebuild a moment to start
		time.Sleep(500 * time.Millisecond)
	}

	// Create manager for serving (will load index when available)
	mgr, _, err := getManager(plcURLForManager)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	addr := fmt.Sprintf("%s:%s", *host, *port)

	fmt.Printf("Starting plcbundle HTTP server...\n")
	fmt.Printf("  Directory: %s\n", dir)
	fmt.Printf("  Listening: http://%s\n", addr)

	if *sync {
		fmt.Printf("  Sync mode: ENABLED\n")
		fmt.Printf("  PLC URL: %s\n", *plcURL)
		fmt.Printf("  Sync interval: %s\n", *syncInterval)
	} else {
		fmt.Printf("  Sync mode: disabled\n")
	}

	// Show current status
	index := mgr.GetIndex()
	bundleCount := index.Count()
	if bundleCount > 0 {
		fmt.Printf("  Bundles available: %d\n", bundleCount)
	} else if hasBundles {
		fmt.Printf("  Bundles available: 0 (rebuilding in background...)\n")
	} else {
		fmt.Printf("  Bundles available: 0\n")
	}

	fmt.Printf("\nPress Ctrl+C to stop\n\n")

	// Start sync if enabled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *sync {
		go runSync(ctx, mgr, *syncInterval)
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      newServerHandler(mgr, *sync, false), // WebSocket disabled for now
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func cmdCompare() {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose output (show all differences)")
	fetchMissing := fs.Bool("fetch-missing", false, "fetch missing bundles from target")
	fs.Parse(os.Args[2:])

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle compare <target>\n")
		fmt.Fprintf(os.Stderr, "  target: path to plc_bundles.json or URL\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  plcbundle compare /path/to/plc_bundles.json\n")
		fmt.Fprintf(os.Stderr, "  plcbundle compare https://example.com/index.json\n")
		fmt.Fprintf(os.Stderr, "  plcbundle compare https://example.com/index.json --fetch-missing\n")
		os.Exit(1)
	}

	target := fs.Arg(0)

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Comparing: %s\n", dir)
	fmt.Printf("  Against: %s\n\n", target)

	// Load local index
	localIndex := mgr.GetIndex()

	// Load target index
	fmt.Printf("Loading target index...\n")
	targetIndex, err := loadTargetIndex(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading target index: %v\n", err)
		os.Exit(1)
	}

	// Perform comparison
	comparison := compareIndexes(localIndex, targetIndex)

	// Display results
	displayComparison(comparison, *verbose)

	// Fetch missing bundles if requested
	if *fetchMissing && len(comparison.MissingBundles) > 0 {
		fmt.Printf("\n")
		if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
			fmt.Fprintf(os.Stderr, "Error: --fetch-missing only works with remote URLs\n")
			os.Exit(1)
		}

		baseURL := strings.TrimSuffix(target, "/index.json")
		baseURL = strings.TrimSuffix(baseURL, "/plc_bundles.json")

		fmt.Printf("Fetching %d missing bundles...\n\n", len(comparison.MissingBundles))
		fetchMissingBundles(mgr, baseURL, comparison.MissingBundles)
	}

	// Exit with error if there are differences
	if comparison.HasDifferences() {
		os.Exit(1)
	}
}
