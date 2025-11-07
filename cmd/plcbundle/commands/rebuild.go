package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

// RebuildCommand handles the rebuild subcommand
func RebuildCommand(args []string) error {
	fs := flag.NewFlagSet("rebuild", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose output")
	workers := fs.Int("workers", 0, "number of parallel workers (0 = CPU count)")
	noProgress := fs.Bool("no-progress", false, "disable progress bar")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Auto-detect CPU count
	if *workers == 0 {
		*workers = runtime.NumCPU()
	}

	// Get working directory
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Create manager WITHOUT auto-rebuild
	config := bundle.DefaultConfig(dir)
	config.AutoRebuild = false
	config.RebuildWorkers = *workers

	mgr, err := bundle.NewManager(config, nil)
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Rebuilding index from: %s\n", dir)
	fmt.Printf("Using %d workers\n", *workers)

	// Find all bundle files
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl.zst"))
	if err != nil {
		return fmt.Errorf("error scanning directory: %w", err)
	}

	// Filter out hidden/temp files
	files = filterBundleFiles(files)

	if len(files) == 0 {
		fmt.Println("No bundle files found")
		return nil
	}

	fmt.Printf("Found %d bundle files\n\n", len(files))

	start := time.Now()

	// Create progress bar
	var progress *ui.ProgressBar
	var progressCallback func(int, int, int64)

	if !*noProgress {
		fmt.Println("Processing bundles:")
		progress = ui.NewProgressBar(len(files))

		progressCallback = func(current, total int, bytesProcessed int64) {
			progress.SetWithBytes(current, bytesProcessed)
		}
	}

	// Use parallel scan
	result, err := mgr.ScanDirectoryParallel(*workers, progressCallback)

	if err != nil {
		if progress != nil {
			progress.Finish()
		}
		return fmt.Errorf("rebuild failed: %w", err)
	}

	if progress != nil {
		progress.Finish()
	}

	elapsed := time.Since(start)

	fmt.Printf("\n✓ Index rebuilt in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total bundles:      %d\n", result.BundleCount)
	fmt.Printf("  Compressed size:    %s\n", formatBytes(result.TotalSize))
	fmt.Printf("  Uncompressed size:  %s\n", formatBytes(result.TotalUncompressed))

	if result.TotalUncompressed > 0 {
		ratio := float64(result.TotalUncompressed) / float64(result.TotalSize)
		fmt.Printf("  Compression ratio:  %.2fx\n", ratio)
	}

	fmt.Printf("  Average speed:      %.1f bundles/sec\n", float64(result.BundleCount)/elapsed.Seconds())

	if elapsed.Seconds() > 0 {
		compressedThroughput := float64(result.TotalSize) / elapsed.Seconds() / (1000 * 1000)
		uncompressedThroughput := float64(result.TotalUncompressed) / elapsed.Seconds() / (1000 * 1000)
		fmt.Printf("  Throughput (compressed):   %.1f MB/s\n", compressedThroughput)
		fmt.Printf("  Throughput (uncompressed): %.1f MB/s\n", uncompressedThroughput)
	}

	fmt.Printf("  Index file:         %s\n", filepath.Join(dir, bundleindex.INDEX_FILE))

	if len(result.MissingGaps) > 0 {
		fmt.Printf("  ⚠️  Missing gaps:     %d bundles\n", len(result.MissingGaps))
	}

	// Verify chain if verbose
	if *verbose {
		fmt.Printf("\nVerifying chain integrity...\n")

		ctx := context.Background()
		verifyResult, err := mgr.VerifyChain(ctx)
		if err != nil {
			fmt.Printf("  ⚠️  Verification error: %v\n", err)
		} else if verifyResult.Valid {
			fmt.Printf("  ✓ Chain is valid (%d bundles verified)\n", len(verifyResult.VerifiedBundles))

			// Show head hash
			index := mgr.GetIndex()
			if lastMeta := index.GetLastBundle(); lastMeta != nil {
				fmt.Printf("  Chain head: %s...\n", lastMeta.Hash[:16])
			}
		} else {
			fmt.Printf("  ✗ Chain verification failed\n")
			fmt.Printf("  Broken at: bundle %06d\n", verifyResult.BrokenAt)
			fmt.Printf("  Error: %s\n", verifyResult.Error)
		}
	}

	return nil
}

func filterBundleFiles(files []string) []string {
	filtered := make([]string, 0, len(files))
	for _, file := range files {
		basename := filepath.Base(file)
		if len(basename) > 0 && (basename[0] == '.' || basename[0] == '_') {
			continue
		}
		filtered = append(filtered, file)
	}
	return filtered
}
