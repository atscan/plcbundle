package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func NewCleanCommand() *cobra.Command {
	var (
		dryRun         bool
		force          bool
		aggressive     bool
		keepTemp       bool
		keepOldMempool bool
		olderThanDays  int
	)

	cmd := &cobra.Command{
		Use:     "clean [flags]",
		Aliases: []string{"cleanup", "gc"},
		Short:   "Clean up temporary and orphaned files",
		Long: `Clean up temporary and orphaned files from repository

Removes:
  • Temporary files (.tmp, .tmp.*)
  • Old mempool files (keeps current)
  • Orphaned bundle files (not in index)
  • DID index cache (if --aggressive)

By default, performs a safe clean. Use --aggressive for deeper cleaning.
Always use --dry-run first to preview what will be deleted.`,

		Example: `  # Preview what would be cleaned (RECOMMENDED FIRST)
  plcbundle clean --dry-run

  # Safe clean (remove temp files and old mempools)
  plcbundle clean

  # Aggressive clean (also remove orphaned bundles and caches)
  plcbundle clean --aggressive

  # Clean but keep temporary download files
  plcbundle clean --keep-temp

  # Force clean without confirmation
  plcbundle clean --force

  # Clean files older than 7 days only
  plcbundle clean --older-than 7

  # Full cleanup with preview
  plcbundle clean --aggressive --dry-run`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return runClean(mgr, dir, cleanOptions{
				dryRun:         dryRun,
				force:          force,
				aggressive:     aggressive,
				keepTemp:       keepTemp,
				keepOldMempool: keepOldMempool,
				olderThanDays:  olderThanDays,
				verbose:        verbose,
			})
		},
	}

	// Flags
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "n", false, "Show what would be deleted without deleting")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Skip confirmation prompt")
	cmd.Flags().BoolVar(&aggressive, "aggressive", false, "Also remove orphaned bundles and caches")
	cmd.Flags().BoolVar(&keepTemp, "keep-temp", false, "Don't delete temporary files")
	cmd.Flags().BoolVar(&keepOldMempool, "keep-old-mempool", false, "Don't delete old mempool files")
	cmd.Flags().IntVar(&olderThanDays, "older-than", 0, "Only clean files older than N days (0 = all)")

	return cmd
}

type cleanOptions struct {
	dryRun         bool
	force          bool
	aggressive     bool
	keepTemp       bool
	keepOldMempool bool
	olderThanDays  int
	verbose        bool
}

type cleanPlan struct {
	tempFiles       []fileInfo
	oldMempoolFiles []fileInfo
	orphanedBundles []fileInfo
	didIndexCache   bool
	totalSize       int64
	totalFiles      int
}

type fileInfo struct {
	path    string
	size    int64
	modTime time.Time
	reason  string
}

func runClean(mgr BundleManager, dir string, opts cleanOptions) error {
	fmt.Printf("Analyzing repository: %s\n\n", dir)

	// Calculate age cutoff
	var cutoffTime time.Time
	if opts.olderThanDays > 0 {
		cutoffTime = time.Now().AddDate(0, 0, -opts.olderThanDays)
		fmt.Printf("Only cleaning files older than %d days (%s)\n\n",
			opts.olderThanDays, cutoffTime.Format("2006-01-02"))
	}

	// Build clean plan
	plan, err := buildCleanPlan(mgr, dir, opts, cutoffTime)
	if err != nil {
		return fmt.Errorf("failed to build clean plan: %w", err)
	}

	// Display plan
	displayCleanPlan(plan, opts)

	// If dry-run, stop here
	if opts.dryRun {
		fmt.Printf("\n💡 This was a dry-run. No files were deleted.\n")
		fmt.Printf("   Run without --dry-run to perform cleanup.\n")
		return nil
	}

	// If nothing to clean
	if plan.totalFiles == 0 {
		fmt.Printf("✓ Repository is clean (nothing to remove)\n")
		return nil
	}

	// Confirm unless forced
	if !opts.force {
		if !confirmClean(plan) {
			fmt.Println("Cancelled")
			return nil
		}
		fmt.Println()
	}

	// Execute cleanup
	return executeClean(plan, opts)
}

func buildCleanPlan(mgr BundleManager, dir string, opts cleanOptions, cutoffTime time.Time) (*cleanPlan, error) {
	plan := &cleanPlan{
		tempFiles:       make([]fileInfo, 0),
		oldMempoolFiles: make([]fileInfo, 0),
		orphanedBundles: make([]fileInfo, 0),
	}

	// Get current mempool info
	mempoolStats := mgr.GetMempoolStats()
	currentMempoolBundle := mempoolStats["target_bundle"].(int)
	currentMempoolFile := fmt.Sprintf("plc_mempool_%06d.jsonl", currentMempoolBundle)

	// Scan directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Get index for orphan detection
	var indexedBundles map[int]bool
	if opts.aggressive {
		indexedBundles = make(map[int]bool)
		index := mgr.GetIndex()
		for _, meta := range index.GetBundles() {
			indexedBundles[meta.BundleNumber] = true
		}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		fullPath := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Skip if newer than cutoff
		if !cutoffTime.IsZero() && info.ModTime().After(cutoffTime) {
			continue
		}

		// 1. Temporary files
		if !opts.keepTemp && isTempFile(name) {
			plan.tempFiles = append(plan.tempFiles, fileInfo{
				path:    fullPath,
				size:    info.Size(),
				modTime: info.ModTime(),
				reason:  "temporary file",
			})
			plan.totalSize += info.Size()
			plan.totalFiles++
			continue
		}

		// 2. Old mempool files
		if !opts.keepOldMempool && isMempoolFile(name) && name != currentMempoolFile {
			plan.oldMempoolFiles = append(plan.oldMempoolFiles, fileInfo{
				path:    fullPath,
				size:    info.Size(),
				modTime: info.ModTime(),
				reason:  "old mempool file",
			})
			plan.totalSize += info.Size()
			plan.totalFiles++
			continue
		}

		// 3. Orphaned bundles (aggressive mode only)
		if opts.aggressive && isBundleFile(name) {
			bundleNum := extractBundleNumber(name)
			if bundleNum > 0 && !indexedBundles[bundleNum] {
				plan.orphanedBundles = append(plan.orphanedBundles, fileInfo{
					path:    fullPath,
					size:    info.Size(),
					modTime: info.ModTime(),
					reason:  "not in index",
				})
				plan.totalSize += info.Size()
				plan.totalFiles++
			}
		}
	}

	// 4. DID index cache (aggressive mode only)
	if opts.aggressive {
		didIndexDir := filepath.Join(dir, ".plcbundle")
		if stat, err := os.Stat(didIndexDir); err == nil && stat.IsDir() {
			plan.didIndexCache = true
		}
	}

	return plan, nil
}

func displayCleanPlan(plan *cleanPlan, opts cleanOptions) {
	if opts.dryRun {
		fmt.Printf("╔════════════════════════════════════════════════════════════════╗\n")
		fmt.Printf("║                     DRY-RUN MODE                               ║\n")
		fmt.Printf("║               (showing what would be deleted)                  ║\n")
		fmt.Printf("╚════════════════════════════════════════════════════════════════╝\n\n")
	} else {
		fmt.Printf("Clean Plan\n")
		fmt.Printf("══════════\n\n")
	}

	// Display by category
	if len(plan.tempFiles) > 0 {
		displayFileCategory("Temporary Files", plan.tempFiles, opts.verbose)
	}

	if len(plan.oldMempoolFiles) > 0 {
		displayFileCategory("Old Mempool Files", plan.oldMempoolFiles, opts.verbose)
	}

	if len(plan.orphanedBundles) > 0 {
		displayFileCategory("Orphaned Bundles", plan.orphanedBundles, opts.verbose)
	}

	if plan.didIndexCache {
		fmt.Printf("📁 DID Index Cache\n")
		fmt.Printf("   .plcbundle/ directory (will be preserved but shards unloaded)\n\n")
	}

	// Summary
	if plan.totalFiles > 0 {
		fmt.Printf("Summary\n")
		fmt.Printf("───────\n")
		fmt.Printf("  Files to delete:  %s\n", formatNumber(plan.totalFiles))
		fmt.Printf("  Space to free:    %s\n", formatBytes(plan.totalSize))
		fmt.Printf("\n")
	} else {
		fmt.Printf("✓ No files to clean\n\n")
	}
}

func displayFileCategory(title string, files []fileInfo, verbose bool) {
	if len(files) == 0 {
		return
	}

	totalSize := int64(0)
	for _, f := range files {
		totalSize += f.size
	}

	fmt.Printf("🗑️  %s (%d files, %s)\n", title, len(files), formatBytes(totalSize))

	if verbose {
		// Sort by size (largest first)
		sorted := make([]fileInfo, len(files))
		copy(sorted, files)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].size > sorted[j].size
		})

		displayCount := len(sorted)
		if displayCount > 20 {
			displayCount = 20
		}

		for i := 0; i < displayCount; i++ {
			f := sorted[i]
			age := time.Since(f.modTime)
			fmt.Printf("   • %-40s  %10s  %s ago\n",
				filepath.Base(f.path),
				formatBytes(f.size),
				formatDurationShort(age))
		}

		if len(sorted) > displayCount {
			fmt.Printf("   ... and %d more\n", len(sorted)-displayCount)
		}
	} else {
		// Compact summary
		fmt.Printf("   %d file(s) • %s\n", len(files), formatBytes(totalSize))
	}
	fmt.Printf("\n")
}

func confirmClean(plan *cleanPlan) bool {
	fmt.Printf("⚠️  This will delete %d file(s) (%s)\n",
		plan.totalFiles, formatBytes(plan.totalSize))
	fmt.Printf("Type 'clean' to confirm: ")

	var response string
	fmt.Scanln(&response)

	return strings.TrimSpace(response) == "clean"
}

func executeClean(plan *cleanPlan, opts cleanOptions) error {
	deleted := 0
	var totalFreed int64
	failed := 0
	var firstError error

	// Delete temp files
	for _, f := range plan.tempFiles {
		if opts.verbose {
			fmt.Printf("  Deleting: %s\n", filepath.Base(f.path))
		}

		if err := os.Remove(f.path); err != nil {
			failed++
			if firstError == nil {
				firstError = err
			}
			if opts.verbose {
				fmt.Printf("  ✗ Failed: %v\n", err)
			}
		} else {
			deleted++
			totalFreed += f.size
		}
	}

	// Delete old mempool files
	for _, f := range plan.oldMempoolFiles {
		if opts.verbose {
			fmt.Printf("  Deleting: %s\n", filepath.Base(f.path))
		}

		if err := os.Remove(f.path); err != nil {
			failed++
			if firstError == nil {
				firstError = err
			}
			if opts.verbose {
				fmt.Printf("  ✗ Failed: %v\n", err)
			}
		} else {
			deleted++
			totalFreed += f.size
		}
	}

	// Delete orphaned bundles (aggressive)
	if len(plan.orphanedBundles) > 0 {
		fmt.Printf("\nRemoving orphaned bundles...\n")
		for _, f := range plan.orphanedBundles {
			if opts.verbose {
				fmt.Printf("  Deleting: %s\n", filepath.Base(f.path))
			}

			if err := os.Remove(f.path); err != nil {
				failed++
				if firstError == nil {
					firstError = err
				}
				if opts.verbose {
					fmt.Printf("  ✗ Failed: %v\n", err)
				}
			} else {
				deleted++
				totalFreed += f.size
			}
		}
	}

	// Summary
	fmt.Printf("\n")
	if failed > 0 {
		fmt.Printf("⚠️  Cleanup completed with errors\n")
		fmt.Printf("  Deleted:     %d files\n", deleted)
		fmt.Printf("  Failed:      %d files\n", failed)
		fmt.Printf("  Space freed: %s\n", formatBytes(totalFreed))
		if firstError != nil {
			fmt.Printf("  First error: %v\n", firstError)
		}
		return fmt.Errorf("cleanup completed with %d errors", failed)
	}

	fmt.Printf("✓ Cleanup complete\n")
	fmt.Printf("  Deleted:     %d files\n", deleted)
	fmt.Printf("  Space freed: %s\n", formatBytes(totalFreed))

	return nil
}

// Helper functions

func isTempFile(name string) bool {
	return strings.HasSuffix(name, ".tmp") ||
		strings.Contains(name, ".tmp.") ||
		strings.HasPrefix(name, "._") ||
		strings.HasPrefix(name, ".~")
}

func isMempoolFile(name string) bool {
	return strings.HasPrefix(name, "plc_mempool_") &&
		strings.HasSuffix(name, ".jsonl")
}

func isBundleFile(name string) bool {
	return strings.HasSuffix(name, ".jsonl.zst") &&
		len(name) == len("000000.jsonl.zst")
}

func extractBundleNumber(name string) int {
	if !strings.HasSuffix(name, ".jsonl.zst") {
		return 0
	}
	numStr := strings.TrimSuffix(name, ".jsonl.zst")
	var num int
	fmt.Sscanf(numStr, "%d", &num)
	return num
}
