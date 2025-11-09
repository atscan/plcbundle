package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

func NewRollbackCommand() *cobra.Command {
	var (
		toBundle        int
		last            int
		force           bool
		rebuildDIDIndex bool
		keepFiles       bool
	)

	cmd := &cobra.Command{
		Use:   "rollback [flags]",
		Short: "Rollback repository to earlier state",
		Long: `Rollback repository to an earlier bundle state

Removes bundles from the repository and resets the state to a specific
bundle. This is useful for:
  • Fixing corrupted bundles
  • Testing and development
  • Reverting unwanted changes
  • Chain integrity issues

IMPORTANT: This is a destructive operation that permanently deletes data.
Always verify your target bundle before proceeding.

The rollback process:
  1. Validates target state
  2. Shows detailed rollback plan
  3. Requires confirmation (unless --force)
  4. Deletes bundle files
  5. Clears mempool (incompatible with new state)
  6. Updates bundle index
  7. Optionally rebuilds DID index`,

		Example: `  # Rollback TO bundle 100 (keeps 1-100, removes 101+)
  plcbundle rollback --to 100

  # Remove last 5 bundles
  plcbundle rollback --last 5

  # Rollback without confirmation
  plcbundle rollback --to 50 --force

  # Rollback and rebuild DID index
  plcbundle rollback --to 100 --rebuild-did-index

  # Rollback but keep bundle files (index-only)
  plcbundle rollback --to 100 --keep-files`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return executeRollback(cmd.Context(), mgr, dir, rollbackOptions{
				toBundle:        toBundle,
				last:            last,
				force:           force,
				rebuildDIDIndex: rebuildDIDIndex,
				keepFiles:       keepFiles,
				verbose:         verbose,
			})
		},
	}

	// Flags
	cmd.Flags().IntVar(&toBundle, "to", 0, "Rollback TO this bundle (keeps it)")
	cmd.Flags().IntVar(&last, "last", 0, "Rollback last N bundles")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Skip confirmation prompt")
	cmd.Flags().BoolVar(&rebuildDIDIndex, "rebuild-did-index", false, "Rebuild DID index after rollback")
	cmd.Flags().BoolVar(&keepFiles, "keep-files", false, "Update index only (don't delete bundle files)")

	return cmd
}

type rollbackOptions struct {
	toBundle        int
	last            int
	force           bool
	rebuildDIDIndex bool
	keepFiles       bool
	verbose         bool
}

// rollbackPlan contains the calculated rollback plan
type rollbackPlan struct {
	targetBundle   int
	toKeep         []*bundleindex.BundleMetadata
	toDelete       []*bundleindex.BundleMetadata
	deletedOps     int
	deletedSize    int64
	hasMempool     bool
	hasDIDIndex    bool
	affectedPeriod string
}

func executeRollback(ctx context.Context, mgr BundleManager, dir string, opts rollbackOptions) error {
	// Step 1: Validate options and calculate plan
	plan, err := calculateRollbackPlan(mgr, opts)
	if err != nil {
		return err
	}

	// Step 2: Display plan and get confirmation
	displayRollbackPlan(dir, plan)

	if !opts.force {
		if !confirmRollback(opts.keepFiles) {
			fmt.Println("Cancelled")
			return nil
		}
		fmt.Println()
	}

	// Step 3: Execute rollback
	if err := performRollback(ctx, mgr, dir, plan, opts); err != nil {
		return err
	}

	// Step 4: Display success summary
	displayRollbackSuccess(plan, opts)

	return nil
}

// calculateRollbackPlan determines what will be affected
func calculateRollbackPlan(mgr BundleManager, opts rollbackOptions) (*rollbackPlan, error) {
	// Validate options
	if opts.toBundle == 0 && opts.last == 0 {
		return nil, fmt.Errorf("either --to or --last must be specified")
	}

	if opts.toBundle > 0 && opts.last > 0 {
		return nil, fmt.Errorf("cannot use both --to and --last together")
	}

	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		return nil, fmt.Errorf("no bundles to rollback")
	}

	// Calculate target bundle
	var targetBundle int
	if opts.toBundle > 0 {
		targetBundle = opts.toBundle
	} else {
		targetBundle = bundles[len(bundles)-1].BundleNumber - opts.last
	}

	// Validate target
	if targetBundle < 0 {
		return nil, fmt.Errorf("invalid target: would result in negative bundle number")
	}

	if targetBundle < 1 && len(bundles) > 0 {
		return nil, fmt.Errorf("invalid rollback: would delete all bundles (use --to 0 explicitly if intended)")
	}

	// Build plan
	plan := &rollbackPlan{
		targetBundle: targetBundle,
		toKeep:       make([]*bundleindex.BundleMetadata, 0),
		toDelete:     make([]*bundleindex.BundleMetadata, 0),
	}

	for _, meta := range bundles {
		if meta.BundleNumber > targetBundle {
			plan.toDelete = append(plan.toDelete, meta)
			plan.deletedOps += meta.OperationCount
			plan.deletedSize += meta.CompressedSize
		} else {
			plan.toKeep = append(plan.toKeep, meta)
		}
	}

	// Check if nothing to do
	if len(plan.toDelete) == 0 {
		return nil, fmt.Errorf("already at bundle %d (nothing to rollback)", targetBundle)
	}

	// Check for mempool
	mempoolStats := mgr.GetMempoolStats()
	plan.hasMempool = mempoolStats["count"].(int) > 0

	// Check for DID index
	didStats := mgr.GetDIDIndexStats()
	plan.hasDIDIndex = didStats["exists"].(bool)

	// Calculate affected time period
	if len(plan.toDelete) > 0 {
		start := plan.toDelete[0].StartTime
		end := plan.toDelete[len(plan.toDelete)-1].EndTime
		plan.affectedPeriod = fmt.Sprintf("%s to %s",
			start.Format("2006-01-02 15:04"),
			end.Format("2006-01-02 15:04"))
	}

	return plan, nil
}

// displayRollbackPlan shows what will happen
func displayRollbackPlan(dir string, plan *rollbackPlan) {
	fmt.Printf("╔════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                      ROLLBACK PLAN                             ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════════╝\n\n")

	fmt.Printf("📁 Repository\n")
	fmt.Printf("   Directory:       %s\n", dir)
	if len(plan.toKeep) > 0 {
		fmt.Printf("   Current state:   %d bundles (%06d → %06d)\n",
			len(plan.toKeep)+len(plan.toDelete),
			plan.toKeep[0].BundleNumber,
			plan.toDelete[len(plan.toDelete)-1].BundleNumber)
	}
	fmt.Printf("   Target:          bundle %06d\n\n", plan.targetBundle)

	fmt.Printf("🗑️  Will Delete\n")
	fmt.Printf("   Bundles:         %d\n", len(plan.toDelete))
	fmt.Printf("   Operations:      %s\n", formatNumber(plan.deletedOps))
	fmt.Printf("   Data size:       %s\n", formatBytes(plan.deletedSize))
	if plan.affectedPeriod != "" {
		fmt.Printf("   Time period:     %s\n", plan.affectedPeriod)
	}
	fmt.Printf("\n")

	// Show sample of deleted bundles
	if len(plan.toDelete) > 0 {
		fmt.Printf("   Bundles to delete:\n")
		displayCount := min(10, len(plan.toDelete))
		for i := 0; i < displayCount; i++ {
			meta := plan.toDelete[i]
			fmt.Printf("   • %06d  (%s, %s, %s ops)\n",
				meta.BundleNumber,
				meta.CreatedAt.Format("2006-01-02 15:04"),
				formatBytes(meta.CompressedSize),
				formatNumber(meta.OperationCount))
		}
		if len(plan.toDelete) > displayCount {
			fmt.Printf("   ... and %d more\n", len(plan.toDelete)-displayCount)
		}
		fmt.Printf("\n")
	}

	// Show impacts
	fmt.Printf("⚠️  Additional Impacts\n")
	if plan.hasMempool {
		fmt.Printf("   • Mempool will be cleared\n")
	}
	if plan.hasDIDIndex {
		fmt.Printf("   • DID index will need rebuilding\n")
	}
	if len(plan.toKeep) == 0 {
		fmt.Printf("   • Repository will be EMPTY after rollback\n")
	}
	fmt.Printf("\n")
}

// confirmRollback prompts user for confirmation
func confirmRollback(keepFiles bool) bool {
	if keepFiles {
		fmt.Printf("Type 'rollback-index' to confirm (index-only mode): ")
	} else {
		fmt.Printf("⚠️  This will permanently DELETE data!\n")
		fmt.Printf("Type 'rollback' to confirm: ")
	}

	var response string
	fmt.Scanln(&response)

	expectedResponse := "rollback"
	if keepFiles {
		expectedResponse = "rollback-index"
	}

	return strings.TrimSpace(response) == expectedResponse
}

// performRollback executes the rollback operations
func performRollback(ctx context.Context, mgr BundleManager, dir string, plan *rollbackPlan, opts rollbackOptions) error {
	totalSteps := 4
	currentStep := 0

	// Step 1: Delete bundle files (or skip if keepFiles)
	currentStep++
	if !opts.keepFiles {
		fmt.Printf("[%d/%d] Deleting bundle files...\n", currentStep, totalSteps)
		if err := deleteBundleFiles(dir, plan.toDelete, opts.verbose); err != nil {
			return fmt.Errorf("failed to delete bundles: %w", err)
		}
		fmt.Printf("      ✓ Deleted %d file(s)\n\n", len(plan.toDelete))
	} else {
		fmt.Printf("[%d/%d] Skipping file deletion (--keep-files)...\n", currentStep, totalSteps)
		fmt.Printf("      ℹ Bundle files remain on disk\n\n")
	}

	// Step 2: Clear mempool
	currentStep++
	fmt.Printf("[%d/%d] Clearing mempool...\n", currentStep, totalSteps)
	if err := clearMempool(mgr, plan.hasMempool); err != nil {
		return err
	}
	fmt.Printf("      ✓ Mempool cleared\n\n")

	// Step 3: Update index
	currentStep++
	fmt.Printf("[%d/%d] Updating bundle index...\n", currentStep, totalSteps)
	if err := updateBundleIndex(mgr, plan); err != nil {
		return err
	}
	fmt.Printf("      ✓ Index updated (%d bundles)\n\n", len(plan.toKeep))

	// Step 4: Handle DID index
	currentStep++
	fmt.Printf("[%d/%d] DID index...\n", currentStep, totalSteps)
	if err := handleDIDIndex(ctx, mgr, plan, opts); err != nil {
		return err
	}

	return nil
}

// deleteBundleFiles removes bundle files from disk
func deleteBundleFiles(dir string, bundles []*bundleindex.BundleMetadata, verbose bool) error {
	deletedCount := 0
	failedCount := 0
	var firstError error

	var progress *ui.ProgressBar
	if !verbose && len(bundles) > 10 {
		progress = ui.NewProgressBar(len(bundles))
	}

	for i, meta := range bundles {
		bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", meta.BundleNumber))

		if err := os.Remove(bundlePath); err != nil {
			if !os.IsNotExist(err) {
				failedCount++
				if firstError == nil {
					firstError = err
				}
				if verbose {
					fmt.Printf("      ⚠️  Failed to delete %06d: %v\n", meta.BundleNumber, err)
				}
				continue
			}
		}

		deletedCount++

		if verbose {
			fmt.Printf("      ✓ Deleted %06d (%s)\n",
				meta.BundleNumber,
				formatBytes(meta.CompressedSize))
		}

		if progress != nil {
			progress.Set(i + 1)
		}
	}

	if progress != nil {
		progress.Finish()
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to delete %d bundles (deleted %d successfully): %w",
			failedCount, deletedCount, firstError)
	}

	return nil
}

// clearMempool clears the mempool
func clearMempool(mgr BundleManager, hasMempool bool) error {
	if !hasMempool {
		return nil
	}

	if err := mgr.ClearMempool(); err != nil {
		return fmt.Errorf("failed to clear mempool: %w", err)
	}

	return nil
}

// updateBundleIndex updates the index to reflect rollback
func updateBundleIndex(mgr BundleManager, plan *rollbackPlan) error {
	index := mgr.GetIndex()
	index.Rebuild(plan.toKeep)

	if err := mgr.SaveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	return nil
}

// handleDIDIndex manages DID index state after rollback
func handleDIDIndex(ctx context.Context, mgr BundleManager, plan *rollbackPlan, opts rollbackOptions) error {
	if !plan.hasDIDIndex {
		fmt.Printf("      (no DID index)\n")
		return nil
	}

	if opts.rebuildDIDIndex {
		fmt.Printf("      Rebuilding DID index...\n")

		bundleCount := len(plan.toKeep)
		if bundleCount == 0 {
			fmt.Printf("      ℹ No bundles to index\n")
			return nil
		}

		var progress *ui.ProgressBar
		if !opts.verbose {
			progress = ui.NewProgressBar(bundleCount)
		}

		err := mgr.BuildDIDIndex(ctx, func(current, total int) {
			if progress != nil {
				progress.Set(current)
			} else if current%100 == 0 || current == total {
				fmt.Printf("      Progress: %d/%d (%.1f%%)   \r",
					current, total, float64(current)/float64(total)*100)
			}
		})

		if progress != nil {
			progress.Finish()
		}

		if err != nil {
			return fmt.Errorf("failed to rebuild DID index: %w", err)
		}

		fmt.Printf("      ✓ DID index rebuilt (%d bundles)\n", bundleCount)
	} else {
		// Get DID index config to show which bundle it's at
		didIndex := mgr.GetDIDIndex()
		config := didIndex.GetConfig()

		fmt.Printf("      ⚠️  DID index is out of date\n")
		if config.LastBundle > plan.targetBundle {
			fmt.Printf("         Index has bundle %06d, but repository now at %06d\n",
				config.LastBundle, plan.targetBundle)
		}
		fmt.Printf("         Run: plcbundle index build\n")
	}

	return nil
}

// displayRollbackSuccess shows the final state
func displayRollbackSuccess(plan *rollbackPlan, opts rollbackOptions) {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                  ROLLBACK COMPLETE                             ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════════╝\n\n")

	if len(plan.toKeep) > 0 {
		lastBundle := plan.toKeep[len(plan.toKeep)-1]
		firstBundle := plan.toKeep[0]

		fmt.Printf("📦 New State\n")
		fmt.Printf("   Bundles:         %d (%06d → %06d)\n",
			len(plan.toKeep),
			firstBundle.BundleNumber,
			lastBundle.BundleNumber)

		totalOps := 0
		totalSize := int64(0)
		for _, meta := range plan.toKeep {
			totalOps += meta.OperationCount
			totalSize += meta.CompressedSize
		}

		fmt.Printf("   Operations:      %s\n", formatNumber(totalOps))
		fmt.Printf("   Data size:       %s\n", formatBytes(totalSize))
		fmt.Printf("   Chain head:      %s...\n", lastBundle.Hash[:16])
		fmt.Printf("   Last updated:    %s\n", lastBundle.EndTime.Format("2006-01-02 15:04:05"))
	} else {
		fmt.Printf("📦 New State\n")
		fmt.Printf("   Repository:      EMPTY (all bundles removed)\n")
		if opts.keepFiles {
			fmt.Printf("   Note:            Bundle files remain on disk\n")
		}
	}

	fmt.Printf("\n")

	// Show what was removed
	fmt.Printf("🗑️  Removed\n")
	fmt.Printf("   Bundles:         %d\n", len(plan.toDelete))
	fmt.Printf("   Operations:      %s\n", formatNumber(plan.deletedOps))
	fmt.Printf("   Data freed:      %s\n", formatBytes(plan.deletedSize))

	if opts.keepFiles {
		fmt.Printf("   Files:           kept on disk\n")
	}

	fmt.Printf("\n")

	// Next steps
	if !opts.rebuildDIDIndex && plan.hasDIDIndex {
		fmt.Printf("💡 Next Steps\n")
		fmt.Printf("   DID index is out of date. Rebuild with:\n")
		fmt.Printf("   plcbundle index build\n\n")
	}
}

// Validation helpers

// validateRollbackSafety performs additional safety checks
func validateRollbackSafety(mgr BundleManager, plan *rollbackPlan) error {
	// Check for chain integrity issues
	if len(plan.toKeep) > 1 {
		// Verify the target bundle exists and has valid hash
		lastKeep := plan.toKeep[len(plan.toKeep)-1]
		if lastKeep.Hash == "" {
			return fmt.Errorf("target bundle %06d has no chain hash - may be corrupted",
				lastKeep.BundleNumber)
		}
	}

	return nil
}
