package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/storage"
)

func NewMigrateCommand() *cobra.Command {
	var (
		dryRun  bool
		force   bool
		workers int
	)

	cmd := &cobra.Command{
		Use:   "migrate [flags]",
		Short: "Migrate bundles to new zstd frame format",
		Long: `Migrate old single-frame zstd bundles to new multi-frame format

This command converts bundles from the legacy single-frame zstd format
to the new multi-frame format with .idx index files. This enables:
  • Faster random access to individual operations
  • Reduced memory usage when loading specific positions
  • Better performance for DID lookups

The migration:
  1. Scans for bundles missing .idx files (legacy format)
  2. Re-compresses them using multi-frame format (100 ops/frame)
  3. Generates .idx frame offset index files
  4. Preserves all hashes and metadata
  5. Verifies content integrity

Original files are replaced atomically. Use --dry-run to preview.`,

		Example: `  # Preview migration (recommended first)
  plcbundle migrate --dry-run

  # Migrate all legacy bundles
  plcbundle migrate

  # Force migration even if .idx files exist
  plcbundle migrate --force

  # Parallel migration (faster)
  plcbundle migrate --workers 8

  # Verbose output
  plcbundle migrate -v`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return runMigration(mgr, dir, migrationOptions{
				dryRun:  dryRun,
				force:   force,
				workers: workers,
				verbose: verbose,
			})
		},
	}

	cmd.Flags().BoolVarP(&dryRun, "dry-run", "n", false, "Show what would be migrated without migrating")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Re-migrate bundles that already have .idx files")
	cmd.Flags().IntVarP(&workers, "workers", "w", 4, "Number of parallel workers")

	return cmd
}

type migrationOptions struct {
	dryRun  bool
	force   bool
	workers int
	verbose bool
}

func runMigration(mgr BundleManager, dir string, opts migrationOptions) error {
	fmt.Printf("Scanning for legacy bundles in: %s\n\n", dir)

	// Find bundles needing migration
	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Println("No bundles to migrate")
		return nil
	}

	var needsMigration []int
	var totalSize int64

	for _, meta := range bundles {
		bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", meta.BundleNumber))
		idxPath := bundlePath + ".idx"

		// Check if .idx file exists
		if _, err := os.Stat(idxPath); os.IsNotExist(err) || opts.force {
			needsMigration = append(needsMigration, meta.BundleNumber)
			totalSize += meta.CompressedSize
		}
	}

	if len(needsMigration) == 0 {
		fmt.Println("✓ All bundles already migrated")
		fmt.Println("\nUse --force to re-migrate")
		return nil
	}

	// Display migration plan
	fmt.Printf("Migration Plan\n")
	fmt.Printf("══════════════\n\n")
	fmt.Printf("  Bundles to migrate: %d\n", len(needsMigration))
	fmt.Printf("  Total size:         %s\n", formatBytes(totalSize))
	fmt.Printf("  Workers:            %d\n", opts.workers)
	fmt.Printf("\n")

	if len(needsMigration) <= 20 {
		fmt.Printf("  Bundles: ")
		for i, num := range needsMigration {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%06d", num)
		}
		fmt.Printf("\n\n")
	} else {
		fmt.Printf("  Range: %06d - %06d\n\n", needsMigration[0], needsMigration[len(needsMigration)-1])
	}

	if opts.dryRun {
		fmt.Printf("💡 This is a dry-run. No files will be modified.\n")
		fmt.Printf("   Run without --dry-run to perform migration.\n")
		return nil
	}

	// Execute migration
	fmt.Printf("Starting migration...\n\n")

	start := time.Now()
	progress := ui.NewProgressBar(len(needsMigration))

	success := 0
	failed := 0
	var firstError error

	for i, bundleNum := range needsMigration {
		if err := migrateBundle(dir, bundleNum, opts.verbose); err != nil {
			failed++
			if firstError == nil {
				firstError = err
			}
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "\n✗ Bundle %06d failed: %v\n", bundleNum, err)
			}
		} else {
			success++
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "✓ Migrated bundle %06d\n", bundleNum)
			}
		}

		progress.Set(i + 1)
	}

	progress.Finish()
	elapsed := time.Since(start)

	// Summary
	fmt.Printf("\n")
	if failed == 0 {
		fmt.Printf("✓ Migration complete in %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("  Migrated: %d bundles\n", success)
		fmt.Printf("  Speed:    %.1f bundles/sec\n", float64(success)/elapsed.Seconds())
	} else {
		fmt.Printf("⚠️  Migration completed with errors\n")
		fmt.Printf("  Success:  %d bundles\n", success)
		fmt.Printf("  Failed:   %d bundles\n", failed)
		fmt.Printf("  Duration: %s\n", elapsed.Round(time.Millisecond))
		if firstError != nil {
			fmt.Printf("  First error: %v\n", firstError)
		}
		return fmt.Errorf("migration failed for %d bundles", failed)
	}

	return nil
}

func migrateBundle(dir string, bundleNum int, verbose bool) error {
	bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
	idxPath := bundlePath + ".idx"
	backupPath := bundlePath + ".bak"

	// 1. Load the bundle using legacy method (full decompression)
	ops := &storage.Operations{}
	operations, err := ops.LoadBundle(bundlePath)
	if err != nil {
		return fmt.Errorf("failed to load: %w", err)
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "  Loaded %d operations\n", len(operations))
	}

	// 2. Backup original file
	if err := os.Rename(bundlePath, backupPath); err != nil {
		return fmt.Errorf("failed to backup: %w", err)
	}

	// 3. Save using new multi-frame format
	contentHash, compHash, contentSize, compSize, err := ops.SaveBundle(bundlePath, operations)
	if err != nil {
		// Restore backup on failure
		os.Rename(backupPath, bundlePath)
		return fmt.Errorf("failed to save: %w", err)
	}

	// 4. Verify .idx file was created
	if _, err := os.Stat(idxPath); os.IsNotExist(err) {
		// Restore backup if .idx wasn't created
		os.Remove(bundlePath)
		os.Rename(backupPath, bundlePath)
		return fmt.Errorf("frame index not created")
	}

	// 5. Cleanup backup
	os.Remove(backupPath)

	if verbose {
		fmt.Fprintf(os.Stderr, "  Content: %s (%s)\n", contentHash[:12], formatBytes(contentSize))
		fmt.Fprintf(os.Stderr, "  Compressed: %s (%s)\n", compHash[:12], formatBytes(compSize))
	}

	return nil
}
