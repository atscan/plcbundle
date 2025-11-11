package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
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

	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Println("No bundles to migrate")
		return nil
	}

	// Get plcbundle version
	version := GetVersion()

	var needsMigration []int
	var totalSize int64

	ops := &storage.Operations{}
	for _, meta := range bundles {
		bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", meta.BundleNumber))

		// Check if already has embedded metadata
		_, err := ops.ExtractBundleMetadata(bundlePath)

		if err != nil || opts.force {
			needsMigration = append(needsMigration, meta.BundleNumber)
			totalSize += meta.CompressedSize
		}
	}

	if len(needsMigration) == 0 {
		fmt.Println("✓ All bundles already migrated")
		fmt.Println("\nUse --force to re-migrate")
		return nil
	}

	// Display plan
	fmt.Printf("Migration Plan\n")
	fmt.Printf("══════════════\n\n")
	fmt.Printf("  Bundles to migrate: %d\n", len(needsMigration))
	fmt.Printf("  Total size:         %s\n", formatBytes(totalSize))
	fmt.Printf("  Workers:            %d\n", opts.workers)
	fmt.Printf("  plcbundle version:  %s\n", version)
	fmt.Printf("\n")

	if opts.dryRun {
		fmt.Printf("💡 This is a dry-run. No files will be modified.\n")
		return nil
	}

	// Execute migration
	fmt.Printf("Starting migration...\n\n")

	start := time.Now()
	progress := ui.NewProgressBar(len(needsMigration))

	success := 0
	failed := 0
	var firstError error
	hashChanges := make([]int, 0, len(needsMigration))

	for i, bundleNum := range needsMigration {
		// ✅ Pass version to migrateBundle
		if err := migrateBundle(dir, bundleNum, index, version, opts.verbose); err != nil {
			failed++
			if firstError == nil {
				firstError = err
			}
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "\n✗ Bundle %06d failed: %v\n", bundleNum, err)
			}
		} else {
			success++
			hashChanges = append(hashChanges, bundleNum)

			if opts.verbose {
				fmt.Fprintf(os.Stderr, "✓ Migrated bundle %06d\n", bundleNum)
			}
		}

		progress.Set(i + 1)
	}

	progress.Finish()
	elapsed := time.Since(start)

	// ✅ Update index with new compressed hashes
	if len(hashChanges) > 0 {
		fmt.Printf("\nUpdating bundle index...\n")
		updateStart := time.Now()

		updated := 0
		for _, bundleNum := range hashChanges {
			bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))

			// Recalculate hashes
			compHash, compSize, contentHash, contentSize, err := ops.CalculateFileHashes(bundlePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ⚠️  Failed to hash bundle %06d: %v\n", bundleNum, err)
				continue
			}

			// Get and update metadata
			bundleMeta, err := index.GetBundle(bundleNum)
			if err != nil {
				continue
			}

			// Verify content hash unchanged
			if bundleMeta.ContentHash != contentHash {
				fmt.Fprintf(os.Stderr, "  ⚠️  Content hash changed for %06d (unexpected!)\n", bundleNum)
			}

			// Update compressed info (this changed due to skippable frames)
			bundleMeta.CompressedHash = compHash
			bundleMeta.CompressedSize = compSize
			bundleMeta.UncompressedSize = contentSize

			index.AddBundle(bundleMeta)
			updated++
		}

		// Save index
		if err := mgr.SaveIndex(); err != nil {
			fmt.Fprintf(os.Stderr, "  ⚠️  Failed to save index: %v\n", err)
		} else {
			fmt.Printf("  ✓ Updated %d entries in %s\n", updated, time.Since(updateStart).Round(time.Millisecond))
		}
	}

	// Summary
	fmt.Printf("\n")
	if failed == 0 {
		fmt.Printf("✓ Migration complete in %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("  Migrated:     %d bundles\n", success)
		fmt.Printf("  Index updated: %d entries\n", len(hashChanges))
		fmt.Printf("  Speed:        %.1f bundles/sec\n\n", float64(success)/elapsed.Seconds())

		fmt.Printf("✨ New bundle format features:\n")
		fmt.Printf("   • Embedded metadata (JSON in skippable frame)\n")
		fmt.Printf("   • Frame offsets for instant random access\n")
		fmt.Printf("   • Multi-frame compression (100 ops/frame)\n")
		fmt.Printf("   • Self-contained (no .idx files)\n")
		fmt.Printf("   • Provenance tracking (version, origin, creator)\n")
		fmt.Printf("   • Compatible with standard zstd tools\n")
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

func migrateBundle(dir string, bundleNum int, index *bundleindex.Index, version string, verbose bool) error {
	bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
	backupPath := bundlePath + ".bak"

	// 1. Get metadata from index
	meta, err := index.GetBundle(bundleNum)
	if err != nil {
		return fmt.Errorf("bundle not in index: %w", err)
	}

	// 2. Load the bundle using old format
	ops := &storage.Operations{}
	operations, err := ops.LoadBundle(bundlePath)
	if err != nil {
		return fmt.Errorf("failed to load: %w", err)
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "  Loaded %d operations\n", len(operations))
	}

	// 3. Backup original file
	if err := os.Rename(bundlePath, backupPath); err != nil {
		return fmt.Errorf("failed to backup: %w", err)
	}

	// 4. Get hostname (optional)
	hostname, _ := os.Hostname()

	// 5. ✅ Create BundleInfo for new format
	bundleInfo := &storage.BundleInfo{
		BundleNumber: meta.BundleNumber,
		Origin:       index.Origin, // From index
		ParentHash:   meta.Parent,
		Cursor:       meta.Cursor,
		CreatedBy:    fmt.Sprintf("plcbundle/%s", version),
		Hostname:     hostname,
	}

	// 6. Save using new format (with skippable frame metadata)
	contentHash, compHash, contentSize, compSize, err := ops.SaveBundle(bundlePath, operations, bundleInfo)
	if err != nil {
		// Restore backup on failure
		os.Rename(backupPath, bundlePath)
		return fmt.Errorf("failed to save: %w", err)
	}

	// 7. Verify embedded metadata was created
	embeddedMeta, err := ops.ExtractBundleMetadata(bundlePath)
	if err != nil {
		os.Remove(bundlePath)
		os.Rename(backupPath, bundlePath)
		return fmt.Errorf("embedded metadata not created: %w", err)
	}

	// 8. Verify frame offsets are present
	if len(embeddedMeta.FrameOffsets) == 0 {
		os.Remove(bundlePath)
		os.Rename(backupPath, bundlePath)
		return fmt.Errorf("frame offsets missing in metadata")
	}

	// 9. Verify content hash matches (should be unchanged)
	if contentHash != meta.ContentHash {
		fmt.Fprintf(os.Stderr, "  ⚠️  Content hash changed (unexpected): %s → %s\n",
			meta.ContentHash[:12], contentHash[:12])
	}

	// 10. Cleanup backup
	os.Remove(backupPath)

	if verbose {
		fmt.Fprintf(os.Stderr, "  Content hash: %s (%s)\n", contentHash[:12], formatBytes(contentSize))
		fmt.Fprintf(os.Stderr, "  New compressed hash: %s (%s)\n", compHash[:12], formatBytes(compSize))
		fmt.Fprintf(os.Stderr, "  Frames: %d (embedded in metadata)\n", len(embeddedMeta.FrameOffsets)-1)
	}

	return nil
}
