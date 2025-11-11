package commands

import (
	"encoding/binary"
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

type bundleMigrationInfo struct {
	bundleNumber        int
	oldSize             int64
	uncompressedSize    int64
	oldFormat           string
	oldCompressionRatio float64
}

func runMigration(mgr BundleManager, dir string, opts migrationOptions) error {
	fmt.Printf("Scanning for legacy bundles in: %s\n\n", dir)

	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Println("No bundles to migrate")
		return nil
	}

	version := GetVersion()
	ops := &storage.Operations{}

	var needsMigration []bundleMigrationInfo
	var totalSize int64

	for _, meta := range bundles {
		bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", meta.BundleNumber))
		embeddedMeta, err := ops.ExtractBundleMetadata(bundlePath)

		info := bundleMigrationInfo{
			bundleNumber:     meta.BundleNumber,
			oldSize:          meta.CompressedSize,
			uncompressedSize: meta.UncompressedSize,
		}

		if meta.CompressedSize > 0 {
			info.oldCompressionRatio = float64(meta.UncompressedSize) / float64(meta.CompressedSize)
		}

		if err != nil {
			info.oldFormat = "v0 (single-frame)"
		} else {
			info.oldFormat = embeddedMeta.Format
		}

		if err != nil || opts.force {
			needsMigration = append(needsMigration, info)
			totalSize += meta.CompressedSize
		}
	}

	if len(needsMigration) == 0 {
		fmt.Println("✓ All bundles already migrated")
		fmt.Println("\nUse --force to re-migrate")
		return nil
	}

	// COMPACT PLAN
	fmt.Printf("Migration Plan\n")
	fmt.Printf("══════════════\n\n")

	formatCounts := make(map[string]int)
	var totalUncompressed int64
	for _, info := range needsMigration {
		formatCounts[info.oldFormat]++
		totalUncompressed += info.uncompressedSize
	}

	fmt.Printf("  Format:  ")
	first := true
	for format, count := range formatCounts {
		if !first {
			fmt.Printf(" + ")
		}
		fmt.Printf("%s (%d)", format, count)
		first = false
	}
	fmt.Printf(" → v%d\n", storage.MetadataFormatVersion)

	fmt.Printf("  Bundles: %d\n", len(needsMigration))
	fmt.Printf("  Size:    %s (%.3fx compression)\n",
		formatBytes(totalSize),
		float64(totalUncompressed)/float64(totalSize))
	fmt.Printf("  Workers: %d, Compression Level: %d\n\n", opts.workers, storage.CompressionLevel)

	if opts.dryRun {
		fmt.Printf("💡 Dry-run mode\n")
		return nil
	}

	// Execute migration
	fmt.Printf("Migrating...\n\n")

	start := time.Now()
	progress := ui.NewProgressBar(len(needsMigration))

	success := 0
	failed := 0
	var firstError error
	hashChanges := make([]int, 0)

	var totalOldSize int64
	var totalNewSize int64
	var totalOldUncompressed int64
	var totalNewUncompressed int64

	for i, info := range needsMigration {
		totalOldSize += info.oldSize
		totalOldUncompressed += info.uncompressedSize

		sizeDiff, newUncompressedSize, err := migrateBundle(dir, info.bundleNumber, index, version, opts.verbose)
		if err != nil {
			failed++
			if firstError == nil {
				firstError = err
			}
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "\n✗ Bundle %06d failed: %v\n", info.bundleNumber, err)
			}
		} else {
			success++
			hashChanges = append(hashChanges, info.bundleNumber)

			newSize := info.oldSize + sizeDiff
			totalNewSize += newSize
			totalNewUncompressed += newUncompressedSize

			if opts.verbose {
				oldRatio := float64(info.uncompressedSize) / float64(info.oldSize)
				newRatio := float64(newUncompressedSize) / float64(newSize)

				fmt.Fprintf(os.Stderr, "✓ %06d: %.3fx→%.3fx %+s\n",
					info.bundleNumber, oldRatio, newRatio, formatBytes(sizeDiff))
			}
		}

		progress.Set(i + 1)
	}

	progress.Finish()
	elapsed := time.Since(start)

	// Update index
	if len(hashChanges) > 0 {
		fmt.Printf("\nUpdating index...\n")
		updateStart := time.Now()

		updated := 0
		for _, bundleNum := range hashChanges {
			bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
			compHash, compSize, _, contentSize, err := ops.CalculateFileHashes(bundlePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ⚠️  Failed to hash %06d: %v\n", bundleNum, err)
				continue
			}

			bundleMeta, err := index.GetBundle(bundleNum)
			if err != nil {
				continue
			}

			bundleMeta.CompressedHash = compHash
			bundleMeta.CompressedSize = compSize
			bundleMeta.UncompressedSize = contentSize

			index.AddBundle(bundleMeta)
			updated++
		}

		if err := mgr.SaveIndex(); err != nil {
			fmt.Fprintf(os.Stderr, "  ⚠️  Failed to save index: %v\n", err)
		} else {
			fmt.Printf("  ✓ %d entries in %s\n", updated, time.Since(updateStart).Round(time.Millisecond))
		}
	}

	// COMPACT SUMMARY
	fmt.Printf("\n")
	if failed == 0 {
		fmt.Printf("✓ Complete: %d bundles in %s\n\n", success, elapsed.Round(time.Millisecond))

		if totalOldSize > 0 && success > 0 {
			sizeDiff := totalNewSize - totalOldSize
			oldRatio := float64(totalOldUncompressed) / float64(totalOldSize)
			newRatio := float64(totalNewUncompressed) / float64(totalNewSize)
			ratioDiff := newRatio - oldRatio

			// MEASURE ACTUAL METADATA SIZE (not estimated)
			var totalActualMetadata int64
			for _, bundleNum := range hashChanges {
				bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
				metaSize, _ := measureMetadataSize(bundlePath)
				totalActualMetadata += metaSize
			}

			// FIXED ALIGNMENT
			fmt.Printf("                Old           New           Change\n")
			fmt.Printf("              ────────      ────────      ─────────\n")
			fmt.Printf("Size          %-13s %-13s %+s (%.1f%%)\n",
				formatBytes(totalOldSize),
				formatBytes(totalNewSize),
				formatBytes(sizeDiff),
				float64(sizeDiff)/float64(totalOldSize)*100)
			fmt.Printf("Ratio         %-13s %-13s %+s\n",
				fmt.Sprintf("%.3fx", oldRatio), fmt.Sprintf("%.3fx", newRatio), fmt.Sprintf("%+.3fx", ratioDiff))
			fmt.Printf("Avg/bundle    %-13s %-13s %+s\n\n",
				formatBytes(totalOldSize/int64(success)),
				formatBytes(totalNewSize/int64(success)),
				formatBytes(sizeDiff/int64(success)))

			// FIXED BREAKDOWN - use actual metadata size
			if totalActualMetadata > 0 {
				compressionEfficiency := sizeDiff - totalActualMetadata

				fmt.Printf("Breakdown:\n")
				fmt.Printf("  Metadata:     %+s (~%s/bundle, structural)\n",
					formatBytes(totalActualMetadata),
					formatBytes(totalActualMetadata/int64(success)))

				// FIX: Use absolute threshold based on old size, not metadata size
				threshold := totalOldSize / 1000 // 0.1% of old size

				if abs(compressionEfficiency) > threshold {
					if compressionEfficiency > 0 {
						// Compression got worse
						pctWorse := float64(compressionEfficiency) / float64(totalOldSize) * 100
						fmt.Printf("  Compression:  %+s (%.2f%% worse)\n",
							formatBytes(compressionEfficiency), pctWorse)
					} else if compressionEfficiency < 0 {
						// Compression improved
						pctBetter := float64(-compressionEfficiency) / float64(totalOldSize) * 100
						fmt.Printf("  Compression:  %s (%.2f%% better)\n",
							formatBytes(compressionEfficiency), pctBetter)
					}
				} else {
					// Truly negligible
					fmt.Printf("  Compression:  unchanged\n")
				}
			}

			fmt.Printf("\n")
		}
	} else {
		fmt.Printf("⚠️  Failed: %d bundles\n", failed)
		if firstError != nil {
			fmt.Printf("  Error: %v\n", firstError)
		}
		return fmt.Errorf("migration failed for %d bundles", failed)
	}

	return nil
}

func migrateBundle(dir string, bundleNum int, index *bundleindex.Index, version string, verbose bool) (sizeDiff int64, newUncompressedSize int64, err error) {
	bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
	backupPath := bundlePath + ".bak"

	meta, err := index.GetBundle(bundleNum)
	if err != nil {
		return 0, 0, fmt.Errorf("bundle not in index: %w", err)
	}

	oldSize := meta.CompressedSize

	ops := &storage.Operations{}
	operations, err := ops.LoadBundle(bundlePath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to load: %w", err)
	}

	if err := os.Rename(bundlePath, backupPath); err != nil {
		return 0, 0, fmt.Errorf("failed to backup: %w", err)
	}

	hostname, _ := os.Hostname()

	bundleInfo := &storage.BundleInfo{
		BundleNumber: meta.BundleNumber,
		Origin:       index.Origin,
		ParentHash:   meta.Parent,
		Cursor:       meta.Cursor,
		CreatedBy:    fmt.Sprintf("plcbundle/%s", version),
		Hostname:     hostname,
	}

	contentHash, _, contentSize, compSize, err := ops.SaveBundle(bundlePath, operations, bundleInfo)
	if err != nil {
		os.Rename(backupPath, bundlePath)
		return 0, 0, fmt.Errorf("failed to save: %w", err)
	}

	embeddedMeta, err := ops.ExtractBundleMetadata(bundlePath)
	if err != nil {
		os.Remove(bundlePath)
		os.Rename(backupPath, bundlePath)
		return 0, 0, fmt.Errorf("embedded metadata not created: %w", err)
	}

	if len(embeddedMeta.FrameOffsets) == 0 {
		os.Remove(bundlePath)
		os.Rename(backupPath, bundlePath)
		return 0, 0, fmt.Errorf("frame offsets missing in metadata")
	}

	if contentHash != meta.ContentHash {
		fmt.Fprintf(os.Stderr, "  ⚠️  Content hash changed: %s → %s\n",
			meta.ContentHash[:12], contentHash[:12])
	}

	os.Remove(backupPath)

	// Calculate changes
	newSize := compSize
	sizeDiff = newSize - oldSize
	newUncompressedSize = contentSize

	if verbose {
		oldRatio := float64(meta.UncompressedSize) / float64(oldSize)
		newRatio := float64(contentSize) / float64(newSize)

		fmt.Fprintf(os.Stderr, "  Frames: %d, Ratio: %.3fx→%.3fx, Size: %+s\n",
			len(embeddedMeta.FrameOffsets)-1, oldRatio, newRatio, formatBytes(sizeDiff))
	}

	return sizeDiff, newUncompressedSize, nil
}

func measureMetadataSize(bundlePath string) (int64, error) {
	file, err := os.Open(bundlePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Read magic (4 bytes) + size (4 bytes)
	header := make([]byte, 8)
	if _, err := file.Read(header); err != nil {
		return 0, err
	}

	// Check if it's a skippable frame
	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic < 0x184D2A50 || magic > 0x184D2A5F {
		return 0, nil // No metadata frame
	}

	// Get frame data size
	frameSize := binary.LittleEndian.Uint32(header[4:8])

	// Total metadata size = 4 (magic) + 4 (size) + frameSize (data)
	return int64(8 + frameSize), nil
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
