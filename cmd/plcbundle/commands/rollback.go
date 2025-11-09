package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

// RollbackCommand handles the rollback subcommand
func RollbackCommand(args []string) error {
	fs := flag.NewFlagSet("rollback", flag.ExitOnError)
	toBundle := fs.Int("to", 0, "rollback TO this bundle (keeps it, removes everything after)")
	last := fs.Int("last", 0, "rollback last N bundles")
	force := fs.Bool("force", false, "skip confirmation")
	rebuildDIDIndex := fs.Bool("rebuild-did-index", false, "rebuild DID index after rollback")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *toBundle == 0 && *last == 0 {
		return fmt.Errorf("usage: plcbundle rollback [options]\n\n" +
			"Options:\n" +
			"  --to <N>              Rollback TO bundle N (keeps N, removes after)\n" +
			"  --last <N>            Rollback last N bundles\n" +
			"  --force               Skip confirmation\n" +
			"  --rebuild-did-index   Rebuild DID index after rollback\n\n" +
			"Examples:\n" +
			"  plcbundle rollback --to 100      # Keep bundles 1-100\n" +
			"  plcbundle rollback --last 5      # Remove last 5 bundles\n" +
			"  plcbundle rollback --to 50 --force --rebuild-did-index")
	}

	if *toBundle > 0 && *last > 0 {
		return fmt.Errorf("cannot use both --to and --last together")
	}

	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Println("No bundles to rollback")
		return nil
	}

	// Determine target bundle
	var targetBundle int
	if *toBundle > 0 {
		targetBundle = *toBundle
	} else {
		targetBundle = bundles[len(bundles)-1].BundleNumber - *last
	}

	if targetBundle < 1 {
		return fmt.Errorf("invalid rollback: would result in no bundles (target: %d)", targetBundle)
	}

	// Find bundles to delete (everything AFTER target)
	var toDelete []*bundleindex.BundleMetadata
	var toKeep []*bundleindex.BundleMetadata

	for _, meta := range bundles {
		if meta.BundleNumber > targetBundle {
			toDelete = append(toDelete, meta)
		} else {
			toKeep = append(toKeep, meta)
		}
	}

	if len(toDelete) == 0 {
		fmt.Printf("Nothing to rollback (already at bundle %d)\n", targetBundle)
		return nil
	}

	// Display rollback plan
	fmt.Printf("╔════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                   ROLLBACK PLAN                        ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════╝\n\n")
	fmt.Printf("  Directory:      %s\n", dir)
	fmt.Printf("  Current state:  %d bundles (%06d - %06d)\n",
		len(bundles), bundles[0].BundleNumber, bundles[len(bundles)-1].BundleNumber)
	fmt.Printf("  Target:         bundle %06d\n", targetBundle)
	fmt.Printf("  Will DELETE:    %d bundle(s)\n\n", len(toDelete))

	fmt.Printf("Bundles to delete:\n")
	for i, meta := range toDelete {
		if i < 10 || i >= len(toDelete)-5 {
			fmt.Printf("  %06d  (%s, %s)\n",
				meta.BundleNumber,
				meta.CreatedAt.Format("2006-01-02 15:04"),
				formatBytes(meta.CompressedSize))
		} else if i == 10 {
			fmt.Printf("  ... (%d more bundles)\n", len(toDelete)-15)
		}
	}
	fmt.Printf("\n")

	// Calculate what will be deleted
	var deletedOps int
	var deletedSize int64
	for _, meta := range toDelete {
		deletedOps += meta.OperationCount
		deletedSize += meta.CompressedSize
	}

	fmt.Printf("⚠️  WARNING: This will delete:\n")
	fmt.Printf("    • %s operations\n", formatNumber(deletedOps))
	fmt.Printf("    • %s of data\n", formatBytes(deletedSize))
	fmt.Printf("    • Mempool will be cleared\n")
	if didStats := mgr.GetDIDIndexStats(); didStats["exists"].(bool) {
		fmt.Printf("    • DID index will be invalidated\n")
	}
	fmt.Printf("\n")

	if !*force {
		fmt.Printf("Type 'rollback' to confirm: ")
		var response string
		fmt.Scanln(&response)
		if strings.TrimSpace(response) != "rollback" {
			fmt.Println("Cancelled")
			return nil
		}
	}

	fmt.Printf("\n")

	// Step 1: Delete bundle files
	fmt.Printf("[1/4] Deleting bundle files...\n")
	deletedCount := 0
	for _, meta := range toDelete {
		bundlePath := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", meta.BundleNumber))
		if err := os.Remove(bundlePath); err != nil {
			if !os.IsNotExist(err) {
				fmt.Printf("  ⚠️  Failed to delete %06d: %v\n", meta.BundleNumber, err)
				continue
			}
		}
		deletedCount++
	}
	fmt.Printf("  ✓ Deleted %d bundle file(s)\n\n", deletedCount)

	// Step 2: Clear mempool
	fmt.Printf("[2/4] Clearing mempool...\n")
	if err := mgr.ClearMempool(); err != nil {
		fmt.Printf("  ⚠️  Warning: failed to clear mempool: %v\n", err)
	} else {
		fmt.Printf("  ✓ Mempool cleared\n\n")
	}

	// Step 3: Update bundle index
	fmt.Printf("[3/4] Updating bundle index...\n")
	index.Rebuild(toKeep)
	if err := mgr.SaveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}
	fmt.Printf("  ✓ Index updated (%d bundles)\n\n", len(toKeep))

	// Step 4: Handle DID index
	fmt.Printf("[4/4] DID index...\n")
	didStats := mgr.GetDIDIndexStats()
	if didStats["exists"].(bool) {
		if *rebuildDIDIndex {
			fmt.Printf("  Rebuilding DID index...\n")
			ctx := context.Background()
			if err := mgr.BuildDIDIndex(ctx, func(current, total int) {
				if current%100 == 0 || current == total {
					fmt.Printf("    Progress: %d/%d (%.1f%%)   \r",
						current, total, float64(current)/float64(total)*100)
				}
			}); err != nil {
				return fmt.Errorf("failed to rebuild DID index: %w", err)
			}
			fmt.Printf("\n  ✓ DID index rebuilt\n")
		} else {
			// Just mark as needing rebuild
			fmt.Printf("  ⚠️  DID index is out of date\n")
			fmt.Printf("     Run: plcbundle index build\n")
		}
	} else {
		fmt.Printf("  (no DID index)\n")
	}

	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              ROLLBACK COMPLETE                         ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════╝\n\n")

	if len(toKeep) > 0 {
		lastBundle := toKeep[len(toKeep)-1]
		fmt.Printf("  New state:\n")
		fmt.Printf("    Bundles:      %d (%06d - %06d)\n",
			len(toKeep), toKeep[0].BundleNumber, lastBundle.BundleNumber)
		fmt.Printf("    Chain head:   %s\n", lastBundle.Hash[:16]+"...")
		fmt.Printf("    Last update:  %s\n", lastBundle.EndTime.Format("2006-01-02 15:04:05"))
	} else {
		fmt.Printf("  State: empty (all bundles removed)\n")
	}

	fmt.Printf("\n")
	return nil
}
