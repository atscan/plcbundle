package commands

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/types"
)

// MempoolCommand handles the mempool subcommand
func MempoolCommand(args []string) error {
	fs := flag.NewFlagSet("mempool", flag.ExitOnError)
	clear := fs.Bool("clear", false, "clear the mempool")
	export := fs.Bool("export", false, "export mempool operations as JSONL to stdout")
	refresh := fs.Bool("refresh", false, "reload mempool from disk")
	validate := fs.Bool("validate", false, "validate chronological order")
	verbose := fs.Bool("v", false, "verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n\n", dir)

	// Handle validate
	if *validate {
		fmt.Printf("Validating mempool chronological order...\n")
		if err := mgr.ValidateMempool(); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
		fmt.Printf("✓ Mempool validation passed\n")
		return nil
	}

	// Handle refresh
	if *refresh {
		fmt.Printf("Refreshing mempool from disk...\n")
		if err := mgr.RefreshMempool(); err != nil {
			return fmt.Errorf("refresh failed: %w", err)
		}

		if err := mgr.ValidateMempool(); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Warning: mempool validation failed after refresh: %v\n", err)
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
			return nil
		}

		fmt.Printf("⚠️  This will clear %d operations from the mempool.\n", count)
		fmt.Printf("Are you sure? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(strings.TrimSpace(response)) != "y" {
			fmt.Println("Cancelled")
			return nil
		}

		if err := mgr.ClearMempool(); err != nil {
			return fmt.Errorf("clear failed: %w", err)
		}

		fmt.Printf("✓ Mempool cleared (%d operations removed)\n", count)
		return nil
	}

	// Handle export
	if *export {
		ops, err := mgr.GetMempoolOperations()
		if err != nil {
			return fmt.Errorf("failed to get mempool operations: %w", err)
		}

		if len(ops) == 0 {
			fmt.Fprintf(os.Stderr, "Mempool is empty\n")
			return nil
		}

		for _, op := range ops {
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			}
		}

		fmt.Fprintf(os.Stderr, "Exported %d operations from mempool\n", len(ops))
		return nil
	}

	// Default: Show mempool stats
	return showMempoolStats(mgr, dir, *verbose)
}

func showMempoolStats(mgr BundleManager, dir string, verbose bool) error {
	stats := mgr.GetMempoolStats()
	count := stats["count"].(int)
	canCreate := stats["can_create_bundle"].(bool)
	targetBundle := stats["target_bundle"].(int)
	minTimestamp := stats["min_timestamp"].(time.Time)
	validated := stats["validated"].(bool)

	fmt.Printf("Mempool Status:\n")
	fmt.Printf("  Target bundle: %06d\n", targetBundle)
	fmt.Printf("  Operations: %d\n", count)
	fmt.Printf("  Can create bundle: %v (need %d)\n", canCreate, types.BUNDLE_SIZE)
	fmt.Printf("  Min timestamp: %s\n", minTimestamp.Format("2006-01-02 15:04:05"))

	validationIcon := "✓"
	if !validated {
		validationIcon = "⚠️"
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

		progress := float64(count) / float64(types.BUNDLE_SIZE) * 100
		fmt.Printf("  Progress: %.1f%% (%d/%d)\n", progress, count, types.BUNDLE_SIZE)

		// Progress bar
		barWidth := 40
		filled := int(float64(barWidth) * float64(count) / float64(types.BUNDLE_SIZE))
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		fmt.Printf("  [%s]\n", bar)
	} else {
		fmt.Printf("  (empty)\n")
	}

	// Verbose: Show sample operations
	if verbose && count > 0 {
		fmt.Println()
		fmt.Printf("Sample operations (showing up to 10):\n")

		ops, err := mgr.GetMempoolOperations()
		if err != nil {
			return fmt.Errorf("error getting operations: %w", err)
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

	return nil
}
