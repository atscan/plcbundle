package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

func NewMempoolCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "mempool [status]",
		Aliases: []string{"mp"},
		Short:   "Manage mempool operations",
		Long: `Manage mempool operations

The mempool stores operations waiting to be bundled. It maintains
strict chronological order and automatically validates consistency.`,

		Example: `  # Show mempool status
  plcbundle mempool
  plcbundle mempool status

  # Clear all operations
  plcbundle mempool clear

  # Export operations as JSONL
  plcbundle mempool dump
  plcbundle mempool dump > operations.jsonl

  # Using alias
  plcbundle mp status`,

		RunE: func(cmd *cobra.Command, args []string) error {
			// Default to status subcommand
			return mempoolStatus(cmd, args)
		},
	}

	// Add subcommands
	cmd.AddCommand(newMempoolStatusCommand())
	cmd.AddCommand(newMempoolClearCommand())
	cmd.AddCommand(newMempoolDumpCommand())

	return cmd
}

// ============================================================================
// MEMPOOL STATUS - Show current state
// ============================================================================

func newMempoolStatusCommand() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"s", "info"},
		Short:   "Show mempool status",
		Long: `Show mempool status and statistics

Displays current mempool state including operation count, progress toward
next bundle, validation status, and memory usage.`,

		Example: `  # Show status
  plcbundle mempool status
  plcbundle mempool

  # Verbose output with samples
  plcbundle mempool status -v`,

		RunE: func(cmd *cobra.Command, args []string) error {
			return mempoolStatus(cmd, args)
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Show sample operations")

	return cmd
}

func mempoolStatus(cmd *cobra.Command, args []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")
	if cmd.Parent() != nil {
		// Called as subcommand, check parent's verbose flag
		if v, err := cmd.Parent().Flags().GetBool("verbose"); err == nil && v {
			verbose = true
		}
	}

	mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
	if err != nil {
		return err
	}
	defer mgr.Close()

	return showMempoolStatus(mgr, dir, verbose)
}

// ============================================================================
// MEMPOOL CLEAR - Remove all operations
// ============================================================================

func newMempoolClearCommand() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:     "clear",
		Aliases: []string{"c", "reset"},
		Short:   "Clear mempool operations",
		Long: `Clear all operations from mempool

Removes all operations from the mempool and saves the empty state.
This is a destructive operation that cannot be undone.

Use cases:
  • Reset after testing
  • Clear corrupted data
  • Force fresh start`,

		Example: `  # Clear with confirmation
  plcbundle mempool clear

  # Force clear without confirmation
  plcbundle mempool clear --force
  plcbundle mempool clear -f`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			stats := mgr.GetMempoolStats()
			count := stats["count"].(int)

			if count == 0 {
				fmt.Println("Mempool is already empty")
				return nil
			}

			fmt.Printf("Working in: %s\n\n", dir)

			if !force {
				fmt.Printf("⚠️  This will clear %d operations from the mempool.\n", count)
				fmt.Printf("Are you sure? [y/N]: ")
				var response string
				fmt.Scanln(&response)
				if strings.ToLower(strings.TrimSpace(response)) != "y" {
					fmt.Println("Cancelled")
					return nil
				}
			}

			if err := mgr.ClearMempool(); err != nil {
				return fmt.Errorf("clear failed: %w", err)
			}

			fmt.Printf("\n✓ Mempool cleared (%d operations removed)\n", count)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Skip confirmation prompt")

	return cmd
}

// ============================================================================
// MEMPOOL DUMP - Export operations as JSONL
// ============================================================================

func newMempoolDumpCommand() *cobra.Command {
	var outputFile string

	cmd := &cobra.Command{
		Use:     "dump",
		Aliases: []string{"export", "d"},
		Short:   "Export mempool as JSONL",
		Long: `Export mempool operations as JSONL

Outputs all operations in the mempool as newline-delimited JSON.
Perfect for backup, analysis, or piping to other tools.`,

		Example: `  # Dump to stdout
  plcbundle mempool dump

  # Save to file
  plcbundle mempool dump > mempool.jsonl
  plcbundle mempool dump -o mempool.jsonl

  # Pipe to jq
  plcbundle mempool dump | jq -r .did

  # Count operations
  plcbundle mempool dump | wc -l

  # Using alias
  plcbundle mempool export`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			ops, err := mgr.GetMempoolOperations()
			if err != nil {
				return fmt.Errorf("failed to get mempool operations: %w", err)
			}

			if len(ops) == 0 {
				fmt.Fprintf(os.Stderr, "Mempool is empty\n")
				return nil
			}

			// Determine output destination
			var output *os.File
			if outputFile != "" {
				output, err = os.Create(outputFile)
				if err != nil {
					return fmt.Errorf("failed to create output file: %w", err)
				}
				defer output.Close()
				fmt.Fprintf(os.Stderr, "Exporting to: %s\n", outputFile)
			} else {
				output = os.Stdout
			}

			// Write JSONL
			for _, op := range ops {
				if len(op.RawJSON) > 0 {
					output.Write(op.RawJSON)
				} else {
					data, _ := json.Marshal(op)
					output.Write(data)
				}
				output.Write([]byte("\n"))
			}

			fmt.Fprintf(os.Stderr, "Exported %d operations from mempool\n", len(ops))
			return nil
		},
	}

	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file (default: stdout)")

	return cmd
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func showMempoolStatus(mgr BundleManager, dir string, verbose bool) error {
	stats := mgr.GetMempoolStats()
	count := stats["count"].(int)
	canCreate := stats["can_create_bundle"].(bool)
	targetBundle := stats["target_bundle"].(int)
	minTimestamp := stats["min_timestamp"].(time.Time)
	validated := stats["validated"].(bool)

	fmt.Printf("Mempool Status\n")
	fmt.Printf("══════════════\n\n")
	fmt.Printf("  Directory:      %s\n", dir)
	fmt.Printf("  Target bundle:  %06d\n", targetBundle)
	fmt.Printf("  Operations:     %d / %d\n", count, types.BUNDLE_SIZE)
	fmt.Printf("  Min timestamp:  %s\n\n", minTimestamp.Format("2006-01-02 15:04:05"))

	// Validation status
	validationIcon := "✓"
	if !validated {
		validationIcon = "⚠️"
	}
	fmt.Printf("  Validated:      %s %v\n", validationIcon, validated)

	if count > 0 {
		// Size information
		if sizeBytes, ok := stats["size_bytes"].(int); ok {
			fmt.Printf("  Size:           %.2f KB\n", float64(sizeBytes)/1024)
		}

		// Time range
		if firstTime, ok := stats["first_time"].(time.Time); ok {
			fmt.Printf("  First op:       %s\n", firstTime.Format("2006-01-02 15:04:05"))
		}
		if lastTime, ok := stats["last_time"].(time.Time); ok {
			fmt.Printf("  Last op:        %s\n", lastTime.Format("2006-01-02 15:04:05"))
		}

		fmt.Printf("\n")

		// Progress bar
		progress := float64(count) / float64(types.BUNDLE_SIZE) * 100
		fmt.Printf("  Progress:       %.1f%% (%d/%d)\n", progress, count, types.BUNDLE_SIZE)

		barWidth := 40
		filled := int(float64(barWidth) * float64(count) / float64(types.BUNDLE_SIZE))
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		fmt.Printf("  [%s]\n\n", bar)

		// Bundle creation status
		if canCreate {
			fmt.Printf("  ✓ Ready to create bundle\n")
		} else {
			remaining := types.BUNDLE_SIZE - count
			fmt.Printf("  Need %s more operations\n", formatNumber(remaining))
		}
	} else {
		fmt.Printf("\n  (empty)\n")
	}

	fmt.Printf("\n")

	// Verbose: Show sample operations
	if verbose && count > 0 {
		fmt.Printf("Sample Operations (first 10)\n")
		fmt.Printf("────────────────────────────\n\n")

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
			fmt.Printf("\n")
		}

		if len(ops) > showCount {
			fmt.Printf("  ... and %d more\n\n", len(ops)-showCount)
		}
	}

	// Show mempool file location
	mempoolFilename := fmt.Sprintf("plc_mempool_%06d.jsonl", targetBundle)
	fmt.Printf("File: %s\n", filepath.Join(dir, mempoolFilename))

	return nil
}
