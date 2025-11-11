package commands

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

func NewOpCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "op",
		Aliases: []string{"operation", "record"},
		Short:   "Operation queries and inspection",
		Long: `Operation queries and inspection

Direct access to individual operations within bundles using either:
  • Bundle number + position (e.g., 42 1337)
  • Global position (e.g., 420000)

Global position format: (bundleNumber × 10,000) + position
Example: 88410345 = bundle 8841, position 345`,

		Example: `  # Get operation as JSON
  plcbundle op get 42 1337
  plcbundle op get 420000

  # Show operation (formatted)
  plcbundle op show 42 1337
  plcbundle op show 88410345

  # Find by CID
  plcbundle op find bafyreig3...`,
	}

	// Add subcommands
	cmd.AddCommand(newOpGetCommand())
	cmd.AddCommand(newOpShowCommand())
	cmd.AddCommand(newOpFindCommand())

	return cmd
}

// ============================================================================
// OP GET - Get operation as JSON
// ============================================================================

func newOpGetCommand() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:   "get <bundle> <position> | <globalPosition>",
		Short: "Get operation as JSON",
		Long: `Get operation as JSON (machine-readable)

Supports two input formats:
  1. Bundle number + position: get 42 1337
  2. Global position: get 420000

Global position = (bundleNumber × 10,000) + position

Use -v/--verbose to see detailed timing breakdown.`,

		Example: `  # By bundle + position
  plcbundle op get 42 1337

  # By global position
  plcbundle op get 88410345

  # With timing metrics
  plcbundle op get 42 1337 -v
  plcbundle op get 88410345 --verbose

  # Pipe to jq
  plcbundle op get 42 1337 | jq .did`,

		Args: cobra.RangeArgs(1, 2),

		RunE: func(cmd *cobra.Command, args []string) error {
			bundleNum, position, err := parseOpArgs(args)
			if err != nil {
				return err
			}

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			ctx := context.Background()

			// Time the operation load
			totalStart := time.Now()
			op, err := mgr.LoadOperation(ctx, bundleNum, position)
			totalDuration := time.Since(totalStart)

			if err != nil {
				return err
			}

			if verbose {
				globalPos := (bundleNum * 10000) + position

				// Log-style output (compact, single-line friendly)
				fmt.Fprintf(os.Stderr, "[Load] Bundle %06d:%04d (pos=%d) in %s",
					bundleNum, position, globalPos, totalDuration)
				fmt.Fprintf(os.Stderr, " | %d bytes", len(op.RawJSON))
				fmt.Fprintf(os.Stderr, "\n")
			}

			// Output raw JSON to stdout
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			} else {
				data, _ := json.Marshal(op)
				fmt.Println(string(data))
			}

			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Show timing metrics")

	return cmd
}

// // ============================================================================
// OP SHOW - Show operation (formatted)
// ============================================================================

func newOpShowCommand() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:   "show <bundle> <position> | <globalPosition>",
		Short: "Show operation (human-readable)",
		Long: `Show operation with formatted output

Displays operation in human-readable format with:
  • Bundle location and global position
  • DID and CID
  • Timestamp and age
  • Nullification status
  • Parsed operation details
  • Performance metrics (with -v)`,

		Example: `  # By bundle + position
  plcbundle op show 42 1337

  # By global position
  plcbundle op show 88410345

  # Verbose with timing and full JSON
  plcbundle op show 42 1337 -v`,

		Args: cobra.RangeArgs(1, 2),

		RunE: func(cmd *cobra.Command, args []string) error {
			bundleNum, position, err := parseOpArgs(args)
			if err != nil {
				return err
			}

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			ctx := context.Background()

			// Time the operation
			loadStart := time.Now()
			op, err := mgr.LoadOperation(ctx, bundleNum, position)
			loadDuration := time.Since(loadStart)

			if err != nil {
				return err
			}

			// Time the parsing
			parseStart := time.Now()
			opData, parseErr := op.GetOperationData()
			parseDuration := time.Since(parseStart)

			return displayOperationWithTiming(bundleNum, position, op, opData, parseErr,
				loadDuration, parseDuration, verbose)
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Show timing metrics and full JSON")

	return cmd
}

// ============================================================================
// OP FIND - Find operation by CID
// ============================================================================

func newOpFindCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find <cid>",
		Short: "Find operation by CID",
		Long: `Find operation by CID across all bundles

Searches the entire repository for an operation with the given CID
and returns its location (bundle + position).

Note: This performs a full scan and can be slow on large repositories.`,

		Example: `  # Find by CID
  plcbundle op find bafyreig3tg4k...

  # Pipe to op get
  plcbundle op find bafyreig3... | awk '{print $3, $5}' | xargs plcbundle op get`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			cid := args[0]

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return findOperationByCID(mgr, cid)
		},
	}

	return cmd
}

// ============================================================================
// Helper Functions
// ============================================================================

// parseOpArgs parses operation arguments (supports both formats)
func parseOpArgs(args []string) (bundleNum, position int, err error) {
	if len(args) == 1 {
		global, err := strconv.Atoi(args[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid position: %w", err)
		}

		if global < types.BUNDLE_SIZE {
			// Small numbers: shorthand for "bundle 1, position N"
			// op get 1 → bundle 1, position 1
			// op get 100 → bundle 1, position 100
			return 1, global, nil
		}

		// Large numbers: global position
		// op get 10000 → bundle 1, position 0
		// op get 88410345 → bundle 8841, position 345
		bundleNum = global / types.BUNDLE_SIZE
		position = global % types.BUNDLE_SIZE

		return bundleNum, position, nil
	}

	if len(args) == 2 {
		// Explicit: bundle + position
		bundleNum, err = strconv.Atoi(args[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid bundle number: %w", err)
		}

		position, err = strconv.Atoi(args[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid position: %w", err)
		}

		return bundleNum, position, nil
	}

	return 0, 0, fmt.Errorf("usage: op <command> <bundle> <position> OR op <command> <globalPosition>")
}

// findOperationByCID searches for an operation by CID
func findOperationByCID(mgr BundleManager, cid string) error {
	ctx := context.Background()

	// ✨ CHECK MEMPOOL FIRST (most recent data)
	fmt.Fprintf(os.Stderr, "Checking mempool...\n")
	mempoolOps, err := mgr.GetMempoolOperations()
	if err == nil && len(mempoolOps) > 0 {
		for pos, op := range mempoolOps {
			if op.CID == cid {
				fmt.Printf("Found in mempool: position %d\n\n", pos)
				fmt.Printf("  DID:        %s\n", op.DID)
				fmt.Printf("  Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05"))

				if op.IsNullified() {
					fmt.Printf("  Status:     ✗ Nullified")
					if nullCID := op.GetNullifyingCID(); nullCID != "" {
						fmt.Printf(" by %s", nullCID)
					}
					fmt.Printf("\n")
				} else {
					fmt.Printf("  Status:     ✓ Active\n")
				}

				return nil
			}
		}
	}

	// Search bundles
	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Fprintf(os.Stderr, "No bundles to search\n")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Searching %d bundles for CID: %s\n\n", len(bundles), cid)

	for _, meta := range bundles {
		bundle, err := mgr.LoadBundle(ctx, meta.BundleNumber)
		if err != nil {
			continue
		}

		for pos, op := range bundle.Operations {
			if op.CID == cid {
				globalPos := (meta.BundleNumber * types.BUNDLE_SIZE) + pos

				fmt.Printf("Found: bundle %06d, position %d\n", meta.BundleNumber, pos)
				fmt.Printf("Global position: %d\n\n", globalPos)

				fmt.Printf("  DID:        %s\n", op.DID)
				fmt.Printf("  Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05"))

				if op.IsNullified() {
					fmt.Printf("  Status:     ✗ Nullified")
					if nullCID := op.GetNullifyingCID(); nullCID != "" {
						fmt.Printf(" by %s", nullCID)
					}
					fmt.Printf("\n")
				} else {
					fmt.Printf("  Status:     ✓ Active\n")
				}

				return nil
			}
		}

		// Progress indicator
		if meta.BundleNumber%100 == 0 {
			fmt.Fprintf(os.Stderr, "Searched through bundle %06d...\r", meta.BundleNumber)
		}
	}

	fmt.Fprintf(os.Stderr, "\nCID not found: %s\n", cid)
	fmt.Fprintf(os.Stderr, "(Searched %d bundles + mempool)\n", len(bundles))
	return fmt.Errorf("CID not found")
}

// displayOperationWithTiming shows formatted operation details with timing
func displayOperationWithTiming(bundleNum, position int, op *plcclient.PLCOperation,
	opData map[string]interface{}, _ error,
	loadDuration, parseDuration time.Duration, verbose bool) error {

	globalPos := (bundleNum * types.BUNDLE_SIZE) + position

	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    Operation %d\n", globalPos)
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	fmt.Printf("Location\n")
	fmt.Printf("────────\n")
	fmt.Printf("  Bundle:          %06d\n", bundleNum)
	fmt.Printf("  Position:        %d\n", position)
	fmt.Printf("  Global position: %d\n\n", globalPos)

	fmt.Printf("Identity\n")
	fmt.Printf("────────\n")
	fmt.Printf("  DID:             %s\n", op.DID)
	fmt.Printf("  CID:             %s\n\n", op.CID)

	fmt.Printf("Timestamp\n")
	fmt.Printf("─────────\n")
	fmt.Printf("  Created:         %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000 MST"))
	fmt.Printf("  Age:             %s\n\n", formatDuration(time.Since(op.CreatedAt)))

	// Status
	status := "✓ Active"
	if op.IsNullified() {
		status = "✗ Nullified"
		if cid := op.GetNullifyingCID(); cid != "" {
			status += fmt.Sprintf(" by %s", cid)
		}
	}
	fmt.Printf("Status\n")
	fmt.Printf("──────\n")
	fmt.Printf("  %s\n\n", status)

	// Performance metrics (always shown if verbose)
	if verbose {
		totalTime := loadDuration + parseDuration

		fmt.Printf("Performance\n")
		fmt.Printf("───────────\n")
		fmt.Printf("  Load time:       %s\n", loadDuration)
		fmt.Printf("  Parse time:      %s\n", parseDuration)
		fmt.Printf("  Total time:      %s\n", totalTime)

		if len(op.RawJSON) > 0 {
			fmt.Printf("  Data size:       %d bytes\n", len(op.RawJSON))
			mbPerSec := float64(len(op.RawJSON)) / loadDuration.Seconds() / (1024 * 1024)
			fmt.Printf("  Load speed:      %.2f MB/s\n", mbPerSec)
		}

		fmt.Printf("\n")
	}

	// Parse operation details
	if opData != nil && !op.IsNullified() {
		fmt.Printf("Details\n")
		fmt.Printf("───────\n")

		if opType, ok := opData["type"].(string); ok {
			fmt.Printf("  Type:            %s\n", opType)
		}

		if handle, ok := opData["handle"].(string); ok {
			fmt.Printf("  Handle:          %s\n", handle)
		} else if aka, ok := opData["alsoKnownAs"].([]interface{}); ok && len(aka) > 0 {
			if akaStr, ok := aka[0].(string); ok {
				handle := strings.TrimPrefix(akaStr, "at://")
				fmt.Printf("  Handle:          %s\n", handle)
			}
		}

		if services, ok := opData["services"].(map[string]interface{}); ok {
			if pds, ok := services["atproto_pds"].(map[string]interface{}); ok {
				if endpoint, ok := pds["endpoint"].(string); ok {
					fmt.Printf("  PDS:             %s\n", endpoint)
				}
			}
		}

		fmt.Printf("\n")
	}

	// Verbose: show full JSON (pretty-printed)
	if verbose {
		fmt.Printf("Raw JSON\n")
		fmt.Printf("────────\n")

		var data []byte
		if len(op.RawJSON) > 0 {
			// Parse and re-format the raw JSON
			var temp interface{}
			if err := json.Unmarshal(op.RawJSON, &temp); err == nil {
				data, _ = json.MarshalIndent(temp, "", "  ")
			} else {
				// Fallback to raw if parse fails
				data = op.RawJSON
			}
		} else {
			data, _ = json.MarshalIndent(op, "", "  ")
		}

		fmt.Println(string(data))
		fmt.Printf("\n")
	}

	return nil
}
