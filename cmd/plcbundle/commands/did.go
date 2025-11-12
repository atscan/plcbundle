package commands

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/didindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

func NewDIDCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "did",
		Aliases: []string{"d"},
		Short:   "DID operations and queries",
		Long: `DID operations and queries

Query and analyze DIDs in the bundle repository. All commands
require a DID index to be built for optimal performance.`,

		Example: `  # Lookup all operations for a DID
  plcbundle did lookup did:plc:524tuhdhh3m7li5gycdn6boe

  # Resolve to current DID document
  plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe

  # Show complete audit log
  plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe

  # Show DID statistics
  plcbundle did stats did:plc:524tuhdhh3m7li5gycdn6boe

  # Batch process from file
  plcbundle did batch dids.txt`,
	}

	// Add subcommands
	cmd.AddCommand(newDIDLookupCommand())
	cmd.AddCommand(newDIDResolveCommand())
	cmd.AddCommand(newDIDHistoryCommand())
	cmd.AddCommand(newDIDBatchCommand())
	cmd.AddCommand(newDIDStatsCommand())

	return cmd
}

// ============================================================================
// DID LOOKUP - Find all operations for a DID
// ============================================================================

func newDIDLookupCommand() *cobra.Command {
	var (
		verbose  bool
		showJSON bool
	)

	cmd := &cobra.Command{
		Use:     "lookup <did|handle>",
		Aliases: []string{"find", "get"},
		Short:   "Find all operations for a DID or handle",
		Long: `Find all operations for a DID or handle

Retrieves all operations (both bundled and mempool) for a specific DID.
Accepts either:
  • DID: did:plc:524tuhdhh3m7li5gycdn6boe
  • Handle: tree.fail (resolves via configured resolver)

Requires DID index to be built. If not available, will fall back to
full scan (slow).`,

		Example: `  # Lookup by DID
  plcbundle did lookup did:plc:524tuhdhh3m7li5gycdn6boe

  # Lookup by handle (requires --resolver-url)
  plcbundle did lookup tree.fail
  plcbundle did lookup ngerakines.me

  # With non-default handle resolver configured
  plcbundle --handle-resolver https://quickdid.smokesignal.tools did lookup tree.fail`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			input := args[0]

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			// Resolve handle to DID with timing
			ctx := context.Background()
			did, _, err := mgr.ResolveHandleOrDID(ctx, input)
			if err != nil {
				return err
			}

			stats := mgr.GetDIDIndexStats()
			if !stats["exists"].(bool) {
				fmt.Fprintf(os.Stderr, "⚠️  DID index not found. Run: plcbundle index build\n")
				fmt.Fprintf(os.Stderr, "    Falling back to full scan (slow)...\n\n")
			}

			totalStart := time.Now()

			// Lookup operations
			lookupStart := time.Now()
			opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, verbose)
			if err != nil {
				return err
			}
			lookupElapsed := time.Since(lookupStart)

			// Check mempool
			mempoolStart := time.Now()
			mempoolOps, err := mgr.GetDIDOperationsFromMempool(did)
			if err != nil {
				return fmt.Errorf("error checking mempool: %w", err)
			}
			mempoolElapsed := time.Since(mempoolStart)

			totalElapsed := time.Since(totalStart)

			if len(opsWithLoc) == 0 && len(mempoolOps) == 0 {
				if showJSON {
					fmt.Println("{\"found\": false, \"operations\": []}")
				} else {
					fmt.Printf("DID not found (searched in %s)\n", totalElapsed)
				}
				return nil
			}

			if showJSON {
				return outputLookupJSON(did, opsWithLoc, mempoolOps, totalElapsed, lookupElapsed, mempoolElapsed)
			}

			return displayLookupResults(did, opsWithLoc, mempoolOps, totalElapsed, lookupElapsed, mempoolElapsed, verbose, stats)
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose debug output")
	cmd.Flags().BoolVar(&showJSON, "json", false, "Output as JSON")

	return cmd
}

// ============================================================================
// DID RESOLVE - Resolve to current document
// ============================================================================

func newDIDResolveCommand() *cobra.Command {
	var (
		verbose bool
		raw     bool
	)

	cmd := &cobra.Command{
		Use:     "resolve <did>",
		Aliases: []string{"doc", "document"},
		Short:   "Resolve DID to current document",
		Long: `Resolve DID to current W3C DID document

Resolves a DID to its current state by applying all non-nullified
operations in chronological order. Returns standard W3C DID document.

Optimized for speed: checks mempool first, then uses DID index for
O(1) lookup of latest operation.`,

		Example: `  # Resolve DID
  plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe

  # Show timings and other details
  plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe --verbose

  # Get raw PLC state (not W3C format)
  plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe --raw

  # Pipe to jq
  plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe | jq .service
  
  # Resolve by handle
  plcbundle did resolve tree.fail`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			input := args[0]

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			ctx := context.Background()

			// Resolve handle to DID with timing
			did, handleResolveTime, err := mgr.ResolveHandleOrDID(ctx, input)
			if err != nil {
				return err
			}

			if verbose {
				fmt.Fprintf(os.Stderr, "Resolving DID: %s\n", did)
				mgr.GetDIDIndex().SetVerbose(true)
			}

			result, err := mgr.ResolveDID(ctx, did)
			if err != nil {
				return err
			}

			// Display timing if requested
			if verbose {
				if handleResolveTime > 0 {
					fmt.Fprintf(os.Stderr, "Handle: %s | ", handleResolveTime)
				}
				if result.Source == "mempool" {
					fmt.Fprintf(os.Stderr, "Mempool check: %s (✓ found)\n", result.MempoolTime)
					fmt.Fprintf(os.Stderr, "Total: %s\n\n", result.TotalTime)
				} else {
					fmt.Fprintf(os.Stderr, "Mempool: %s | Index: %s | Load: %s | Total: %s\n",
						result.MempoolTime, result.IndexTime, result.LoadOpTime, result.TotalTime)
					fmt.Fprintf(os.Stderr, "Source: bundle %06d, position %d\n\n",
						result.BundleNumber, result.Position)
				}
			}

			// Output document
			data, _ := json.MarshalIndent(result.Document, "", "  ")
			fmt.Println(string(data))

			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose debug output")
	cmd.Flags().BoolVar(&raw, "raw", false, "Output raw PLC state (not W3C document)")

	return cmd
}

// ============================================================================
// DID HISTORY - Show complete audit log
// ============================================================================

func newDIDHistoryCommand() *cobra.Command {
	var (
		verbose          bool
		showJSON         bool
		compact          bool
		includeNullified bool
	)

	cmd := &cobra.Command{
		Use:     "history <did>",
		Aliases: []string{"log", "audit"},
		Short:   "Show complete DID audit log",
		Long: `Show complete DID audit log

Displays all operations for a DID in chronological order, showing
the complete history including nullified operations.

This provides a full audit trail of all changes to the DID.`,

		Example: `  # Show full history
  plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe

  # Include nullified operations
  plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe --include-nullified

  # Compact one-line format
  plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe --compact

  # JSON output
  plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe --json`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			did := args[0]

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			ctx := context.Background()

			// Get all operations with locations
			opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, verbose)
			if err != nil {
				return err
			}

			// Get mempool operations
			mempoolOps, err := mgr.GetDIDOperationsFromMempool(did)
			if err != nil {
				return err
			}

			if len(opsWithLoc) == 0 && len(mempoolOps) == 0 {
				fmt.Fprintf(os.Stderr, "DID not found: %s\n", did)
				return nil
			}

			if showJSON {
				return outputHistoryJSON(did, opsWithLoc, mempoolOps)
			}

			return displayHistory(did, opsWithLoc, mempoolOps, compact, includeNullified)
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	cmd.Flags().BoolVar(&showJSON, "json", false, "Output as JSON")
	cmd.Flags().BoolVar(&compact, "compact", false, "Compact one-line format")
	cmd.Flags().BoolVar(&includeNullified, "include-nullified", false, "Show nullified operations")

	return cmd
}

// ============================================================================
// DID BATCH - Process multiple DIDs from file or stdin
// ============================================================================

func newDIDBatchCommand() *cobra.Command {
	var (
		action     string
		workers    int
		outputFile string
		fromStdin  bool
	)

	cmd := &cobra.Command{
		Use:   "batch [file]",
		Short: "Process multiple DIDs from file or stdin",
		Long: `Process multiple DIDs from file or stdin

Read DIDs from a file (one per line) or stdin and perform batch operations.
Supports parallel processing for better performance.

Actions:
  lookup   - Lookup all DIDs and show summary
  resolve  - Resolve all DIDs to documents
  export   - Export all operations to JSONL

Input formats:
  - File path: reads DIDs from file
  - "-" or --stdin: reads DIDs from stdin
  - Omit file + use --stdin: reads from stdin`,

		Example: `  # Batch lookup from file
  plcbundle did batch dids.txt --action lookup

  # Read from stdin
  cat dids.txt | plcbundle did batch --stdin --action lookup
  cat dids.txt | plcbundle did batch - --action resolve

  # Export operations for DIDs from stdin
  echo "did:plc:524tuhdhh3m7li5gycdn6boe" | plcbundle did batch - --action export

  # Pipe results
  plcbundle did batch dids.txt --action resolve -o resolved.jsonl

  # Parallel processing
  cat dids.txt | plcbundle did batch --stdin --action lookup --workers 8

  # Chain commands
  grep "did:plc:" some_file.txt | plcbundle did batch - --action export > ops.jsonl`,

		Args: cobra.MaximumNArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			var filename string

			// Determine input source
			if len(args) > 0 {
				filename = args[0]
				if filename == "-" {
					fromStdin = true
				}
			} else if !fromStdin {
				return fmt.Errorf("either provide filename or use --stdin flag\n" +
					"Examples:\n" +
					"  plcbundle did batch dids.txt\n" +
					"  plcbundle did batch --stdin\n" +
					"  cat dids.txt | plcbundle did batch -")
			}

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return processBatchDIDs(mgr, filename, batchOptions{
				action:     action,
				workers:    workers,
				outputFile: outputFile,
				fromStdin:  fromStdin,
			})
		},
	}

	cmd.Flags().StringVar(&action, "action", "lookup", "Action: lookup, resolve, export")
	cmd.Flags().IntVar(&workers, "workers", 4, "Number of parallel workers")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file (default: stdout)")
	cmd.Flags().BoolVar(&fromStdin, "stdin", false, "Read DIDs from stdin")

	return cmd
}

// ============================================================================
// DID STATS - Show DID activity statistics
// ============================================================================

func newDIDStatsCommand() *cobra.Command {
	var (
		showGlobal bool
		showJSON   bool
	)

	cmd := &cobra.Command{
		Use:   "stats [did]",
		Short: "Show DID activity statistics",
		Long: `Show DID activity statistics

Display statistics for a specific DID or global DID index stats.

With DID: shows operation count, first/last activity, bundle distribution
Without DID: shows global index statistics`,

		Example: `  # Stats for specific DID
  plcbundle did stats did:plc:524tuhdhh3m7li5gycdn6boe

  # Global index stats
  plcbundle did stats --global
  plcbundle did stats

  # JSON output
  plcbundle did stats did:plc:524tuhdhh3m7li5gycdn6boe --json`,

		Args: cobra.MaximumNArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			// Global stats
			if len(args) == 0 || showGlobal {
				return showGlobalDIDStats(mgr, dir, showJSON)
			}

			// Specific DID stats
			did := args[0]
			return showDIDStats(mgr, did, showJSON)
		},
	}

	cmd.Flags().BoolVar(&showGlobal, "global", false, "Show global index stats")
	cmd.Flags().BoolVar(&showJSON, "json", false, "Output as JSON")

	return cmd
}

// ============================================================================
// Helper Functions
// ============================================================================

type batchOptions struct {
	action     string
	workers    int
	outputFile string
	fromStdin  bool
}

func processBatchDIDs(mgr BundleManager, filename string, opts batchOptions) error {
	// Determine input source
	var input *os.File
	var err error

	if opts.fromStdin {
		input = os.Stdin
		fmt.Fprintf(os.Stderr, "Reading DIDs from stdin...\n")
	} else {
		input, err = os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer input.Close()
		fmt.Fprintf(os.Stderr, "Reading DIDs from: %s\n", filename)
	}

	// Read DIDs
	var dids []string
	scanner := bufio.NewScanner(input)

	// Increase buffer size for large input
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Basic validation
		if !strings.HasPrefix(line, "did:plc:") {
			fmt.Fprintf(os.Stderr, "⚠️  Line %d: skipping invalid DID: %s\n", lineNum, line)
			continue
		}

		dids = append(dids, line)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	if len(dids) == 0 {
		return fmt.Errorf("no valid DIDs found in input")
	}

	fmt.Fprintf(os.Stderr, "Processing %d DIDs with action '%s' (%d workers)\n\n",
		len(dids), opts.action, opts.workers)

	// Setup output
	var output *os.File
	if opts.outputFile != "" {
		output, err = os.Create(opts.outputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer output.Close()
		fmt.Fprintf(os.Stderr, "Output: %s\n\n", opts.outputFile)
	} else {
		output = os.Stdout
	}

	// Process based on action
	switch opts.action {
	case "lookup":
		return batchLookup(mgr, dids, output, opts.workers)
	case "resolve":
		return batchResolve(mgr, dids, output, opts.workers)
	case "export":
		return batchExport(mgr, dids, output, opts.workers)
	default:
		return fmt.Errorf("unknown action: %s (valid: lookup, resolve, export)", opts.action)
	}
}

func showGlobalDIDStats(mgr BundleManager, dir string, showJSON bool) error {
	stats := mgr.GetDIDIndexStats()

	if !stats["exists"].(bool) {
		fmt.Printf("DID index does not exist\n")
		fmt.Printf("Run: plcbundle index build\n")
		return nil
	}

	if showJSON {
		data, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	indexedDIDs := stats["indexed_dids"].(int64)
	mempoolDIDs := stats["mempool_dids"].(int64)
	totalDIDs := stats["total_dids"].(int64)

	fmt.Printf("\nDID Index Statistics\n")
	fmt.Printf("════════════════════\n\n")
	fmt.Printf("  Location:      %s/.plcbundle/\n", dir)

	if mempoolDIDs > 0 {
		fmt.Printf("  Indexed DIDs:  %s (in bundles)\n", formatNumber(int(indexedDIDs)))
		fmt.Printf("  Mempool DIDs:  %s (not yet bundled)\n", formatNumber(int(mempoolDIDs)))
		fmt.Printf("  Total DIDs:    %s\n", formatNumber(int(totalDIDs)))
	} else {
		fmt.Printf("  Total DIDs:    %s\n", formatNumber(int(totalDIDs)))
	}

	fmt.Printf("  Shard count:   %d\n", stats["shard_count"])
	fmt.Printf("  Last bundle:   %06d\n", stats["last_bundle"])
	fmt.Printf("  Updated:       %s\n\n", stats["updated_at"].(time.Time).Format("2006-01-02 15:04:05"))

	fmt.Printf("  Cached shards: %d / %d\n", stats["cached_shards"], stats["cache_limit"])

	if cachedList, ok := stats["cache_order"].([]int); ok && len(cachedList) > 0 {
		fmt.Printf("  Hot shards:    ")
		for i, shard := range cachedList {
			if i > 0 {
				fmt.Printf(", ")
			}
			if i >= 10 {
				fmt.Printf("... (+%d more)", len(cachedList)-10)
				break
			}
			fmt.Printf("%02x", shard)
		}
		fmt.Printf("\n")
	}

	fmt.Printf("\n")
	return nil
}

func showDIDStats(mgr BundleManager, did string, showJSON bool) error {
	ctx := context.Background()

	// Get operations
	opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, false)
	if err != nil {
		return err
	}

	mempoolOps, err := mgr.GetDIDOperationsFromMempool(did)
	if err != nil {
		return err
	}

	if len(opsWithLoc) == 0 && len(mempoolOps) == 0 {
		fmt.Fprintf(os.Stderr, "DID not found: %s\n", did)
		return nil
	}

	// Calculate stats
	totalOps := len(opsWithLoc) + len(mempoolOps)
	nullifiedCount := 0
	for _, owl := range opsWithLoc {
		if owl.Operation.IsNullified() {
			nullifiedCount++
		}
	}

	bundleSpan := 0
	if len(opsWithLoc) > 0 {
		bundles := make(map[int]bool)
		for _, owl := range opsWithLoc {
			bundles[owl.Bundle] = true
		}
		bundleSpan = len(bundles)
	}

	if showJSON {
		output := map[string]interface{}{
			"did":              did,
			"total_operations": totalOps,
			"bundled":          len(opsWithLoc),
			"mempool":          len(mempoolOps),
			"nullified":        nullifiedCount,
			"active":           totalOps - nullifiedCount,
			"bundle_span":      bundleSpan,
		}
		data, _ := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Printf("\nDID Statistics\n")
	fmt.Printf("══════════════\n\n")
	fmt.Printf("  DID:              %s\n\n", did)
	fmt.Printf("  Total operations: %d\n", totalOps)
	fmt.Printf("  Active:           %d\n", totalOps-nullifiedCount)
	if nullifiedCount > 0 {
		fmt.Printf("  Nullified:        %d\n", nullifiedCount)
	}
	if len(opsWithLoc) > 0 {
		fmt.Printf("  Bundled:          %d\n", len(opsWithLoc))
		fmt.Printf("  Bundle span:      %d bundles\n", bundleSpan)
	}
	if len(mempoolOps) > 0 {
		fmt.Printf("  Mempool:          %d\n", len(mempoolOps))
	}
	fmt.Printf("\n")

	return nil
}

func displayHistory(did string, opsWithLoc []PLCOperationWithLocation, mempoolOps []plcclient.PLCOperation, compact bool, includeNullified bool) error {
	if compact {
		return displayHistoryCompact(did, opsWithLoc, mempoolOps, includeNullified)
	}
	return displayHistoryDetailed(did, opsWithLoc, mempoolOps, includeNullified)
}

func displayHistoryCompact(did string, opsWithLoc []PLCOperationWithLocation, mempoolOps []plcclient.PLCOperation, includeNullified bool) error {
	fmt.Printf("DID History: %s\n\n", did)

	for _, owl := range opsWithLoc {
		if !includeNullified && owl.Operation.IsNullified() {
			continue
		}

		status := "✓"
		if owl.Operation.IsNullified() {
			status = "✗"
		}

		fmt.Printf("%s [%06d:%04d] %s  %s\n",
			status,
			owl.Bundle,
			owl.Position,
			owl.Operation.CreatedAt.Format("2006-01-02 15:04:05"),
			owl.Operation.CID)
	}

	for _, op := range mempoolOps {
		fmt.Printf("✓ [mempool  ] %s  %s\n",
			op.CreatedAt.Format("2006-01-02 15:04:05"),
			op.CID)
	}

	return nil
}

func displayHistoryDetailed(did string, opsWithLoc []PLCOperationWithLocation, mempoolOps []plcclient.PLCOperation, includeNullified bool) error {
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    DID Audit Log\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")
	fmt.Printf("DID: %s\n\n", did)

	for i, owl := range opsWithLoc {
		if !includeNullified && owl.Operation.IsNullified() {
			continue
		}

		op := owl.Operation
		status := "✓ Active"
		if op.IsNullified() {
			status = "✗ Nullified"
		}

		fmt.Printf("Operation %d [Bundle %06d, Position %04d]\n", i+1, owl.Bundle, owl.Position)
		fmt.Printf("   CID:        %s\n", op.CID)
		fmt.Printf("   Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000 MST"))
		fmt.Printf("   Status:     %s\n", status)

		if opData, err := op.GetOperationData(); err == nil && opData != nil {
			showOperationDetails(&op)
		}

		fmt.Printf("\n")
	}

	if len(mempoolOps) > 0 {
		fmt.Printf("Mempool Operations (%d)\n", len(mempoolOps))
		fmt.Printf("══════════════════════════════════════════════════════════════\n\n")

		for i, op := range mempoolOps {
			fmt.Printf("Operation %d [Mempool]\n", i+1)
			fmt.Printf("   CID:        %s\n", op.CID)
			fmt.Printf("   Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000 MST"))
			fmt.Printf("   Status:     ✓ Active\n")
			fmt.Printf("\n")
		}
	}

	return nil
}

func outputHistoryJSON(did string, opsWithLoc []PLCOperationWithLocation, mempoolOps []plcclient.PLCOperation) error {
	output := map[string]interface{}{
		"did":     did,
		"bundled": make([]map[string]interface{}, 0),
		"mempool": make([]map[string]interface{}, 0),
	}

	for _, owl := range opsWithLoc {
		output["bundled"] = append(output["bundled"].([]map[string]interface{}), map[string]interface{}{
			"bundle":     owl.Bundle,
			"position":   owl.Position,
			"cid":        owl.Operation.CID,
			"nullified":  owl.Operation.IsNullified(),
			"created_at": owl.Operation.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	for _, op := range mempoolOps {
		output["mempool"] = append(output["mempool"].([]map[string]interface{}), map[string]interface{}{
			"cid":        op.CID,
			"nullified":  op.IsNullified(),
			"created_at": op.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	data, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(data))

	return nil
}

func batchLookup(mgr BundleManager, dids []string, output *os.File, _ int) error {
	progress := ui.NewProgressBar(len(dids))
	ctx := context.Background()

	// CSV header
	fmt.Fprintf(output, "did,status,operation_count,bundled,mempool,nullified\n")

	found := 0
	notFound := 0
	errorCount := 0

	for i, did := range dids {
		opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, false)
		if err != nil {
			errorCount++
			fmt.Fprintf(output, "%s,error,0,0,0,0\n", did)
			progress.Set(i + 1)
			continue
		}

		mempoolOps, _ := mgr.GetDIDOperationsFromMempool(did)

		if len(opsWithLoc) == 0 && len(mempoolOps) == 0 {
			notFound++
			fmt.Fprintf(output, "%s,not_found,0,0,0,0\n", did)
		} else {
			found++

			// Count nullified
			nullified := 0
			for _, owl := range opsWithLoc {
				if owl.Operation.IsNullified() {
					nullified++
				}
			}

			fmt.Fprintf(output, "%s,found,%d,%d,%d,%d\n",
				did,
				len(opsWithLoc)+len(mempoolOps),
				len(opsWithLoc),
				len(mempoolOps),
				nullified)
		}

		progress.Set(i + 1)
	}

	progress.Finish()

	fmt.Fprintf(os.Stderr, "\n✓ Batch lookup complete\n")
	fmt.Fprintf(os.Stderr, "  DIDs input: %d\n", len(dids))
	fmt.Fprintf(os.Stderr, "  Found:      %d\n", found)
	fmt.Fprintf(os.Stderr, "  Not found:  %d\n", notFound)
	if errorCount > 0 {
		fmt.Fprintf(os.Stderr, "  Errors:     %d\n", errorCount)
	}

	return nil
}

func batchResolve(mgr BundleManager, dids []string, output *os.File, workers int) error {
	ctx := context.Background()
	overallStart := time.Now()

	// =================================================================
	// PHASE 1: Batch lookup locations
	// =================================================================
	fmt.Fprintf(os.Stderr, "Phase 1/2: Looking up %d DID locations...\n", len(dids))
	phase1Start := time.Now()

	type didLocation struct {
		did         string
		bundle      int
		position    int
		fromMempool bool
		mempoolOp   *plcclient.PLCOperation
		err         error
	}

	locations := make([]didLocation, len(dids))

	// Mempool check
	for i, did := range dids {
		if mempoolOp := findLatestInMempool(mgr, did); mempoolOp != nil {
			locations[i] = didLocation{did: did, fromMempool: true, mempoolOp: mempoolOp}
		}
	}

	// Batch index lookup
	needsLookup := make([]string, 0)
	lookupMap := make(map[string]int)
	for i, did := range dids {
		if !locations[i].fromMempool {
			needsLookup = append(needsLookup, did)
			lookupMap[did] = i
		}
	}

	if len(needsLookup) > 0 {
		batchResults, _ := mgr.GetDIDIndex().BatchGetDIDLocations(needsLookup)
		for did, locs := range batchResults {
			var latest *didindex.OpLocation
			for i := range locs {
				if !locs[i].Nullified() && (latest == nil || locs[i].IsAfter(*latest)) {
					latest = &locs[i]
				}
			}

			idx := lookupMap[did]
			if latest != nil {
				locations[idx] = didLocation{did: did, bundle: latest.BundleInt(), position: latest.PositionInt()}
			} else {
				locations[idx] = didLocation{did: did, err: fmt.Errorf("not found")}
			}
		}

		for _, did := range needsLookup {
			if idx := lookupMap[did]; locations[idx].bundle == 0 && locations[idx].err == nil {
				locations[idx] = didLocation{did: did, err: fmt.Errorf("not found")}
			}
		}
	}

	phase1Duration := time.Since(phase1Start)
	fmt.Fprintf(os.Stderr, "  ✓ %s\n\n", phase1Duration.Round(time.Millisecond))

	// =================================================================
	// PHASE 2: Group by bundle, load ops, resolve (MERGED, parallel)
	// =================================================================
	fmt.Fprintf(os.Stderr, "Phase 2/2: Loading and resolving (%d workers)...\n", workers)

	// Group DIDs by bundle
	type bundleGroup struct {
		bundleNum int
		dids      []int // indices into locations array
	}

	bundleMap := make(map[int][]int)
	mempoolDIDs := make([]int, 0)
	errorDIDs := make([]int, 0)

	for i, loc := range locations {
		if loc.err != nil {
			errorDIDs = append(errorDIDs, i)
		} else if loc.fromMempool {
			mempoolDIDs = append(mempoolDIDs, i)
		} else {
			bundleMap[loc.bundle] = append(bundleMap[loc.bundle], i)
		}
	}

	bundles := make([]bundleGroup, 0, len(bundleMap))
	for bn, didIndices := range bundleMap {
		bundles = append(bundles, bundleGroup{bundleNum: bn, dids: didIndices})
	}

	fmt.Fprintf(os.Stderr, "  %d bundles, %d mempool, %d errors\n",
		len(bundles), len(mempoolDIDs), len(errorDIDs))

	// Setup output
	writer := bufio.NewWriterSize(output, 512*1024)
	defer writer.Flush()

	var (
		resolved  int
		failed    int
		processed int
		mu        sync.Mutex // Single lock for all counters
	)

	progress := ui.NewProgressBar(len(dids))

	writeResult := func(doc *plcclient.DIDDocument, err error) {
		mu.Lock()
		defer mu.Unlock()

		processed++
		progress.Set(processed)

		if err != nil {
			failed++
		} else {
			resolved++
			data, _ := json.Marshal(doc)
			writer.Write(data)
			writer.WriteByte('\n')
			if resolved%100 == 0 {
				writer.Flush()
			}
		}
	}

	// Process mempool DIDs (already have ops)
	for _, idx := range mempoolDIDs {
		loc := locations[idx]
		doc, err := plcclient.ResolveDIDDocument(loc.did, []plcclient.PLCOperation{*loc.mempoolOp})
		writeResult(doc, err)
	}

	// Process errors
	for range errorDIDs {
		writeResult(nil, fmt.Errorf("not found"))
	}

	// Process bundles in parallel - LoadOperations once per bundle
	bundleJobs := make(chan bundleGroup, len(bundles))
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range bundleJobs {
				// Collect all positions needed from this bundle
				positions := make([]int, len(job.dids))
				for i, didIdx := range job.dids {
					positions[i] = locations[didIdx].position
				}

				// Load operations once for this bundle
				ops, err := mgr.LoadOperations(ctx, job.bundleNum, positions)

				if err != nil {
					// All DIDs from this bundle fail
					for range job.dids {
						writeResult(nil, err)
					}
					continue
				}

				// Resolve each DID using loaded operations
				for i, didIdx := range job.dids {
					loc := locations[didIdx]

					if op, ok := ops[positions[i]]; ok {
						doc, resolveErr := plcclient.ResolveDIDDocument(loc.did, []plcclient.PLCOperation{*op})
						writeResult(doc, resolveErr)
					} else {
						writeResult(nil, fmt.Errorf("operation not loaded"))
					}
				}
			}
		}()
	}

	// Send bundle jobs
	for _, bg := range bundles {
		bundleJobs <- bg
	}
	close(bundleJobs)

	wg.Wait()
	writer.Flush()
	progress.Finish()

	totalDuration := time.Since(overallStart)

	fmt.Fprintf(os.Stderr, "\n✓ Batch resolve complete\n")
	fmt.Fprintf(os.Stderr, "  Resolved:    %d/%d\n", resolved, len(dids))
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "  Failed:      %d\n", failed)
	}
	fmt.Fprintf(os.Stderr, "  Total:       %s (%.1f DIDs/sec)\n",
		totalDuration.Round(time.Millisecond),
		float64(resolved)/totalDuration.Seconds())

	return nil
}

// Helper function to find latest non-nullified op in mempool
func findLatestInMempool(mgr BundleManager, did string) *plcclient.PLCOperation {
	ops, err := mgr.GetDIDOperationsFromMempool(did)
	if err != nil || len(ops) == 0 {
		return nil
	}

	// Search backwards from most recent
	for i := len(ops) - 1; i >= 0; i-- {
		if !ops[i].IsNullified() {
			return &ops[i]
		}
	}

	return nil
}

func batchExport(mgr BundleManager, dids []string, output *os.File, _ int) error {
	progress := ui.NewProgressBar(len(dids))
	ctx := context.Background()

	totalOps := 0
	processedDIDs := 0
	errorCount := 0

	// Use buffered writer for better performance
	writer := bufio.NewWriterSize(output, 512*1024)
	defer writer.Flush()

	for i, did := range dids {
		opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, false)
		if err != nil {
			errorCount++
			if i < 10 { // Only log first few errors
				fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", did, err)
			}
			progress.Set(i + 1)
			continue
		}

		// Get mempool operations too
		mempoolOps, _ := mgr.GetDIDOperationsFromMempool(did)

		if len(opsWithLoc) == 0 && len(mempoolOps) == 0 {
			progress.Set(i + 1)
			continue
		}

		processedDIDs++

		// Export bundled operations
		for _, owl := range opsWithLoc {
			if len(owl.Operation.RawJSON) > 0 {
				writer.Write(owl.Operation.RawJSON)
			} else {
				data, _ := json.Marshal(owl.Operation)
				writer.Write(data)
			}
			writer.WriteByte('\n')
			totalOps++
		}

		// Export mempool operations
		for _, op := range mempoolOps {
			if len(op.RawJSON) > 0 {
				writer.Write(op.RawJSON)
			} else {
				data, _ := json.Marshal(op)
				writer.Write(data)
			}
			writer.WriteByte('\n')
			totalOps++
		}

		// Flush periodically
		if i%100 == 0 {
			writer.Flush()
		}

		progress.Set(i + 1)
	}

	writer.Flush()
	progress.Finish()

	fmt.Fprintf(os.Stderr, "\n✓ Batch export complete\n")
	fmt.Fprintf(os.Stderr, "  DIDs input:     %d\n", len(dids))
	fmt.Fprintf(os.Stderr, "  DIDs processed: %d\n", processedDIDs)
	fmt.Fprintf(os.Stderr, "  Operations:     %s\n", formatNumber(totalOps))
	if errorCount > 0 {
		fmt.Fprintf(os.Stderr, "  Errors:         %d\n", errorCount)
	}

	return nil
}

// ============================================================================
// Shared Helper Functions (used by both DID and legacy index commands)
// ============================================================================

func outputLookupJSON(did string, opsWithLoc []PLCOperationWithLocation, mempoolOps []plcclient.PLCOperation, totalElapsed, lookupElapsed, mempoolElapsed time.Duration) error {
	output := map[string]interface{}{
		"found": true,
		"did":   did,
		"timing": map[string]interface{}{
			"total_ms":   totalElapsed.Milliseconds(),
			"lookup_ms":  lookupElapsed.Milliseconds(),
			"mempool_ms": mempoolElapsed.Milliseconds(),
		},
		"bundled": make([]map[string]interface{}, 0),
		"mempool": make([]map[string]interface{}, 0),
	}

	for _, owl := range opsWithLoc {
		output["bundled"] = append(output["bundled"].([]map[string]interface{}), map[string]interface{}{
			"bundle":     owl.Bundle,
			"position":   owl.Position,
			"cid":        owl.Operation.CID,
			"nullified":  owl.Operation.IsNullified(),
			"created_at": owl.Operation.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	for _, op := range mempoolOps {
		output["mempool"] = append(output["mempool"].([]map[string]interface{}), map[string]interface{}{
			"cid":        op.CID,
			"nullified":  op.IsNullified(),
			"created_at": op.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	data, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(data))

	return nil
}

func displayLookupResults(did string, opsWithLoc []PLCOperationWithLocation, mempoolOps []plcclient.PLCOperation, totalElapsed, lookupElapsed, mempoolElapsed time.Duration, verbose bool, stats map[string]interface{}) error {
	nullifiedCount := 0
	for _, owl := range opsWithLoc {
		if owl.Operation.IsNullified() {
			nullifiedCount++
		}
	}

	totalOps := len(opsWithLoc) + len(mempoolOps)
	activeOps := len(opsWithLoc) - nullifiedCount + len(mempoolOps)

	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    DID Lookup Results\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")
	fmt.Printf("DID: %s\n\n", did)

	fmt.Printf("Summary\n───────\n")
	fmt.Printf("  Total operations:   %d\n", totalOps)
	fmt.Printf("  Active operations:  %d\n", activeOps)
	if nullifiedCount > 0 {
		fmt.Printf("  Nullified:          %d\n", nullifiedCount)
	}
	if len(opsWithLoc) > 0 {
		fmt.Printf("  Bundled:            %d\n", len(opsWithLoc))
	}
	if len(mempoolOps) > 0 {
		fmt.Printf("  Mempool:            %d\n", len(mempoolOps))
	}
	fmt.Printf("\n")

	fmt.Printf("Performance\n───────────\n")
	fmt.Printf("  Index lookup:       %s\n", lookupElapsed)
	fmt.Printf("  Mempool check:      %s\n", mempoolElapsed)
	fmt.Printf("  Total time:         %s\n\n", totalElapsed)

	// Show operations
	if len(opsWithLoc) > 0 {
		fmt.Printf("Bundled Operations (%d total)\n", len(opsWithLoc))
		fmt.Printf("══════════════════════════════════════════════════════════════\n\n")

		for i, owl := range opsWithLoc {
			op := owl.Operation
			status := "✓ Active"
			if op.IsNullified() {
				status = "✗ Nullified"
			}

			fmt.Printf("Operation %d [Bundle %06d, Position %04d]\n", i+1, owl.Bundle, owl.Position)
			fmt.Printf("   CID:        %s\n", op.CID)
			fmt.Printf("   Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000 MST"))
			fmt.Printf("   Status:     %s\n", status)

			if verbose && !op.IsNullified() {
				showOperationDetails(&op)
			}

			fmt.Printf("\n")
		}
	}

	fmt.Printf("✓ Lookup complete in %s\n", totalElapsed)
	return nil
}

func showOperationDetails(op *plcclient.PLCOperation) {
	if opData, err := op.GetOperationData(); err == nil && opData != nil {
		if opType, ok := opData["type"].(string); ok {
			fmt.Printf("   Type:       %s\n", opType)
		}

		if handle, ok := opData["handle"].(string); ok {
			fmt.Printf("   Handle:     %s\n", handle)
		} else if aka, ok := opData["alsoKnownAs"].([]interface{}); ok && len(aka) > 0 {
			if akaStr, ok := aka[0].(string); ok {
				handle := strings.TrimPrefix(akaStr, "at://")
				fmt.Printf("   Handle:     %s\n", handle)
			}
		}
	}
}
