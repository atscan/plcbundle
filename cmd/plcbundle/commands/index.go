package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

// IndexCommand handles the index subcommand
func IndexCommand(args []string) error {
	if len(args) < 1 {
		printIndexUsage()
		return fmt.Errorf("subcommand required")
	}

	subcommand := args[0]

	switch subcommand {
	case "build":
		return indexBuild(args[1:])
	case "stats":
		return indexStats(args[1:])
	case "lookup":
		return indexLookup(args[1:])
	case "resolve":
		return indexResolve(args[1:])
	default:
		printIndexUsage()
		return fmt.Errorf("unknown index subcommand: %s", subcommand)
	}
}

func printIndexUsage() {
	fmt.Printf(`Usage: plcbundle index <command> [options]

Commands:
  build     Build DID index from bundles
  stats     Show index statistics
  lookup    Lookup a specific DID
  resolve   Resolve DID to current document

Examples:
  plcbundle index build
  plcbundle index stats
  plcbundle index lookup did:plc:524tuhdhh3m7li5gycdn6boe
  plcbundle index resolve did:plc:524tuhdhh3m7li5gycdn6boe
`)
}

func indexBuild(args []string) error {
	fs := flag.NewFlagSet("index build", flag.ExitOnError)
	force := fs.Bool("force", false, "rebuild even if index exists")

	if err := fs.Parse(args); err != nil {
		return err
	}

	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	stats := mgr.GetDIDIndexStats()
	if stats["exists"].(bool) && !*force {
		fmt.Printf("DID index already exists (use --force to rebuild)\n")
		fmt.Printf("Directory: %s\n", dir)
		fmt.Printf("Total DIDs: %d\n", stats["total_dids"])
		return nil
	}

	fmt.Printf("Building DID index in: %s\n", dir)

	index := mgr.GetIndex()
	bundleCount := index.Count()

	if bundleCount == 0 {
		fmt.Printf("No bundles to index\n")
		return nil
	}

	fmt.Printf("Indexing %d bundles...\n\n", bundleCount)

	progress := ui.NewProgressBar(bundleCount)
	start := time.Now()
	ctx := context.Background()

	err = mgr.BuildDIDIndex(ctx, func(current, total int) {
		progress.Set(current)
	})

	progress.Finish()

	if err != nil {
		return fmt.Errorf("error building index: %w", err)
	}

	elapsed := time.Since(start)
	stats = mgr.GetDIDIndexStats()

	fmt.Printf("\n✓ DID index built in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total DIDs: %s\n", formatNumber(int(stats["total_dids"].(int64))))
	fmt.Printf("  Shards: %d\n", stats["shard_count"])
	fmt.Printf("  Location: %s/.plcbundle/\n", dir)

	return nil
}

func indexStats(args []string) error {
	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	stats := mgr.GetDIDIndexStats()

	if !stats["exists"].(bool) {
		fmt.Printf("DID index does not exist\n")
		fmt.Printf("Run: plcbundle index build\n")
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

func indexLookup(args []string) error {
	fs := flag.NewFlagSet("index lookup", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose debug output")
	showJSON := fs.Bool("json", false, "output as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: plcbundle index lookup <did> [-v] [--json]")
	}

	did := fs.Arg(0)

	mgr, _, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	stats := mgr.GetDIDIndexStats()
	if !stats["exists"].(bool) {
		fmt.Fprintf(os.Stderr, "⚠️  DID index does not exist. Run: plcbundle index build\n")
		fmt.Fprintf(os.Stderr, "    Falling back to full scan (this will be slow)...\n\n")
	}

	if !*showJSON {
		fmt.Printf("Looking up: %s\n", did)
		if *verbose {
			fmt.Printf("Verbose mode: enabled\n")
		}
		fmt.Printf("\n")
	}

	totalStart := time.Now()
	ctx := context.Background()

	// Lookup operations
	lookupStart := time.Now()
	opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, *verbose)
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
		if *showJSON {
			fmt.Println("{\"found\": false, \"operations\": []}")
		} else {
			fmt.Printf("DID not found (searched in %s)\n", totalElapsed)
		}
		return nil
	}

	if *showJSON {
		return outputLookupJSON(did, opsWithLoc, mempoolOps, totalElapsed, lookupElapsed, mempoolElapsed)
	}

	return displayLookupResults(did, opsWithLoc, mempoolOps, totalElapsed, lookupElapsed, mempoolElapsed, *verbose, stats)
}

func indexResolve(args []string) error {
	fs := flag.NewFlagSet("index resolve", flag.ExitOnError)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: plcbundle index resolve <did>")
	}

	did := fs.Arg(0)

	mgr, _, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	ctx := context.Background()
	fmt.Fprintf(os.Stderr, "Resolving: %s\n", did)

	start := time.Now()

	// Check mempool first
	mempoolOps, _ := mgr.GetDIDOperationsFromMempool(did)
	if len(mempoolOps) > 0 {
		for i := len(mempoolOps) - 1; i >= 0; i-- {
			if !mempoolOps[i].IsNullified() {
				doc, err := plcclient.ResolveDIDDocument(did, []plcclient.PLCOperation{mempoolOps[i]})
				if err != nil {
					return fmt.Errorf("resolution failed: %w", err)
				}

				totalTime := time.Since(start)
				fmt.Fprintf(os.Stderr, "Total: %s (resolved from mempool)\n\n", totalTime)

				data, _ := json.MarshalIndent(doc, "", "  ")
				fmt.Println(string(data))
				return nil
			}
		}
	}

	// Use index
	op, err := mgr.GetLatestDIDOperation(ctx, did)
	if err != nil {
		return fmt.Errorf("failed to get latest operation: %w", err)
	}

	doc, err := plcclient.ResolveDIDDocument(did, []plcclient.PLCOperation{*op})
	if err != nil {
		return fmt.Errorf("resolution failed: %w", err)
	}

	totalTime := time.Since(start)
	fmt.Fprintf(os.Stderr, "Total: %s\n\n", totalTime)

	data, _ := json.MarshalIndent(doc, "", "  ")
	fmt.Println(string(data))

	return nil
}

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

	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("✓ Lookup complete in %s\n", totalElapsed)
	if stats["exists"].(bool) {
		fmt.Printf("  Method: DID index (fast)\n")
	} else {
		fmt.Printf("  Method: Full scan (slow)\n")
	}
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")

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
