package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/plcclient"
)

func cmdDIDIndex() {
	if len(os.Args) < 3 {
		printDIDIndexUsage()
		os.Exit(1)
	}

	subcommand := os.Args[2]

	switch subcommand {
	case "build":
		cmdDIDIndexBuild()
	case "stats":
		cmdDIDIndexStats()
	case "lookup":
		cmdDIDIndexLookup()
	case "resolve":
		cmdDIDIndexResolve()
	default:
		fmt.Fprintf(os.Stderr, "Unknown index subcommand: %s\n", subcommand)
		printDIDIndexUsage()
		os.Exit(1)
	}
}

func printDIDIndexUsage() {
	fmt.Printf(`Usage: plcbundle index <command> [options]

Commands:
  build     Build DID index from bundles
  stats     Show index statistics
  lookup    Lookup a specific DID
  resolve   Resolve DID to current document

Examples:
  plcbundle index build
  plcbundle index stats
  plcbundle index lookup -v did:plc:524tuhdhh3m7li5gycdn6boe
  plcbundle index resolve did:plc:524tuhdhh3m7li5gycdn6boe
`)
}

func cmdDIDIndexBuild() {
	fs := flag.NewFlagSet("index build", flag.ExitOnError)
	force := fs.Bool("force", false, "rebuild even if index exists")
	fs.Parse(os.Args[3:])

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	// Check if index exists
	stats := mgr.GetDIDIndexStats()
	if stats["exists"].(bool) && !*force {
		fmt.Printf("DID index already exists (use --force to rebuild)\n")
		fmt.Printf("Directory: %s\n", dir)
		fmt.Printf("Total DIDs: %d\n", stats["total_dids"])
		return
	}

	fmt.Printf("Building DID index in: %s\n", dir)

	index := mgr.GetIndex()
	bundleCount := index.Count()

	if bundleCount == 0 {
		fmt.Printf("No bundles to index\n")
		return
	}

	fmt.Printf("Indexing %d bundles...\n\n", bundleCount)

	progress := NewProgressBar(bundleCount)

	start := time.Now()
	ctx := context.Background()

	err = mgr.BuildDIDIndex(ctx, func(current, total int) {
		progress.Set(current)
	})

	progress.Finish()

	if err != nil {
		fmt.Fprintf(os.Stderr, "\nError building index: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)

	stats = mgr.GetDIDIndexStats()

	fmt.Printf("\n✓ DID index built in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total DIDs: %s\n", formatNumber(int(stats["total_dids"].(int64))))
	fmt.Printf("  Shards: %d\n", stats["shard_count"])
	fmt.Printf("  Location: %s/.plcbundle/\n", dir)
}

func cmdDIDIndexStats() {
	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	stats := mgr.GetDIDIndexStats()

	if !stats["exists"].(bool) {
		fmt.Printf("DID index does not exist\n")
		fmt.Printf("Run: plcbundle index build\n")
		return
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
	fmt.Printf("  Updated:       %s\n", stats["updated_at"].(time.Time).Format("2006-01-02 15:04:05"))
	fmt.Printf("\n")
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
}

func cmdDIDIndexLookup() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle index lookup <did> [-v]\n")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("index lookup", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose debug output")
	showJSON := fs.Bool("json", false, "output as JSON")
	fs.Parse(os.Args[3:])

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle index lookup <did> [-v] [--json]\n")
		os.Exit(1)
	}

	did := fs.Arg(0)

	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
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

	// === TIMING START ===
	totalStart := time.Now()
	ctx := context.Background()

	// === STEP 1: Index/Scan Lookup ===
	lookupStart := time.Now()
	opsWithLoc, err := mgr.GetDIDOperationsWithLocations(ctx, did, *verbose)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	lookupElapsed := time.Since(lookupStart)

	// === STEP 2: Mempool Lookup ===
	mempoolStart := time.Now()
	mempoolOps, err := mgr.GetDIDOperationsFromMempool(did)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error checking mempool: %v\n", err)
		os.Exit(1)
	}
	mempoolElapsed := time.Since(mempoolStart)

	totalElapsed := time.Since(totalStart)

	// === NOT FOUND ===
	if len(opsWithLoc) == 0 && len(mempoolOps) == 0 {
		if *showJSON {
			fmt.Println("{\"found\": false, \"operations\": []}")
		} else {
			fmt.Printf("DID not found (searched in %s)\n", totalElapsed)
		}
		return
	}

	// === JSON OUTPUT MODE ===
	if *showJSON {
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
		return
	}

	// === CALCULATE STATISTICS ===
	nullifiedCount := 0
	for _, owl := range opsWithLoc {
		if owl.Operation.IsNullified() {
			nullifiedCount++
		}
	}

	totalOps := len(opsWithLoc) + len(mempoolOps)
	activeOps := len(opsWithLoc) - nullifiedCount + len(mempoolOps)

	// === DISPLAY SUMMARY ===
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    DID Lookup Results\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	fmt.Printf("DID: %s\n\n", did)

	fmt.Printf("Summary\n")
	fmt.Printf("───────\n")
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

	// === TIMING BREAKDOWN ===
	fmt.Printf("Performance\n")
	fmt.Printf("───────────\n")
	fmt.Printf("  Index lookup:       %s\n", lookupElapsed)
	fmt.Printf("  Mempool check:      %s\n", mempoolElapsed)
	fmt.Printf("  Total time:         %s\n", totalElapsed)

	if len(opsWithLoc) > 0 {
		avgPerOp := lookupElapsed / time.Duration(len(opsWithLoc))
		fmt.Printf("  Avg per operation:  %s\n", avgPerOp)
	}
	fmt.Printf("\n")

	// === BUNDLED OPERATIONS ===
	if len(opsWithLoc) > 0 {
		fmt.Printf("Bundled Operations (%d total)\n", len(opsWithLoc))
		fmt.Printf("══════════════════════════════════════════════════════════════\n\n")

		for i, owl := range opsWithLoc {
			op := owl.Operation
			status := "✓ Active"
			statusSymbol := "✓"
			if op.IsNullified() {
				status = "✗ Nullified"
				statusSymbol = "✗"
			}

			fmt.Printf("%s Operation %d [Bundle %06d, Position %04d]\n",
				statusSymbol, i+1, owl.Bundle, owl.Position)
			fmt.Printf("   CID:        %s\n", op.CID)
			fmt.Printf("   Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000 MST"))
			fmt.Printf("   Status:     %s\n", status)

			if op.IsNullified() {
				if nullCID := op.GetNullifyingCID(); nullCID != "" {
					fmt.Printf("   Nullified:  %s\n", nullCID)
				}
			}

			// Show operation type if verbose
			if *verbose {
				if opData, err := op.GetOperationData(); err == nil && opData != nil {
					if opType, ok := opData["type"].(string); ok {
						fmt.Printf("   Type:       %s\n", opType)
					}

					// Show handle if present
					if handle, ok := opData["handle"].(string); ok {
						fmt.Printf("   Handle:     %s\n", handle)
					} else if aka, ok := opData["alsoKnownAs"].([]interface{}); ok && len(aka) > 0 {
						if akaStr, ok := aka[0].(string); ok {
							handle := strings.TrimPrefix(akaStr, "at://")
							fmt.Printf("   Handle:     %s\n", handle)
						}
					}

					// Show service if present
					if services, ok := opData["services"].(map[string]interface{}); ok {
						if pds, ok := services["atproto_pds"].(map[string]interface{}); ok {
							if endpoint, ok := pds["endpoint"].(string); ok {
								fmt.Printf("   PDS:        %s\n", endpoint)
							}
						}
					}
				}
			}

			fmt.Printf("\n")
		}
	}

	// === MEMPOOL OPERATIONS ===
	if len(mempoolOps) > 0 {
		fmt.Printf("Mempool Operations (%d total, not yet bundled)\n", len(mempoolOps))
		fmt.Printf("══════════════════════════════════════════════════════════════\n\n")

		for i, op := range mempoolOps {
			status := "✓ Active"
			statusSymbol := "✓"
			if op.IsNullified() {
				status = "✗ Nullified"
				statusSymbol = "✗"
			}

			fmt.Printf("%s Operation %d [Mempool]\n", statusSymbol, i+1)
			fmt.Printf("   CID:        %s\n", op.CID)
			fmt.Printf("   Created:    %s\n", op.CreatedAt.Format("2006-01-02 15:04:05.000 MST"))
			fmt.Printf("   Status:     %s\n", status)

			if op.IsNullified() {
				if nullCID := op.GetNullifyingCID(); nullCID != "" {
					fmt.Printf("   Nullified:  %s\n", nullCID)
				}
			}

			// Show operation type if verbose
			if *verbose {
				if opData, err := op.GetOperationData(); err == nil && opData != nil {
					if opType, ok := opData["type"].(string); ok {
						fmt.Printf("   Type:       %s\n", opType)
					}

					// Show handle
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

			fmt.Printf("\n")
		}
	}

	// === TIMELINE (if multiple operations) ===
	if totalOps > 1 && !*verbose {
		fmt.Printf("Timeline\n")
		fmt.Printf("────────\n")

		allTimes := make([]time.Time, 0, totalOps)
		for _, owl := range opsWithLoc {
			allTimes = append(allTimes, owl.Operation.CreatedAt)
		}
		for _, op := range mempoolOps {
			allTimes = append(allTimes, op.CreatedAt)
		}

		if len(allTimes) > 0 {
			firstTime := allTimes[0]
			lastTime := allTimes[len(allTimes)-1]
			timespan := lastTime.Sub(firstTime)

			fmt.Printf("  First operation:    %s\n", firstTime.Format("2006-01-02 15:04:05"))
			fmt.Printf("  Latest operation:   %s\n", lastTime.Format("2006-01-02 15:04:05"))
			fmt.Printf("  Timespan:           %s\n", formatDuration(timespan))
			fmt.Printf("  Activity age:       %s ago\n", formatDuration(time.Since(lastTime)))
		}
		fmt.Printf("\n")
	}

	// === FINAL SUMMARY ===
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("✓ Lookup complete in %s\n", totalElapsed)
	if stats["exists"].(bool) {
		fmt.Printf("  Method: DID index (fast)\n")
	} else {
		fmt.Printf("  Method: Full scan (slow)\n")
	}
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
}

func cmdDIDIndexResolve() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle index resolve <did> [-v]\n")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("index resolve", flag.ExitOnError)
	//verbose := fs.Bool("v", false, "verbose debug output")
	fs.Parse(os.Args[3:])

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle index resolve <did> [-v]\n")
		os.Exit(1)
	}

	did := fs.Arg(0)

	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	ctx := context.Background()
	fmt.Fprintf(os.Stderr, "Resolving: %s\n", did)

	start := time.Now()

	// ✨ STEP 0: Check mempool first (most recent data)
	mempoolStart := time.Now()
	var latestOp *plcclient.PLCOperation
	foundInMempool := false

	if mgr.GetMempool() != nil {
		mempoolOps, err := mgr.GetMempoolOperations()
		if err == nil && len(mempoolOps) > 0 {
			// Search backward for this DID
			for i := len(mempoolOps) - 1; i >= 0; i-- {
				if mempoolOps[i].DID == did && !mempoolOps[i].IsNullified() {
					latestOp = &mempoolOps[i]
					foundInMempool = true
					break
				}
			}
		}
	}
	mempoolTime := time.Since(mempoolStart)

	if foundInMempool {
		fmt.Fprintf(os.Stderr, "Mempool check: %s (✓ found in mempool)\n", mempoolTime)

		// Build document from mempool operation
		ops := []plcclient.PLCOperation{*latestOp}
		doc, err := plcclient.ResolveDIDDocument(did, ops)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Build document failed: %v\n", err)
			os.Exit(1)
		}

		totalTime := time.Since(start)
		fmt.Fprintf(os.Stderr, "Total: %s (resolved from mempool)\n\n", totalTime)

		// Output to stdout
		data, _ := json.MarshalIndent(doc, "", "  ")
		fmt.Println(string(data))
		return
	}

	fmt.Fprintf(os.Stderr, "Mempool check: %s (not found)\n", mempoolTime)

	// Not in mempool - check index
	stats := mgr.GetDIDIndexStats()
	if !stats["exists"].(bool) {
		fmt.Fprintf(os.Stderr, "⚠️  DID index does not exist. Run: plcbundle index build\n\n")
		os.Exit(1)
	}

	// STEP 1: Index lookup timing
	indexStart := time.Now()
	locations, err := mgr.GetDIDIndex().GetDIDLocations(did)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	indexTime := time.Since(indexStart)

	if len(locations) == 0 {
		fmt.Fprintf(os.Stderr, "DID not found in index or mempool\n")
		os.Exit(1)
	}

	// Find latest non-nullified location
	var latestLoc *bundle.OpLocation
	for i := range locations {
		if locations[i].Nullified {
			continue
		}
		if latestLoc == nil ||
			locations[i].Bundle > latestLoc.Bundle ||
			(locations[i].Bundle == latestLoc.Bundle && locations[i].Position > latestLoc.Position) {
			latestLoc = &locations[i]
		}
	}

	if latestLoc == nil {
		fmt.Fprintf(os.Stderr, "No valid operations (all nullified)\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Index lookup: %s (shard access)\n", indexTime)

	// STEP 2: Operation loading timing (single op, not full bundle!)
	opStart := time.Now()
	op, err := mgr.LoadOperation(ctx, int(latestLoc.Bundle), int(latestLoc.Position))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading operation: %v\n", err)
		os.Exit(1)
	}
	opTime := time.Since(opStart)

	fmt.Fprintf(os.Stderr, "Operation load: %s (bundle %d, pos %d)\n",
		opTime, latestLoc.Bundle, latestLoc.Position)

	// STEP 3: Build DID document
	ops := []plcclient.PLCOperation{*op}
	doc, err := plcclient.ResolveDIDDocument(did, ops)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Build document failed: %v\n", err)
		os.Exit(1)
	}

	totalTime := time.Since(start)
	fmt.Fprintf(os.Stderr, "Total: %s\n\n", totalTime)

	// Output to stdout
	data, _ := json.MarshalIndent(doc, "", "  ")
	fmt.Println(string(data))

}
