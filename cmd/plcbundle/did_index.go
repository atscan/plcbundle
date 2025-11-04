package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/plc"
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

	fmt.Printf("\nDID Index Statistics\n")
	fmt.Printf("════════════════════\n\n")
	fmt.Printf("  Location:      %s/.plcbundle/\n", dir)
	fmt.Printf("  Total DIDs:    %s\n", formatNumber(int(stats["total_dids"].(int64))))
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
	fs.Parse(os.Args[3:])

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle index lookup <did> [-v]\n")
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

	fmt.Printf("Looking up: %s\n", did)
	if *verbose {
		fmt.Printf("Verbose mode: enabled\n")
	}
	fmt.Printf("\n")

	start := time.Now()
	ctx := context.Background()

	// Get bundled operations only
	bundledOps, err := mgr.GetDIDOperationsBundledOnly(ctx, did, *verbose)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Get mempool operations separately
	mempoolOps, err := mgr.GetDIDOperationsFromMempool(did)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error checking mempool: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)

	if len(bundledOps) == 0 && len(mempoolOps) == 0 {
		fmt.Printf("DID not found (searched in %s)\n", elapsed)
		return
	}

	// Count nullified operations
	nullifiedCount := 0
	for _, op := range bundledOps {
		if op.IsNullified() {
			nullifiedCount++
		}
	}

	// Display summary
	totalOps := len(bundledOps) + len(mempoolOps)
	fmt.Printf("Found %d total operations in %s\n", totalOps, elapsed)
	if len(bundledOps) > 0 {
		fmt.Printf("  Bundled: %d (%d active, %d nullified)\n", len(bundledOps), len(bundledOps)-nullifiedCount, nullifiedCount)
	}
	if len(mempoolOps) > 0 {
		fmt.Printf("  Mempool: %d (not yet bundled)\n", len(mempoolOps))
	}
	fmt.Printf("\n")

	// Show bundled operations
	if len(bundledOps) > 0 {
		fmt.Printf("Bundled operations:\n")
		for i, op := range bundledOps {
			status := "✓"
			if op.IsNullified() {
				status = "✗"
			}

			fmt.Printf("  %s %d. CID: %s\n", status, i+1, op.CID)
			fmt.Printf("       Time: %s\n", op.CreatedAt.Format("2006-01-02 15:04:05"))

			if op.IsNullified() {
				if nullCID := op.GetNullifyingCID(); nullCID != "" {
					fmt.Printf("       Nullified by: %s\n", nullCID)
				} else {
					fmt.Printf("       Nullified: true\n")
				}
			}
		}
		fmt.Printf("\n")
	}

	// Show mempool operations
	if len(mempoolOps) > 0 {
		fmt.Printf("Mempool operations (not yet bundled):\n")
		for i, op := range mempoolOps {
			status := "✓"
			if op.IsNullified() {
				status = "✗"
			}

			fmt.Printf("  %s %d. CID: %s\n", status, i+1, op.CID)
			fmt.Printf("       Time: %s\n", op.CreatedAt.Format("2006-01-02 15:04:05"))
		}
	}
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
	var latestOp *plc.PLCOperation
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
		ops := []plc.PLCOperation{*latestOp}
		doc, err := plc.ResolveDIDDocument(did, ops)
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

	// STEP 2: Bundle loading timing
	bundleStart := time.Now()
	bndl, err := mgr.LoadBundle(ctx, int(latestLoc.Bundle))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading bundle: %v\n", err)
		os.Exit(1)
	}
	bundleTime := time.Since(bundleStart)

	if int(latestLoc.Position) >= len(bndl.Operations) {
		fmt.Fprintf(os.Stderr, "Invalid position\n")
		os.Exit(1)
	}

	op := bndl.Operations[latestLoc.Position]

	fmt.Fprintf(os.Stderr, "Bundle load: %s (bundle %d, pos %d)\n",
		bundleTime, latestLoc.Bundle, latestLoc.Position)

	// STEP 3: Build DID document
	ops := []plc.PLCOperation{op}
	doc, err := plc.ResolveDIDDocument(did, ops)
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
