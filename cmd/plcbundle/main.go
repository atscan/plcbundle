package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/yourusername/plc-bundle-lib/bundle"
	"github.com/yourusername/plc-bundle-lib/plc"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "init":
		cmdInit()
	case "fetch":
		cmdFetch()
	case "scan":
		cmdScan()
	case "verify":
		cmdVerify()
	case "info":
		cmdInfo()
	case "export":
		cmdExport()
	case "version":
		fmt.Printf("plcbundle version %s\n", version)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`plcbundle - PLC Bundle Management Tool

Usage:
  plcbundle <command> [options]

Commands:
  init       Initialize a new bundle directory
  fetch      Fetch next bundle from PLC directory
  scan       Scan directory and rebuild index
  verify     Verify bundle integrity
  info       Show bundle information
  export     Export operations from bundles
  version    Show version

Options vary by command. Use 'plcbundle <command> -h' for help.`)
}

func cmdInit() {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	dir := fs.String("dir", "./plc_bundles", "bundle directory")
	fs.Parse(os.Args[2:])

	if err := os.MkdirAll(*dir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Create empty index
	config := bundle.DefaultConfig(*dir)
	_, err := bundle.NewManager(config, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ Initialized bundle directory: %s\n", *dir)
}

func cmdFetch() {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	dir := fs.String("dir", "./plc_bundles", "bundle directory")
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL")
	count := fs.Int("count", 1, "number of bundles to fetch")
	fs.Parse(os.Args[2:])

	config := bundle.DefaultConfig(*dir)
	client := plc.NewClient(*plcURL)
	defer client.Close()

	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	ctx := context.Background()

	for i := 0; i < *count; i++ {
		fmt.Printf("Fetching bundle %d/%d...\n", i+1, *count)

		b, err := mgr.FetchNextBundle(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching bundle: %v\n", err)
			os.Exit(1)
		}

		if err := mgr.SaveBundle(ctx, b); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving bundle: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✓ Saved bundle %06d (%d operations, %d DIDs)\n",
			b.BundleNumber, len(b.Operations), b.DIDCount)
	}
}

func cmdScan() {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	dir := fs.String("dir", "./plc_bundles", "bundle directory")
	fs.Parse(os.Args[2:])

	config := bundle.DefaultConfig(*dir)
	mgr, err := bundle.NewManager(config, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	result, err := mgr.ScanDirectory()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Scan failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ Scanned directory: %s\n", result.BundleDir)
	fmt.Printf("  Bundles found: %d\n", result.BundleCount)
	if result.BundleCount > 0 {
		fmt.Printf("  Range: %06d - %06d\n", result.FirstBundle, result.LastBundle)
		fmt.Printf("  Total size: %.2f MB\n", float64(result.TotalSize)/(1024*1024))
		if len(result.MissingGaps) > 0 {
			fmt.Printf("  ⚠ Missing bundles: %v\n", result.MissingGaps)
		}
	}
	fmt.Printf("  Index: %s\n", result.IndexUpdated)
}

func cmdVerify() {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "./plc_bundles", "bundle directory")
	bundleNum := fs.Int("bundle", 0, "specific bundle to verify (0 = verify chain)")
	fs.Parse(os.Args[2:])

	config := bundle.DefaultConfig(*dir)
	mgr, err := bundle.NewManager(config, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	ctx := context.Background()

	if *bundleNum > 0 {
		// Verify specific bundle
		result, err := mgr.VerifyBundle(ctx, *bundleNum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Verification failed: %v\n", err)
			os.Exit(1)
		}

		if result.Valid {
			fmt.Printf("✓ Bundle %06d is valid\n", *bundleNum)
			fmt.Printf("  Hash: %s\n", result.LocalHash[:16]+"...")
		} else {
			fmt.Printf("✗ Bundle %06d is invalid\n", *bundleNum)
			if result.Error != nil {
				fmt.Printf("  Error: %v\n", result.Error)
			}
			os.Exit(1)
		}
	} else {
		// Verify entire chain
		result, err := mgr.VerifyChain(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Chain verification failed: %v\n", err)
			os.Exit(1)
		}

		if result.Valid {
			fmt.Printf("✓ Chain is valid (%d bundles verified)\n", result.ChainLength)
		} else {
			fmt.Printf("✗ Chain is broken at bundle %06d\n", result.BrokenAt)
			fmt.Printf("  Error: %s\n", result.Error)
			os.Exit(1)
		}
	}
}

func cmdInfo() {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	dir := fs.String("dir", "./plc_bundles", "bundle directory")
	bundleNum := fs.Int("bundle", 0, "specific bundle info (0 = general info)")
	fs.Parse(os.Args[2:])

	config := bundle.DefaultConfig(*dir)
	mgr, err := bundle.NewManager(config, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	if *bundleNum > 0 {
		// Show specific bundle info
		ctx := context.Background()
		b, err := mgr.LoadBundle(ctx, *bundleNum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Bundle %06d:\n", b.BundleNumber)
		fmt.Printf("  Time range: %s - %s\n", b.StartTime.Format("2006-01-02 15:04:05"), b.EndTime.Format("2006-01-02 15:04:05"))
		fmt.Printf("  Operations: %d\n", len(b.Operations))
		fmt.Printf("  Unique DIDs: %d\n", b.DIDCount)
		fmt.Printf("  Hash: %s\n", b.Hash)
		fmt.Printf("  Compressed: %.2f MB\n", float64(b.CompressedSize)/(1024*1024))
		fmt.Printf("  Uncompressed: %.2f MB\n", float64(b.UncompressedSize)/(1024*1024))
		fmt.Printf("  Compression ratio: %.2fx\n", b.CompressionRatio())
		fmt.Printf("  Cursor: %s\n", b.Cursor)
		if b.PrevBundleHash != "" {
			fmt.Printf("  Prev bundle hash: %s...\n", b.PrevBundleHash[:16])
		}
	} else {
		// Show general info
		info := mgr.GetInfo()
		fmt.Printf("Bundle Directory: %s\n", info["bundle_dir"])
		fmt.Printf("Bundle count: %v\n", info["bundle_count"])
		if bc, ok := info["bundle_count"].(int); ok && bc > 0 {
			fmt.Printf("Range: %06d - %06d\n", info["first_bundle"], info["last_bundle"])
			fmt.Printf("Total size: %.2f MB\n", float64(info["total_size"].(int64))/(1024*1024))
			if gaps, ok := info["gaps"].(int); ok && gaps > 0 {
				fmt.Printf("⚠ Missing bundles: %d\n", gaps)
			}
		}
		fmt.Printf("Index updated: %s\n", info["updated_at"])
	}
}

func cmdExport() {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	dir := fs.String("dir", "./plc_bundles", "bundle directory")
	count := fs.Int("count", 1000, "number of operations to export")
	after := fs.String("after", "", "timestamp to start after (RFC3339)")
	fs.Parse(os.Args[2:])

	config := bundle.DefaultConfig(*dir)
	mgr, err := bundle.NewManager(config, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	// Parse after time
	var afterTime time.Time
	if *after != "" {
		afterTime, err = time.Parse(time.RFC3339, *after)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid after time: %v\n", err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	ops, err := mgr.ExportOperations(ctx, afterTime, *count)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Export failed: %v\n", err)
		os.Exit(1)
	}

	// Output as JSONL
	for _, op := range ops {
		if len(op.RawJSON) > 0 {
			fmt.Println(string(op.RawJSON))
		}
	}

	fmt.Fprintf(os.Stderr, "Exported %d operations\n", len(ops))
}
