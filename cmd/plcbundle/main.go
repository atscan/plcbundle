package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
  fetch      Fetch next bundle from PLC directory
  scan       Scan current directory for bundles (incremental)
  verify     Verify bundle integrity
  info       Show bundle information
  export     Export operations from bundles
  version    Show version

The tool works with the current directory or nearest bundle directory.
Bundle directory is detected by presence of .jsonl.zst files or plc_bundles.json.`)
}

// findBundleDir finds the bundle directory (current dir or parent dirs)
func findBundleDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Check current directory
	if isBundleDir(cwd) {
		return cwd, nil
	}

	// Check for common subdirectories
	candidates := []string{
		filepath.Join(cwd, "plc_bundles"),
		filepath.Join(cwd, "bundles"),
		filepath.Join(cwd, "data"),
	}

	for _, dir := range candidates {
		if isBundleDir(dir) {
			return dir, nil
		}
	}

	// Walk up parent directories (like git)
	dir := cwd
	for {
		parent := filepath.Dir(dir)
		if parent == dir {
			break // Reached root
		}
		if isBundleDir(parent) {
			return parent, nil
		}
		dir = parent
	}

	// Default to current directory (will be auto-created)
	return cwd, nil
}

// isBundleDir checks if a directory is a bundle directory
func isBundleDir(dir string) bool {
	// Check for index file
	indexPath := filepath.Join(dir, bundle.INDEX_FILE)
	if _, err := os.Stat(indexPath); err == nil {
		return true
	}

	// Check for any .jsonl.zst files
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl.zst"))
	if err == nil && len(files) > 0 {
		return true
	}

	return false
}

// getManager creates or opens a bundle manager in the detected directory
func getManager(plcURL string) (*bundle.Manager, string, error) {
	dir, err := findBundleDir()
	if err != nil {
		return nil, "", err
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create directory: %w", err)
	}

	config := bundle.DefaultConfig(dir)

	var client *plc.Client
	if plcURL != "" {
		client = plc.NewClient(plcURL)
	}

	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		return nil, "", err
	}

	return mgr, dir, nil
}

func cmdFetch() {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL")
	count := fs.Int("count", 1, "number of bundles to fetch")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager(*plcURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)

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
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Scanning: %s\n", dir)

	// Get current index state
	index := mgr.GetIndex()
	indexedBundles := make(map[int]bool)
	for _, meta := range index.GetBundles() {
		indexedBundles[meta.BundleNumber] = true
	}

	// Find all bundle files
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl.zst"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error scanning directory: %v\n", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Println("No bundle files found")
		return
	}

	// Parse and sort bundle numbers
	var bundleNumbers []int
	for _, file := range files {
		base := filepath.Base(file)
		var num int
		if _, err := fmt.Sscanf(base, "%06d.jsonl.zst", &num); err == nil {
			bundleNumbers = append(bundleNumbers, num)
		}
	}

	// Sort bundle numbers
	for i := 0; i < len(bundleNumbers); i++ {
		for j := i + 1; j < len(bundleNumbers); j++ {
			if bundleNumbers[i] > bundleNumbers[j] {
				bundleNumbers[i], bundleNumbers[j] = bundleNumbers[j], bundleNumbers[i]
			}
		}
	}

	skippedCount := 0
	newCount := 0

	fmt.Printf("Found %d bundle files\n", len(bundleNumbers))

	// ← ctx line removed here

	// Process each bundle incrementally
	for _, num := range bundleNumbers {
		// Skip if already indexed
		if indexedBundles[num] {
			skippedCount++
			continue
		}

		fmt.Printf("  Processing bundle %06d...", num)

		path := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", num))

		// Load bundle file
		b, err := loadBundleFile(path, num)
		if err != nil {
			fmt.Printf(" ERROR: %v\n", err)
			continue
		}

		// Calculate metadata
		meta := calculateBundleMetadata(index, num, path, b.Operations)

		// Add to index
		index.AddBundle(meta)

		// Save index immediately (incremental)
		if err := mgr.SaveIndex(); err != nil {
			fmt.Printf(" ERROR saving index: %v\n", err)
			continue
		}

		newCount++
		fmt.Printf(" ✓ (%d ops, %d DIDs)\n", len(b.Operations), meta.DIDCount)
	}

	fmt.Printf("\n")
	fmt.Printf("✓ Scan complete\n")
	fmt.Printf("  Total bundles: %d\n", len(bundleNumbers))
	fmt.Printf("  Already indexed: %d\n", skippedCount)
	fmt.Printf("  Newly scanned: %d\n", newCount)
	fmt.Printf("  Index: %s\n", filepath.Join(dir, bundle.INDEX_FILE))
}

// loadBundleFile loads and parses a bundle file directly
func loadBundleFile(path string, num int) (*bundle.Bundle, error) {
	config := bundle.DefaultConfig(filepath.Dir(path))
	ops, err := bundle.NewOperations(config.CompressionLevel, config.Logger)
	if err != nil {
		return nil, err
	}
	defer ops.Close()

	operations, err := ops.LoadBundle(path)
	if err != nil {
		return nil, err
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	return &bundle.Bundle{
		BundleNumber: num,
		Operations:   operations,
		StartTime:    operations[0].CreatedAt,
		EndTime:      operations[len(operations)-1].CreatedAt,
	}, nil
}

// calculateBundleMetadata calculates metadata for a bundle
func calculateBundleMetadata(index *bundle.Index, num int, path string, ops []plc.PLCOperation) *bundle.BundleMetadata {
	// Get file size
	info, _ := os.Stat(path)
	compressedSize := info.Size()

	// Calculate unique DIDs
	didSet := make(map[string]bool)
	for _, op := range ops {
		didSet[op.DID] = true
	}

	// Calculate uncompressed size by summing raw JSON lengths
	uncompressedSize := int64(0)
	for _, op := range ops {
		uncompressedSize += int64(len(op.RawJSON)) + 1 // +1 for newline
	}

	// Calculate hashes
	uncompressedHash := calculateUncompressedHash(ops)
	compressedData, _ := os.ReadFile(path)
	compressedHash := computeHash(compressedData)

	// Get cursor and prev hash from index
	cursor := ""
	prevHash := ""

	if num > 1 {
		if prevMeta, err := index.GetBundle(num - 1); err == nil {
			cursor = prevMeta.EndTime.Format(time.RFC3339Nano)
			prevHash = prevMeta.Hash
		}
	}

	return &bundle.BundleMetadata{
		BundleNumber:     num,
		StartTime:        ops[0].CreatedAt,
		EndTime:          ops[len(ops)-1].CreatedAt,
		OperationCount:   len(ops),
		DIDCount:         len(didSet),
		Hash:             uncompressedHash,
		CompressedHash:   compressedHash,
		CompressedSize:   compressedSize,
		UncompressedSize: uncompressedSize,
		Cursor:           cursor,
		PrevBundleHash:   prevHash,
		CreatedAt:        time.Now().UTC(),
	}
}

// calculateUncompressedHash calculates hash of JSONL data
func calculateUncompressedHash(ops []plc.PLCOperation) string {
	var data []byte
	for _, op := range ops {
		data = append(data, op.RawJSON...)
		data = append(data, '\n')
	}
	return computeHash(data)
}

// computeHash computes SHA256 hash
func computeHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func cmdVerify() {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	bundleNum := fs.Int("bundle", 0, "specific bundle to verify (0 = verify chain)")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)

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
	bundleNum := fs.Int("bundle", 0, "specific bundle info (0 = general info)")
	fs.Parse(os.Args[2:])

	mgr, dir, err := getManager("")
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
		fmt.Printf("  Directory: %s\n", dir)
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
		fmt.Printf("Bundle Directory: %s\n", dir)
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
	count := fs.Int("count", 1000, "number of operations to export")
	after := fs.String("after", "", "timestamp to start after (RFC3339)")
	fs.Parse(os.Args[2:])

	mgr, _, err := getManager("")
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
