// cmd/plcbundle/detector.go
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"tangled.org/atscan.net/plcbundle/detector"
	"tangled.org/atscan.net/plcbundle/plc"
)

type defaultLogger struct{}

func (d *defaultLogger) Printf(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)
}

func cmdDetector() {
	if len(os.Args) < 3 {
		printDetectorUsage()
		os.Exit(1)
	}

	subcommand := os.Args[2]

	switch subcommand {
	case "list":
		cmdDetectorList()
	case "test":
		cmdDetectorTest()
	case "run":
		cmdDetectorRun()
	case "filter":
		cmdDetectorFilter()
	case "info":
		cmdDetectorInfo()
	default:
		fmt.Fprintf(os.Stderr, "Unknown detector subcommand: %s\n", subcommand)
		printDetectorUsage()
		os.Exit(1)
	}
}

func printDetectorUsage() {
	fmt.Printf(`Usage: plcbundle detector <command> [options]

Commands:
  list          List available detectors
  test          Test a detector on specific bundles
  run           Run detector and output CSV results
  filter        Filter JSONL operations from stdin
  info          Show detailed detector information

Examples:
  # List all built-in detectors
  plcbundle detector list

  # Run built-in detector
  plcbundle detector run invalid_handle --bundles 1-100

  # Run custom JavaScript detector
  plcbundle detector run ./my_detector.js --bundles 1-100

  # Run multiple detectors (built-in + custom)
  plcbundle detector run invalid_handle ./my_detector.js --bundles 1-100

  # Run all built-in detectors
  plcbundle detector run all --bundles 1-100

  # Filter with custom detector
  plcbundle backfill | plcbundle detector filter ./my_detector.js > clean.jsonl
`)
}

// cmdDetectorFilter reads JSONL from stdin, filters OUT spam, outputs clean operations
func cmdDetectorFilter() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle detector filter <detector1|script.js> [detector2...] [--confidence 0.9]\n")
		os.Exit(1)
	}

	// Parse detector names and flags
	var detectorNames []string
	var flagArgs []string
	for i := 3; i < len(os.Args); i++ {
		if strings.HasPrefix(os.Args[i], "-") {
			flagArgs = os.Args[i:]
			break
		}
		detectorNames = append(detectorNames, os.Args[i])
	}

	if len(detectorNames) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one detector name required\n")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("detector filter", flag.ExitOnError)
	confidence := fs.Float64("confidence", 0.90, "minimum confidence")
	fs.Parse(flagArgs)

	// Load detectors (common logic)
	setup, err := parseAndLoadDetectors(detectorNames, *confidence)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer setup.cleanup()

	fmt.Fprintf(os.Stderr, "Filtering with %d detector(s), min confidence: %.2f\n\n", len(setup.detectors), *confidence)

	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	cleanCount, filteredCount, totalCount := 0, 0, 0
	totalBytes, filteredBytes := int64(0), int64(0)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		totalCount++
		totalBytes += int64(len(line))

		var op plc.PLCOperation
		if err := json.Unmarshal(line, &op); err != nil {
			continue
		}

		// Run detection (common logic)
		labels, _ := detectOperation(ctx, setup.detectors, op, setup.confidence)

		if len(labels) == 0 {
			cleanCount++
			fmt.Println(string(line))
		} else {
			filteredCount++
			filteredBytes += int64(len(line))
		}

		if totalCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed: %d | Clean: %d | Filtered: %d\r", totalCount, cleanCount, filteredCount)
		}
	}

	// Stats
	fmt.Fprintf(os.Stderr, "\n\n✓ Filter complete\n")
	fmt.Fprintf(os.Stderr, "  Total: %d | Clean: %d (%.2f%%) | Filtered: %d (%.2f%%)\n",
		totalCount, cleanCount, float64(cleanCount)/float64(totalCount)*100,
		filteredCount, float64(filteredCount)/float64(totalCount)*100)
	fmt.Fprintf(os.Stderr, "  Size saved: %s (%.2f%%)\n", formatBytes(filteredBytes), float64(filteredBytes)/float64(totalBytes)*100)
}

func cmdDetectorList() {
	registry := detector.DefaultRegistry()
	detectors := registry.List()

	// Sort by name
	sort.Slice(detectors, func(i, j int) bool {
		return detectors[i].Name() < detectors[j].Name()
	})

	fmt.Printf("Available detectors:\n\n")
	for _, d := range detectors {
		fmt.Printf("  %-20s %s (v%s)\n", d.Name(), d.Description(), d.Version())
	}
	fmt.Printf("\nUse 'plcbundle detector info <name>' for details\n")
}

func cmdDetectorTest() {
	// Extract detector name first
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle detector test <detector-name> --bundle N\n")
		os.Exit(1)
	}

	detectorName := os.Args[3]

	// Parse flags from os.Args[4:]
	fs := flag.NewFlagSet("detector test", flag.ExitOnError)
	bundleNum := fs.Int("bundle", 0, "bundle number to test")
	confidence := fs.Float64("confidence", 0.90, "minimum confidence threshold")
	verbose := fs.Bool("v", false, "verbose output")
	fs.Parse(os.Args[4:]) // ← Changed from os.Args[3:]

	if *bundleNum == 0 {
		fmt.Fprintf(os.Stderr, "Error: --bundle required\n")
		os.Exit(1)
	}

	// Load bundle
	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	ctx := context.Background()
	bundle, err := mgr.LoadBundle(ctx, *bundleNum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading bundle: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Testing detector '%s' on bundle %06d...\n", detectorName, *bundleNum)
	fmt.Printf("Min confidence: %.2f\n\n", *confidence)

	// Run detector
	registry := detector.DefaultRegistry()
	config := detector.DefaultConfig()
	config.MinConfidence = *confidence

	runner := detector.NewRunner(registry, config, &defaultLogger{})
	results, err := runner.RunOnBundle(ctx, detectorName, bundle)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Detection failed: %v\n", err)
		os.Exit(1)
	}

	// Calculate stats
	stats := detector.CalculateStats(results, len(bundle.Operations))

	// Display results
	fmt.Printf("Results:\n")
	fmt.Printf("  Total operations:    %d\n", stats.TotalOperations)
	fmt.Printf("  Matches found:       %d (%.2f%%)\n", stats.MatchedCount, stats.MatchRate*100)
	fmt.Printf("\n")

	if len(stats.ByReason) > 0 {
		fmt.Printf("Breakdown by reason:\n")
		for reason, count := range stats.ByReason {
			pct := float64(count) / float64(stats.MatchedCount) * 100
			fmt.Printf("  %-25s %d (%.1f%%)\n", reason, count, pct)
		}
		fmt.Printf("\n")
	}

	if len(stats.ByCategory) > 0 {
		fmt.Printf("Breakdown by category:\n")
		for category, count := range stats.ByCategory {
			pct := float64(count) / float64(stats.MatchedCount) * 100
			fmt.Printf("  %-25s %d (%.1f%%)\n", category, count, pct)
		}
		fmt.Printf("\n")
	}

	if len(stats.ByConfidence) > 0 {
		fmt.Printf("Confidence distribution:\n")
		for bucket, count := range stats.ByConfidence {
			pct := float64(count) / float64(stats.MatchedCount) * 100
			fmt.Printf("  %-25s %d (%.1f%%)\n", bucket, count, pct)
		}
		fmt.Printf("\n")
	}

	if *verbose && len(results) > 0 {
		fmt.Printf("Sample matches (first 10):\n")
		displayCount := 10
		if len(results) < displayCount {
			displayCount = len(results)
		}

		for i := 0; i < displayCount; i++ {
			res := results[i]
			fmt.Printf("  %d. Position %d: %s\n", i+1, res.Position, res.DID)
			fmt.Printf("     Reason: %s (confidence: %.2f)\n", res.Match.Reason, res.Match.Confidence)
			if res.Match.Note != "" {
				fmt.Printf("     Note: %s\n", res.Match.Note)
			}
		}

		if len(results) > displayCount {
			fmt.Printf("  ... and %d more\n", len(results)-displayCount)
		}
	}
}

func cmdDetectorRun() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle detector run <detector1|script.js> [detector2...] [--bundles 1-100]\n")
		os.Exit(1)
	}

	// Parse detector names and flags
	var detectorNames []string
	var flagArgs []string
	for i := 3; i < len(os.Args); i++ {
		if strings.HasPrefix(os.Args[i], "-") {
			flagArgs = os.Args[i:]
			break
		}
		detectorNames = append(detectorNames, os.Args[i])
	}

	if len(detectorNames) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one detector name required\n")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("detector run", flag.ExitOnError)
	bundleRange := fs.String("bundles", "", "bundle range, default: all bundles")
	confidence := fs.Float64("confidence", 0.90, "minimum confidence")
	pprofPort := fs.String("pprof", "", "enable pprof on port (e.g., :6060)")
	fs.Parse(flagArgs)

	// Start pprof server if requested
	if *pprofPort != "" {
		go func() {
			fmt.Fprintf(os.Stderr, "pprof server starting on http://localhost%s/debug/pprof/\n", *pprofPort)
			if err := http.ListenAndServe(*pprofPort, nil); err != nil {
				fmt.Fprintf(os.Stderr, "pprof server failed: %v\n", err)
			}
		}()
		time.Sleep(100 * time.Millisecond) // Let server start
	}

	// Load manager
	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	// Determine bundle range
	var start, end int
	if *bundleRange == "" {
		index := mgr.GetIndex()
		bundles := index.GetBundles()
		if len(bundles) == 0 {
			fmt.Fprintf(os.Stderr, "Error: no bundles available\n")
			os.Exit(1)
		}
		start = bundles[0].BundleNumber
		end = bundles[len(bundles)-1].BundleNumber
		fmt.Fprintf(os.Stderr, "Using all bundles: %d-%d\n", start, end)
	} else {
		start, end, err = parseBundleRange(*bundleRange)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	// Load detectors (common logic)
	setup, err := parseAndLoadDetectors(detectorNames, *confidence)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer setup.cleanup()

	fmt.Fprintf(os.Stderr, "Running %d detector(s) on bundles %d-%d\n", len(setup.detectors), start, end)
	fmt.Fprintf(os.Stderr, "Min confidence: %.2f\n\n", *confidence)

	ctx := context.Background()
	fmt.Println("bundle,position,cid,size,confidence,labels")

	// Stats
	totalOps, matchCount := 0, 0
	totalBytes, matchedBytes := int64(0), int64(0)
	totalBundles := end - start + 1
	progress := NewProgressBar(totalBundles)
	progress.showBytes = true

	// Process bundles
	for bundleNum := start; bundleNum <= end; bundleNum++ {
		bundle, err := mgr.LoadBundle(ctx, bundleNum)
		if err != nil {
			continue
		}

		totalOps += len(bundle.Operations)

		for position, op := range bundle.Operations {
			opSize := len(op.RawJSON)
			if opSize == 0 {
				data, _ := json.Marshal(op)
				opSize = len(data)
			}
			totalBytes += int64(opSize)

			// Run detection (common logic)
			labels, confidence := detectOperation(ctx, setup.detectors, op, setup.confidence)

			if len(labels) > 0 {
				matchCount++
				matchedBytes += int64(opSize)

				cidShort := op.CID
				if len(cidShort) > 4 {
					cidShort = cidShort[len(cidShort)-4:]
				}

				fmt.Printf("%d,%d,%s,%d,%.2f,%s\n",
					bundleNum, position, cidShort, opSize, confidence, strings.Join(labels, ";"))
			}
		}

		progress.SetWithBytes(bundleNum-start+1, totalBytes)
	}

	progress.Finish()

	// Stats
	fmt.Fprintf(os.Stderr, "\n✓ Detection complete\n")
	fmt.Fprintf(os.Stderr, "  Total operations:   %d\n", totalOps)
	fmt.Fprintf(os.Stderr, "  Matches found:      %d (%.2f%%)\n", matchCount, float64(matchCount)/float64(totalOps)*100)
	fmt.Fprintf(os.Stderr, "  Total size:         %s\n", formatBytes(totalBytes))
	fmt.Fprintf(os.Stderr, "  Matched size:       %s (%.2f%%)\n", formatBytes(matchedBytes), float64(matchedBytes)/float64(totalBytes)*100)
}

func cmdDetectorInfo() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle detector info <name>\n")
		os.Exit(1)
	}

	detectorName := os.Args[3]

	registry := detector.DefaultRegistry()
	d, err := registry.Get(detectorName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Detector: %s\n", d.Name())
	fmt.Printf("Version: %s\n", d.Version())
	fmt.Printf("Description: %s\n", d.Description())
	fmt.Printf("\n")

	// Show example usage
	fmt.Printf("Usage examples:\n")
	fmt.Printf("  # Test on single bundle\n")
	fmt.Printf("  plcbundle detector test %s --bundle 42\n\n", d.Name())
	fmt.Printf("  # Run on range and save\n")
	fmt.Printf("  plcbundle detector run %s --bundles 1-100 --output results.csv\n\n", d.Name())
	fmt.Printf("  # Use with filter creation\n")
	fmt.Printf("  plcbundle filter detect --detector %s --bundles 1-100\n", d.Name())
}

// Helper functions

func parseBundleRange(rangeStr string) (start, end int, err error) {
	// Handle single bundle number
	if !strings.Contains(rangeStr, "-") {
		var num int
		_, err = fmt.Sscanf(rangeStr, "%d", &num)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid bundle number: %w", err)
		}
		return num, num, nil
	}

	// Handle range (e.g., "1-100")
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format (expected: N or start-end)")
	}

	_, err = fmt.Sscanf(parts[0], "%d", &start)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start: %w", err)
	}

	_, err = fmt.Sscanf(parts[1], "%d", &end)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end: %w", err)
	}

	if start > end {
		return 0, 0, fmt.Errorf("start must be <= end")
	}

	return start, end, nil
}

// Common detector setup
type detectorSetup struct {
	detectors       []detector.Detector
	scriptDetectors []interface{ Close() error }
	confidence      float64
}

func (ds *detectorSetup) cleanup() {
	for _, sd := range ds.scriptDetectors {
		sd.Close()
	}
}

// parseAndLoadDetectors handles common detector loading logic
func parseAndLoadDetectors(detectorNames []string, confidence float64) (*detectorSetup, error) {
	registry := detector.DefaultRegistry()

	if len(detectorNames) == 1 && detectorNames[0] == "all" {
		detectorNames = registry.Names()
		fmt.Fprintf(os.Stderr, "Using all detectors: %s\n", strings.Join(detectorNames, ", "))
	}

	setup := &detectorSetup{
		detectors:       make([]detector.Detector, 0, len(detectorNames)),
		scriptDetectors: make([]interface{ Close() error }, 0),
		confidence:      confidence,
	}

	for _, name := range detectorNames {
		if strings.HasSuffix(name, ".js") {
			sd, err := detector.NewScriptDetector(name) // Simple single process
			if err != nil {
				setup.cleanup()
				return nil, fmt.Errorf("error loading script %s: %w", name, err)
			}
			setup.scriptDetectors = append(setup.scriptDetectors, sd)
			registry.Register(sd)
			setup.detectors = append(setup.detectors, sd)
			fmt.Fprintf(os.Stderr, "✓ Started detector server: %s\n", sd.Name())
		} else {
			d, err := registry.Get(name)
			if err != nil {
				setup.cleanup()
				return nil, err
			}
			setup.detectors = append(setup.detectors, d)
		}
	}

	return setup, nil
}

// detectOperation runs all detectors on an operation and returns labels + confidence
func detectOperation(ctx context.Context, detectors []detector.Detector, op plc.PLCOperation, minConfidence float64) ([]string, float64) {
	// Parse Operation ONCE before running detectors
	opData, err := op.GetOperationData()
	if err != nil {
		return nil, 0
	}
	op.ParsedOperation = opData // Set for detectors to use

	var matchedLabels []string
	var maxConfidence float64

	for _, det := range detectors {
		match, err := det.Detect(ctx, op) // ← op now has ParsedOperation set
		if err != nil || match == nil || match.Confidence < minConfidence {
			continue
		}

		// Extract labels
		var labels []string
		if labelList, ok := match.Metadata["labels"].([]string); ok {
			labels = labelList
		} else if labelList, ok := match.Metadata["labels"].([]interface{}); ok {
			for _, l := range labelList {
				if str, ok := l.(string); ok {
					labels = append(labels, str)
				}
			}
		}

		if len(labels) == 0 {
			labels = []string{det.Name()}
		}

		matchedLabels = append(matchedLabels, labels...)
		if match.Confidence > maxConfidence {
			maxConfidence = match.Confidence
		}
	}

	return matchedLabels, maxConfidence
}
