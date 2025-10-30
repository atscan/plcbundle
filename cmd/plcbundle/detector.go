// cmd/plcbundle/detector.go
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

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
	case "filter": // ← Add this
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
		fmt.Fprintf(os.Stderr, "\nFilters OUT operations that match detectors (outputs clean data)\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  plcbundle backfill | plcbundle detector filter all > clean.jsonl\n")
		fmt.Fprintf(os.Stderr, "  plcbundle export --bundle 1 | plcbundle detector filter invalid_handle > clean.jsonl\n")
		fmt.Fprintf(os.Stderr, "  plcbundle backfill | plcbundle detector filter ./my_detector.js > clean.jsonl\n")
		os.Exit(1)
	}

	// Manually separate detector names from flags
	var detectorNames []string
	var flagArgs []string

	for i := 3; i < len(os.Args); i++ {
		arg := os.Args[i]
		if strings.HasPrefix(arg, "-") {
			flagArgs = os.Args[i:]
			break
		}
		detectorNames = append(detectorNames, arg)
	}

	if len(detectorNames) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one detector name required\n")
		os.Exit(1)
	}

	// Parse flags
	fs := flag.NewFlagSet("detector filter", flag.ExitOnError)
	confidence := fs.Float64("confidence", 0.90, "minimum confidence")
	fs.Parse(flagArgs)

	// Setup registry
	registry := detector.DefaultRegistry()

	// Track script detectors for cleanup
	var scriptDetectors []*detector.ScriptDetector
	defer func() {
		for _, sd := range scriptDetectors {
			sd.Close()
		}
	}()

	// Handle "all" keyword
	if len(detectorNames) == 1 && detectorNames[0] == "all" {
		detectorNames = registry.Names()
		fmt.Fprintf(os.Stderr, "Using all detectors: %s\n", strings.Join(detectorNames, ", "))
	}

	// Get all detectors
	detectors := make([]detector.Detector, 0, len(detectorNames))
	for _, name := range detectorNames {
		// Check if it's a .js file (script detector)
		if strings.HasSuffix(name, ".js") {
			scriptDetector, err := detector.NewScriptDetector(name)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading script %s: %v\n", name, err)
				os.Exit(1)
			}
			scriptDetectors = append(scriptDetectors, scriptDetector)
			registry.Register(scriptDetector)
			detectors = append(detectors, scriptDetector)
			fmt.Fprintf(os.Stderr, "✓ Started detector server: %s\n", scriptDetector.Name())
		} else {
			d, err := registry.Get(name)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			detectors = append(detectors, d)
		}
	}

	// Log to stderr
	fmt.Fprintf(os.Stderr, "Filtering OUT spam with %d detector(s)\n", len(detectors))
	if len(detectorNames) <= 5 {
		fmt.Fprintf(os.Stderr, "Detectors: %s\n", strings.Join(detectorNames, ", "))
	}
	fmt.Fprintf(os.Stderr, "Min confidence: %.2f\n\n", *confidence)

	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)

	// Set large buffer for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	cleanCount := 0
	filteredCount := 0
	totalCount := 0
	totalBytes := int64(0)
	filteredBytes := int64(0)

	// Read JSONL from stdin
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		totalCount++
		opSize := int64(len(line))
		totalBytes += opSize

		// Parse operation
		var op plc.PLCOperation
		if err := json.Unmarshal(line, &op); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to parse line %d: %v\n", totalCount, err)
			continue
		}

		// Run all detectors on this operation
		isSpam := false

		for _, det := range detectors {
			match, err := det.Detect(ctx, op)
			if err != nil {
				continue
			}

			if match != nil && match.Confidence >= *confidence {
				// Detected as spam - filter it out
				isSpam = true
				break
			}
		}

		// Output only if NOT spam (clean operation)
		if !isSpam {
			cleanCount++
			fmt.Println(string(line))
		} else {
			filteredCount++
			filteredBytes += opSize
		}

		// Progress to stderr
		if totalCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed: %d | Clean: %d | Filtered: %d | Saved: %s\r",
				totalCount, cleanCount, filteredCount, formatBytes(filteredBytes))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "\nError reading stdin: %v\n", err)
		os.Exit(1)
	}

	// Final stats to stderr
	fmt.Fprintf(os.Stderr, "\n\n")
	fmt.Fprintf(os.Stderr, "✓ Filter complete\n")
	fmt.Fprintf(os.Stderr, "  Total operations: %d\n", totalCount)
	fmt.Fprintf(os.Stderr, "  Clean: %d (%.2f%%)\n", cleanCount, float64(cleanCount)/float64(totalCount)*100)
	fmt.Fprintf(os.Stderr, "  Filtered out: %d (%.2f%%)\n", filteredCount, float64(filteredCount)/float64(totalCount)*100)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  Total size: %s\n", formatBytes(totalBytes))
	fmt.Fprintf(os.Stderr, "  Filtered size: %s (%.2f%%)\n", formatBytes(filteredBytes), float64(filteredBytes)/float64(totalBytes)*100)
	fmt.Fprintf(os.Stderr, "  Clean size: %s (%.2f%%)\n", formatBytes(totalBytes-filteredBytes), float64(totalBytes-filteredBytes)/float64(totalBytes)*100)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  Detectors used: %d\n", len(detectors))
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
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  plcbundle detector run invalid_handle --bundles 1-100\n")
		fmt.Fprintf(os.Stderr, "  plcbundle detector run invalid_handle aka_spam --bundles 1-100\n")
		fmt.Fprintf(os.Stderr, "  plcbundle detector run ./my_detector.js --bundles 1-100\n")
		fmt.Fprintf(os.Stderr, "  plcbundle detector run all  # runs on all bundles\n")
		os.Exit(1)
	}

	// Manually separate detector names from flags
	var detectorNames []string
	var flagArgs []string

	for i := 3; i < len(os.Args); i++ {
		arg := os.Args[i]
		if strings.HasPrefix(arg, "-") {
			flagArgs = os.Args[i:]
			break
		}
		detectorNames = append(detectorNames, arg)
	}

	if len(detectorNames) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one detector name required\n")
		os.Exit(1)
	}

	// Parse flags
	fs := flag.NewFlagSet("detector run", flag.ExitOnError)
	bundleRange := fs.String("bundles", "", "bundle range (e.g., '1-100'), default: all bundles")
	confidence := fs.Float64("confidence", 0.90, "minimum confidence")
	fs.Parse(flagArgs)

	// Load manager (needed to determine bundle range)
	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	// Determine bundle range
	var start, end int
	if *bundleRange == "" {
		// Default to all bundles
		index := mgr.GetIndex()
		bundles := index.GetBundles()
		if len(bundles) == 0 {
			fmt.Fprintf(os.Stderr, "Error: no bundles available\n")
			os.Exit(1)
		}
		start = bundles[0].BundleNumber
		end = bundles[len(bundles)-1].BundleNumber
		fmt.Fprintf(os.Stderr, "No --bundles specified, using all available bundles: %d-%d\n", start, end)
	} else {
		// Parse provided range
		start, end, err = parseBundleRange(*bundleRange)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	// Setup registry
	registry := detector.DefaultRegistry()
	config := detector.DefaultConfig()
	config.MinConfidence = *confidence

	// Track script detectors for cleanup
	var scriptDetectors []*detector.ScriptDetector
	defer func() {
		for _, sd := range scriptDetectors {
			sd.Close()
		}
	}()

	// Handle "all" keyword - expand to all available detectors
	if len(detectorNames) == 1 && detectorNames[0] == "all" {
		detectorNames = registry.Names()
		fmt.Fprintf(os.Stderr, "Using all available detectors: %s\n", strings.Join(detectorNames, ", "))
	}

	// Load detectors (built-in or scripts)
	detectors := make([]detector.Detector, 0, len(detectorNames))
	for _, name := range detectorNames {
		// Check if it's a .js file (script detector)
		if strings.HasSuffix(name, ".js") {
			scriptDetector, err := detector.NewScriptDetector(name)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading script %s: %v\n", name, err)
				os.Exit(1)
			}
			// Track for cleanup
			scriptDetectors = append(scriptDetectors, scriptDetector)
			// Register it so it can be used
			registry.Register(scriptDetector)
			detectors = append(detectors, scriptDetector)
			fmt.Fprintf(os.Stderr, "✓ Started detector server: %s\n", scriptDetector.Name())
		} else {
			// Try to get built-in detector
			d, err := registry.Get(name)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			detectors = append(detectors, d)
		}
	}

	// Log to stderr
	fmt.Fprintf(os.Stderr, "Running %d detector(s) on bundles %d-%d...\n", len(detectors), start, end)
	if len(detectorNames) <= 5 {
		fmt.Fprintf(os.Stderr, "Detectors: %s\n", strings.Join(detectorNames, ", "))
	}
	fmt.Fprintf(os.Stderr, "Min confidence: %.2f\n\n", *confidence)

	ctx := context.Background()

	// Write CSV header to stdout
	fmt.Println("bundle,position,cid,size,confidence,labels")

	// Track statistics
	totalOps := 0
	matchCount := 0
	totalBytes := int64(0)
	matchedBytes := int64(0)
	bundlesProcessed := 0
	detectorMatchCounts := make(map[string]int)

	totalBundles := end - start + 1

	// Create progress bar with byte tracking enabled
	fmt.Fprintf(os.Stderr, "Processing bundles:\n")
	progress := NewProgressBar(totalBundles)
	progress.showBytes = true

	// Process bundles and stream results
	for bundleNum := start; bundleNum <= end; bundleNum++ {
		bundle, err := mgr.LoadBundle(ctx, bundleNum)
		if err != nil {
			progress.Finish()
			fmt.Fprintf(os.Stderr, "\n⚠️  Warning: failed to load bundle %d: %v\n", bundleNum, err)
			progress = NewProgressBar(totalBundles)
			progress.showBytes = true
			progress.SetWithBytes(bundleNum-start, totalBytes)
			continue
		}

		bundlesProcessed++
		totalOps += len(bundle.Operations)

		// Process each operation with all detectors
		for position, op := range bundle.Operations {
			// Calculate operation size
			var opSize int
			if len(op.RawJSON) > 0 {
				opSize = len(op.RawJSON)
			} else {
				data, _ := json.Marshal(op)
				opSize = len(data)
			}
			totalBytes += int64(opSize)

			// Collect all matches for this operation
			var matchedLabels []string
			var maxConfidence float64

			// Run all detectors on this operation
			for _, det := range detectors {
				match, err := det.Detect(ctx, op)
				if err != nil {
					continue
				}

				// Skip if no match or confidence too low
				if match == nil || match.Confidence < *confidence {
					continue
				}

				// Extract labels from match metadata
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

				// If no labels in metadata, use detector name
				if len(labels) == 0 {
					labels = []string{det.Name()}
				}

				// Collect all labels
				matchedLabels = append(matchedLabels, labels...)
				detectorMatchCounts[det.Name()]++

				// Track highest confidence
				if match.Confidence > maxConfidence {
					maxConfidence = match.Confidence
				}
			}

			// Output only if at least one detector matched
			if len(matchedLabels) > 0 {
				matchCount++
				matchedBytes += int64(opSize)

				// Extract last 4 chars of CID
				cidShort := op.CID
				if len(cidShort) > 4 {
					cidShort = cidShort[len(cidShort)-4:]
				}

				fmt.Printf("%d,%d,%s,%d,%.2f,%s\n",
					bundleNum,
					position,
					cidShort,
					opSize,
					maxConfidence,
					strings.Join(matchedLabels, ";"),
				)
			}
		}

		// Update progress with bytes
		progress.SetWithBytes(bundleNum-start+1, totalBytes)
	}

	// Finish progress bar
	progress.Finish()

	// Final stats to stderr
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "✓ Detection complete\n")
	fmt.Fprintf(os.Stderr, "  Bundles processed:  %d\n", bundlesProcessed)
	fmt.Fprintf(os.Stderr, "  Total operations:   %d\n", totalOps)
	fmt.Fprintf(os.Stderr, "  Matches found:      %d (%.2f%%)\n", matchCount, float64(matchCount)/float64(totalOps)*100)
	fmt.Fprintf(os.Stderr, "  Clean operations:   %d (%.2f%%)\n", totalOps-matchCount, float64(totalOps-matchCount)/float64(totalOps)*100)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  Total size:         %s\n", formatBytes(totalBytes))
	fmt.Fprintf(os.Stderr, "  Matched size:       %s (%.2f%%)\n", formatBytes(matchedBytes), float64(matchedBytes)/float64(totalBytes)*100)
	fmt.Fprintf(os.Stderr, "  Clean size:         %s (%.2f%%)\n", formatBytes(totalBytes-matchedBytes), float64(totalBytes-matchedBytes)/float64(totalBytes)*100)

	if matchedBytes > 0 {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  💾 Potential savings if filtered: %s (%.2f%% reduction)\n",
			formatBytes(matchedBytes),
			float64(matchedBytes)/float64(totalBytes)*100)
	}

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  Detectors used:     %d\n", len(detectors))

	// Show breakdown by detector if multiple used
	if len(detectors) > 1 {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  Matches by detector:\n")
		for _, det := range detectors {
			name := det.Name()
			count := detectorMatchCounts[name]
			if count > 0 {
				pct := float64(count) / float64(matchCount) * 100
				fmt.Fprintf(os.Stderr, "    %-20s %d (%.1f%%)\n", name, count, pct)
			} else {
				fmt.Fprintf(os.Stderr, "    %-20s 0\n", name)
			}
		}
	}
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
