package commands

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
	"tangled.org/atscan.net/plcbundle/detector"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

func NewDetectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "detector",
		Aliases: []string{"detect"},
		Short:   "Detect and filter spam/invalid operations",
		Long: `Detect and filter spam/invalid operations

Run detection algorithms on PLC operations to identify spam, 
invalid handles, service abuse, and other problematic patterns.

Built-in detectors:
  • invalid_handle    - Invalid handle patterns (underscores, etc)
  • aka_spam          - Excessive/garbage alsoKnownAs entries
  • spam_pds          - Known spam PDS endpoints
  • service_abuse     - Abused service structures
  • noop              - Benchmark detector (returns no matches)

Custom detectors:
  Load JavaScript detectors from .js files with a detect() function.`,

		Example: `  # List available detectors
  plcbundle detector list

  # Run detector on bundles
  plcbundle detector run invalid_handle --bundles 1-100

  # Run with parallel processing
  plcbundle detector run invalid_handle --bundles 1-100 --workers 8

  # Run custom detector script
  plcbundle detector run ./my_detector.js --bundles 1-100

  # Run multiple detectors
  plcbundle detector run invalid_handle aka_spam --bundles 1-100

  # Run all detectors
  plcbundle detector run all --bundles 1-100

  # Filter JSONL from stdin
  cat ops.jsonl | plcbundle detector filter invalid_handle > clean.jsonl

  # Get detector info
  plcbundle detector info invalid_handle`,
	}

	// Add subcommands
	cmd.AddCommand(newDetectorListCommand())
	cmd.AddCommand(newDetectorTestCommand())
	cmd.AddCommand(newDetectorRunCommand())
	cmd.AddCommand(newDetectorFilterCommand())
	cmd.AddCommand(newDetectorInfoCommand())

	return cmd
}

// ============================================================================
// DETECTOR LIST
// ============================================================================

func newDetectorListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List available detectors",
		Long:  `List all available built-in and loaded detectors`,

		Example: `  # List all detectors
  plcbundle detector list`,

		RunE: func(cmd *cobra.Command, args []string) error {
			registry := detector.DefaultRegistry()
			detectors := registry.List()

			sort.Slice(detectors, func(i, j int) bool {
				return detectors[i].Name() < detectors[j].Name()
			})

			fmt.Printf("Available detectors:\n\n")
			for _, d := range detectors {
				fmt.Printf("  %-20s %s (v%s)\n", d.Name(), d.Description(), d.Version())
			}
			fmt.Printf("\nUse 'plcbundle detector info <name>' for details\n")

			return nil
		},
	}
}

// ============================================================================
// DETECTOR TEST
// ============================================================================

func newDetectorTestCommand() *cobra.Command {
	var (
		bundleNum  int
		confidence float64
		verbose    bool
	)

	cmd := &cobra.Command{
		Use:   "test <detector-name>",
		Short: "Test detector on specific bundle",
		Long:  `Test a detector on a specific bundle and show results`,

		Example: `  # Test on bundle 42
  plcbundle detector test invalid_handle --bundle 42

  # Verbose output with samples
  plcbundle detector test aka_spam --bundle 100 -v

  # Custom confidence threshold
  plcbundle detector test spam_pds --bundle 50 --confidence 0.85`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			detectorName := args[0]

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			ctx := context.Background()
			bundle, err := mgr.LoadBundle(ctx, bundleNum)
			if err != nil {
				return fmt.Errorf("failed to load bundle: %w", err)
			}

			fmt.Printf("Testing detector '%s' on bundle %06d...\n", detectorName, bundleNum)
			fmt.Printf("Min confidence: %.2f\n\n", confidence)

			registry := detector.DefaultRegistry()
			config := detector.DefaultConfig()
			config.MinConfidence = confidence

			runner := detector.NewRunner(registry, config, &commandLogger{})
			results, err := runner.RunOnBundle(ctx, detectorName, bundle)
			if err != nil {
				return fmt.Errorf("detection failed: %w", err)
			}

			stats := detector.CalculateStats(results, len(bundle.Operations))

			fmt.Printf("Results:\n")
			fmt.Printf("  Total operations:    %d\n", stats.TotalOperations)
			fmt.Printf("  Matches found:       %d (%.2f%%)\n\n", stats.MatchedCount, stats.MatchRate*100)

			if len(stats.ByReason) > 0 {
				fmt.Printf("Breakdown by reason:\n")
				for reason, count := range stats.ByReason {
					pct := float64(count) / float64(stats.MatchedCount) * 100
					fmt.Printf("  %-25s %d (%.1f%%)\n", reason, count, pct)
				}
				fmt.Printf("\n")
			}

			if verbose && len(results) > 0 {
				fmt.Printf("Sample matches (first 10):\n")
				displayCount := min(10, len(results))

				for i := 0; i < displayCount; i++ {
					res := results[i]
					globalPos := (res.BundleNumber * 10000) + res.Position
					fmt.Printf("  %d. Position %d: %s\n", i+1, globalPos, res.DID)
					fmt.Printf("     Reason: %s (confidence: %.2f)\n", res.Match.Reason, res.Match.Confidence)
					if res.Match.Note != "" {
						fmt.Printf("     Note: %s\n", res.Match.Note)
					}
				}
			}

			return nil
		},
	}

	cmd.Flags().IntVar(&bundleNum, "bundle", 0, "Bundle number to test (required)")
	cmd.Flags().Float64Var(&confidence, "confidence", 0.90, "Minimum confidence threshold")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Show sample matches")

	cmd.MarkFlagRequired("bundle")

	return cmd
}

// ============================================================================
// DETECTOR RUN (with parallel processing)
// ============================================================================

func newDetectorRunCommand() *cobra.Command {
	var (
		bundleRange string
		confidence  float64
		pprofPort   string
		workers     int
		noProgress  bool
	)

	cmd := &cobra.Command{
		Use:   "run <detector1|script.js> [detector2...] [flags]",
		Short: "Run detector(s) and output CSV results",
		Long: `Run one or more detectors on bundles and output results as CSV

Output format uses global position: (bundleNumber × 10,000) + position
Example: 88410345 = bundle 8841, position 345

Supports parallel processing across multiple workers for better performance.

Detectors can be:
  • Built-in detector names (invalid_handle, aka_spam, etc)
  • Path to JavaScript detector file (./my_detector.js)
  • Special keyword 'all' to run all built-in detectors`,

		Example: `  # Run single detector
  plcbundle detector run invalid_handle --bundles 1-100

  # Run with 8 parallel workers (faster)
  plcbundle detector run invalid_handle --bundles 1-1000 --workers 8

  # Run multiple detectors in parallel
  plcbundle detector run invalid_handle aka_spam --bundles 1-100 -w 4

  # Run custom script
  plcbundle detector run ./my_detector.js --bundles 1-100

  # Run all built-in detectors
  plcbundle detector run all --bundles 1-100 --workers 8

  # Save results to file
  plcbundle detector run all --bundles 1-100 -w 8 > results.csv

  # Disable progress bar (for scripting)
  plcbundle detector run spam --bundles 1-100 --no-progress

  # Enable profiling
  plcbundle detector run all --bundles 1-100 --pprof :6060`,

		Args: cobra.MinimumNArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			detectorNames := args

			// Start pprof if requested
			if pprofPort != "" {
				go func() {
					fmt.Fprintf(os.Stderr, "pprof server starting on http://localhost%s/debug/pprof/\n", pprofPort)
					http.ListenAndServe(pprofPort, nil)
				}()
			}

			// Auto-detect workers if not specified
			if workers <= 0 {
				workers = runtime.NumCPU()
				if workers < 1 {
					workers = 1
				}
			}

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			// Determine range
			var start, end int
			if bundleRange == "" {
				index := mgr.GetIndex()
				bundles := index.GetBundles()
				if len(bundles) == 0 {
					return fmt.Errorf("no bundles available")
				}
				start = bundles[0].BundleNumber
				end = bundles[len(bundles)-1].BundleNumber
				fmt.Fprintf(os.Stderr, "Using all bundles: %d-%d\n", start, end)
			} else {
				start, end, err = parseBundleRange(bundleRange)
				if err != nil {
					return err
				}
			}

			// Load detectors
			setup, err := parseAndLoadDetectors(detectorNames, confidence)
			if err != nil {
				return err
			}
			defer setup.cleanup()

			fmt.Fprintf(os.Stderr, "Running %d detector(s) on bundles %d-%d (%d workers)\n",
				len(setup.detectors), start, end, workers)
			fmt.Fprintf(os.Stderr, "Min confidence: %.2f\n\n", confidence)

			return runDetectionParallel(cmd.Context(), mgr, setup, start, end, workers, !noProgress)
		},
	}

	cmd.Flags().StringVar(&bundleRange, "bundles", "", "Bundle range (e.g., '1-100', default: all)")
	cmd.Flags().Float64Var(&confidence, "confidence", 0.90, "Minimum confidence threshold")
	cmd.Flags().IntVarP(&workers, "workers", "w", 0, "Number of parallel workers (0 = auto-detect CPU count)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress bar")
	cmd.Flags().StringVar(&pprofPort, "pprof", "", "Enable pprof on port (e.g., ':6060')")

	return cmd
}

// ============================================================================
// DETECTOR FILTER
// ============================================================================

func newDetectorFilterCommand() *cobra.Command {
	var confidence float64

	cmd := &cobra.Command{
		Use:   "filter <detector1|script.js> [detector2...]",
		Short: "Filter JSONL operations from stdin",
		Long: `Filter JSONL operations from stdin using detectors

Reads operations from stdin, runs detectors, and outputs only
operations that DO NOT match (clean operations).

Perfect for cleaning datasets or pre-processing.`,

		Example: `  # Filter with built-in detector
  cat ops.jsonl | plcbundle detector filter invalid_handle > clean.jsonl

  # Filter with custom script
  plcbundle export --all | plcbundle detector filter ./spam.js > clean.jsonl

  # Chain multiple detectors
  cat ops.jsonl | plcbundle detector filter invalid_handle aka_spam > clean.jsonl

  # Custom confidence
  cat ops.jsonl | plcbundle detector filter spam_pds --confidence 0.95 > clean.jsonl`,

		Args: cobra.MinimumNArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			detectorNames := args

			setup, err := parseAndLoadDetectors(detectorNames, confidence)
			if err != nil {
				return err
			}
			defer setup.cleanup()

			fmt.Fprintf(os.Stderr, "Filtering with %d detector(s), min confidence: %.2f\n\n", len(setup.detectors), confidence)

			return filterFromStdin(cmd.Context(), setup)
		},
	}

	cmd.Flags().Float64Var(&confidence, "confidence", 0.90, "Minimum confidence threshold")

	return cmd
}

// ============================================================================
// DETECTOR INFO
// ============================================================================

func newDetectorInfoCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "info <detector-name>",
		Short: "Show detailed detector information",
		Long:  `Show detailed information about a specific detector`,

		Example: `  # Show detector info
  plcbundle detector info invalid_handle
  plcbundle detector info aka_spam`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			detectorName := args[0]

			registry := detector.DefaultRegistry()
			d, err := registry.Get(detectorName)
			if err != nil {
				return err
			}

			fmt.Printf("Detector: %s\n", d.Name())
			fmt.Printf("Version: %s\n", d.Version())
			fmt.Printf("Description: %s\n\n", d.Description())

			fmt.Printf("Usage examples:\n")
			fmt.Printf("  # Test on single bundle\n")
			fmt.Printf("  plcbundle detector test %s --bundle 42\n\n", d.Name())
			fmt.Printf("  # Run on range and save\n")
			fmt.Printf("  plcbundle detector run %s --bundles 1-100 > results.csv\n\n", d.Name())
			fmt.Printf("  # Filter JSONL stream\n")
			fmt.Printf("  cat ops.jsonl | plcbundle detector filter %s > clean.jsonl\n\n", d.Name())

			return nil
		},
	}
}

// ============================================================================
// PARALLEL DETECTION IMPLEMENTATION
// ============================================================================

type detectionMatch struct {
	globalPos  int
	cid        string
	size       int
	confidence float64
	labels     []string
}

// detectionResult holds results from processing a bundle
type detectionResult struct {
	bundleNum    int
	matches      []detectionMatch
	totalOps     int
	totalBytes   int64
	matchedBytes int64
	err          error
}

func runDetectionParallel(ctx context.Context, mgr BundleManager, setup *detectorSetup, start, end int, workers int, showProgress bool) error {
	totalBundles := end - start + 1

	// ✨ FIX: Don't create more workers than bundles
	if workers > totalBundles {
		workers = totalBundles
	}

	// ✨ FIX: Use unbuffered channels to avoid blocking issues
	jobs := make(chan int, workers*2) // Small buffer for job numbers only
	results := make(chan detectionResult, workers*2)

	// Shared counters
	var (
		totalOps     int64
		matchCount   int64
		totalBytes   int64
		matchedBytes int64
	)

	// Progress tracking
	var progress *ui.ProgressBar
	if showProgress {
		progress = ui.NewProgressBar(totalBundles)
	}

	// CSV header
	fmt.Println("position,cid,size,confidence,labels")

	// Start workers BEFORE sending jobs
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for bundleNum := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Process bundle
				res := processBundleForDetection(ctx, mgr, setup, bundleNum)

				// Send result (may block if collector is slow, but that's OK)
				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
			}
		}(w)
	}

	// Send jobs in separate goroutine
	go func() {
		defer close(jobs)
		for bundleNum := start; bundleNum <= end; bundleNum++ {
			select {
			case jobs <- bundleNum:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect results in separate goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
		close(results)
	}()

	// Process results in main goroutine
	processed := 0
	allMatches := make([]detectionMatch, 0, 1000)

	for res := range results {
		processed++

		if res.err != nil {
			if res.err != context.Canceled {
				fmt.Fprintf(os.Stderr, "\nWarning: bundle %06d failed: %v\n", res.bundleNum, res.err)
			}
		} else {
			// Update counters
			atomic.AddInt64(&totalOps, int64(res.totalOps))
			atomic.AddInt64(&matchCount, int64(len(res.matches)))
			atomic.AddInt64(&totalBytes, res.totalBytes)
			atomic.AddInt64(&matchedBytes, res.matchedBytes)

			// Collect matches
			allMatches = append(allMatches, res.matches...)

			// ✨ FIX: Output immediately (don't buffer too much)
			if len(allMatches) >= 500 {
				for _, match := range allMatches {
					fmt.Printf("%d,%s,%d,%.2f,%s\n",
						match.globalPos, match.cid, match.size,
						match.confidence, strings.Join(match.labels, ";"))
				}
				allMatches = allMatches[:0]
			}
		}

		if progress != nil {
			progress.Set(processed)
		}
	}

	// Flush remaining matches
	for _, match := range allMatches {
		fmt.Printf("%d,%s,%d,%.2f,%s\n",
			match.globalPos, match.cid, match.size,
			match.confidence, strings.Join(match.labels, ";"))
	}

	if progress != nil {
		progress.Finish()
	}

	// Wait for cleanup
	<-done

	// Summary
	finalTotalOps := atomic.LoadInt64(&totalOps)
	finalMatchCount := atomic.LoadInt64(&matchCount)
	finalTotalBytes := atomic.LoadInt64(&totalBytes)
	finalMatchedBytes := atomic.LoadInt64(&matchedBytes)

	if finalTotalOps == 0 {
		fmt.Fprintf(os.Stderr, "\n⚠️  No operations processed\n")
		return nil
	}

	fmt.Fprintf(os.Stderr, "\n✓ Detection complete\n")
	fmt.Fprintf(os.Stderr, "  Total operations:   %d\n", finalTotalOps)
	fmt.Fprintf(os.Stderr, "  Matches found:      %d (%.2f%%)\n",
		finalMatchCount, float64(finalMatchCount)/float64(finalTotalOps)*100)
	fmt.Fprintf(os.Stderr, "  Total size:         %s\n", formatBytes(finalTotalBytes))
	fmt.Fprintf(os.Stderr, "  Matched size:       %s (%.2f%%)\n",
		formatBytes(finalMatchedBytes),
		float64(finalMatchedBytes)/float64(finalTotalBytes)*100)

	return nil
}

func processBundleForDetection(ctx context.Context, mgr BundleManager, setup *detectorSetup, bundleNum int) detectionResult {
	res := detectionResult{bundleNum: bundleNum}

	bundle, err := mgr.LoadBundle(ctx, bundleNum)
	if err != nil {
		res.err = err
		return res
	}

	res.totalOps = len(bundle.Operations)
	matches := make([]detectionMatch, 0)

	for position, op := range bundle.Operations {
		// Calculate global position
		globalPos := (bundleNum * 10000) + position

		opSize := len(op.RawJSON)
		if opSize == 0 {
			data, _ := json.Marshal(op)
			opSize = len(data)
		}
		res.totalBytes += int64(opSize)

		labels, conf := detectOperation(ctx, setup.detectors, op, setup.confidence)

		if len(labels) > 0 {
			res.matchedBytes += int64(opSize)

			cidShort := op.CID
			if len(cidShort) > 4 {
				cidShort = cidShort[len(cidShort)-4:]
			}

			matches = append(matches, detectionMatch{
				globalPos:  globalPos,
				cid:        cidShort,
				size:       opSize,
				confidence: conf,
				labels:     labels,
			})
		}
	}

	res.matches = matches
	return res
}

// ============================================================================
// FILTER FROM STDIN
// ============================================================================

func filterFromStdin(ctx context.Context, setup *detectorSetup) error {
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

		var op plcclient.PLCOperation
		if err := json.Unmarshal(line, &op); err != nil {
			continue
		}

		labels, _ := detectOperation(ctx, setup.detectors, op, setup.confidence)

		if len(labels) == 0 {
			// Clean - output to stdout
			cleanCount++
			fmt.Println(string(line))
		} else {
			// Matched - filter out
			filteredCount++
			filteredBytes += int64(len(line))
		}

		if totalCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Processed: %d | Clean: %d | Filtered: %d\r", totalCount, cleanCount, filteredCount)
		}
	}

	fmt.Fprintf(os.Stderr, "\n\n✓ Filter complete\n")
	fmt.Fprintf(os.Stderr, "  Total: %d | Clean: %d (%.2f%%) | Filtered: %d (%.2f%%)\n",
		totalCount, cleanCount, float64(cleanCount)/float64(totalCount)*100,
		filteredCount, float64(filteredCount)/float64(totalCount)*100)
	fmt.Fprintf(os.Stderr, "  Size saved: %s (%.2f%%)\n", formatBytes(filteredBytes), float64(filteredBytes)/float64(totalBytes)*100)

	return nil
}

// ============================================================================
// SHARED HELPERS
// ============================================================================

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

func parseAndLoadDetectors(detectorNames []string, confidence float64) (*detectorSetup, error) {
	registry := detector.DefaultRegistry()

	// Handle "all" keyword
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
		// JavaScript detector
		if strings.HasSuffix(name, ".js") {
			sd, err := detector.NewScriptDetector(name)
			if err != nil {
				setup.cleanup()
				return nil, fmt.Errorf("failed to load script %s: %w", name, err)
			}
			setup.scriptDetectors = append(setup.scriptDetectors, sd)
			registry.Register(sd)
			setup.detectors = append(setup.detectors, sd)
			fmt.Fprintf(os.Stderr, "✓ Loaded detector: %s\n", sd.Name())
		} else {
			// Built-in detector
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

func detectOperation(ctx context.Context, detectors []detector.Detector, op plcclient.PLCOperation, minConfidence float64) ([]string, float64) {
	opData, err := op.GetOperationData()
	if err != nil {
		return nil, 0
	}
	op.ParsedOperation = opData

	var matchedLabels []string
	var maxConfidence float64

	for _, det := range detectors {
		match, err := det.Detect(ctx, op)
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
