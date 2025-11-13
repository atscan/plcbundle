package commands

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/jmespath/go-jmespath"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
)

func NewQueryCommand() *cobra.Command {
	var (
		bundleRange string
		threads     int
		format      string
		limit       int
		noProgress  bool
	)

	cmd := &cobra.Command{
		Use:     "query <expression> [flags]",
		Aliases: []string{"q", "history"},
		Short:   "Query ops using JMESPath",
		Long: `Query operations using JMESPath expressions

Stream through operations in bundles and evaluate JMESPath expressions
on each operation. Supports parallel processing for better performance.

The JMESPath expression is evaluated against each operation's JSON structure.
Only operations where the expression returns a non-null value are output.

Output formats:
  jsonl - Output matching operations as JSONL (default)
  count - Only output count of matches`,

		Example: `  # Extract DID field from all operations
  plcbundle query 'did' --bundles 1-10

  # Extract PDS endpoints
  plcbundle query 'operation.services.atproto_pds.endpoint' --bundles 1-100

  # Wildcard service endpoints
  plcbundle query 'operation.services.*.endpoint' --bundles 1-100

  # Filter with conditions
  plcbundle query 'operation.alsoKnownAs[?contains(@, ` + "`bsky`" + `)]' --bundles 1-100

  # Complex queries
  plcbundle query 'operation | {did: did, handle: operation.handle}' --bundles 1-10

  # Count matches only
  plcbundle query 'operation.services.atproto_pds' --bundles 1-1000 --format count

  # Parallel processing with 8 workers
  plcbundle query 'did' --bundles 1-1000 --threads 8

  # Limit results
  plcbundle query 'did' --bundles 1-100 --limit 1000

  # Disable progress bar (for scripting)
  plcbundle query 'operation.handle' --bundles 1-100 --no-progress

  # Auto-detect CPU cores
  plcbundle query 'did' --bundles 1-1000 --threads 0`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			expression := args[0]

			// Auto-detect threads
			if threads <= 0 {
				threads = runtime.NumCPU()
				if threads < 1 {
					threads = 1
				}
			}

			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			// Determine bundle range
			var start, end int
			if bundleRange == "" {
				index := mgr.GetIndex()
				bundles := index.GetBundles()
				if len(bundles) == 0 {
					return fmt.Errorf("no bundles available")
				}
				start = bundles[0].BundleNumber
				end = bundles[len(bundles)-1].BundleNumber
			} else {
				start, end, err = parseBundleRange(bundleRange)
				if err != nil {
					return err
				}
			}

			return runQuery(cmd.Context(), mgr, queryOptions{
				expression: expression,
				start:      start,
				end:        end,
				threads:    threads,
				format:     format,
				limit:      limit,
				noProgress: noProgress,
			})
		},
	}

	cmd.Flags().StringVar(&bundleRange, "bundles", "", "Bundle selection: number (42) or range (1-50)")
	cmd.Flags().IntVar(&threads, "threads", 0, "Number of worker threads (0 = auto-detect CPU cores)")
	cmd.Flags().StringVar(&format, "format", "jsonl", "Output format: jsonl|count")
	cmd.Flags().IntVar(&limit, "limit", 0, "Limit number of results (0 = unlimited)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress output")

	return cmd
}

type queryOptions struct {
	expression string
	start      int
	end        int
	threads    int
	format     string
	limit      int
	noProgress bool
}

func runQuery(ctx context.Context, mgr BundleManager, opts queryOptions) error {
	// Compile JMESPath expression once
	compiled, err := jmespath.Compile(opts.expression)
	if err != nil {
		return fmt.Errorf("invalid JMESPath expression: %w", err)
	}

	totalBundles := opts.end - opts.start + 1

	// Adjust threads if more than bundles
	if opts.threads > totalBundles {
		opts.threads = totalBundles
	}

	fmt.Fprintf(os.Stderr, "Query: %s\n", opts.expression)
	fmt.Fprintf(os.Stderr, "Bundles: %d-%d (%d total)\n", opts.start, opts.end, totalBundles)
	fmt.Fprintf(os.Stderr, "Threads: %d\n", opts.threads)
	fmt.Fprintf(os.Stderr, "Format: %s\n", opts.format)
	if opts.limit > 0 {
		fmt.Fprintf(os.Stderr, "Limit: %d\n", opts.limit)
	}
	fmt.Fprintf(os.Stderr, "\n")

	// Shared counters
	var (
		totalOps       int64
		matchCount     int64
		bytesProcessed int64
	)

	// Progress tracking with bytes
	var progress *ui.ProgressBar
	if !opts.noProgress {
		// Use bundle-aware progress bar with byte tracking
		progress = NewBundleProgressBar(mgr, opts.start, opts.end)
	}

	// Setup channels
	jobs := make(chan int, opts.threads*2)
	results := make(chan queryResult, opts.threads*2)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < opts.threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bundleNum := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				res := processBundleQuery(ctx, mgr, bundleNum, compiled, opts.limit > 0, &matchCount, int64(opts.limit))
				results <- res
			}
		}()
	}

	// Result collector
	go func() {
		wg.Wait()
		close(results)
	}()

	// Send jobs
	go func() {
		defer close(jobs)
		for bundleNum := opts.start; bundleNum <= opts.end; bundleNum++ {
			select {
			case jobs <- bundleNum:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect and output results
	processed := 0
	for res := range results {
		processed++

		if res.err != nil {
			fmt.Fprintf(os.Stderr, "\nWarning: bundle %06d failed: %v\n", res.bundleNum, res.err)
		} else {
			atomic.AddInt64(&totalOps, int64(res.opsProcessed))
			atomic.AddInt64(&bytesProcessed, res.bytesProcessed)

			// Output matches (unless count-only mode)
			if opts.format != "count" {
				for _, match := range res.matches {
					// Check if limit reached
					if opts.limit > 0 && atomic.LoadInt64(&matchCount) >= int64(opts.limit) {
						break
					}
					fmt.Println(match)
				}
			}
		}

		if progress != nil {
			progress.SetWithBytes(processed, atomic.LoadInt64(&bytesProcessed))
		}

		// Early exit if limit reached
		if opts.limit > 0 && atomic.LoadInt64(&matchCount) >= int64(opts.limit) {
			break
		}
	}

	if progress != nil {
		progress.Finish()
	}

	// Output summary
	finalMatchCount := atomic.LoadInt64(&matchCount)
	finalTotalOps := atomic.LoadInt64(&totalOps)
	finalBytes := atomic.LoadInt64(&bytesProcessed)

	fmt.Fprintf(os.Stderr, "\n")
	if opts.format == "count" {
		fmt.Println(finalMatchCount)
	}

	fmt.Fprintf(os.Stderr, "✓ Query complete\n")
	fmt.Fprintf(os.Stderr, "  Total operations: %s\n", formatNumber(int(finalTotalOps)))
	fmt.Fprintf(os.Stderr, "  Matches: %s", formatNumber(int(finalMatchCount)))
	if finalTotalOps > 0 {
		fmt.Fprintf(os.Stderr, " (%.2f%%)", float64(finalMatchCount)/float64(finalTotalOps)*100)
	}
	fmt.Fprintf(os.Stderr, "\n")
	if finalBytes > 0 {
		fmt.Fprintf(os.Stderr, "  Data processed: %s\n", formatBytes(finalBytes))
	}

	return nil
}

type queryResult struct {
	bundleNum      int
	matches        []string
	opsProcessed   int
	bytesProcessed int64
	err            error
}

func processBundleQuery(
	ctx context.Context,
	mgr BundleManager,
	bundleNum int,
	compiled *jmespath.JMESPath,
	checkLimit bool,
	matchCount *int64,
	limit int64,
) queryResult {
	res := queryResult{bundleNum: bundleNum}

	bundle, err := mgr.LoadBundle(ctx, bundleNum)
	if err != nil {
		res.err = err
		return res
	}

	res.opsProcessed = len(bundle.Operations)
	matches := make([]string, 0)

	// Track bytes processed
	for _, op := range bundle.Operations {
		// Early exit if limit reached
		if checkLimit && atomic.LoadInt64(matchCount) >= limit {
			break
		}

		// Track bytes
		opSize := int64(len(op.RawJSON))
		if opSize == 0 {
			data, _ := json.Marshal(op)
			opSize = int64(len(data))
		}
		res.bytesProcessed += opSize

		// Convert operation to map for JMESPath
		var opData map[string]interface{}
		if len(op.RawJSON) > 0 {
			if err := json.Unmarshal(op.RawJSON, &opData); err != nil {
				continue
			}
		} else {
			data, _ := json.Marshal(op)
			json.Unmarshal(data, &opData)
		}

		// Evaluate JMESPath expression
		result, err := compiled.Search(opData)
		if err != nil {
			continue
		}

		// Skip null results
		if result == nil {
			continue
		}

		// Increment match counter
		atomic.AddInt64(matchCount, 1)

		// Convert result to JSON string
		resultJSON, err := json.Marshal(result)
		if err != nil {
			continue
		}

		matches = append(matches, string(resultJSON))
	}

	res.matches = matches
	return res
}
