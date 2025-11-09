package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/ui"
)

func NewVerifyCommand() *cobra.Command {
	var (
		bundleNum int
		rangeStr  string
		chain     bool
		parallel  bool
		workers   int
	)

	cmd := &cobra.Command{
		Use:   "verify [flags]",
		Short: "Verify bundle integrity and chain",
		Long: `Verify bundle integrity and cryptographic chain

Verifies bundle file hashes, content integrity, and chain linkage.
By default, verifies the entire chain. You can verify specific bundles
or ranges using flags.

Verification checks:
  • Compressed file hash matches index
  • Content hash matches index
  • File sizes match metadata
  • Chain parent references are correct
  • Chain hash calculations are valid`,

		Example: `  # Verify entire chain
  plcbundle verify
  plcbundle verify --chain

  # Verify specific bundle
  plcbundle verify --bundle 42

  # Verify range of bundles
  plcbundle verify --range 1-100

  # Verbose output
  plcbundle verify --chain -v

  # Parallel verification (faster for ranges)
  plcbundle verify --range 1-1000 --parallel --workers 8`,

		RunE: func(cmd *cobra.Command, args []string) error {
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			if !verbose {
				fmt.Fprintf(os.Stderr, "Working in: %s\n\n", dir)
			}

			ctx := context.Background()

			// Determine what to verify
			if rangeStr != "" {
				return verifyRange(ctx, mgr, rangeStr, verbose, parallel, workers)
			}

			if bundleNum > 0 {
				return verifySingleBundle(ctx, mgr, bundleNum, verbose)
			}

			// Default: verify entire chain
			return verifyChain(ctx, mgr, verbose)
		},
	}

	cmd.Flags().IntVarP(&bundleNum, "bundle", "b", 0, "Verify specific bundle number")
	cmd.Flags().StringVarP(&rangeStr, "range", "r", "", "Verify bundle range (e.g., '1-100')")
	cmd.Flags().BoolVarP(&chain, "chain", "c", false, "Verify entire chain (default)")
	cmd.Flags().BoolVar(&parallel, "parallel", false, "Use parallel verification for ranges")
	cmd.Flags().IntVar(&workers, "workers", 4, "Number of parallel workers")

	return cmd
}

func verifySingleBundle(ctx context.Context, mgr *bundle.Manager, bundleNum int, verbose bool) error {
	fmt.Fprintf(os.Stderr, "Verifying bundle %06d...\n", bundleNum)

	start := time.Now()
	result, err := mgr.VerifyBundle(ctx, bundleNum)
	elapsed := time.Since(start)

	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	if result.Valid {
		fmt.Fprintf(os.Stderr, "✓ Bundle %06d is valid (%s)\n", bundleNum, elapsed.Round(time.Millisecond))
		if verbose {
			fmt.Fprintf(os.Stderr, "\nDetails:\n")
			fmt.Fprintf(os.Stderr, "  File exists: %v\n", result.FileExists)
			fmt.Fprintf(os.Stderr, "  Hash match: %v\n", result.HashMatch)
			fmt.Fprintf(os.Stderr, "  Hash: %s...%s\n", result.LocalHash[:16], result.LocalHash[len(result.LocalHash)-16:])
			fmt.Fprintf(os.Stderr, "  Verification time: %s\n", elapsed)
		}
		return nil
	}

	fmt.Fprintf(os.Stderr, "✗ Bundle %06d is invalid (%s)\n", bundleNum, elapsed.Round(time.Millisecond))
	if result.Error != nil {
		fmt.Fprintf(os.Stderr, "  Error: %v\n", result.Error)
	}
	if !result.FileExists {
		fmt.Fprintf(os.Stderr, "  File not found\n")
	}
	if !result.HashMatch && result.FileExists {
		fmt.Fprintf(os.Stderr, "  Expected hash: %s...\n", result.ExpectedHash[:16])
		fmt.Fprintf(os.Stderr, "  Actual hash:   %s...\n", result.LocalHash[:16])
	}
	return fmt.Errorf("bundle verification failed")
}

func verifyChain(ctx context.Context, mgr *bundle.Manager, verbose bool) error {
	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Fprintf(os.Stderr, "No bundles to verify\n")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Verifying chain of %d bundles...\n\n", len(bundles))

	start := time.Now()

	var progress *ui.ProgressBar
	if !verbose {
		progress = ui.NewProgressBar(len(bundles))
	}

	verifiedCount := 0
	errorCount := 0
	var firstError error

	for i, meta := range bundles {
		bundleNum := meta.BundleNumber

		if verbose {
			fmt.Fprintf(os.Stderr, "  Verifying bundle %06d... ", bundleNum)
		}

		result, err := mgr.VerifyBundle(ctx, bundleNum)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "ERROR\n")
			}
			fmt.Fprintf(os.Stderr, "\n✗ Failed to verify bundle %06d: %v\n", bundleNum, err)
			errorCount++
			if firstError == nil {
				firstError = err
			}
			continue
		}

		if !result.Valid {
			if verbose {
				fmt.Fprintf(os.Stderr, "INVALID\n")
			}
			fmt.Fprintf(os.Stderr, "\n✗ Bundle %06d hash verification failed\n", bundleNum)
			if result.Error != nil {
				fmt.Fprintf(os.Stderr, "  Error: %v\n", result.Error)
				if firstError == nil {
					firstError = result.Error
				}
			}
			errorCount++
			continue
		}

		// Verify chain link
		if i > 0 {
			prevMeta := bundles[i-1]
			if meta.Parent != prevMeta.Hash {
				if verbose {
					fmt.Fprintf(os.Stderr, "CHAIN BROKEN\n")
				}
				fmt.Fprintf(os.Stderr, "\n✗ Chain broken at bundle %06d\n", bundleNum)
				fmt.Fprintf(os.Stderr, "  Expected parent: %s...\n", prevMeta.Hash[:16])
				fmt.Fprintf(os.Stderr, "  Actual parent:   %s...\n", meta.Parent[:16])
				errorCount++
				if firstError == nil {
					firstError = fmt.Errorf("chain broken at bundle %d", bundleNum)
				}
				continue
			}
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "✓\n")
		}
		verifiedCount++

		if progress != nil {
			progress.Set(i + 1)
		}
	}

	if progress != nil {
		progress.Finish()
	}

	elapsed := time.Since(start)

	fmt.Fprintf(os.Stderr, "\n")
	if errorCount == 0 {
		fmt.Fprintf(os.Stderr, "✓ Chain is valid (%d bundles verified)\n", verifiedCount)
		fmt.Fprintf(os.Stderr, "  First bundle: %06d\n", bundles[0].BundleNumber)
		fmt.Fprintf(os.Stderr, "  Last bundle:  %06d\n", bundles[len(bundles)-1].BundleNumber)
		fmt.Fprintf(os.Stderr, "  Chain head:   %s...%s\n",
			bundles[len(bundles)-1].Hash[:16],
			bundles[len(bundles)-1].Hash[len(bundles[len(bundles)-1].Hash)-16:])

		// Timing information
		fmt.Fprintf(os.Stderr, "\nPerformance:\n")
		fmt.Fprintf(os.Stderr, "  Time:       %s\n", elapsed.Round(time.Millisecond))
		if elapsed.Seconds() > 0 {
			bundlesPerSec := float64(verifiedCount) / elapsed.Seconds()
			fmt.Fprintf(os.Stderr, "  Throughput: %.1f bundles/sec\n", bundlesPerSec)

			// Calculate data throughput
			index := mgr.GetIndex()
			stats := index.GetStats()
			if totalSize, ok := stats["total_size"].(int64); ok && totalSize > 0 {
				mbPerSec := float64(totalSize) / elapsed.Seconds() / (1024 * 1024)
				fmt.Fprintf(os.Stderr, "  Data rate:  %.1f MB/sec (compressed)\n", mbPerSec)
			}
		}
		return nil
	}

	fmt.Fprintf(os.Stderr, "✗ Chain verification failed\n")
	fmt.Fprintf(os.Stderr, "  Verified: %d/%d bundles\n", verifiedCount, len(bundles))
	fmt.Fprintf(os.Stderr, "  Errors: %d\n", errorCount)
	fmt.Fprintf(os.Stderr, "  Time: %s\n", elapsed.Round(time.Millisecond))
	return fmt.Errorf("chain verification failed")
}

func verifyRange(ctx context.Context, mgr *bundle.Manager, rangeStr string, verbose bool, parallel bool, workers int) error {
	start, end, err := parseBundleRange(rangeStr)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Verifying bundles %06d - %06d", start, end)
	if parallel {
		fmt.Fprintf(os.Stderr, " (parallel, %d workers)", workers)
	}
	fmt.Fprintf(os.Stderr, "\n\n")

	total := end - start + 1
	overallStart := time.Now()

	var verifyErr error
	if parallel {
		verifyErr = verifyRangeParallel(ctx, mgr, start, end, workers, verbose)
	} else {
		verifyErr = verifyRangeSequential(ctx, mgr, start, end, total, verbose)
	}

	elapsed := time.Since(overallStart)

	// Add timing summary
	fmt.Fprintf(os.Stderr, "\nPerformance:\n")
	fmt.Fprintf(os.Stderr, "  Time:       %s\n", elapsed.Round(time.Millisecond))
	if elapsed.Seconds() > 0 {
		bundlesPerSec := float64(total) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "  Throughput: %.1f bundles/sec\n", bundlesPerSec)

		// Estimate average time per bundle
		avgTime := elapsed / time.Duration(total)
		fmt.Fprintf(os.Stderr, "  Avg/bundle: %s\n", avgTime.Round(time.Millisecond))
	}

	return verifyErr
}

func verifyRangeSequential(ctx context.Context, mgr *bundle.Manager, start, end, total int, verbose bool) error {
	var progress *ui.ProgressBar
	if !verbose {
		progress = ui.NewProgressBar(total)
	}

	verified := 0
	failed := 0
	failedBundles := []int{}

	for bundleNum := start; bundleNum <= end; bundleNum++ {
		result, err := mgr.VerifyBundle(ctx, bundleNum)

		if verbose {
			fmt.Fprintf(os.Stderr, "Bundle %06d: ", bundleNum)
		}

		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "ERROR - %v\n", err)
			}
			failed++
			failedBundles = append(failedBundles, bundleNum)
		} else if !result.Valid {
			if verbose {
				fmt.Fprintf(os.Stderr, "INVALID - %v\n", result.Error)
			}
			failed++
			failedBundles = append(failedBundles, bundleNum)
		} else {
			if verbose {
				fmt.Fprintf(os.Stderr, "✓\n")
			}
			verified++
		}

		if progress != nil {
			progress.Set(bundleNum - start + 1)
		}
	}

	if progress != nil {
		progress.Finish()
	}

	fmt.Fprintf(os.Stderr, "\n")
	if failed == 0 {
		fmt.Fprintf(os.Stderr, "✓ All %d bundles verified successfully\n", verified)
		return nil
	}

	fmt.Fprintf(os.Stderr, "✗ Verification failed\n")
	fmt.Fprintf(os.Stderr, "  Verified: %d/%d\n", verified, total)
	fmt.Fprintf(os.Stderr, "  Failed: %d\n", failed)

	if len(failedBundles) > 0 && len(failedBundles) <= 20 {
		fmt.Fprintf(os.Stderr, "\nFailed bundles: ")
		for i, num := range failedBundles {
			if i > 0 {
				fmt.Fprintf(os.Stderr, ", ")
			}
			fmt.Fprintf(os.Stderr, "%06d", num)
		}
		fmt.Fprintf(os.Stderr, "\n")
	} else if len(failedBundles) > 20 {
		fmt.Fprintf(os.Stderr, "\nFailed bundles: %06d, %06d, ... and %d more\n",
			failedBundles[0], failedBundles[1], len(failedBundles)-2)
	}

	return fmt.Errorf("verification failed for %d bundles", failed)
}

func verifyRangeParallel(ctx context.Context, mgr *bundle.Manager, start, end, workers int, verbose bool) error {
	type job struct {
		bundleNum int
	}

	type result struct {
		bundleNum int
		valid     bool
		err       error
	}

	total := end - start + 1
	jobs := make(chan job, total)
	results := make(chan result, total)

	// Start workers
	for w := 0; w < workers; w++ {
		go func() {
			for j := range jobs {
				vr, err := mgr.VerifyBundle(ctx, j.bundleNum)
				results <- result{
					bundleNum: j.bundleNum,
					valid:     err == nil && vr.Valid,
					err:       err,
				}
			}
		}()
	}

	// Send jobs
	for bundleNum := start; bundleNum <= end; bundleNum++ {
		jobs <- job{bundleNum: bundleNum}
	}
	close(jobs)

	// Collect results with progress
	progress := ui.NewProgressBar(total)
	verified := 0
	failed := 0
	failedBundles := []int{}

	for i := 0; i < total; i++ {
		res := <-results

		if res.valid {
			verified++
		} else {
			failed++
			failedBundles = append(failedBundles, res.bundleNum)
			if verbose {
				fmt.Fprintf(os.Stderr, "\nBundle %06d: FAILED - %v\n", res.bundleNum, res.err)
			}
		}

		progress.Set(i + 1)
	}

	progress.Finish()

	fmt.Fprintf(os.Stderr, "\n")
	if failed == 0 {
		fmt.Fprintf(os.Stderr, "✓ All %d bundles verified successfully\n", verified)
		return nil
	}

	fmt.Fprintf(os.Stderr, "✗ Verification failed\n")
	fmt.Fprintf(os.Stderr, "  Verified: %d/%d\n", verified, total)
	fmt.Fprintf(os.Stderr, "  Failed: %d\n", failed)

	if len(failedBundles) > 0 && len(failedBundles) <= 20 {
		fmt.Fprintf(os.Stderr, "\nFailed bundles: ")
		for i, num := range failedBundles {
			if i > 0 {
				fmt.Fprintf(os.Stderr, ", ")
			}
			fmt.Fprintf(os.Stderr, "%06d", num)
		}
		fmt.Fprintf(os.Stderr, "\n")
	} else if len(failedBundles) > 20 {
		fmt.Fprintf(os.Stderr, "\nFailed bundles: %06d, %06d, ... and %d more\n",
			failedBundles[0], failedBundles[1], len(failedBundles)-2)
	}

	return fmt.Errorf("verification failed for %d bundles", failed)
}
