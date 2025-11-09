package commands

import (
	"context"
	"fmt"

	flag "github.com/spf13/pflag"

	"tangled.org/atscan.net/plcbundle/bundle"
)

// VerifyCommand handles the verify subcommand
func VerifyCommand(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	bundleNum := fs.Int("bundle", 0, "specific bundle to verify (0 = verify chain)")
	verbose := fs.Bool("v", false, "verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)

	ctx := context.Background()

	if *bundleNum > 0 {
		return verifySingleBundle(ctx, mgr, *bundleNum, *verbose)
	}

	return verifyChain(ctx, mgr, *verbose)
}

func verifySingleBundle(ctx context.Context, mgr *bundle.Manager, bundleNum int, verbose bool) error {
	fmt.Printf("Verifying bundle %06d...\n", bundleNum)

	result, err := mgr.VerifyBundle(ctx, bundleNum)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	if result.Valid {
		fmt.Printf("✓ Bundle %06d is valid\n", bundleNum)
		if verbose {
			fmt.Printf("  File exists: %v\n", result.FileExists)
			fmt.Printf("  Hash match: %v\n", result.HashMatch)
			fmt.Printf("  Hash: %s...\n", result.LocalHash[:16])
		}
		return nil
	}

	fmt.Printf("✗ Bundle %06d is invalid\n", bundleNum)
	if result.Error != nil {
		fmt.Printf("  Error: %v\n", result.Error)
	}
	if !result.FileExists {
		fmt.Printf("  File not found\n")
	}
	if !result.HashMatch && result.FileExists {
		fmt.Printf("  Expected hash: %s...\n", result.ExpectedHash[:16])
		fmt.Printf("  Actual hash:   %s...\n", result.LocalHash[:16])
	}
	return fmt.Errorf("bundle verification failed")
}

func verifyChain(ctx context.Context, mgr *bundle.Manager, verbose bool) error {
	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Println("No bundles to verify")
		return nil
	}

	fmt.Printf("Verifying chain of %d bundles...\n\n", len(bundles))

	verifiedCount := 0
	errorCount := 0
	lastPercent := -1

	for i, meta := range bundles {
		bundleNum := meta.BundleNumber

		percent := (i * 100) / len(bundles)
		if percent != lastPercent || verbose {
			if verbose {
				fmt.Printf("  [%3d%%] Verifying bundle %06d...", percent, bundleNum)
			} else if percent%10 == 0 && percent != lastPercent {
				fmt.Printf("  [%3d%%] Verified %d/%d bundles...\n", percent, i, len(bundles))
			}
			lastPercent = percent
		}

		result, err := mgr.VerifyBundle(ctx, bundleNum)
		if err != nil {
			if verbose {
				fmt.Printf(" ERROR\n")
			}
			fmt.Printf("\n✗ Failed to verify bundle %06d: %v\n", bundleNum, err)
			errorCount++
			continue
		}

		if !result.Valid {
			if verbose {
				fmt.Printf(" INVALID\n")
			}
			fmt.Printf("\n✗ Bundle %06d hash verification failed\n", bundleNum)
			if result.Error != nil {
				fmt.Printf("  Error: %v\n", result.Error)
			}
			errorCount++
			continue
		}

		if i > 0 {
			prevMeta := bundles[i-1]
			if meta.Parent != prevMeta.Hash {
				if verbose {
					fmt.Printf(" CHAIN BROKEN\n")
				}
				fmt.Printf("\n✗ Chain broken at bundle %06d\n", bundleNum)
				fmt.Printf("  Expected parent: %s...\n", prevMeta.Hash[:16])
				fmt.Printf("  Actual parent:   %s...\n", meta.Parent[:16])
				errorCount++
				continue
			}
		}

		if verbose {
			fmt.Printf(" ✓\n")
		}
		verifiedCount++
	}

	fmt.Println()
	if errorCount == 0 {
		fmt.Printf("✓ Chain is valid (%d bundles verified)\n", verifiedCount)
		fmt.Printf("  First bundle: %06d\n", bundles[0].BundleNumber)
		fmt.Printf("  Last bundle:  %06d\n", bundles[len(bundles)-1].BundleNumber)
		fmt.Printf("  Chain head:   %s...\n", bundles[len(bundles)-1].Hash[:16])
		return nil
	}

	fmt.Printf("✗ Chain verification failed\n")
	fmt.Printf("  Verified: %d/%d bundles\n", verifiedCount, len(bundles))
	fmt.Printf("  Errors: %d\n", errorCount)
	return fmt.Errorf("chain verification failed")
}
