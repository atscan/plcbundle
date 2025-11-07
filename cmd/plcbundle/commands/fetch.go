package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

// FetchCommand handles the fetch subcommand
func FetchCommand(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL")
	count := fs.Int("count", 0, "number of bundles to fetch (0 = fetch all available)")
	verbose := fs.Bool("verbose", false, "verbose sync logging")

	if err := fs.Parse(args); err != nil {
		return err
	}

	mgr, dir, err := getManager(*plcURL)
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Working in: %s\n", dir)

	ctx := context.Background()

	// Get starting bundle info
	index := mgr.GetIndex()
	lastBundle := index.GetLastBundle()
	startBundle := 1
	if lastBundle != nil {
		startBundle = lastBundle.BundleNumber + 1
	}

	fmt.Printf("Starting from bundle %06d\n", startBundle)

	if *count > 0 {
		fmt.Printf("Fetching %d bundles...\n", *count)
	} else {
		fmt.Printf("Fetching all available bundles...\n")
	}

	fetchedCount := 0
	consecutiveErrors := 0
	maxConsecutiveErrors := 3

	for {
		// Check if we've reached the requested count
		if *count > 0 && fetchedCount >= *count {
			break
		}

		currentBundle := startBundle + fetchedCount

		if *count > 0 {
			fmt.Printf("Fetching bundle %d/%d (bundle %06d)...\n", fetchedCount+1, *count, currentBundle)
		} else {
			fmt.Printf("Fetching bundle %06d...\n", currentBundle)
		}

		b, err := mgr.FetchNextBundle(ctx, !*verbose)
		if err != nil {
			// Check if we've reached the end
			if isEndOfDataError(err) {
				fmt.Printf("\n✓ Caught up! No more complete bundles available.\n")
				fmt.Printf("  Last bundle: %06d\n", currentBundle-1)
				break
			}

			// Handle other errors
			consecutiveErrors++
			fmt.Fprintf(os.Stderr, "Error fetching bundle %06d: %v\n", currentBundle, err)

			if consecutiveErrors >= maxConsecutiveErrors {
				return fmt.Errorf("too many consecutive errors, stopping")
			}

			fmt.Printf("Waiting 5 seconds before retry...\n")
			time.Sleep(5 * time.Second)
			continue
		}

		// Reset error counter on success
		consecutiveErrors = 0

		if err := mgr.SaveBundle(ctx, b, !*verbose); err != nil {
			return fmt.Errorf("error saving bundle %06d: %w", b.BundleNumber, err)
		}

		fetchedCount++
		fmt.Printf("✓ Saved bundle %06d (%d operations, %d DIDs)\n",
			b.BundleNumber, len(b.Operations), b.DIDCount)
	}

	if fetchedCount > 0 {
		fmt.Printf("\n✓ Fetch complete: %d bundles retrieved\n", fetchedCount)
		fmt.Printf("  Current range: %06d - %06d\n", startBundle, startBundle+fetchedCount-1)
	} else {
		fmt.Printf("\n✓ Already up to date!\n")
	}

	return nil
}
