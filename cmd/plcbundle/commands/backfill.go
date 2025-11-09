package commands

import (
	"context"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"
)

// BackfillCommand handles the backfill subcommand
func BackfillCommand(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	plcURL := fs.String("plc", "https://plc.directory", "PLC directory URL")
	startFrom := fs.Int("start", 1, "bundle number to start from")
	endAt := fs.Int("end", 0, "bundle number to end at (0 = until caught up)")
	verbose := fs.Bool("verbose", false, "verbose sync logging")

	if err := fs.Parse(args); err != nil {
		return err
	}

	mgr, dir, err := getManager(*plcURL)
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Fprintf(os.Stderr, "Starting backfill from: %s\n", dir)
	fmt.Fprintf(os.Stderr, "Starting from bundle: %06d\n", *startFrom)
	if *endAt > 0 {
		fmt.Fprintf(os.Stderr, "Ending at bundle: %06d\n", *endAt)
	} else {
		fmt.Fprintf(os.Stderr, "Ending: when caught up\n")
	}
	fmt.Fprintf(os.Stderr, "\n")

	ctx := context.Background()

	currentBundle := *startFrom
	processedCount := 0
	fetchedCount := 0
	loadedCount := 0
	operationCount := 0

	for {
		if *endAt > 0 && currentBundle > *endAt {
			break
		}

		fmt.Fprintf(os.Stderr, "Processing bundle %06d... ", currentBundle)

		// Try to load from disk first
		bundle, err := mgr.LoadBundle(ctx, currentBundle)

		if err != nil {
			// Bundle doesn't exist, fetch it
			fmt.Fprintf(os.Stderr, "fetching... ")

			bundle, err = mgr.FetchNextBundle(ctx, !*verbose)
			if err != nil {
				if isEndOfDataError(err) {
					fmt.Fprintf(os.Stderr, "\n✓ Caught up! No more complete bundles available.\n")
					break
				}
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
				break
			}

			if _, err := mgr.SaveBundle(ctx, bundle, !*verbose); err != nil {
				return fmt.Errorf("error saving: %w", err)
			}

			fetchedCount++
			fmt.Fprintf(os.Stderr, "saved... ")
		} else {
			loadedCount++
		}

		// Output operations to stdout (JSONL)
		for _, op := range bundle.Operations {
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			}
		}

		operationCount += len(bundle.Operations)
		processedCount++

		fmt.Fprintf(os.Stderr, "✓ (%d ops, %d DIDs)\n", len(bundle.Operations), bundle.DIDCount)

		currentBundle++

		// Progress summary every 100 bundles
		if processedCount%100 == 0 {
			fmt.Fprintf(os.Stderr, "\n--- Progress: %d bundles processed (%d fetched, %d loaded) ---\n",
				processedCount, fetchedCount, loadedCount)
			fmt.Fprintf(os.Stderr, "    Total operations: %d\n\n", operationCount)
		}
	}

	// Final summary
	fmt.Fprintf(os.Stderr, "\n✓ Backfill complete\n")
	fmt.Fprintf(os.Stderr, "  Bundles processed: %d\n", processedCount)
	fmt.Fprintf(os.Stderr, "  Newly fetched: %d\n", fetchedCount)
	fmt.Fprintf(os.Stderr, "  Loaded from disk: %d\n", loadedCount)
	fmt.Fprintf(os.Stderr, "  Total operations: %d\n", operationCount)
	fmt.Fprintf(os.Stderr, "  Range: %06d - %06d\n", *startFrom, currentBundle-1)

	return nil
}

// isEndOfDataError checks if error indicates end of available data
func isEndOfDataError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return containsAny(errMsg,
		"insufficient operations",
		"no more operations available",
		"reached latest data")
}

// Helper functions

func containsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if contains(s, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
