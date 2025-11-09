package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/goccy/go-json"
)

// ExportCommand handles the export subcommand
func ExportCommand(args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	bundles := fs.String("bundles", "", "bundle number or range (e.g., '42' or '1-100')")
	all := fs.Bool("all", false, "export all bundles")
	count := fs.Int("count", 0, "limit number of operations (0 = all)")
	after := fs.String("after", "", "timestamp to start after (RFC3339)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if !*all && *bundles == "" {
		return fmt.Errorf("usage: plcbundle export --bundles <number|range> [options]\n" +
			"   or: plcbundle export --all [options]\n\n" +
			"Examples:\n" +
			"  plcbundle export --bundles 42\n" +
			"  plcbundle export --bundles 1-100\n" +
			"  plcbundle export --all\n" +
			"  plcbundle export --all --count 50000\n" +
			"  plcbundle export --bundles 42 | jq .")
	}

	mgr, _, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	// Determine bundle range
	var start, end int
	if *all {
		index := mgr.GetIndex()
		bundleList := index.GetBundles()
		if len(bundleList) == 0 {
			return fmt.Errorf("no bundles available")
		}
		start = bundleList[0].BundleNumber
		end = bundleList[len(bundleList)-1].BundleNumber

		fmt.Fprintf(os.Stderr, "Exporting all bundles (%d-%d)\n", start, end)
	} else {
		var err error
		start, end, err = parseBundleRange(*bundles)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Exporting bundles %d-%d\n", start, end)
	}

	if *count > 0 {
		fmt.Fprintf(os.Stderr, "Limit: %d operations\n", *count)
	}
	if *after != "" {
		fmt.Fprintf(os.Stderr, "After: %s\n", *after)
	}
	fmt.Fprintf(os.Stderr, "\n")

	// Parse after time
	var afterTime time.Time
	if *after != "" {
		afterTime, err = time.Parse(time.RFC3339, *after)
		if err != nil {
			return fmt.Errorf("invalid after time: %w", err)
		}
	}

	ctx := context.Background()
	exported := 0

	// Export operations
	for bundleNum := start; bundleNum <= end; bundleNum++ {
		if *count > 0 && exported >= *count {
			break
		}

		fmt.Fprintf(os.Stderr, "Processing bundle %d...\r", bundleNum)

		bundle, err := mgr.LoadBundle(ctx, bundleNum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nWarning: failed to load bundle %d: %v\n", bundleNum, err)
			continue
		}

		for _, op := range bundle.Operations {
			if !afterTime.IsZero() && op.CreatedAt.Before(afterTime) {
				continue
			}

			if *count > 0 && exported >= *count {
				break
			}

			// Output as JSONL
			if len(op.RawJSON) > 0 {
				fmt.Println(string(op.RawJSON))
			} else {
				data, _ := json.Marshal(op)
				fmt.Println(string(data))
			}

			exported++
		}
	}

	fmt.Fprintf(os.Stderr, "\n\n✓ Export complete\n")
	fmt.Fprintf(os.Stderr, "  Exported: %d operations\n", exported)

	return nil
}
