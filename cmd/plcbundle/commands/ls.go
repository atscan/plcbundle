package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

func NewLsCommand() *cobra.Command {
	var (
		last      int
		reverse   bool
		format    string
		noHeader  bool
		separator string
	)

	cmd := &cobra.Command{
		Use:   "ls [flags]",
		Short: "List bundles (machine-readable)",
		Long: `List bundles in machine-readable format

Outputs bundle information in a clean, parseable format suitable for
piping to sed, awk, grep, or other text processing tools.

No colors, no pager - just consistent, tab-separated data perfect for
shell scripts and automation.`,

		Example: `  # List all bundles
  plcbundle ls

  # Last 10 bundles
  plcbundle ls -n 10

  # Oldest first
  plcbundle ls --reverse

  # Custom format
  plcbundle ls --format "bundle,hash,date,size"

  # CSV format
  plcbundle ls --separator ","

  # Scripting examples
  plcbundle ls | awk '{print $1}'           # Just bundle numbers
  plcbundle ls | grep 000150                # Find specific bundle
  plcbundle ls -n 5 | cut -f1,4             # First and 4th columns
  plcbundle ls --format bundle,hash         # Custom columns
  plcbundle ls --separator "," > bundles.csv # Export to CSV`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, _, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return listBundles(mgr, lsOptions{
				last:      last,
				reverse:   reverse,
				format:    format,
				noHeader:  noHeader,
				separator: separator,
			})
		},
	}

	// Flags
	cmd.Flags().IntVarP(&last, "last", "n", 0, "Show only last N bundles (0 = all)")
	cmd.Flags().BoolVar(&reverse, "reverse", false, "Show oldest first")
	cmd.Flags().StringVar(&format, "format", "bundle,hash,date,ops,dids,size",
		"Output format: bundle,hash,date,ops,dids,size,uncompressed,ratio,timespan")
	cmd.Flags().BoolVar(&noHeader, "no-header", false, "Omit header row")
	cmd.Flags().StringVar(&separator, "separator", "\t", "Field separator (default: tab)")

	return cmd
}

type lsOptions struct {
	last      int
	reverse   bool
	format    string
	noHeader  bool
	separator string
}

func listBundles(mgr BundleManager, opts lsOptions) error {
	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		return nil
	}

	// Apply limit
	displayBundles := bundles
	if opts.last > 0 && opts.last < len(bundles) {
		displayBundles = bundles[len(bundles)-opts.last:]
	}

	// Reverse if not --reverse (default is newest first, like log)
	if !opts.reverse {
		reversed := make([]*bundleindex.BundleMetadata, len(displayBundles))
		for i, b := range displayBundles {
			reversed[len(displayBundles)-1-i] = b
		}
		displayBundles = reversed
	}

	// Parse format string
	fields := parseFormatString(opts.format)

	// Print header (unless disabled)
	if !opts.noHeader {
		printHeader(fields, opts.separator)
	}

	// Print each bundle
	for _, meta := range displayBundles {
		printBundleFields(meta, fields, opts.separator)
	}

	return nil
}

// parseFormatString parses comma-separated field names
func parseFormatString(format string) []string {
	parts := strings.Split(format, ",")
	fields := make([]string, 0, len(parts))
	for _, p := range parts {
		field := strings.TrimSpace(p)
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

// printHeader prints the header row
func printHeader(fields []string, sep string) {
	headers := make([]string, len(fields))
	for i, field := range fields {
		headers[i] = getFieldHeader(field)
	}
	fmt.Println(strings.Join(headers, sep))
}

// getFieldHeader returns the header name for a field
func getFieldHeader(field string) string {
	switch field {
	case "bundle":
		return "bundle"
	case "hash":
		return "hash"
	case "content":
		return "content_hash"
	case "parent":
		return "parent_hash"
	case "date", "time":
		return "date"
	case "age":
		return "age"
	case "ops", "operations":
		return "ops"
	case "dids":
		return "dids"
	case "size", "compressed":
		return "size"
	case "uncompressed":
		return "uncompressed"
	case "ratio":
		return "ratio"
	case "timespan", "duration":
		return "timespan"
	case "start":
		return "start_time"
	case "end":
		return "end_time"
	default:
		return field
	}
}

// printBundleFields prints a bundle's fields
func printBundleFields(meta *bundleindex.BundleMetadata, fields []string, sep string) {
	values := make([]string, len(fields))

	for i, field := range fields {
		values[i] = getFieldValue(meta, field)
	}

	fmt.Println(strings.Join(values, sep))
}

// getFieldValue returns the value for a specific field
func getFieldValue(meta *bundleindex.BundleMetadata, field string) string {
	switch field {
	case "bundle":
		return fmt.Sprintf("%06d", meta.BundleNumber)

	case "hash":
		return meta.Hash

	case "hash_short":
		if len(meta.Hash) >= 12 {
			return meta.Hash[:12]
		}
		return meta.Hash

	case "content":
		return meta.ContentHash

	case "content_short":
		if len(meta.ContentHash) >= 12 {
			return meta.ContentHash[:12]
		}
		return meta.ContentHash

	case "parent":
		return meta.Parent

	case "parent_short":
		if len(meta.Parent) >= 12 {
			return meta.Parent[:12]
		}
		return meta.Parent

	case "date", "time":
		return meta.EndTime.Format("2006-01-02T15:04:05Z")

	case "date_short":
		return meta.EndTime.Format("2006-01-02")

	case "timestamp", "unix":
		return fmt.Sprintf("%d", meta.EndTime.Unix())

	case "age":
		age := time.Since(meta.EndTime)
		return formatDurationShort(age)

	case "age_seconds":
		return fmt.Sprintf("%.0f", time.Since(meta.EndTime).Seconds())

	case "ops", "operations":
		return fmt.Sprintf("%d", meta.OperationCount)

	case "dids":
		return fmt.Sprintf("%d", meta.DIDCount)

	case "size", "compressed":
		return fmt.Sprintf("%d", meta.CompressedSize)

	case "size_mb":
		return fmt.Sprintf("%.2f", float64(meta.CompressedSize)/(1024*1024))

	case "uncompressed":
		return fmt.Sprintf("%d", meta.UncompressedSize)

	case "uncompressed_mb":
		return fmt.Sprintf("%.2f", float64(meta.UncompressedSize)/(1024*1024))

	case "ratio":
		if meta.CompressedSize > 0 {
			ratio := float64(meta.UncompressedSize) / float64(meta.CompressedSize)
			return fmt.Sprintf("%.2f", ratio)
		}
		return "0"

	case "timespan", "duration":
		duration := meta.EndTime.Sub(meta.StartTime)
		return formatDurationShort(duration)

	case "timespan_seconds":
		duration := meta.EndTime.Sub(meta.StartTime)
		return fmt.Sprintf("%.0f", duration.Seconds())

	case "start":
		return meta.StartTime.Format("2006-01-02T15:04:05Z")

	case "end":
		return meta.EndTime.Format("2006-01-02T15:04:05Z")

	case "created":
		return meta.CreatedAt.Format("2006-01-02T15:04:05Z")

	default:
		return ""
	}
}
