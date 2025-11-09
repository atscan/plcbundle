package commands

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

func NewLogCommand() *cobra.Command {
	var (
		last     int
		oneline  bool
		noHashes bool
		reverse  bool
		noPager  bool
	)

	cmd := &cobra.Command{
		Use:     "log [flags]",
		Aliases: []string{"history"},
		Short:   "Show bundle history",
		Long: `Show bundle history in chronological order

Displays a log of all bundles in the repository, similar to 'git log'.
By default shows all bundles in reverse chronological order (newest first)
with chain hashes and automatically pipes through a pager.

The log shows:
  • Bundle number and timestamp
  • Operation and DID counts
  • Compressed and uncompressed sizes
  • Chain and content hashes (default, use --no-hashes to hide)
  • Coverage timespan

Output is automatically piped through 'less' (or $PAGER) when stdout
is a terminal, just like 'git log'. Use --no-pager to disable.`,

		Example: `  # Show all bundles (newest first, auto-paged)
  plcbundle log

  # Show last 10 bundles
  plcbundle log --last 10
  plcbundle log -n 10

  # One-line format
  plcbundle log --oneline

  # Hide hashes
  plcbundle log --no-hashes

  # Oldest first (ascending order)
  plcbundle log --reverse

  # Disable pager (direct output)
  plcbundle log --no-pager

  # Combination
  plcbundle log -n 20 --oneline

  # Using alias
  plcbundle history -n 5`,

		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
			if err != nil {
				return err
			}
			defer mgr.Close()

			return showBundleLog(mgr, dir, logOptions{
				last:       last,
				oneline:    oneline,
				showHashes: !noHashes, // Invert: hashes ON by default
				reverse:    reverse,
				noPager:    noPager,
			})
		},
	}

	// Flags
	cmd.Flags().IntVarP(&last, "last", "n", 0, "Show only last N bundles (0 = all)")
	cmd.Flags().BoolVar(&oneline, "oneline", false, "Show one line per bundle (compact)")
	cmd.Flags().BoolVar(&noHashes, "no-hashes", false, "Hide chain and content hashes")
	cmd.Flags().BoolVar(&reverse, "reverse", false, "Show oldest first (ascending order)")
	cmd.Flags().BoolVar(&noPager, "no-pager", false, "Disable pager (output directly)")

	return cmd
}

type logOptions struct {
	last       int
	oneline    bool
	showHashes bool
	reverse    bool
	noPager    bool
}

func showBundleLog(mgr BundleManager, dir string, opts logOptions) error {
	index := mgr.GetIndex()
	bundles := index.GetBundles()

	if len(bundles) == 0 {
		fmt.Printf("No bundles in repository\n")
		fmt.Printf("Directory: %s\n\n", dir)
		fmt.Printf("Get started:\n")
		fmt.Printf("  plcbundle clone <url>    Clone from remote\n")
		fmt.Printf("  plcbundle sync           Fetch from PLC directory\n")
		return nil
	}

	// Apply limit
	displayBundles := bundles
	if opts.last > 0 && opts.last < len(bundles) {
		displayBundles = bundles[len(bundles)-opts.last:]
	}

	// Reverse if not --reverse (default is newest first)
	if !opts.reverse {
		reversed := make([]*bundleindex.BundleMetadata, len(displayBundles))
		for i, b := range displayBundles {
			reversed[len(displayBundles)-1-i] = b
		}
		displayBundles = reversed
	}

	// Determine output destination (pager or stdout)
	var output io.WriteCloser
	var useColor bool

	if !opts.noPager && isTTY(os.Stdout) {
		// Try to use pager
		pager, err := startPager()
		if err == nil {
			output = pager
			useColor = true
			defer pager.Close()
		} else {
			output = os.Stdout
			useColor = isTTY(os.Stdout)
		}
	} else {
		output = os.Stdout
		useColor = !opts.noPager && isTTY(os.Stdout)
	}

	// Render to output
	renderBundleLog(output, dir, index, bundles, displayBundles, opts, useColor)

	return nil
}

func renderBundleLog(w io.Writer, dir string, index *bundleindex.Index, allBundles, displayBundles []*bundleindex.BundleMetadata, opts logOptions, useColor bool) {
	// Color codes
	var (
		colorReset     = ""
		colorBundleNum = ""
		colorHash      = ""
		colorDate      = ""
		colorAge       = ""
		colorSize      = ""
		colorHeader    = ""
		colorDim       = ""
	)

	if useColor {
		colorReset = "\033[0m"
		colorBundleNum = "\033[1;36m" // Bright cyan + bold
		colorHash = "\033[33m"        // Yellow
		colorDate = "\033[32m"        // Green
		colorAge = "\033[90m"         // Dim
		colorSize = "\033[35m"        // Magenta
		colorHeader = "\033[1;37m"    // Bright white + bold
		colorDim = "\033[2m"          // Dim
	}

	// Header
	if !opts.oneline {
		fmt.Fprintf(w, "%sBundle History%s\n", colorHeader, colorReset)
		fmt.Fprintf(w, "Directory: %s\n", dir)
		if origin := index.Origin; origin != "" {
			fmt.Fprintf(w, "Origin:    %s\n", origin)
		}
		fmt.Fprintf(w, "Total:     %d bundles", len(allBundles))
		if opts.last > 0 && opts.last < len(allBundles) {
			fmt.Fprintf(w, " (showing last %d)", opts.last)
		}
		fmt.Fprintf(w, "\n\n")
	}

	// Display bundles
	for i, meta := range displayBundles {
		if opts.oneline {
			displayBundleOneLine(w, meta, opts.showHashes, useColor, colorBundleNum, colorHash, colorDate, colorAge, colorSize, colorReset)
		} else {
			displayBundleDetailed(w, meta, opts.showHashes, useColor, colorBundleNum, colorHash, colorDate, colorAge, colorSize, colorDim, colorReset)

			// Add separator between bundles (except last)
			if i < len(displayBundles)-1 {
				fmt.Fprintf(w, "%s────────────────────────────────────────────────────────%s\n\n",
					colorDim, colorReset)
			}
		}
	}

	// Summary footer
	if !opts.oneline && len(displayBundles) > 0 {
		fmt.Fprintf(w, "\n")
		displayLogSummary(w, allBundles, displayBundles, opts.last, useColor, colorHeader, colorReset)
	}
}

func displayBundleOneLine(w io.Writer, meta *bundleindex.BundleMetadata, showHashes bool, useColor bool, colorBundle, colorHash, colorDate, colorAge, colorSize, colorReset string) {
	age := time.Since(meta.EndTime)
	ageStr := formatDurationShort(age)

	// Build hash preview
	hashPreview := ""
	if showHashes && len(meta.Hash) >= 12 {
		hashPreview = fmt.Sprintf(" %s%s%s", colorHash, meta.Hash[:12], colorReset)
	}

	fmt.Fprintf(w, "%s%06d%s%s  %s%s%s  %s%s ago%s  %d ops, %d DIDs, %s%s%s\n",
		colorBundle, meta.BundleNumber, colorReset,
		hashPreview,
		colorDate, meta.EndTime.Format("2006-01-02 15:04"), colorReset,
		colorAge, ageStr, colorReset,
		meta.OperationCount,
		meta.DIDCount,
		colorSize, formatBytes(meta.CompressedSize), colorReset)
}

func displayBundleDetailed(w io.Writer, meta *bundleindex.BundleMetadata, showHashes bool, useColor bool, colorBundle, colorHash, colorDate, colorAge, colorSize, colorDim, colorReset string) {
	fmt.Fprintf(w, "%sBundle %06d%s\n", colorBundle, meta.BundleNumber, colorReset)

	// Timestamp and age
	age := time.Since(meta.EndTime)
	fmt.Fprintf(w, "  Date:       %s%s%s %s(%s ago)%s\n",
		colorDate, meta.EndTime.Format("2006-01-02 15:04:05"), colorReset,
		colorAge, formatDuration(age), colorReset)

	// Timespan of bundle
	duration := meta.EndTime.Sub(meta.StartTime)
	fmt.Fprintf(w, "  Timespan:   %s → %s (%s)\n",
		meta.StartTime.Format("15:04:05"),
		meta.EndTime.Format("15:04:05"),
		formatDuration(duration))

	// Operations and DIDs
	fmt.Fprintf(w, "  Operations: %s\n", formatNumber(meta.OperationCount))
	fmt.Fprintf(w, "  DIDs:       %s unique\n", formatNumber(meta.DIDCount))

	// Sizes
	ratio := float64(meta.UncompressedSize) / float64(meta.CompressedSize)
	fmt.Fprintf(w, "  Size:       %s%s%s → %s (%.2fx compression)\n",
		colorSize,
		formatBytes(meta.UncompressedSize),
		colorReset,
		formatBytes(meta.CompressedSize),
		ratio)

	// Hashes (shown by default now)
	if showHashes {
		fmt.Fprintf(w, "\n  Hashes:\n")
		fmt.Fprintf(w, "    %sChain:%s   %s%s%s\n",
			colorDim, colorReset,
			colorHash, meta.Hash, colorReset)
		fmt.Fprintf(w, "    %sContent:%s %s%s%s\n",
			colorDim, colorReset,
			colorHash, meta.ContentHash, colorReset)
		if meta.Parent != "" {
			fmt.Fprintf(w, "    %sParent:%s  %s%s%s\n",
				colorDim, colorReset,
				colorHash, meta.Parent, colorReset)
		}
	}
}

func displayLogSummary(w io.Writer, allBundles, displayedBundles []*bundleindex.BundleMetadata, limit int, useColor bool, colorHeader, colorReset string) {
	first := displayedBundles[0]
	last := displayedBundles[len(displayedBundles)-1]

	// Determine order for summary
	var earliest, latest *bundleindex.BundleMetadata
	if first.BundleNumber < last.BundleNumber {
		earliest = first
		latest = last
	} else {
		earliest = last
		latest = first
	}

	fmt.Fprintf(w, "%sSummary%s\n", colorHeader, colorReset)
	fmt.Fprintf(w, "───────\n")

	if limit > 0 && limit < len(allBundles) {
		fmt.Fprintf(w, "  Showing:    %d of %d bundles\n", len(displayedBundles), len(allBundles))
	} else {
		fmt.Fprintf(w, "  Bundles:    %d total\n", len(allBundles))
	}

	fmt.Fprintf(w, "  Range:      %06d → %06d\n", earliest.BundleNumber, latest.BundleNumber)

	// Calculate total stats for displayed bundles
	totalOps := 0
	totalSize := int64(0)
	totalUncompressed := int64(0)
	for _, meta := range displayedBundles {
		totalOps += meta.OperationCount
		totalSize += meta.CompressedSize
		totalUncompressed += meta.UncompressedSize
	}

	fmt.Fprintf(w, "  Operations: %s\n", formatNumber(totalOps))
	fmt.Fprintf(w, "  Size:       %s (compressed)\n", formatBytes(totalSize))

	timespan := latest.EndTime.Sub(earliest.StartTime)
	fmt.Fprintf(w, "  Timespan:   %s\n", formatDuration(timespan))
}

// isTTY checks if the given file is a terminal
func isTTY(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}

// startPager starts a pager process and returns a WriteCloser
func startPager() (io.WriteCloser, error) {
	// Check for PAGER environment variable
	pagerCmd := os.Getenv("PAGER")
	if pagerCmd == "" {
		// Default to less with options
		pagerCmd = "less"
	}

	// Parse pager command (handle args)
	parts := strings.Fields(pagerCmd)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty pager command")
	}

	cmdName := parts[0]
	cmdArgs := parts[1:]

	// Special handling for less - add color support flags
	if cmdName == "less" || strings.HasSuffix(cmdName, "/less") {
		// Add flags if not already present
		hasR := false
		for _, arg := range cmdArgs {
			if strings.Contains(arg, "R") {
				hasR = true
				break
			}
		}
		if !hasR {
			// -R: enable raw control characters (for colors)
			// -F: quit if output fits on one screen
			// -X: don't clear screen on exit
			cmdArgs = append([]string{"-R", "-F", "-X"}, cmdArgs...)
		}
	}

	cmd := exec.Command(cmdName, cmdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Return a WriteCloser that closes stdin and waits for process
	return &pagerWriter{
		writer:  stdin,
		cmd:     cmd,
		started: true,
	}, nil
}

// pagerWriter wraps a pager process
type pagerWriter struct {
	writer  io.WriteCloser
	cmd     *exec.Cmd
	started bool
}

func (pw *pagerWriter) Write(p []byte) (n int, err error) {
	return pw.writer.Write(p)
}

func (pw *pagerWriter) Close() error {
	pw.writer.Close()
	if pw.started {
		pw.cmd.Wait()
	}
	return nil
}

// formatDurationShort formats duration in short form
func formatDurationShort(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.0fh", d.Hours())
	}
	days := d.Hours() / 24
	if days < 30 {
		return fmt.Sprintf("%.0fd", days)
	}
	if days < 365 {
		return fmt.Sprintf("%.0fmo", days/30)
	}
	return fmt.Sprintf("%.1fy", days/365)
}
