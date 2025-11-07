package commands

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

// CompareCommand handles the compare subcommand
func CompareCommand(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	verbose := fs.Bool("v", false, "verbose output (show all differences)")
	fetchMissing := fs.Bool("fetch-missing", false, "fetch missing bundles from target")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: plcbundle compare <target> [options]\n" +
			"  target: URL or path to remote plcbundle server/index\n\n" +
			"Examples:\n" +
			"  plcbundle compare https://plc.example.com\n" +
			"  plcbundle compare https://plc.example.com/index.json\n" +
			"  plcbundle compare /path/to/plc_bundles.json\n" +
			"  plcbundle compare https://plc.example.com --fetch-missing")
	}

	target := fs.Arg(0)

	mgr, dir, err := getManager("")
	if err != nil {
		return err
	}
	defer mgr.Close()

	fmt.Printf("Comparing: %s\n", dir)
	fmt.Printf("  Against: %s\n\n", target)

	// Load local index
	localIndex := mgr.GetIndex()

	// Load target index
	fmt.Printf("Loading target index...\n")
	targetIndex, err := loadTargetIndex(target)
	if err != nil {
		return fmt.Errorf("error loading target index: %w", err)
	}

	// Perform comparison
	comparison := compareIndexes(localIndex, targetIndex)

	// Display results
	displayComparison(comparison, *verbose)

	// Fetch missing bundles if requested
	if *fetchMissing && len(comparison.MissingBundles) > 0 {
		fmt.Printf("\n")
		if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
			return fmt.Errorf("--fetch-missing only works with remote URLs")
		}

		baseURL := strings.TrimSuffix(target, "/index.json")
		baseURL = strings.TrimSuffix(baseURL, "/plc_bundles.json")

		fmt.Printf("Fetching %d missing bundles...\n\n", len(comparison.MissingBundles))
		if err := fetchMissingBundles(mgr, baseURL, comparison.MissingBundles); err != nil {
			return err
		}
	}

	if comparison.HasDifferences() {
		return fmt.Errorf("indexes have differences")
	}

	return nil
}

func loadTargetIndex(target string) (*bundleindex.Index, error) {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		return loadIndexFromURL(target)
	}
	return bundleindex.LoadIndex(target)
}

func loadIndexFromURL(url string) (*bundleindex.Index, error) {
	if !strings.HasSuffix(url, ".json") {
		url = strings.TrimSuffix(url, "/") + "/index.json"
	}

	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var idx bundleindex.Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	return &idx, nil
}

func compareIndexes(local, target *bundleindex.Index) *IndexComparison {
	localBundles := local.GetBundles()
	targetBundles := target.GetBundles()

	localMap := make(map[int]*bundleindex.BundleMetadata)
	targetMap := make(map[int]*bundleindex.BundleMetadata)

	for _, b := range localBundles {
		localMap[b.BundleNumber] = b
	}
	for _, b := range targetBundles {
		targetMap[b.BundleNumber] = b
	}

	comparison := &IndexComparison{
		LocalCount:        len(localBundles),
		TargetCount:       len(targetBundles),
		MissingBundles:    make([]int, 0),
		ExtraBundles:      make([]int, 0),
		HashMismatches:    make([]HashMismatch, 0),
		ContentMismatches: make([]HashMismatch, 0),
	}

	// Get ranges
	if len(localBundles) > 0 {
		comparison.LocalRange = [2]int{localBundles[0].BundleNumber, localBundles[len(localBundles)-1].BundleNumber}
		comparison.LocalUpdated = local.UpdatedAt
		comparison.LocalTotalSize = local.TotalSize
	}

	if len(targetBundles) > 0 {
		comparison.TargetRange = [2]int{targetBundles[0].BundleNumber, targetBundles[len(targetBundles)-1].BundleNumber}
		comparison.TargetUpdated = target.UpdatedAt
		comparison.TargetTotalSize = target.TotalSize
	}

	// Find missing bundles
	for bundleNum := range targetMap {
		if _, exists := localMap[bundleNum]; !exists {
			comparison.MissingBundles = append(comparison.MissingBundles, bundleNum)
		}
	}
	sort.Ints(comparison.MissingBundles)

	// Find extra bundles
	for bundleNum := range localMap {
		if _, exists := targetMap[bundleNum]; !exists {
			comparison.ExtraBundles = append(comparison.ExtraBundles, bundleNum)
		}
	}
	sort.Ints(comparison.ExtraBundles)

	// Compare hashes
	for bundleNum, localMeta := range localMap {
		if targetMeta, exists := targetMap[bundleNum]; exists {
			comparison.CommonCount++

			chainMismatch := localMeta.Hash != targetMeta.Hash
			contentMismatch := localMeta.ContentHash != targetMeta.ContentHash

			if chainMismatch || contentMismatch {
				mismatch := HashMismatch{
					BundleNumber:      bundleNum,
					LocalHash:         localMeta.Hash,
					TargetHash:        targetMeta.Hash,
					LocalContentHash:  localMeta.ContentHash,
					TargetContentHash: targetMeta.ContentHash,
				}

				if chainMismatch {
					comparison.HashMismatches = append(comparison.HashMismatches, mismatch)
				}
				if contentMismatch && !chainMismatch {
					comparison.ContentMismatches = append(comparison.ContentMismatches, mismatch)
				}
			}
		}
	}

	return comparison
}

func displayComparison(c *IndexComparison, verbose bool) {
	fmt.Printf("Comparison Results\n")
	fmt.Printf("══════════════════\n\n")

	fmt.Printf("Summary\n───────\n")
	fmt.Printf("  Local bundles:      %d\n", c.LocalCount)
	fmt.Printf("  Target bundles:     %d\n", c.TargetCount)
	fmt.Printf("  Common bundles:     %d\n", c.CommonCount)
	fmt.Printf("  Missing bundles:    %s\n", formatCount(len(c.MissingBundles)))
	fmt.Printf("  Extra bundles:      %s\n", formatCount(len(c.ExtraBundles)))
	fmt.Printf("  Hash mismatches:    %s\n", formatCountCritical(len(c.HashMismatches)))
	fmt.Printf("  Content mismatches: %s\n", formatCount(len(c.ContentMismatches)))

	if c.LocalCount > 0 {
		fmt.Printf("\n  Local range:        %06d - %06d\n", c.LocalRange[0], c.LocalRange[1])
		fmt.Printf("  Local size:         %.2f MB\n", float64(c.LocalTotalSize)/(1024*1024))
		fmt.Printf("  Local updated:      %s\n", c.LocalUpdated.Format("2006-01-02 15:04:05"))
	}

	if c.TargetCount > 0 {
		fmt.Printf("\n  Target range:       %06d - %06d\n", c.TargetRange[0], c.TargetRange[1])
		fmt.Printf("  Target size:        %.2f MB\n", float64(c.TargetTotalSize)/(1024*1024))
		fmt.Printf("  Target updated:     %s\n", c.TargetUpdated.Format("2006-01-02 15:04:05"))
	}

	// Show differences
	if len(c.HashMismatches) > 0 {
		showHashMismatches(c.HashMismatches, verbose)
	}

	if len(c.MissingBundles) > 0 {
		showMissingBundles(c.MissingBundles, verbose)
	}

	if len(c.ExtraBundles) > 0 {
		showExtraBundles(c.ExtraBundles, verbose)
	}

	// Final status
	fmt.Printf("\n")
	if !c.HasDifferences() {
		fmt.Printf("✓ Indexes are identical\n")
	} else {
		fmt.Printf("✗ Indexes have differences\n")
		if len(c.HashMismatches) > 0 {
			fmt.Printf("\n⚠️  WARNING: Chain hash mismatches detected!\n")
			fmt.Printf("This indicates different bundle content or chain integrity issues.\n")
		}
	}
}

func showHashMismatches(mismatches []HashMismatch, verbose bool) {
	fmt.Printf("\n⚠️  CHAIN HASH MISMATCHES (CRITICAL)\n")
	fmt.Printf("════════════════════════════════════\n\n")

	displayCount := len(mismatches)
	if displayCount > 10 && !verbose {
		displayCount = 10
	}

	for i := 0; i < displayCount; i++ {
		m := mismatches[i]
		fmt.Printf("  Bundle %06d:\n", m.BundleNumber)
		fmt.Printf("    Chain Hash:\n")
		fmt.Printf("      Local:  %s\n", m.LocalHash)
		fmt.Printf("      Target: %s\n", m.TargetHash)

		if m.LocalContentHash != m.TargetContentHash {
			fmt.Printf("    Content Hash (also differs):\n")
			fmt.Printf("      Local:  %s\n", m.LocalContentHash)
			fmt.Printf("      Target: %s\n", m.TargetContentHash)
		}
		fmt.Printf("\n")
	}

	if len(mismatches) > displayCount {
		fmt.Printf("  ... and %d more (use -v to show all)\n\n", len(mismatches)-displayCount)
	}
}

func showMissingBundles(bundles []int, verbose bool) {
	fmt.Printf("\nMissing Bundles (in target but not local)\n")
	fmt.Printf("──────────────────────────────────────────\n")

	if verbose || len(bundles) <= 20 {
		displayCount := len(bundles)
		if displayCount > 20 && !verbose {
			displayCount = 20
		}

		for i := 0; i < displayCount; i++ {
			fmt.Printf("  %06d\n", bundles[i])
		}

		if len(bundles) > displayCount {
			fmt.Printf("  ... and %d more (use -v to show all)\n", len(bundles)-displayCount)
		}
	} else {
		displayBundleRanges(bundles)
	}
}

func showExtraBundles(bundles []int, verbose bool) {
	fmt.Printf("\nExtra Bundles (in local but not target)\n")
	fmt.Printf("────────────────────────────────────────\n")

	if verbose || len(bundles) <= 20 {
		displayCount := len(bundles)
		if displayCount > 20 && !verbose {
			displayCount = 20
		}

		for i := 0; i < displayCount; i++ {
			fmt.Printf("  %06d\n", bundles[i])
		}

		if len(bundles) > displayCount {
			fmt.Printf("  ... and %d more (use -v to show all)\n", len(bundles)-displayCount)
		}
	} else {
		displayBundleRanges(bundles)
	}
}

func displayBundleRanges(bundles []int) {
	if len(bundles) == 0 {
		return
	}

	rangeStart := bundles[0]
	rangeEnd := bundles[0]

	for i := 1; i < len(bundles); i++ {
		if bundles[i] == rangeEnd+1 {
			rangeEnd = bundles[i]
		} else {
			if rangeStart == rangeEnd {
				fmt.Printf("  %06d\n", rangeStart)
			} else {
				fmt.Printf("  %06d - %06d\n", rangeStart, rangeEnd)
			}
			rangeStart = bundles[i]
			rangeEnd = bundles[i]
		}
	}

	if rangeStart == rangeEnd {
		fmt.Printf("  %06d\n", rangeStart)
	} else {
		fmt.Printf("  %06d - %06d\n", rangeStart, rangeEnd)
	}
}

func fetchMissingBundles(mgr BundleManager, baseURL string, missingBundles []int) error {
	client := &http.Client{Timeout: 60 * time.Second}

	successCount := 0
	errorCount := 0

	info := mgr.GetInfo()
	bundleDir := info["bundle_dir"].(string)

	for _, bundleNum := range missingBundles {
		fmt.Printf("Fetching bundle %06d... ", bundleNum)

		url := fmt.Sprintf("%s/data/%d", baseURL, bundleNum)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			errorCount++
			continue
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("ERROR: status %d\n", resp.StatusCode)
			resp.Body.Close()
			errorCount++
			continue
		}

		filename := fmt.Sprintf("%06d.jsonl.zst", bundleNum)
		filepath := filepath.Join(bundleDir, filename)

		outFile, err := os.Create(filepath)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			resp.Body.Close()
			errorCount++
			continue
		}

		_, err = io.Copy(outFile, resp.Body)
		outFile.Close()
		resp.Body.Close()

		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			os.Remove(filepath)
			errorCount++
			continue
		}

		fmt.Printf("✓\n")
		successCount++
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("\n✓ Fetch complete: %d succeeded, %d failed\n", successCount, errorCount)

	if errorCount > 0 {
		return fmt.Errorf("some bundles failed to download")
	}

	return nil
}

// Types

type IndexComparison struct {
	LocalCount        int
	TargetCount       int
	CommonCount       int
	MissingBundles    []int
	ExtraBundles      []int
	HashMismatches    []HashMismatch
	ContentMismatches []HashMismatch
	LocalRange        [2]int
	TargetRange       [2]int
	LocalTotalSize    int64
	TargetTotalSize   int64
	LocalUpdated      time.Time
	TargetUpdated     time.Time
}

type HashMismatch struct {
	BundleNumber      int
	LocalHash         string
	TargetHash        string
	LocalContentHash  string
	TargetContentHash string
}

func (ic *IndexComparison) HasDifferences() bool {
	return len(ic.MissingBundles) > 0 || len(ic.ExtraBundles) > 0 ||
		len(ic.HashMismatches) > 0 || len(ic.ContentMismatches) > 0
}

func formatCount(count int) string {
	if count == 0 {
		return "\033[32m0 ✓\033[0m"
	}
	return fmt.Sprintf("\033[33m%d ⚠️\033[0m", count)
}

func formatCountCritical(count int) string {
	if count == 0 {
		return "\033[32m0 ✓\033[0m"
	}
	return fmt.Sprintf("\033[31m%d ✗\033[0m", count)
}
