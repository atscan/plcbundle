package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/atscan/plcbundle/bundle"
)

// IndexComparison holds comparison results
type IndexComparison struct {
	LocalCount      int
	TargetCount     int
	CommonCount     int
	MissingBundles  []int // In target but not in local
	ExtraBundles    []int // In local but not in target
	HashMismatches  []HashMismatch
	LocalRange      [2]int
	TargetRange     [2]int
	LocalTotalSize  int64
	TargetTotalSize int64
	LocalUpdated    time.Time
	TargetUpdated   time.Time
}

type HashMismatch struct {
	BundleNumber int
	LocalHash    string
	TargetHash   string
}

func (ic *IndexComparison) HasDifferences() bool {
	return len(ic.MissingBundles) > 0 || len(ic.ExtraBundles) > 0 || len(ic.HashMismatches) > 0
}

// loadTargetIndex loads an index from a file or URL
func loadTargetIndex(target string) (*bundle.Index, error) {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		// Load from URL
		return loadIndexFromURL(target)
	}

	// Load from file
	return bundle.LoadIndex(target)
}

// loadIndexFromURL downloads and parses an index from a URL
func loadIndexFromURL(url string) (*bundle.Index, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

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

	var idx bundle.Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	return &idx, nil
}

// compareIndexes compares two indexes
func compareIndexes(local, target *bundle.Index) *IndexComparison {
	localBundles := local.GetBundles()
	targetBundles := target.GetBundles()

	// Create maps for quick lookup
	localMap := make(map[int]*bundle.BundleMetadata)
	targetMap := make(map[int]*bundle.BundleMetadata)

	for _, b := range localBundles {
		localMap[b.BundleNumber] = b
	}
	for _, b := range targetBundles {
		targetMap[b.BundleNumber] = b
	}

	comparison := &IndexComparison{
		LocalCount:     len(localBundles),
		TargetCount:    len(targetBundles),
		MissingBundles: make([]int, 0),
		ExtraBundles:   make([]int, 0),
		HashMismatches: make([]HashMismatch, 0),
	}

	// Get ranges
	if len(localBundles) > 0 {
		comparison.LocalRange = [2]int{localBundles[0].BundleNumber, localBundles[len(localBundles)-1].BundleNumber}
		comparison.LocalUpdated = local.UpdatedAt
		localStats := local.GetStats()
		comparison.LocalTotalSize = localStats["total_size"].(int64)
	}

	if len(targetBundles) > 0 {
		comparison.TargetRange = [2]int{targetBundles[0].BundleNumber, targetBundles[len(targetBundles)-1].BundleNumber}
		comparison.TargetUpdated = target.UpdatedAt
		targetStats := target.GetStats()
		comparison.TargetTotalSize = targetStats["total_size"].(int64)
	}

	// Find missing bundles (in target but not in local)
	for bundleNum := range targetMap {
		if _, exists := localMap[bundleNum]; !exists {
			comparison.MissingBundles = append(comparison.MissingBundles, bundleNum)
		}
	}
	sort.Ints(comparison.MissingBundles)

	// Find extra bundles (in local but not in target)
	for bundleNum := range localMap {
		if _, exists := targetMap[bundleNum]; !exists {
			comparison.ExtraBundles = append(comparison.ExtraBundles, bundleNum)
		}
	}
	sort.Ints(comparison.ExtraBundles)

	// Find hash mismatches
	for bundleNum, localMeta := range localMap {
		if targetMeta, exists := targetMap[bundleNum]; exists {
			comparison.CommonCount++
			if localMeta.CompressedHash != targetMeta.CompressedHash {
				comparison.HashMismatches = append(comparison.HashMismatches, HashMismatch{
					BundleNumber: bundleNum,
					LocalHash:    localMeta.CompressedHash,
					TargetHash:   targetMeta.CompressedHash,
				})
			}
		}
	}

	return comparison
}

// displayComparison displays the comparison results
func displayComparison(c *IndexComparison, verbose bool) {
	fmt.Printf("Comparison Results\n")
	fmt.Printf("══════════════════\n\n")

	// Summary
	fmt.Printf("Summary\n")
	fmt.Printf("───────\n")
	fmt.Printf("  Local bundles:    %d\n", c.LocalCount)
	fmt.Printf("  Target bundles:   %d\n", c.TargetCount)
	fmt.Printf("  Common bundles:   %d\n", c.CommonCount)
	fmt.Printf("  Missing bundles:  %d\n", len(c.MissingBundles))
	fmt.Printf("  Extra bundles:    %d\n", len(c.ExtraBundles))
	fmt.Printf("  Hash mismatches:  %d\n", len(c.HashMismatches))

	if c.LocalCount > 0 {
		fmt.Printf("\n  Local range:      %06d - %06d\n", c.LocalRange[0], c.LocalRange[1])
		fmt.Printf("  Local size:       %.2f MB\n", float64(c.LocalTotalSize)/(1024*1024))
		fmt.Printf("  Local updated:    %s\n", c.LocalUpdated.Format("2006-01-02 15:04:05"))
	}

	if c.TargetCount > 0 {
		fmt.Printf("\n  Target range:     %06d - %06d\n", c.TargetRange[0], c.TargetRange[1])
		fmt.Printf("  Target size:      %.2f MB\n", float64(c.TargetTotalSize)/(1024*1024))
		fmt.Printf("  Target updated:   %s\n", c.TargetUpdated.Format("2006-01-02 15:04:05"))
	}

	// Missing bundles
	if len(c.MissingBundles) > 0 {
		fmt.Printf("\n")
		fmt.Printf("Missing Bundles (in target but not local)\n")
		fmt.Printf("──────────────────────────────────────────\n")

		if verbose || len(c.MissingBundles) <= 20 {
			// Show all or up to 20
			displayCount := len(c.MissingBundles)
			if displayCount > 20 && !verbose {
				displayCount = 20
			}

			for i := 0; i < displayCount; i++ {
				fmt.Printf("  %06d\n", c.MissingBundles[i])
			}

			if len(c.MissingBundles) > displayCount {
				fmt.Printf("  ... and %d more (use -v to show all)\n", len(c.MissingBundles)-displayCount)
			}
		} else {
			// Show ranges
			displayBundleRanges(c.MissingBundles)
		}
	}

	// Extra bundles
	if len(c.ExtraBundles) > 0 {
		fmt.Printf("\n")
		fmt.Printf("Extra Bundles (in local but not target)\n")
		fmt.Printf("────────────────────────────────────────\n")

		if verbose || len(c.ExtraBundles) <= 20 {
			displayCount := len(c.ExtraBundles)
			if displayCount > 20 && !verbose {
				displayCount = 20
			}

			for i := 0; i < displayCount; i++ {
				fmt.Printf("  %06d\n", c.ExtraBundles[i])
			}

			if len(c.ExtraBundles) > displayCount {
				fmt.Printf("  ... and %d more (use -v to show all)\n", len(c.ExtraBundles)-displayCount)
			}
		} else {
			displayBundleRanges(c.ExtraBundles)
		}
	}

	// Hash mismatches
	if len(c.HashMismatches) > 0 {
		fmt.Printf("\n")
		fmt.Printf("Hash Mismatches\n")
		fmt.Printf("───────────────\n")

		displayCount := len(c.HashMismatches)
		if displayCount > 10 && !verbose {
			displayCount = 10
		}

		for i := 0; i < displayCount; i++ {
			m := c.HashMismatches[i]
			fmt.Printf("  Bundle %06d:\n", m.BundleNumber)
			fmt.Printf("    Local:  %s\n", m.LocalHash[:16]+"...")
			fmt.Printf("    Target: %s\n", m.TargetHash[:16]+"...")
		}

		if len(c.HashMismatches) > displayCount {
			fmt.Printf("  ... and %d more (use -v to show all)\n", len(c.HashMismatches)-displayCount)
		}
	}

	// Final status
	fmt.Printf("\n")
	if !c.HasDifferences() {
		fmt.Printf("✓ Indexes are identical\n")
	} else {
		fmt.Printf("✗ Indexes have differences\n")
	}
}

// displayBundleRanges displays bundle numbers as ranges
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
			// Print current range
			if rangeStart == rangeEnd {
				fmt.Printf("  %06d\n", rangeStart)
			} else {
				fmt.Printf("  %06d - %06d\n", rangeStart, rangeEnd)
			}
			rangeStart = bundles[i]
			rangeEnd = bundles[i]
		}
	}

	// Print last range
	if rangeStart == rangeEnd {
		fmt.Printf("  %06d\n", rangeStart)
	} else {
		fmt.Printf("  %06d - %06d\n", rangeStart, rangeEnd)
	}
}

// fetchMissingBundles downloads missing bundles from target server
func fetchMissingBundles(mgr *bundle.Manager, baseURL string, missingBundles []int) {
	client := &http.Client{
		Timeout: 60 * time.Second,
	}

	successCount := 0
	errorCount := 0

	for _, bundleNum := range missingBundles {
		fmt.Printf("Fetching bundle %06d... ", bundleNum)

		// Download bundle data
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

		// Save to file
		filename := fmt.Sprintf("%06d.jsonl.zst", bundleNum)
		filepath := filepath.Join(mgr.GetInfo()["bundle_dir"].(string), filename)

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

		// Scan and index the bundle
		_, err = mgr.ScanAndIndexBundle(filepath, bundleNum)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			errorCount++
			continue
		}

		fmt.Printf("✓\n")
		successCount++

		// Small delay to be nice
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("\n")
	fmt.Printf("✓ Fetch complete: %d succeeded, %d failed\n", successCount, errorCount)
}
