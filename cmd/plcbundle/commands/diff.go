// repo/cmd/plcbundle/commands/diff.go
package commands

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

func NewDiffCommand() *cobra.Command {
	var (
		verbose        bool
		bundleNum      int
		showOperations bool
		showSample     int
	)

	cmd := &cobra.Command{
		Use:     "diff <target>",
		Aliases: []string{"compare"},
		Short:   "Compare repositories",
		Long: `Compare local repository against remote or local target

Compares bundle indexes to find differences such as:
  • Missing bundles (in target but not local)
  • Extra bundles (in local but not target)
  • Hash mismatches (different content)
  • Content mismatches (different data)

For deeper analysis of specific bundles, use --bundle flag to see
detailed differences in metadata and operations.

The target can be:
  • Remote HTTP URL (e.g., https://plc.example.com)
  • Remote index URL (e.g., https://plc.example.com/index.json)
  • Local file path (e.g., /path/to/plc_bundles.json)`,

		Example: `  # High-level comparison
  plcbundle diff https://plc.example.com

  # Show all differences (verbose)
  plcbundle diff https://plc.example.com -v

  # Deep dive into specific bundle
  plcbundle diff https://plc.example.com --bundle 23

  # Compare bundle with operation samples
  plcbundle diff https://plc.example.com --bundle 23 --show-operations

  # Show first 50 operations
  plcbundle diff https://plc.example.com --bundle 23 --sample 50

  # Using alias
  plcbundle compare https://plc.example.com`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			target := args[0]

			mgr, dir, err := getManagerFromCommand(cmd, "")
			if err != nil {
				return err
			}
			defer mgr.Close()

			// If specific bundle requested, do detailed diff
			if bundleNum > 0 {
				return diffSpecificBundle(mgr, target, bundleNum, showOperations, showSample)
			}

			// Otherwise, do high-level index comparison
			return diffIndexes(mgr, dir, target, verbose)
		},
	}

	// Flags
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Show all differences (verbose output)")
	cmd.Flags().IntVarP(&bundleNum, "bundle", "b", 0, "Deep diff of specific bundle")
	cmd.Flags().BoolVar(&showOperations, "show-operations", false, "Show operation differences (use with --bundle)")
	cmd.Flags().IntVar(&showSample, "sample", 10, "Number of sample operations to show (use with --bundle)")

	return cmd
}

// diffIndexes performs high-level index comparison
func diffIndexes(mgr BundleManager, dir string, target string, verbose bool) error {
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
	displayComparison(comparison, verbose)

	// If there are hash mismatches, suggest deep dive
	if len(comparison.HashMismatches) > 0 {
		fmt.Printf("\n💡 Tip: Use --bundle flag to investigate specific mismatches:\n")
		fmt.Printf("   plcbundle diff %s --bundle %d --show-operations\n",
			target, comparison.HashMismatches[0].BundleNumber)
	}

	if comparison.HasDifferences() {
		return fmt.Errorf("indexes have differences")
	}

	return nil
}

// diffSpecificBundle performs detailed comparison of a specific bundle
func diffSpecificBundle(mgr BundleManager, target string, bundleNum int, showOps bool, sampleSize int) error {
	ctx := context.Background()

	fmt.Printf("Deep Diff: Bundle %06d\n", bundleNum)
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	// Load local bundle
	fmt.Printf("Loading local bundle %06d...\n", bundleNum)
	localBundle, err := mgr.LoadBundle(ctx, bundleNum)
	if err != nil {
		return fmt.Errorf("failed to load local bundle: %w", err)
	}

	// Load remote index to get metadata
	fmt.Printf("Loading remote index...\n")
	remoteIndex, err := loadTargetIndex(target)
	if err != nil {
		return fmt.Errorf("failed to load remote index: %w", err)
	}

	remoteMeta, err := remoteIndex.GetBundle(bundleNum)
	if err != nil {
		return fmt.Errorf("bundle not found in remote index: %w", err)
	}

	// Load remote bundle
	fmt.Printf("Loading remote bundle %06d...\n\n", bundleNum)
	remoteOps, err := loadRemoteBundle(target, bundleNum)
	if err != nil {
		return fmt.Errorf("failed to load remote bundle: %w", err)
	}

	// Compare metadata
	displayBundleMetadataComparison(localBundle, remoteMeta)

	// Compare operations
	if showOps {
		fmt.Printf("\n")
		displayOperationComparison(localBundle.Operations, remoteOps, sampleSize)
	}

	// Compare hashes in detail
	fmt.Printf("\n")
	displayHashAnalysis(localBundle, remoteMeta)

	return nil
}

// displayBundleMetadataComparison shows metadata comparison
func displayBundleMetadataComparison(local *bundle.Bundle, remote *bundleindex.BundleMetadata) {
	fmt.Printf("Metadata Comparison\n")
	fmt.Printf("───────────────────\n\n")

	// Basic info
	fmt.Printf("  Bundle Number:      %06d\n", local.BundleNumber)

	// Times
	timeMatch := local.StartTime.Equal(remote.StartTime) && local.EndTime.Equal(remote.EndTime)
	fmt.Printf("  Start Time:         %s  %s\n",
		formatTimeDiff(local.StartTime, remote.StartTime),
		statusIcon(timeMatch))
	fmt.Printf("    Local:  %s\n", local.StartTime.Format(time.RFC3339))
	fmt.Printf("    Remote: %s\n", remote.StartTime.Format(time.RFC3339))

	fmt.Printf("  End Time:           %s  %s\n",
		formatTimeDiff(local.EndTime, remote.EndTime),
		statusIcon(timeMatch))
	fmt.Printf("    Local:  %s\n", local.EndTime.Format(time.RFC3339))
	fmt.Printf("    Remote: %s\n", remote.EndTime.Format(time.RFC3339))

	// Counts
	opCountMatch := len(local.Operations) == remote.OperationCount
	didCountMatch := local.DIDCount == remote.DIDCount

	fmt.Printf("  Operation Count:    %s  %s\n",
		formatCountDiff(len(local.Operations), remote.OperationCount),
		statusIcon(opCountMatch))
	fmt.Printf("  DID Count:          %s  %s\n",
		formatCountDiff(local.DIDCount, remote.DIDCount),
		statusIcon(didCountMatch))

	// Sizes
	sizeMatch := local.CompressedSize == remote.CompressedSize
	fmt.Printf("  Compressed Size:    %s  %s\n",
		formatSizeDiff(local.CompressedSize, remote.CompressedSize),
		statusIcon(sizeMatch))

	uncompMatch := local.UncompressedSize == remote.UncompressedSize
	fmt.Printf("  Uncompressed Size:  %s  %s\n",
		formatSizeDiff(local.UncompressedSize, remote.UncompressedSize),
		statusIcon(uncompMatch))
}

// displayHashAnalysis shows detailed hash comparison
func displayHashAnalysis(local *bundle.Bundle, remote *bundleindex.BundleMetadata) {
	fmt.Printf("Hash Analysis\n")
	fmt.Printf("═════════════\n\n")

	// Content hash (most important)
	contentMatch := local.ContentHash == remote.ContentHash
	fmt.Printf("  Content Hash:       %s\n", statusIcon(contentMatch))
	fmt.Printf("    Local:  %s\n", local.ContentHash)
	fmt.Printf("    Remote: %s\n", remote.ContentHash)
	if !contentMatch {
		fmt.Printf("    ⚠️  Different bundle content!\n")
	}
	fmt.Printf("\n")

	// Compressed hash
	compMatch := local.CompressedHash == remote.CompressedHash
	fmt.Printf("  Compressed Hash:    %s\n", statusIcon(compMatch))
	fmt.Printf("    Local:  %s\n", local.CompressedHash)
	fmt.Printf("    Remote: %s\n", remote.CompressedHash)
	if !compMatch && contentMatch {
		fmt.Printf("    ℹ️  Different compression (same content)\n")
	}
	fmt.Printf("\n")

	// Chain hash
	chainMatch := local.Hash == remote.Hash
	fmt.Printf("  Chain Hash:         %s\n", statusIcon(chainMatch))
	fmt.Printf("    Local:  %s\n", local.Hash)
	fmt.Printf("    Remote: %s\n", remote.Hash)
	if !chainMatch {
		// Analyze why chain hash differs
		parentMatch := local.Parent == remote.Parent
		fmt.Printf("\n  Chain Components:\n")
		fmt.Printf("    Parent:  %s\n", statusIcon(parentMatch))
		fmt.Printf("      Local:  %s\n", local.Parent)
		fmt.Printf("      Remote: %s\n", remote.Parent)

		if !parentMatch {
			fmt.Printf("    ⚠️  Different parent → chain diverged at earlier bundle\n")
		} else if !contentMatch {
			fmt.Printf("    ⚠️  Same parent but different content → different operations\n")
		}
	}
}

// displayOperationComparison shows operation differences
func displayOperationComparison(localOps []plcclient.PLCOperation, remoteOps []plcclient.PLCOperation, sampleSize int) {
	fmt.Printf("Operation Comparison\n")
	fmt.Printf("════════════════════\n\n")

	if len(localOps) != len(remoteOps) {
		fmt.Printf("  ⚠️  Different operation counts: local=%d, remote=%d\n\n",
			len(localOps), len(remoteOps))
	}

	// Build CID sets for comparison
	localCIDs := make(map[string]int)
	remoteCIDs := make(map[string]int)

	for i, op := range localOps {
		localCIDs[op.CID] = i
	}
	for i, op := range remoteOps {
		remoteCIDs[op.CID] = i
	}

	// Find differences
	var missingInLocal []string
	var missingInRemote []string
	var positionMismatches []string

	for cid, remotePos := range remoteCIDs {
		if localPos, exists := localCIDs[cid]; !exists {
			missingInLocal = append(missingInLocal, cid)
		} else if localPos != remotePos {
			positionMismatches = append(positionMismatches, cid)
		}
	}

	for cid := range localCIDs {
		if _, exists := remoteCIDs[cid]; !exists {
			missingInRemote = append(missingInRemote, cid)
		}
	}

	// Display differences
	if len(missingInLocal) > 0 {
		fmt.Printf("  Missing in Local (%d operations):\n", len(missingInLocal))
		displaySample := min(sampleSize, len(missingInLocal))
		for i := 0; i < displaySample; i++ {
			cid := missingInLocal[i]
			pos := remoteCIDs[cid]
			fmt.Printf("    - [%04d] %s\n", pos, cid)
		}
		if len(missingInLocal) > displaySample {
			fmt.Printf("    ... and %d more\n", len(missingInLocal)-displaySample)
		}
		fmt.Printf("\n")
	}

	if len(missingInRemote) > 0 {
		fmt.Printf("  Missing in Remote (%d operations):\n", len(missingInRemote))
		displaySample := min(sampleSize, len(missingInRemote))
		for i := 0; i < displaySample; i++ {
			cid := missingInRemote[i]
			pos := localCIDs[cid]
			fmt.Printf("    + [%04d] %s\n", pos, cid)
		}
		if len(missingInRemote) > displaySample {
			fmt.Printf("    ... and %d more\n", len(missingInRemote)-displaySample)
		}
		fmt.Printf("\n")
	}

	if len(positionMismatches) > 0 {
		fmt.Printf("  Position Mismatches (%d operations):\n", len(positionMismatches))
		displaySample := min(sampleSize, len(positionMismatches))
		for i := 0; i < displaySample; i++ {
			cid := positionMismatches[i]
			localPos := localCIDs[cid]
			remotePos := remoteCIDs[cid]
			fmt.Printf("    ~ %s\n", cid)
			fmt.Printf("      Local:  position %04d\n", localPos)
			fmt.Printf("      Remote: position %04d\n", remotePos)
		}
		if len(positionMismatches) > displaySample {
			fmt.Printf("    ... and %d more\n", len(positionMismatches)-displaySample)
		}
		fmt.Printf("\n")
	}

	if len(missingInLocal) == 0 && len(missingInRemote) == 0 && len(positionMismatches) == 0 {
		fmt.Printf("  ✓ All operations match (same CIDs, same order)\n\n")
	}

	// Show sample operations for context
	if len(localOps) > 0 {
		fmt.Printf("Sample Operations (first %d):\n", min(sampleSize, len(localOps)))
		fmt.Printf("────────────────────────────────\n")
		for i := 0; i < min(sampleSize, len(localOps)); i++ {
			op := localOps[i]
			remoteMatch := ""
			if remotePos, exists := remoteCIDs[op.CID]; exists {
				if remotePos == i {
					remoteMatch = " ✓"
				} else {
					remoteMatch = fmt.Sprintf(" ~ (remote pos: %04d)", remotePos)
				}
			} else {
				remoteMatch = " ✗ (missing in remote)"
			}

			fmt.Printf("  [%04d] %s%s\n", i, op.CID, remoteMatch)
			fmt.Printf("         DID: %s\n", op.DID)
			fmt.Printf("         Time: %s\n", op.CreatedAt.Format(time.RFC3339))
		}
		fmt.Printf("\n")
	}
}

// loadRemoteBundle loads a bundle from remote server
func loadRemoteBundle(baseURL string, bundleNum int) ([]plcclient.PLCOperation, error) {
	// Determine the data URL
	url := baseURL
	if !strings.HasSuffix(url, ".json") {
		url = strings.TrimSuffix(url, "/")
	}
	url = strings.TrimSuffix(url, "/index.json")
	url = strings.TrimSuffix(url, "/plc_bundles.json")
	url = fmt.Sprintf("%s/jsonl/%d", url, bundleNum)

	client := &http.Client{Timeout: 60 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Parse JSONL
	var operations []plcclient.PLCOperation
	decoder := json.NewDecoder(resp.Body)

	for {
		var op plcclient.PLCOperation
		if err := decoder.Decode(&op); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to parse operation: %w", err)
		}
		operations = append(operations, op)
	}

	return operations, nil
}

// loadTargetIndex loads index from URL or local path
func loadTargetIndex(target string) (*bundleindex.Index, error) {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		return loadIndexFromURL(target)
	}
	return bundleindex.LoadIndex(target)
}

// loadIndexFromURL loads index from remote URL
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

// compareIndexes performs the actual comparison
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

// displayComparison shows comparison results
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

// showHashMismatches displays hash mismatches
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

// showMissingBundles displays missing bundles
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

// showExtraBundles displays extra bundles
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

// displayBundleRanges displays bundles as ranges
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

// Helper formatting functions

func statusIcon(match bool) string {
	if match {
		return "✓"
	}
	return "✗"
}

func formatTimeDiff(local, remote time.Time) string {
	if local.Equal(remote) {
		return "identical"
	}
	diff := local.Sub(remote)
	if diff < 0 {
		diff = -diff
	}
	return fmt.Sprintf("differs by %s", diff)
}

func formatCountDiff(local, remote int) string {
	if local == remote {
		return fmt.Sprintf("%d", local)
	}
	return fmt.Sprintf("local=%d, remote=%d", local, remote)
}

func formatSizeDiff(local, remote int64) string {
	if local == remote {
		return formatBytes(local)
	}
	diff := local - remote
	sign := "+"
	if diff < 0 {
		sign = "-"
		diff = -diff
	}
	return fmt.Sprintf("local=%s, remote=%s (%s%s)",
		formatBytes(local), formatBytes(remote), sign, formatBytes(diff))
}

// IndexComparison holds comparison results
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

// HashMismatch represents a hash mismatch between bundles
type HashMismatch struct {
	BundleNumber      int
	LocalHash         string
	TargetHash        string
	LocalContentHash  string
	TargetContentHash string
}

// HasDifferences checks if there are any differences
func (ic *IndexComparison) HasDifferences() bool {
	return len(ic.MissingBundles) > 0 || len(ic.ExtraBundles) > 0 ||
		len(ic.HashMismatches) > 0 || len(ic.ContentMismatches) > 0
}

// formatCount formats count with color coding
func formatCount(count int) string {
	if count == 0 {
		return "\033[32m0 ✓\033[0m"
	}
	return fmt.Sprintf("\033[33m%d ⚠️\033[0m", count)
}

// formatCountCritical formats count with critical color coding
func formatCountCritical(count int) string {
	if count == 0 {
		return "\033[32m0 ✓\033[0m"
	}
	return fmt.Sprintf("\033[31m%d ✗\033[0m", count)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
