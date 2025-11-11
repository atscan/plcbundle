package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/internal/storage"
)

// ============================================================================
// TYPES (defined at package level to avoid conflicts)
// ============================================================================

type DIDActivity struct {
	DID   string
	Count int
}

type DomainCount struct {
	Domain string
	Count  int
}

type EndpointCount struct {
	Endpoint string
	Count    int
}

type TimeSlot struct {
	Time  time.Time
	Count int
}

type inspectOptions struct {
	showJSON     bool
	verify       bool
	showSamples  bool
	sampleCount  int
	skipMetadata bool
	skipPatterns bool
	skipCrypto   bool
	verbose      bool
}

type inspectResult struct {
	// Metadata
	Metadata *storage.BundleMetadata

	// Basic stats
	FilePath         string
	FileSize         int64
	HasMetadataFrame bool
	HasFrameIndex    bool

	// Operation analysis
	TotalOps       int
	NullifiedOps   int
	ActiveOps      int
	UniqueDIDs     int
	OperationTypes map[string]int

	// DID patterns
	TopDIDs      []DIDActivity
	SingleOpDIDs int
	MultiOpDIDs  int

	// Handle patterns
	TotalHandles   int
	TopDomains     []DomainCount
	InvalidHandles int

	// Service patterns
	TotalServices   int
	UniqueEndpoints int
	TopPDSEndpoints []EndpointCount

	// Temporal
	TimeDistribution []TimeSlot
	AvgOpsPerMinute  float64

	// Size analysis
	AvgOpSize   int
	MinOpSize   int
	MaxOpSize   int
	TotalOpSize int64

	// Crypto verification
	ContentHashValid    bool
	CompressedHashValid bool
	MetadataValid       bool

	// Timing
	LoadTime    time.Duration
	AnalyzeTime time.Duration
	VerifyTime  time.Duration
	TotalTime   time.Duration
}

type bundleAnalysis struct {
	TotalOps        int
	NullifiedOps    int
	ActiveOps       int
	UniqueDIDs      int
	OperationTypes  map[string]int
	SingleOpDIDs    int
	MultiOpDIDs     int
	TotalHandles    int
	InvalidHandles  int
	TotalServices   int
	UniqueEndpoints int
	AvgOpsPerMinute float64
	AvgOpSize       int
	MinOpSize       int
	MaxOpSize       int
	TotalOpSize     int64

	// For top-N calculations
	didActivity    map[string]int
	domainCounts   map[string]int
	endpointCounts map[string]int
	timeSlots      map[int64]int

	// Results
	TopDIDs          []DIDActivity
	TopDomains       []DomainCount
	TopPDSEndpoints  []EndpointCount
	TimeDistribution []TimeSlot
}

// ============================================================================
// COMMAND DEFINITION
// ============================================================================

func NewInspectCommand() *cobra.Command {
	var (
		showJSON     bool
		verify       bool
		showSamples  bool
		sampleCount  int
		skipMetadata bool
		skipPatterns bool
		skipCrypto   bool
	)

	cmd := &cobra.Command{
		Use:   "inspect <bundle-number|bundle-file>",
		Short: "Deep analysis of bundle contents",
		Long: `Deep analysis of bundle contents

Performs comprehensive analysis of a bundle including:
  • Embedded metadata (from skippable frame)
  • Operation type breakdown
  • DID activity patterns
  • Handle and domain statistics
  • Service endpoint analysis
  • Temporal distribution
  • Cryptographic verification
  • Size analysis

Can inspect either by bundle number (from repository) or direct file path.`,

		Example: `  # Inspect from repository
  plcbundle inspect 42

  # Inspect specific file
  plcbundle inspect /path/to/000042.jsonl.zst
  plcbundle inspect 000042.jsonl.zst

  # Skip certain analysis sections
  plcbundle inspect 42 --skip-patterns --skip-crypto

  # Show sample operations
  plcbundle inspect 42 --samples --sample-count 20

  # Verify all hashes
  plcbundle inspect 42 --verify

  # JSON output (for scripting)
  plcbundle inspect 42 --json`,

		Args: cobra.ExactArgs(1),

		RunE: func(cmd *cobra.Command, args []string) error {
			input := args[0]
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")

			return runInspect(cmd, input, inspectOptions{
				showJSON:     showJSON,
				verify:       verify,
				showSamples:  showSamples,
				sampleCount:  sampleCount,
				skipMetadata: skipMetadata,
				skipPatterns: skipPatterns,
				skipCrypto:   skipCrypto,
				verbose:      verbose,
			})
		},
	}

	cmd.Flags().BoolVar(&showJSON, "json", false, "Output as JSON")
	cmd.Flags().BoolVar(&verify, "verify", false, "Verify cryptographic hashes")
	cmd.Flags().BoolVar(&showSamples, "samples", false, "Show sample operations")
	cmd.Flags().IntVar(&sampleCount, "sample-count", 10, "Number of samples to show")
	cmd.Flags().BoolVar(&skipMetadata, "skip-metadata", false, "Skip embedded metadata section")
	cmd.Flags().BoolVar(&skipPatterns, "skip-patterns", false, "Skip pattern analysis")
	cmd.Flags().BoolVar(&skipCrypto, "skip-crypto", false, "Skip cryptographic verification")

	return cmd
}

// ============================================================================
// MAIN LOGIC
// ============================================================================

func runInspect(cmd *cobra.Command, input string, opts inspectOptions) error {
	totalStart := time.Now()

	// Determine if input is bundle number or file path
	bundlePath, bundleNum, err := resolveBundlePath(cmd, input)
	if err != nil {
		return err
	}

	result := &inspectResult{
		FilePath:        bundlePath,
		OperationTypes:  make(map[string]int),
		TopDIDs:         make([]DIDActivity, 0),
		TopDomains:      make([]DomainCount, 0),
		TopPDSEndpoints: make([]EndpointCount, 0),
	}

	// Check file exists
	info, err := os.Stat(bundlePath)
	if err != nil {
		return fmt.Errorf("bundle file not found: %w", err)
	}
	result.FileSize = info.Size()

	// Check for frame index
	indexPath := bundlePath + ".idx"
	if _, err := os.Stat(indexPath); err == nil {
		result.HasFrameIndex = true
	}

	fmt.Fprintf(os.Stderr, "Inspecting: %s\n", filepath.Base(bundlePath))
	fmt.Fprintf(os.Stderr, "File size: %s\n\n", formatBytes(result.FileSize))

	// SECTION 1: Extract embedded metadata (fast!)
	if !opts.skipMetadata {
		fmt.Fprintf(os.Stderr, "Reading embedded metadata...\n")
		metaStart := time.Now()

		ops := &storage.Operations{}
		meta, err := ops.ExtractBundleMetadata(bundlePath)
		if err != nil {
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "  No embedded metadata: %v\n", err)
			}
			result.HasMetadataFrame = false
		} else {
			result.HasMetadataFrame = true
			result.Metadata = meta
			if opts.verbose {
				fmt.Fprintf(os.Stderr, "  ✓ Extracted in %s\n", time.Since(metaStart))
			}
		}
		fmt.Fprintf(os.Stderr, "\n")
	}

	// SECTION 2: Load and analyze operations
	fmt.Fprintf(os.Stderr, "Loading and analyzing operations...\n")
	loadStart := time.Now()

	analysis, err := analyzeBundle(bundlePath, opts)
	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	result.LoadTime = time.Since(loadStart)
	result.TotalOps = analysis.TotalOps
	result.NullifiedOps = analysis.NullifiedOps
	result.ActiveOps = analysis.ActiveOps
	result.UniqueDIDs = analysis.UniqueDIDs
	result.OperationTypes = analysis.OperationTypes
	result.TopDIDs = analysis.TopDIDs
	result.SingleOpDIDs = analysis.SingleOpDIDs
	result.MultiOpDIDs = analysis.MultiOpDIDs
	result.TotalHandles = analysis.TotalHandles
	result.TopDomains = analysis.TopDomains
	result.InvalidHandles = analysis.InvalidHandles
	result.TotalServices = analysis.TotalServices
	result.UniqueEndpoints = analysis.UniqueEndpoints
	result.TopPDSEndpoints = analysis.TopPDSEndpoints
	result.TimeDistribution = analysis.TimeDistribution
	result.AvgOpsPerMinute = analysis.AvgOpsPerMinute
	result.AvgOpSize = analysis.AvgOpSize
	result.MinOpSize = analysis.MinOpSize
	result.MaxOpSize = analysis.MaxOpSize
	result.TotalOpSize = analysis.TotalOpSize

	fmt.Fprintf(os.Stderr, "  ✓ Analyzed in %s\n\n", result.LoadTime)

	// SECTION 3: Cryptographic verification
	if opts.verify && !opts.skipCrypto {
		fmt.Fprintf(os.Stderr, "Verifying cryptographic hashes...\n")
		verifyStart := time.Now()

		// ✅ Pass cmd parameter
		result.ContentHashValid, result.CompressedHashValid, result.MetadataValid =
			verifyCrypto(cmd, bundlePath, result.Metadata, bundleNum, opts.verbose)

		result.VerifyTime = time.Since(verifyStart)
		fmt.Fprintf(os.Stderr, "  ✓ Verified in %s\n\n", result.VerifyTime)
	}

	result.TotalTime = time.Since(totalStart)

	// Display results
	if opts.showJSON {
		return displayInspectJSON(result)
	}

	return displayInspectHuman(result, analysis, opts)
}

// ============================================================================
// ANALYSIS FUNCTIONS
// ============================================================================

func analyzeBundle(path string, opts inspectOptions) (*bundleAnalysis, error) {
	ops := &storage.Operations{}
	operations, err := ops.LoadBundle(path)
	if err != nil {
		return nil, err
	}

	analysis := &bundleAnalysis{
		TotalOps:       len(operations),
		OperationTypes: make(map[string]int),
		didActivity:    make(map[string]int),
		domainCounts:   make(map[string]int),
		endpointCounts: make(map[string]int),
		timeSlots:      make(map[int64]int),
	}

	// Analyze each operation
	for _, op := range operations {
		// Nullification
		if op.IsNullified() {
			analysis.NullifiedOps++
		} else {
			analysis.ActiveOps++
		}

		// DID activity
		analysis.didActivity[op.DID]++

		// Size stats
		opSize := len(op.RawJSON)
		if opSize == 0 {
			data, _ := json.Marshal(op)
			opSize = len(data)
		}

		analysis.TotalOpSize += int64(opSize)
		if analysis.MinOpSize == 0 || opSize < analysis.MinOpSize {
			analysis.MinOpSize = opSize
		}
		if opSize > analysis.MaxOpSize {
			analysis.MaxOpSize = opSize
		}

		// Parse operation for detailed analysis
		opData, err := op.GetOperationData()
		if err != nil || opData == nil {
			continue
		}

		// Operation type
		if opType, ok := opData["type"].(string); ok {
			analysis.OperationTypes[opType]++
		}

		// Handle analysis
		if !opts.skipPatterns {
			analyzeHandles(opData, analysis)
			analyzeServices(opData, analysis)
		}

		// Time distribution (group by minute)
		timeSlot := op.CreatedAt.Unix() / 60
		analysis.timeSlots[timeSlot]++
	}

	// Calculate derived stats
	analysis.UniqueDIDs = len(analysis.didActivity)
	if analysis.TotalOps > 0 {
		analysis.AvgOpSize = int(analysis.TotalOpSize / int64(analysis.TotalOps))
	}

	// Count single vs multi-op DIDs
	for _, count := range analysis.didActivity {
		if count == 1 {
			analysis.SingleOpDIDs++
		} else {
			analysis.MultiOpDIDs++
		}
	}

	// Top DIDs
	analysis.TopDIDs = getTopDIDs(analysis.didActivity, 10)

	// Top domains
	analysis.TopDomains = getTopDomains(analysis.domainCounts, 10)

	// Top endpoints
	analysis.TopPDSEndpoints = getTopEndpoints(analysis.endpointCounts, 10)

	// Unique endpoints
	analysis.UniqueEndpoints = len(analysis.endpointCounts)

	// Time distribution
	analysis.TimeDistribution = getTimeDistribution(analysis.timeSlots)

	// Calculate ops per minute
	if len(operations) > 1 {
		duration := operations[len(operations)-1].CreatedAt.Sub(operations[0].CreatedAt)
		if duration.Minutes() > 0 {
			analysis.AvgOpsPerMinute = float64(len(operations)) / duration.Minutes()
		}
	}

	return analysis, nil
}

func analyzeHandles(opData map[string]interface{}, analysis *bundleAnalysis) {
	if aka, ok := opData["alsoKnownAs"].([]interface{}); ok {
		for _, a := range aka {
			if akaStr, ok := a.(string); ok {
				if strings.HasPrefix(akaStr, "at://") {
					analysis.TotalHandles++

					// Extract domain
					handle := strings.TrimPrefix(akaStr, "at://")
					if idx := strings.Index(handle, "/"); idx > 0 {
						handle = handle[:idx]
					}

					// Count domain (TLD)
					parts := strings.Split(handle, ".")
					if len(parts) >= 2 {
						domain := parts[len(parts)-1]
						if len(parts) >= 2 {
							domain = parts[len(parts)-2] + "." + domain
						}
						analysis.domainCounts[domain]++
					}

					// Check for invalid patterns
					if strings.Contains(handle, "_") {
						analysis.InvalidHandles++
					}
				}
			}
		}
	}
}

func analyzeServices(opData map[string]interface{}, analysis *bundleAnalysis) {
	if services, ok := opData["services"].(map[string]interface{}); ok {
		analysis.TotalServices += len(services)

		// Extract PDS endpoints
		if pds, ok := services["atproto_pds"].(map[string]interface{}); ok {
			if endpoint, ok := pds["endpoint"].(string); ok {
				// Normalize endpoint
				endpoint = strings.TrimPrefix(endpoint, "https://")
				endpoint = strings.TrimPrefix(endpoint, "http://")
				if idx := strings.Index(endpoint, "/"); idx > 0 {
					endpoint = endpoint[:idx]
				}
				analysis.endpointCounts[endpoint]++
			}
		}
	}
}

func getTopDIDs(didActivity map[string]int, limit int) []DIDActivity {
	var results []DIDActivity
	for did, count := range didActivity {
		results = append(results, DIDActivity{DID: did, Count: count})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Count > results[j].Count
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

func getTopDomains(domainCounts map[string]int, limit int) []DomainCount {
	var results []DomainCount
	for domain, count := range domainCounts {
		results = append(results, DomainCount{Domain: domain, Count: count})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Count > results[j].Count
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

func getTopEndpoints(endpointCounts map[string]int, limit int) []EndpointCount {
	var results []EndpointCount
	for endpoint, count := range endpointCounts {
		results = append(results, EndpointCount{Endpoint: endpoint, Count: count})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Count > results[j].Count
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

func getTimeDistribution(timeSlots map[int64]int) []TimeSlot {
	var results []TimeSlot
	for slot, count := range timeSlots {
		results = append(results, TimeSlot{
			Time:  time.Unix(slot*60, 0),
			Count: count,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Time.Before(results[j].Time)
	})

	return results
}

// ============================================================================
// DISPLAY FUNCTIONS
// ============================================================================

func displayInspectHuman(result *inspectResult, analysis *bundleAnalysis, opts inspectOptions) error {
	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("                    Bundle Deep Inspection\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════\n\n")

	// File info
	fmt.Printf("📁 File Information\n")
	fmt.Printf("───────────────────\n")
	fmt.Printf("  Path:                %s\n", filepath.Base(result.FilePath))
	fmt.Printf("  Size:                %s\n", formatBytes(result.FileSize))
	fmt.Printf("  Has metadata frame:  %v\n", result.HasMetadataFrame)
	fmt.Printf("  Has frame index:     %v\n\n", result.HasFrameIndex)

	// Embedded metadata
	if result.HasMetadataFrame && result.Metadata != nil && !opts.skipMetadata {
		meta := result.Metadata
		fmt.Printf("📋 Embedded Metadata (Skippable Frame)\n")
		fmt.Printf("──────────────────────────────────────\n")
		fmt.Printf("  Bundle Number:       %06d\n", meta.BundleNumber)
		if meta.Origin != "" {
			fmt.Printf("  Origin:              %s\n", meta.Origin)
		}
		fmt.Printf("  Operations:          %s\n", formatNumber(meta.OperationCount))
		fmt.Printf("  DIDs:                %s unique\n", formatNumber(meta.DIDCount))
		fmt.Printf("  Frames:              %d\n", meta.FrameCount)
		fmt.Printf("  Uncompressed:        %s\n", formatBytes(meta.UncompressedSize))
		fmt.Printf("  Compressed:          %s (%.2fx)\n",
			formatBytes(meta.CompressedSize),
			float64(meta.UncompressedSize)/float64(meta.CompressedSize))
		fmt.Printf("  Timespan:            %s → %s\n",
			meta.StartTime.Format("2006-01-02 15:04:05"),
			meta.EndTime.Format("2006-01-02 15:04:05"))
		fmt.Printf("  Duration:            %s\n",
			formatDuration(meta.EndTime.Sub(meta.StartTime)))

		if meta.ContentHash != "" {
			fmt.Printf("\n  Hashes:\n")
			fmt.Printf("    Content:           %s\n", meta.ContentHash[:16]+"...")
			fmt.Printf("    Compressed:        %s\n", meta.CompressedHash[:16]+"...")
			if meta.ParentHash != "" {
				fmt.Printf("    Parent:            %s\n", meta.ParentHash[:16]+"...")
			}
		}
		fmt.Printf("\n")
	}

	// Operations breakdown
	fmt.Printf("📊 Operations Analysis\n")
	fmt.Printf("──────────────────────\n")
	fmt.Printf("  Total operations:    %s\n", formatNumber(result.TotalOps))
	fmt.Printf("  Active:              %s (%.1f%%)\n",
		formatNumber(result.ActiveOps),
		float64(result.ActiveOps)/float64(result.TotalOps)*100)
	if result.NullifiedOps > 0 {
		fmt.Printf("  Nullified:           %s (%.1f%%)\n",
			formatNumber(result.NullifiedOps),
			float64(result.NullifiedOps)/float64(result.TotalOps)*100)
	}

	if len(result.OperationTypes) > 0 {
		fmt.Printf("\n  Operation Types:\n")

		// Sort by count
		var types []struct {
			name  string
			count int
		}
		for name, count := range result.OperationTypes {
			types = append(types, struct {
				name  string
				count int
			}{name, count})
		}
		sort.Slice(types, func(i, j int) bool {
			return types[i].count > types[j].count
		})

		for _, t := range types {
			pct := float64(t.count) / float64(result.TotalOps) * 100
			fmt.Printf("    %-25s %s (%.1f%%)\n", t.name, formatNumber(t.count), pct)
		}
	}
	fmt.Printf("\n")

	// DID patterns
	fmt.Printf("👤 DID Activity Patterns\n")
	fmt.Printf("────────────────────────\n")
	fmt.Printf("  Unique DIDs:         %s\n", formatNumber(result.UniqueDIDs))
	fmt.Printf("  Single-op DIDs:      %s (%.1f%%)\n",
		formatNumber(result.SingleOpDIDs),
		float64(result.SingleOpDIDs)/float64(result.UniqueDIDs)*100)
	fmt.Printf("  Multi-op DIDs:       %s (%.1f%%)\n",
		formatNumber(result.MultiOpDIDs),
		float64(result.MultiOpDIDs)/float64(result.UniqueDIDs)*100)

	if len(result.TopDIDs) > 0 {
		fmt.Printf("\n  Most Active DIDs:\n")
		for i, da := range result.TopDIDs {
			if i >= 5 {
				break
			}
			fmt.Printf("    %d. %s (%d ops)\n", i+1, da.DID, da.Count)
		}
	}
	fmt.Printf("\n")

	// Handle patterns
	if !opts.skipPatterns && result.TotalHandles > 0 {
		fmt.Printf("🏷️  Handle Statistics\n")
		fmt.Printf("────────────────────\n")
		fmt.Printf("  Total handles:       %s\n", formatNumber(result.TotalHandles))
		if result.InvalidHandles > 0 {
			fmt.Printf("  Invalid patterns:    %s (%.1f%%)\n",
				formatNumber(result.InvalidHandles),
				float64(result.InvalidHandles)/float64(result.TotalHandles)*100)
		}

		if len(result.TopDomains) > 0 {
			fmt.Printf("\n  Top Domains:\n")
			for i, dc := range result.TopDomains {
				if i >= 10 {
					break
				}
				pct := float64(dc.Count) / float64(result.TotalHandles) * 100
				fmt.Printf("    %-25s %s (%.1f%%)\n", dc.Domain, formatNumber(dc.Count), pct)
			}
		}
		fmt.Printf("\n")
	}

	// Service patterns
	if !opts.skipPatterns && result.TotalServices > 0 {
		fmt.Printf("🌐 Service Endpoints\n")
		fmt.Printf("────────────────────\n")
		fmt.Printf("  Total services:      %s\n", formatNumber(result.TotalServices))
		fmt.Printf("  Unique endpoints:    %s\n", formatNumber(result.UniqueEndpoints))

		if len(result.TopPDSEndpoints) > 0 {
			fmt.Printf("\n  Top PDS Endpoints:\n")
			for i, ec := range result.TopPDSEndpoints {
				if i >= 10 {
					break
				}
				fmt.Printf("    %-40s %s ops\n", ec.Endpoint, formatNumber(ec.Count))
			}
		}
		fmt.Printf("\n")
	}

	// Temporal analysis
	fmt.Printf("⏱️  Temporal Distribution\n")
	fmt.Printf("───────────────────────\n")
	if len(result.TimeDistribution) > 0 {
		first := result.TimeDistribution[0]
		last := result.TimeDistribution[len(result.TimeDistribution)-1]
		duration := last.Time.Sub(first.Time)

		fmt.Printf("  Start:               %s\n", first.Time.Format("2006-01-02 15:04:05"))
		fmt.Printf("  End:                 %s\n", last.Time.Format("2006-01-02 15:04:05"))
		fmt.Printf("  Duration:            %s\n", formatDuration(duration))
		fmt.Printf("  Avg ops/minute:      %.1f\n", result.AvgOpsPerMinute)
		fmt.Printf("  Time slots:          %d minutes\n", len(result.TimeDistribution))

		// Find peak activity
		maxSlot := result.TimeDistribution[0]
		for _, slot := range result.TimeDistribution {
			if slot.Count > maxSlot.Count {
				maxSlot = slot
			}
		}
		fmt.Printf("  Peak activity:       %d ops at %s\n",
			maxSlot.Count, maxSlot.Time.Format("15:04"))
	}
	fmt.Printf("\n")

	// Size analysis
	fmt.Printf("📏 Size Analysis\n")
	fmt.Printf("────────────────\n")
	fmt.Printf("  Total data:          %s\n", formatBytes(result.TotalOpSize))
	fmt.Printf("  Average per op:      %s\n", formatBytes(int64(result.AvgOpSize)))
	fmt.Printf("  Min operation:       %s\n", formatBytes(int64(result.MinOpSize)))
	fmt.Printf("  Max operation:       %s\n\n", formatBytes(int64(result.MaxOpSize)))

	// Cryptographic verification
	if opts.verify && !opts.skipCrypto {
		fmt.Printf("🔐 Cryptographic Verification\n")
		fmt.Printf("─────────────────────────────\n")

		status := func(valid bool) string {
			if valid {
				return "✓ Valid"
			}
			return "✗ Invalid"
		}

		fmt.Printf("  Content hash:        %s\n", status(result.ContentHashValid))
		fmt.Printf("  Compressed hash:     %s\n", status(result.CompressedHashValid))
		if result.HasMetadataFrame {
			fmt.Printf("  Metadata integrity:  %s\n", status(result.MetadataValid))
		}
		fmt.Printf("\n")
	}

	// Performance summary
	fmt.Printf("⚡ Performance\n")
	fmt.Printf("──────────────\n")
	fmt.Printf("  Load time:           %s\n", result.LoadTime)
	if opts.verify {
		fmt.Printf("  Verify time:         %s\n", result.VerifyTime)
	}
	fmt.Printf("  Total time:          %s\n", result.TotalTime)
	if result.LoadTime.Seconds() > 0 {
		opsPerSec := float64(result.TotalOps) / result.LoadTime.Seconds()
		mbPerSec := float64(result.TotalOpSize) / result.LoadTime.Seconds() / (1024 * 1024)
		fmt.Printf("  Throughput:          %.0f ops/sec, %.2f MB/s\n", opsPerSec, mbPerSec)
	}
	fmt.Printf("\n")

	return nil
}

func displayInspectJSON(result *inspectResult) error {
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	return nil
}

func verifyCrypto(cmd *cobra.Command, path string, meta *storage.BundleMetadata, bundleNum int, verbose bool) (contentValid, compressedValid, metadataValid bool) {
	ops := &storage.Operations{}

	// Calculate actual hashes
	compHash, compSize, contentHash, contentSize, err := ops.CalculateFileHashes(path)
	if err != nil {
		if verbose {
			fmt.Fprintf(os.Stderr, "  Hash calculation failed: %v\n", err)
		}
		return false, false, false
	}

	contentValid = true
	compressedValid = true
	metadataValid = true

	// Verify against embedded metadata if available
	if meta != nil {
		if meta.ContentHash != "" && meta.ContentHash != contentHash {
			contentValid = false
			if verbose {
				fmt.Fprintf(os.Stderr, "  ✗ Content hash mismatch!\n")
				fmt.Fprintf(os.Stderr, "    Expected: %s\n", meta.ContentHash)
				fmt.Fprintf(os.Stderr, "    Actual:   %s\n", contentHash)
			}
		}

		if meta.CompressedHash != "" && meta.CompressedHash != compHash {
			compressedValid = false
			if verbose {
				fmt.Fprintf(os.Stderr, "  ✗ Compressed hash mismatch!\n")
			}
		}

		if meta.UncompressedSize != contentSize {
			metadataValid = false
			if verbose {
				fmt.Fprintf(os.Stderr, "  ✗ Uncompressed size mismatch: meta=%d, actual=%d\n",
					meta.UncompressedSize, contentSize)
			}
		}

		if meta.CompressedSize != compSize {
			metadataValid = false
			if verbose {
				fmt.Fprintf(os.Stderr, "  ✗ Compressed size mismatch: meta=%d, actual=%d\n",
					meta.CompressedSize, compSize)
			}
		}
	}

	// Also verify against repository index if bundle number is known
	if bundleNum > 0 {
		mgr, _, err := getManager(nil)
		if err == nil {
			defer mgr.Close()

			ctx := context.Background()
			vr, err := mgr.VerifyBundle(ctx, bundleNum)
			if err == nil {
				contentValid = contentValid && vr.Valid
				compressedValid = compressedValid && vr.HashMatch
			}
		}
	}

	return contentValid, compressedValid, metadataValid
}

func resolveBundlePath(cmd *cobra.Command, input string) (path string, bundleNum int, err error) {
	// Check if it's a file path
	if strings.HasSuffix(input, ".zst") || strings.Contains(input, "/") || strings.Contains(input, "\\") {
		absPath, err := filepath.Abs(input)
		if err != nil {
			return "", 0, err
		}

		// Try to extract bundle number from filename
		base := filepath.Base(absPath)
		fmt.Sscanf(base, "%d", &bundleNum)

		return absPath, bundleNum, nil
	}

	// Try to parse as bundle number
	if _, err := fmt.Sscanf(input, "%d", &bundleNum); err == nil {
		// Load from repository
		mgr, dir, err := getManager(&ManagerOptions{Cmd: cmd})
		if err != nil {
			return "", 0, err
		}
		defer mgr.Close()

		path := filepath.Join(dir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
		if _, err := os.Stat(path); err != nil {
			return "", 0, fmt.Errorf("bundle %d not found in repository", bundleNum)
		}

		return path, bundleNum, nil
	}

	return "", 0, fmt.Errorf("invalid input: must be bundle number or file path")
}
