package bundle

import (
	"fmt"
	"path/filepath"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

// Bundle represents a PLC bundle
type Bundle struct {
	BundleNumber int                      `json:"bundle_number"`
	StartTime    time.Time                `json:"start_time"`
	EndTime      time.Time                `json:"end_time"`
	Operations   []plcclient.PLCOperation `json:"-"`
	DIDCount     int                      `json:"did_count"`

	Hash        string `json:"hash"`         // Chain hash (primary)
	ContentHash string `json:"content_hash"` // Content hash
	Parent      string `json:"parent"`

	CompressedHash   string    `json:"compressed_hash"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	Cursor           string    `json:"cursor"`
	BoundaryCIDs     []string  `json:"boundary_cids,omitempty"`
	Compressed       bool      `json:"compressed"`
	CreatedAt        time.Time `json:"created_at"`
}

// GetFilePath returns the file path for this bundle
func (b *Bundle) GetFilePath(bundleDir string) string {
	return filepath.Join(bundleDir, fmt.Sprintf("%06d.jsonl.zst", b.BundleNumber))
}

// GetUncompressedFilePath returns the uncompressed file path
func (b *Bundle) GetUncompressedFilePath(bundleDir string) string {
	return filepath.Join(bundleDir, fmt.Sprintf("%06d.jsonl", b.BundleNumber))
}

// OperationCount returns the number of operations (always BUNDLE_SIZE for complete bundles)
func (b *Bundle) OperationCount() int {
	if len(b.Operations) > 0 {
		return len(b.Operations)
	}
	return types.BUNDLE_SIZE
}

// CompressionRatio returns the compression ratio
func (b *Bundle) CompressionRatio() float64 {
	if b.CompressedSize == 0 {
		return 0
	}
	return float64(b.UncompressedSize) / float64(b.CompressedSize)
}

// ValidateForSave performs validation before saving (no hash required)
func (b *Bundle) ValidateForSave() error {
	if b.BundleNumber < 1 {
		return fmt.Errorf("invalid bundle number: %d", b.BundleNumber)
	}
	if len(b.Operations) != types.BUNDLE_SIZE {
		return fmt.Errorf("invalid operation count: expected %d, got %d", types.BUNDLE_SIZE, len(b.Operations))
	}
	if b.StartTime.After(b.EndTime) {
		return fmt.Errorf("start_time is after end_time")
	}
	return nil
}

// Validate performs full validation (including hashes)
func (b *Bundle) Validate() error {
	if err := b.ValidateForSave(); err != nil {
		return err
	}
	if b.Hash == "" {
		return fmt.Errorf("missing hash")
	}
	if b.CompressedHash == "" {
		return fmt.Errorf("missing compressed hash")
	}
	return nil
}

// VerificationResult contains the result of bundle verification
type VerificationResult struct {
	BundleNumber int
	Valid        bool
	HashMatch    bool
	FileExists   bool
	Error        error
	LocalHash    string
	ExpectedHash string
}

// ChainVerificationResult contains the result of chain verification
type ChainVerificationResult struct {
	Valid           bool
	ChainLength     int
	BrokenAt        int
	Error           string
	VerifiedBundles []int
}

// DirectoryScanResult contains results from scanning a directory
type DirectoryScanResult struct {
	BundleDir         string
	BundleCount       int
	FirstBundle       int
	LastBundle        int
	MissingGaps       []int
	TotalSize         int64 // Compressed size
	TotalUncompressed int64 // Uncompressed size (NEW)
	IndexUpdated      bool
}

// Config holds configuration for bundle operations
type Config struct {
	BundleDir         string
	HandleResolverURL string
	VerifyOnLoad      bool
	AutoRebuild       bool
	AutoInit          bool                     // Allow auto-creating empty repository
	RebuildWorkers    int                      // Number of workers for parallel rebuild (0 = auto-detect)
	RebuildProgress   func(current, total int) // Progress callback for rebuild
	Logger            types.Logger
	Verbose           bool
	Quiet             bool
}

// DefaultConfig returns default configuration
func DefaultConfig(bundleDir string) *Config {
	return &Config{
		BundleDir:         bundleDir,
		HandleResolverURL: "https://quickdid.atscan.net",
		VerifyOnLoad:      true,
		AutoRebuild:       true,
		AutoInit:          false,
		RebuildWorkers:    0,   // 0 means auto-detect CPU count
		RebuildProgress:   nil, // No progress callback by default
		Logger:            nil,
		Verbose:           false,
		Quiet:             false,
	}
}

// CloneOptions configures cloning behavior
type CloneOptions struct {
	RemoteURL    string
	Workers      int
	SkipExisting bool
	ProgressFunc func(downloaded, total int, bytesDownloaded, bytesTotal int64)
	SaveInterval time.Duration
	Verbose      bool
	Logger       types.Logger
}

// CloneResult contains cloning results
type CloneResult struct {
	RemoteBundles int
	Downloaded    int
	Failed        int
	Skipped       int
	TotalBytes    int64
	Duration      time.Duration
	Interrupted   bool
	FailedBundles []int
}

// PLCOperationWithLocation contains an operation with its bundle/position metadata
type PLCOperationWithLocation struct {
	Operation plcclient.PLCOperation
	Bundle    int
	Position  int
}

func (b *Bundle) ToMetadata() *bundleindex.BundleMetadata {
	return &bundleindex.BundleMetadata{
		BundleNumber:     b.BundleNumber,
		StartTime:        b.StartTime,
		EndTime:          b.EndTime,
		OperationCount:   b.OperationCount(),
		DIDCount:         b.DIDCount,
		Hash:             b.Hash,        // Chain hash
		ContentHash:      b.ContentHash, // Content hash
		Parent:           b.Parent,
		CompressedHash:   b.CompressedHash,
		CompressedSize:   b.CompressedSize,
		UncompressedSize: b.UncompressedSize,
		Cursor:           b.Cursor,
		CreatedAt:        b.CreatedAt,
	}
}

// ResolveDIDResult contains DID resolution with timing metrics
type ResolveDIDResult struct {
	Document       *plcclient.DIDDocument
	MempoolTime    time.Duration
	IndexTime      time.Duration
	LoadOpTime     time.Duration
	TotalTime      time.Duration
	ResolvedHandle string
	Source         string // "mempool" or "bundle"
	BundleNumber   int    // if from bundle
	Position       int    // if from bundle
}

type resolverTiming struct {
	totalTime   int64
	mempoolTime int64
	indexTime   int64
	loadOpTime  int64
	source      string // "mempool" or "bundle"
}
