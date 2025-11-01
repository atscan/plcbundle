package bundle

import (
	"fmt"
	"path/filepath"
	"time"

	"tangled.org/atscan.net/plcbundle/plc"
)

const (
	// BUNDLE_SIZE is the standard number of operations per bundle
	BUNDLE_SIZE = 10000
)

// Bundle represents a PLC bundle
type Bundle struct {
	BundleNumber int                `json:"bundle_number"`
	StartTime    time.Time          `json:"start_time"`
	EndTime      time.Time          `json:"end_time"`
	Operations   []plc.PLCOperation `json:"-"`
	DIDCount     int                `json:"did_count"`

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
	return BUNDLE_SIZE
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
	if len(b.Operations) != BUNDLE_SIZE {
		return fmt.Errorf("invalid operation count: expected %d, got %d", BUNDLE_SIZE, len(b.Operations))
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

// BundleMetadata represents metadata about a bundle
type BundleMetadata struct {
	BundleNumber   int       `json:"bundle_number"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	OperationCount int       `json:"operation_count"`
	DIDCount       int       `json:"did_count"`

	// Primary hash - cumulative chain hash (includes all history)
	Hash string `json:"hash"`

	// Content hash - SHA256 of bundle operations only
	ContentHash string `json:"content_hash"`

	// Parent chain hash - links to previous bundle
	Parent string `json:"parent,omitempty"`

	// File hashes and sizes
	CompressedHash   string    `json:"compressed_hash"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	Cursor           string    `json:"cursor"`
	CreatedAt        time.Time `json:"created_at"`
}

func (b *Bundle) ToMetadata() *BundleMetadata {
	return &BundleMetadata{
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

// Logger interface for bundle operations
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Config holds configuration for bundle operations
type Config struct {
	BundleDir       string
	VerifyOnLoad    bool
	AutoRebuild     bool
	RebuildWorkers  int                      // Number of workers for parallel rebuild (0 = auto-detect)
	RebuildProgress func(current, total int) // Progress callback for rebuild
	Logger          Logger
}

// DefaultConfig returns default configuration
func DefaultConfig(bundleDir string) *Config {
	return &Config{
		BundleDir:       bundleDir,
		VerifyOnLoad:    true,
		AutoRebuild:     true,
		RebuildWorkers:  0,   // 0 means auto-detect CPU count
		RebuildProgress: nil, // No progress callback by default
		Logger:          nil,
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
	Logger       Logger
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
