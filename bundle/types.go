package bundle

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/yourusername/plc-bundle-lib/plc"
)

const (
	// BUNDLE_SIZE is the standard number of operations per bundle
	BUNDLE_SIZE = 10000

	// DefaultCompression is the default compression level
	DefaultCompression = CompressionBetter
)

// CompressionLevel represents zstd compression levels
type CompressionLevel int

const (
	CompressionFastest CompressionLevel = 1
	CompressionDefault CompressionLevel = 3
	CompressionBetter  CompressionLevel = 5
	CompressionBest    CompressionLevel = 9
)

// Bundle represents a PLC bundle
type Bundle struct {
	BundleNumber     int                `json:"bundle_number"`
	StartTime        time.Time          `json:"start_time"`
	EndTime          time.Time          `json:"end_time"`
	Operations       []plc.PLCOperation `json:"-"` // Not serialized to JSON
	DIDCount         int                `json:"did_count"`
	Hash             string             `json:"hash"`
	CompressedHash   string             `json:"compressed_hash"`
	CompressedSize   int64              `json:"compressed_size"`
	UncompressedSize int64              `json:"uncompressed_size"`
	Cursor           string             `json:"cursor"`
	PrevBundleHash   string             `json:"prev_bundle_hash,omitempty"`
	BoundaryCIDs     []string           `json:"boundary_cids,omitempty"`
	Compressed       bool               `json:"compressed"`
	CreatedAt        time.Time          `json:"created_at"`
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

// BundleMetadata contains metadata about a bundle (for index)
type BundleMetadata struct {
	BundleNumber     int       `json:"bundle_number"`
	StartTime        time.Time `json:"start_time"`
	EndTime          time.Time `json:"end_time"`
	OperationCount   int       `json:"operation_count"`
	DIDCount         int       `json:"did_count"`
	Hash             string    `json:"hash"`
	CompressedHash   string    `json:"compressed_hash"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	Cursor           string    `json:"cursor"`
	PrevBundleHash   string    `json:"prev_bundle_hash,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

// ToMetadata converts a Bundle to BundleMetadata
func (b *Bundle) ToMetadata() *BundleMetadata {
	return &BundleMetadata{
		BundleNumber:     b.BundleNumber,
		StartTime:        b.StartTime,
		EndTime:          b.EndTime,
		OperationCount:   b.OperationCount(),
		DIDCount:         b.DIDCount,
		Hash:             b.Hash,
		CompressedHash:   b.CompressedHash,
		CompressedSize:   b.CompressedSize,
		UncompressedSize: b.UncompressedSize,
		Cursor:           b.Cursor,
		PrevBundleHash:   b.PrevBundleHash,
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
	BundleDir    string
	BundleCount  int
	FirstBundle  int
	LastBundle   int
	MissingGaps  []int
	TotalSize    int64
	IndexUpdated bool
}

// Logger interface for bundle operations
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Config holds configuration for bundle operations
type Config struct {
	BundleDir        string
	CompressionLevel CompressionLevel
	VerifyOnLoad     bool
	Logger           Logger
}

// DefaultConfig returns default configuration
func DefaultConfig(bundleDir string) *Config {
	return &Config{
		BundleDir:        bundleDir,
		CompressionLevel: CompressionBetter,
		VerifyOnLoad:     true,
		Logger:           nil, // Will use defaultLogger in manager
	}
}
