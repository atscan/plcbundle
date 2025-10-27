package plcbundle

import (
	"context"
	"time"

	"github.com/yourusername/plc-bundle-lib/bundle"
	"github.com/yourusername/plc-bundle-lib/plc"
)

// Re-export commonly used types for convenience
type (
	Bundle                  = bundle.Bundle
	BundleMetadata          = bundle.BundleMetadata
	Index                   = bundle.Index
	Manager                 = bundle.Manager
	Config                  = bundle.Config
	CompressionLevel        = bundle.CompressionLevel
	VerificationResult      = bundle.VerificationResult
	ChainVerificationResult = bundle.ChainVerificationResult
	DirectoryScanResult     = bundle.DirectoryScanResult
	Logger                  = bundle.Logger

	PLCOperation  = plc.PLCOperation
	PLCClient     = plc.Client
	ExportOptions = plc.ExportOptions
)

// Re-export constants
const (
	BUNDLE_SIZE = bundle.BUNDLE_SIZE
	INDEX_FILE  = bundle.INDEX_FILE

	CompressionFastest = bundle.CompressionFastest
	CompressionDefault = bundle.CompressionDefault
	CompressionBetter  = bundle.CompressionBetter
	CompressionBest    = bundle.CompressionBest
)

// NewManager creates a new bundle manager (convenience wrapper)
func NewManager(config *Config, plcClient *PLCClient) (*Manager, error) {
	return bundle.NewManager(config, plcClient)
}

// NewPLCClient creates a new PLC client (convenience wrapper)
func NewPLCClient(baseURL string, opts ...plc.ClientOption) *PLCClient {
	return plc.NewClient(baseURL, opts...)
}

// DefaultConfig returns default configuration (convenience wrapper)
func DefaultConfig(bundleDir string) *Config {
	return bundle.DefaultConfig(bundleDir)
}

// NewIndex creates a new empty index (convenience wrapper)
func NewIndex() *Index {
	return bundle.NewIndex()
}

// LoadIndex loads an index from a file (convenience wrapper)
func LoadIndex(path string) (*Index, error) {
	return bundle.LoadIndex(path)
}

// BundleManager provides a high-level API for bundle operations
type BundleManager struct {
	mgr *Manager
}

// New creates a new BundleManager with default settings
func New(bundleDir string, plcURL string) (*BundleManager, error) {
	config := DefaultConfig(bundleDir)
	var plcClient *PLCClient
	if plcURL != "" {
		plcClient = NewPLCClient(plcURL)
	}

	mgr, err := NewManager(config, plcClient)
	if err != nil {
		return nil, err
	}

	return &BundleManager{mgr: mgr}, nil
}

// Close closes the manager
func (bm *BundleManager) Close() {
	bm.mgr.Close()
}

// FetchNext fetches the next bundle from PLC
func (bm *BundleManager) FetchNext(ctx context.Context) (*Bundle, error) {
	b, err := bm.mgr.FetchNextBundle(ctx)
	if err != nil {
		return nil, err
	}
	return b, bm.mgr.SaveBundle(ctx, b)
}

// Load loads a bundle by number
func (bm *BundleManager) Load(ctx context.Context, bundleNumber int) (*Bundle, error) {
	return bm.mgr.LoadBundle(ctx, bundleNumber)
}

// Verify verifies a bundle
func (bm *BundleManager) Verify(ctx context.Context, bundleNumber int) (*VerificationResult, error) {
	return bm.mgr.VerifyBundle(ctx, bundleNumber)
}

// VerifyChain verifies the entire chain
func (bm *BundleManager) VerifyChain(ctx context.Context) (*ChainVerificationResult, error) {
	return bm.mgr.VerifyChain(ctx)
}

// Scan scans the directory and rebuilds the index
func (bm *BundleManager) Scan() (*DirectoryScanResult, error) {
	return bm.mgr.ScanDirectory()
}

// GetIndex returns the index
func (bm *BundleManager) GetIndex() *Index {
	return bm.mgr.GetIndex()
}

// GetInfo returns manager info
func (bm *BundleManager) GetInfo() map[string]interface{} {
	return bm.mgr.GetInfo()
}

// Export exports operations from bundles
func (bm *BundleManager) Export(ctx context.Context, afterTime time.Time, count int) ([]PLCOperation, error) {
	return bm.mgr.ExportOperations(ctx, afterTime, count)
}

// Scan scans the directory and rebuilds the index
func (bm *BundleManager) Scan() (*DirectoryScanResult, error) {
	return bm.mgr.ScanDirectory()
}

// ScanBundle scans a single bundle file
func (bm *BundleManager) ScanBundle(path string, bundleNumber int) (*BundleMetadata, error) {
	return bm.mgr.ScanBundle(path, bundleNumber)
}

// IsBundleIndexed checks if a bundle is in the index
func (bm *BundleManager) IsBundleIndexed(bundleNumber int) bool {
	return bm.mgr.IsBundleIndexed(bundleNumber)
}
