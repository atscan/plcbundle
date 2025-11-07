package bundle

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/didindex"
	"tangled.org/atscan.net/plcbundle/internal/mempool"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

// defaultLogger is a simple logger implementation
type defaultLogger struct{}

func (d defaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (d defaultLogger) Println(v ...interface{}) {
	log.Println(v...)
}

// Manager handles bundle operations
type Manager struct {
	config     *Config
	operations *storage.Operations
	index      *bundleindex.Index
	indexPath  string
	plcClient  *plcclient.Client
	logger     types.Logger
	mempool    *mempool.Mempool
	didIndex   *didindex.Manager

	syncer *internalsync.Fetcher
	cloner *internalsync.Cloner

	bundleCache  map[int]*Bundle
	cacheMu      sync.RWMutex
	maxCacheSize int
}

// NewManager creates a new bundle manager
func NewManager(config *Config, plcClient *plcclient.Client) (*Manager, error) {
	if config == nil {
		config = DefaultConfig("./plc_bundles")
	}

	if config.Logger == nil {
		config.Logger = defaultLogger{}
	}

	// Ensure directory exists
	if err := os.MkdirAll(config.BundleDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create bundle directory: %w", err)
	}

	// Initialize operations handler
	ops, err := storage.NewOperations(config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize operations: %w", err)
	}

	// Determine origin
	var origin string
	if plcClient != nil {
		origin = plcClient.GetBaseURL()
	}

	// Load or create index
	indexPath := filepath.Join(config.BundleDir, bundleindex.INDEX_FILE)
	index, err := bundleindex.LoadIndex(indexPath)

	// Check for bundle files in directory
	bundleFiles, _ := filepath.Glob(filepath.Join(config.BundleDir, "*.jsonl.zst"))
	bundleFiles = filterBundleFiles(bundleFiles)
	hasBundleFiles := len(bundleFiles) > 0

	// Check if clone/download is in progress (look for .tmp files)
	tmpFiles, _ := filepath.Glob(filepath.Join(config.BundleDir, "*.tmp"))
	cloneInProgress := len(tmpFiles) > 0

	needsRebuild := false

	if err != nil {
		// Index doesn't exist or is invalid
		if hasBundleFiles {
			if cloneInProgress {
				config.Logger.Printf("Clone/download in progress, skipping auto-rebuild")
			} else {
				config.Logger.Printf("No valid index found, but detected %d bundle files", len(bundleFiles))
				needsRebuild = true
			}
		} else {
			// No index and no bundles - create fresh index
			config.Logger.Printf("Creating new index at %s", indexPath)
			index = bundleindex.NewIndex(origin)
			if err := index.Save(indexPath); err != nil {
				return nil, fmt.Errorf("failed to save new index: %w", err)
			}
		}
	} else {
		// Index exists - auto-populate origin if missing
		if index.Origin == "" {
			if origin != "" {
				config.Logger.Printf("⚠️  Upgrading old index: setting origin to %s", origin)
				index.Origin = origin
				if err := index.Save(indexPath); err != nil {
					return nil, fmt.Errorf("failed to update index with origin: %w", err)
				}
			} else {
				config.Logger.Printf("⚠️  Warning: index has no origin and no PLC client configured")
			}
		}

		// Validate origin matches if both are set
		if index.Origin != "" && origin != "" && index.Origin != origin {
			return nil, fmt.Errorf(
				"origin mismatch: index has origin %q but PLC client points to %q\n"+
					"Cannot mix bundles from different sources. Use a different directory or reconfigure PLC client",
				index.Origin, origin,
			)
		}

		config.Logger.Printf("Loaded index with %d bundles (origin: %s)", index.Count(), index.Origin)

		// Check if there are bundle files not in the index
		if hasBundleFiles && len(bundleFiles) > index.Count() {
			if cloneInProgress {
				config.Logger.Printf("Clone/download in progress (%d .tmp files), skipping auto-rebuild", len(tmpFiles))
			} else {
				config.Logger.Printf("Detected %d bundle files but index only has %d entries - rebuilding",
					len(bundleFiles), index.Count())
				needsRebuild = true
			}
		}
	}

	if index != nil && plcClient != nil {
		currentOrigin := plcClient.GetBaseURL()

		// Check if origins match
		if index.Origin != "" && index.Origin != currentOrigin {
			return nil, fmt.Errorf(
				"origin mismatch: index has origin %q but PLC client points to %q. "+
					"Cannot mix bundles from different sources",
				index.Origin, currentOrigin,
			)
		}

		// Set origin if not set (for backward compatibility with old indexes)
		if index.Origin == "" && currentOrigin != "" {
			index.Origin = currentOrigin
			config.Logger.Printf("Setting origin for existing index: %s", currentOrigin)
			if err := index.Save(indexPath); err != nil {
				return nil, fmt.Errorf("failed to update index with origin: %w", err)
			}
		}
	}

	// Perform rebuild if needed (using parallel scan)
	if needsRebuild && config.AutoRebuild {
		config.Logger.Printf("Rebuilding index from %d bundle files...", len(bundleFiles))

		// Create temporary manager for scanning
		tempMgr := &Manager{
			config:     config,
			operations: ops,
			index:      bundleindex.NewIndex("test-origin"),
			indexPath:  indexPath,
			logger:     config.Logger,
		}

		// Use parallel scan with auto-detected CPU count
		workers := config.RebuildWorkers
		if workers <= 0 {
			workers = runtime.NumCPU()
			if workers < 1 {
				workers = 1
			}
		}

		config.Logger.Printf("Using %d workers for parallel scan", workers)

		// Create progress callback wrapper with new signature
		var progressCallback func(current, total int, bytesProcessed int64)
		if config.RebuildProgress != nil {
			// Wrap the old-style callback to work with new signature
			oldCallback := config.RebuildProgress
			progressCallback = func(current, total int, bytesProcessed int64) {
				oldCallback(current, total)
			}
		} else {
			// Default: log every 100 bundles
			progressCallback = func(current, total int, bytesProcessed int64) {
				if current%100 == 0 || current == total {
					mbProcessed := float64(bytesProcessed) / (1024 * 1024)
					config.Logger.Printf("Rebuild progress: %d/%d bundles (%.1f%%), %.1f MB processed",
						current, total, float64(current)/float64(total)*100, mbProcessed)
				}
			}
		}

		start := time.Now()

		// Scan directory to rebuild index (parallel)
		result, err := tempMgr.ScanDirectoryParallel(workers, progressCallback)
		if err != nil {
			return nil, fmt.Errorf("failed to rebuild index: %w", err)
		}

		elapsed := time.Since(start)

		// Reload the rebuilt index
		index, err = bundleindex.LoadIndex(indexPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load rebuilt index: %w", err)
		}

		// Calculate throughput
		mbPerSec := float64(result.TotalUncompressed) / elapsed.Seconds() / (1024 * 1024)

		config.Logger.Printf("✓ Index rebuilt with %d bundles in %s",
			index.Count(), elapsed.Round(time.Millisecond))
		config.Logger.Printf("  Speed: %.1f bundles/sec, %.1f MB/s (uncompressed)",
			float64(result.BundleCount)/elapsed.Seconds(), mbPerSec)

		// Verify all chain hashes are present
		bundles := index.GetBundles()
		missingHashes := 0
		for i, meta := range bundles {
			if meta.ContentHash == "" {
				missingHashes++
			}
			if i > 0 && meta.Hash == "" {
				missingHashes++
			}
		}
		if missingHashes > 0 {
			config.Logger.Printf("⚠️  Warning: %d bundles have missing hashes", missingHashes)
		}
	}

	if index == nil {
		index = bundleindex.NewIndex("test-origin")
	}

	// Initialize mempool for next bundle
	lastBundle := index.GetLastBundle()
	nextBundleNum := 1
	var minTimestamp time.Time

	if lastBundle != nil {
		nextBundleNum = lastBundle.BundleNumber + 1
		minTimestamp = lastBundle.EndTime
	}

	mempool, err := mempool.NewMempool(config.BundleDir, nextBundleNum, minTimestamp, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize mempool: %w", err)
	}

	// Initialize DID index manager
	didIndex := didindex.NewManager(config.BundleDir, config.Logger)

	// Initialize sync components
	fetcher := internalsync.NewFetcher(plcClient, ops, config.Logger)
	cloner := internalsync.NewCloner(ops, config.BundleDir, config.Logger)

	return &Manager{
		config:       config,
		operations:   ops,
		index:        index,
		indexPath:    indexPath,
		plcClient:    plcClient,
		logger:       config.Logger,
		mempool:      mempool,
		didIndex:     didIndex, // Updated type
		bundleCache:  make(map[int]*Bundle),
		maxCacheSize: 2,
		syncer:       fetcher,
		cloner:       cloner,
	}, nil
}

// Close cleans up resources
func (m *Manager) Close() {
	if m.operations != nil {
		m.operations.Close()
	}
	if m.plcClient != nil {
		m.plcClient.Close()
	}
	if m.mempool != nil {
		if err := m.mempool.Save(); err != nil {
			m.logger.Printf("Warning: failed to save mempool: %v", err)
		}
	}
	if m.didIndex != nil {
		m.didIndex.Close()
	}
}

// GetIndex returns the current index
func (m *Manager) GetIndex() *bundleindex.Index {
	return m.index
}

// SaveIndex saves the index to disk
func (m *Manager) SaveIndex() error {
	return m.index.Save(m.indexPath)
}

// LoadBundle with caching
func (m *Manager) LoadBundle(ctx context.Context, bundleNumber int) (*Bundle, error) {
	// Check cache first
	m.cacheMu.RLock()
	if cached, ok := m.bundleCache[bundleNumber]; ok {
		m.cacheMu.RUnlock()
		return cached, nil
	}
	m.cacheMu.RUnlock()

	// Load from disk (existing code)
	bundle, err := m.loadBundleFromDisk(ctx, bundleNumber)
	if err != nil {
		return nil, err
	}

	// Add to cache
	m.cacheMu.Lock()
	m.bundleCache[bundleNumber] = bundle

	// Simple LRU: if cache too big, remove oldest
	if len(m.bundleCache) > m.maxCacheSize {
		// Remove a random one (or implement proper LRU)
		for k := range m.bundleCache {
			delete(m.bundleCache, k)
			break
		}
	}
	m.cacheMu.Unlock()

	return bundle, nil
}

// loadBundleFromDisk loads a bundle from disk
func (m *Manager) loadBundleFromDisk(ctx context.Context, bundleNumber int) (*Bundle, error) {
	// Get metadata from index
	meta, err := m.index.GetBundle(bundleNumber)
	if err != nil {
		return nil, fmt.Errorf("bundle not in index: %w", err)
	}

	// Load file
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	if !m.operations.FileExists(path) {
		return nil, fmt.Errorf("bundle file not found: %s", path)
	}

	// Verify hash if enabled
	if m.config.VerifyOnLoad {
		valid, actualHash, err := m.operations.VerifyHash(path, meta.CompressedHash)
		if err != nil {
			return nil, fmt.Errorf("failed to verify hash: %w", err)
		}
		if !valid {
			return nil, fmt.Errorf("hash mismatch: expected %s, got %s", meta.CompressedHash, actualHash)
		}
	}

	// Load operations
	operations, err := m.operations.LoadBundle(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load bundle: %w", err)
	}

	// Create bundle struct
	bundle := &Bundle{
		BundleNumber:     meta.BundleNumber,
		StartTime:        meta.StartTime,
		EndTime:          meta.EndTime,
		Operations:       operations,
		DIDCount:         meta.DIDCount,
		Hash:             meta.Hash,
		ContentHash:      meta.ContentHash,
		Parent:           meta.Parent,
		CompressedHash:   meta.CompressedHash,
		CompressedSize:   meta.CompressedSize,
		UncompressedSize: meta.UncompressedSize,
		Cursor:           meta.Cursor,
		Compressed:       true,
		CreatedAt:        meta.CreatedAt,
	}

	return bundle, nil
}

// SaveBundle saves a bundle to disk and updates the index
func (m *Manager) SaveBundle(ctx context.Context, bundle *Bundle, quiet bool) error {
	if err := bundle.ValidateForSave(); err != nil {
		return fmt.Errorf("bundle validation failed: %w", err)
	}

	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundle.BundleNumber))

	// Save to disk
	uncompressedHash, compressedHash, uncompressedSize, compressedSize, err := m.operations.SaveBundle(path, bundle.Operations)
	if err != nil {
		return fmt.Errorf("failed to save bundle: %w", err)
	}

	bundle.ContentHash = uncompressedHash
	bundle.CompressedHash = compressedHash
	bundle.UncompressedSize = uncompressedSize
	bundle.CompressedSize = compressedSize
	bundle.CreatedAt = time.Now().UTC()

	// Get parent
	var parent string
	if bundle.BundleNumber > 1 {
		prevBundle := m.index.GetLastBundle()
		if prevBundle != nil {
			parent = prevBundle.Hash
		} else {
			if prevMeta, err := m.index.GetBundle(bundle.BundleNumber - 1); err == nil {
				parent = prevMeta.Hash
			}
		}
	}

	bundle.Parent = parent
	bundle.Hash = m.operations.CalculateChainHash(parent, bundle.ContentHash)

	// Add to index
	m.index.AddBundle(bundle.ToMetadata())

	// Save index
	if err := m.SaveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	// Clean up old mempool (silent unless verbose)
	oldMempoolFile := m.mempool.GetFilename()
	if err := m.mempool.Delete(); err != nil && !quiet {
		m.logger.Printf("Warning: failed to delete old mempool %s: %v", oldMempoolFile, err)
	}

	// Create new mempool
	nextBundle := bundle.BundleNumber + 1
	minTimestamp := bundle.EndTime

	newMempool, err := mempool.NewMempool(m.config.BundleDir, nextBundle, minTimestamp, m.logger)
	if err != nil {
		return fmt.Errorf("failed to create new mempool: %w", err)
	}

	m.mempool = newMempool

	// Update DID index if enabled (ONLY when bundle is created)
	if m.didIndex != nil && m.didIndex.Exists() {
		if err := m.updateDIDIndexForBundle(ctx, bundle); err != nil {
			m.logger.Printf("Warning: failed to update DID index: %v", err)
		}
	}

	return nil
}

// GetMempoolStats returns mempool statistics
func (m *Manager) GetMempoolStats() map[string]interface{} {
	return m.mempool.Stats()
}

// GetMempoolOperations returns all operations currently in mempool
func (m *Manager) GetMempoolOperations() ([]plcclient.PLCOperation, error) {
	if m.mempool == nil {
		return nil, fmt.Errorf("mempool not initialized")
	}

	count := m.mempool.Count()
	if count == 0 {
		return []plcclient.PLCOperation{}, nil
	}

	return m.mempool.Peek(count), nil
}

// VerifyBundle verifies a bundle's integrity
func (m *Manager) VerifyBundle(ctx context.Context, bundleNumber int) (*VerificationResult, error) {
	result := &VerificationResult{
		BundleNumber: bundleNumber,
	}

	// Get from index
	meta, err := m.index.GetBundle(bundleNumber)
	if err != nil {
		result.Error = err
		return result, nil
	}

	result.ExpectedHash = meta.CompressedHash

	// Check file exists
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	result.FileExists = m.operations.FileExists(path)
	if !result.FileExists {
		result.Error = fmt.Errorf("file not found")
		return result, nil
	}

	// Verify BOTH compressed and content hashes
	compHash, compSize, contentHash, contentSize, err := m.operations.CalculateFileHashes(path)
	if err != nil {
		result.Error = err
		return result, nil
	}

	result.LocalHash = compHash

	// Verify compressed hash
	if compHash != meta.CompressedHash {
		result.HashMatch = false
		result.Valid = false
		result.Error = fmt.Errorf("compressed hash mismatch: expected %s, got %s", meta.CompressedHash, compHash)
		return result, nil
	}

	// Verify content hash
	if contentHash != meta.ContentHash {
		result.HashMatch = false
		result.Valid = false
		result.Error = fmt.Errorf("content hash mismatch: expected %s, got %s", meta.ContentHash, contentHash)
		return result, nil
	}

	// Verify sizes match
	if compSize != meta.CompressedSize {
		result.Valid = false
		result.Error = fmt.Errorf("compressed size mismatch: expected %d, got %d", meta.CompressedSize, compSize)
		return result, nil
	}

	if contentSize != meta.UncompressedSize {
		result.Valid = false
		result.Error = fmt.Errorf("uncompressed size mismatch: expected %d, got %d", meta.UncompressedSize, contentSize)
		return result, nil
	}

	result.HashMatch = true
	result.Valid = true

	return result, nil
}

// GetInfo returns information about the bundle manager
func (m *Manager) GetInfo() map[string]interface{} {
	stats := m.index.GetStats()
	stats["bundle_dir"] = m.config.BundleDir
	stats["index_path"] = m.indexPath
	stats["verify_on_load"] = m.config.VerifyOnLoad
	return stats
}

// ExportOperations exports operations from bundles
func (m *Manager) ExportOperations(ctx context.Context, afterTime time.Time, count int) ([]plcclient.PLCOperation, error) {
	if count <= 0 {
		count = 1000
	}

	var result []plcclient.PLCOperation
	seenCIDs := make(map[string]bool)

	bundles := m.index.GetBundles()

	for _, meta := range bundles {
		if result != nil && len(result) >= count {
			break
		}

		// Skip bundles before afterTime
		if !afterTime.IsZero() && meta.EndTime.Before(afterTime) {
			continue
		}

		// Load bundle
		bundle, err := m.LoadBundle(ctx, meta.BundleNumber)
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", meta.BundleNumber, err)
			continue
		}

		// Add operations
		for _, op := range bundle.Operations {
			if !afterTime.IsZero() && op.CreatedAt.Before(afterTime) {
				continue
			}

			if seenCIDs[op.CID] {
				continue
			}

			seenCIDs[op.CID] = true
			result = append(result, op)

			if len(result) >= count {
				break
			}
		}
	}

	return result, nil
}

// IsBundleIndexed checks if a bundle is already in the index
func (m *Manager) IsBundleIndexed(bundleNumber int) bool {
	_, err := m.index.GetBundle(bundleNumber)
	return err == nil
}

// RefreshMempool reloads mempool from disk
func (m *Manager) RefreshMempool() error {
	if m.mempool == nil {
		return fmt.Errorf("mempool not initialized")
	}
	return m.mempool.Load()
}

// ClearMempool clears all operations from the mempool and saves
func (m *Manager) ClearMempool() error {
	if m.mempool == nil {
		return fmt.Errorf("mempool not initialized")
	}

	m.logger.Printf("Clearing mempool...")

	count := m.mempool.Count()

	m.mempool.Clear()

	if err := m.mempool.Save(); err != nil {
		return fmt.Errorf("failed to save mempool: %w", err)
	}

	m.logger.Printf("Cleared %d operations from mempool", count)

	return nil
}

// ValidateMempool validates mempool
func (m *Manager) ValidateMempool() error {
	if m.mempool == nil {
		return fmt.Errorf("mempool not initialized")
	}
	return m.mempool.Validate()
}

// StreamBundleRaw streams the raw compressed bundle file
func (m *Manager) StreamBundleRaw(ctx context.Context, bundleNumber int) (io.ReadCloser, error) {
	// Get metadata from index
	meta, err := m.index.GetBundle(bundleNumber)
	if err != nil {
		return nil, fmt.Errorf("bundle not in index: %w", err)
	}

	// Build file path
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	if !m.operations.FileExists(path) {
		return nil, fmt.Errorf("bundle file not found: %s", path)
	}

	// Optionally verify hash before streaming
	if m.config.VerifyOnLoad {
		valid, actualHash, err := m.operations.VerifyHash(path, meta.CompressedHash)
		if err != nil {
			return nil, fmt.Errorf("failed to verify hash: %w", err)
		}
		if !valid {
			return nil, fmt.Errorf("hash mismatch: expected %s, got %s", meta.CompressedHash, actualHash)
		}
	}

	return m.operations.StreamRaw(path)
}

// StreamBundleDecompressed streams the decompressed bundle data as JSONL
func (m *Manager) StreamBundleDecompressed(ctx context.Context, bundleNumber int) (io.ReadCloser, error) {
	// Get metadata from index
	_, err := m.index.GetBundle(bundleNumber)
	if err != nil {
		return nil, fmt.Errorf("bundle not in index: %w", err)
	}

	// Build file path
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	if !m.operations.FileExists(path) {
		return nil, fmt.Errorf("bundle file not found: %s", path)
	}

	return m.operations.StreamDecompressed(path)
}

// RefreshIndex reloads the index from disk if it has been modified
func (m *Manager) RefreshIndex() error {
	// Check if index file has been modified
	info, err := os.Stat(m.indexPath)
	if err != nil {
		return err
	}

	// If index was modified after we loaded it, reload
	if info.ModTime().After(m.index.UpdatedAt) {
		m.logger.Printf("Index file modified, reloading...")

		newIndex, err := bundleindex.LoadIndex(m.indexPath)
		if err != nil {
			return fmt.Errorf("failed to reload index: %w", err)
		}

		m.index = newIndex
		m.logger.Printf("Index reloaded: %d bundles", m.index.Count())
	}

	return nil
}

// GetMempool returns the current mempool
func (m *Manager) GetMempool() *mempool.Mempool {
	return m.mempool
}

// SaveMempool saves the current mempool state to disk
func (m *Manager) SaveMempool() error {
	if m.mempool == nil {
		return fmt.Errorf("mempool not initialized")
	}
	return m.mempool.Save()
}

// GetPLCOrigin returns the PLC directory origin URL
func (m *Manager) GetPLCOrigin() string {
	if m.plcClient == nil {
		return ""
	}
	return m.plcClient.GetBaseURL()
}

// GetCurrentCursor returns the current latest cursor position (including mempool)
func (m *Manager) GetCurrentCursor() int {
	index := m.GetIndex()
	bundles := index.GetBundles()
	cursor := len(bundles) * types.BUNDLE_SIZE

	// Add mempool operations
	mempoolStats := m.GetMempoolStats()
	if count, ok := mempoolStats["count"].(int); ok {
		cursor += count
	}

	return cursor
}

// LoadOperation loads a single operation from a bundle efficiently
func (m *Manager) LoadOperation(ctx context.Context, bundleNumber int, position int) (*plcclient.PLCOperation, error) {
	// Validate bundle exists in index
	_, err := m.index.GetBundle(bundleNumber)
	if err != nil {
		return nil, fmt.Errorf("bundle not in index: %w", err)
	}

	// Validate position
	if position < 0 || position >= types.BUNDLE_SIZE {
		return nil, fmt.Errorf("invalid position: %d (must be 0-%d)", position, types.BUNDLE_SIZE-1)
	}

	// Build file path
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	if !m.operations.FileExists(path) {
		return nil, fmt.Errorf("bundle file not found: %s", path)
	}

	// Load just the one operation
	return m.operations.LoadOperationAtPosition(path, position)
}

// filterBundleFiles filters out files starting with . or _
func filterBundleFiles(files []string) []string {
	filtered := make([]string, 0, len(files))
	for _, file := range files {
		basename := filepath.Base(file)
		if len(basename) > 0 && (basename[0] == '.' || basename[0] == '_') {
			continue
		}
		filtered = append(filtered, file)
	}
	return filtered
}

// ==========================================
// DID INDEX INTEGRATION (adapter methods)
// ==========================================

// Implement BundleProvider interface for didindex
func (m *Manager) LoadBundleForDIDIndex(ctx context.Context, bundleNumber int) (*didindex.BundleData, error) {
	bundle, err := m.LoadBundle(ctx, bundleNumber)
	if err != nil {
		return nil, err
	}

	return &didindex.BundleData{
		BundleNumber: bundle.BundleNumber,
		Operations:   bundle.Operations,
	}, nil
}

func (m *Manager) GetBundleIndex() didindex.BundleIndexProvider {
	return &bundleIndexAdapter{index: m.index}
}

// bundleIndexAdapter adapts Index to BundleIndexProvider interface
type bundleIndexAdapter struct {
	index *bundleindex.Index
}

func (a *bundleIndexAdapter) GetBundles() []*didindex.BundleMetadata {
	bundles := a.index.GetBundles()
	result := make([]*didindex.BundleMetadata, len(bundles))
	for i, b := range bundles {
		result[i] = &didindex.BundleMetadata{
			BundleNumber: b.BundleNumber,
			StartTime:    b.StartTime,
			EndTime:      b.EndTime,
		}
	}
	return result
}

func (a *bundleIndexAdapter) GetBundle(bundleNumber int) (*didindex.BundleMetadata, error) {
	meta, err := a.index.GetBundle(bundleNumber)
	if err != nil {
		return nil, err
	}
	return &didindex.BundleMetadata{
		BundleNumber: meta.BundleNumber,
		StartTime:    meta.StartTime,
		EndTime:      meta.EndTime,
	}, nil
}

func (a *bundleIndexAdapter) GetLastBundle() *didindex.BundleMetadata {
	meta := a.index.GetLastBundle()
	if meta == nil {
		return nil
	}
	return &didindex.BundleMetadata{
		BundleNumber: meta.BundleNumber,
		StartTime:    meta.StartTime,
		EndTime:      meta.EndTime,
	}
}

// GetDIDIndex returns the DID index manager
func (m *Manager) GetDIDIndex() *didindex.Manager {
	return m.didIndex
}

// BuildDIDIndex builds the complete DID index
func (m *Manager) BuildDIDIndex(ctx context.Context, progressCallback func(current, total int)) error {
	if m.didIndex == nil {
		m.didIndex = didindex.NewManager(m.config.BundleDir, m.logger)
	}

	return m.didIndex.BuildIndexFromScratch(ctx, m, progressCallback)
}

// updateDIDIndexForBundle updates index when a new bundle is added
func (m *Manager) updateDIDIndexForBundle(ctx context.Context, bundle *Bundle) error {
	if m.didIndex == nil {
		return nil
	}

	// Convert to didindex.BundleData
	bundleData := &didindex.BundleData{
		BundleNumber: bundle.BundleNumber,
		Operations:   bundle.Operations,
	}

	return m.didIndex.UpdateIndexForBundle(ctx, bundleData)
}

// GetDIDIndexStats returns DID index statistics
func (m *Manager) GetDIDIndexStats() map[string]interface{} {
	if m.didIndex == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	stats := m.didIndex.GetStats()
	stats["enabled"] = true
	stats["exists"] = m.didIndex.Exists()

	indexedDIDs := stats["total_dids"].(int64)

	// Get unique DIDs from mempool
	mempoolDIDCount := int64(0)
	if m.mempool != nil {
		mempoolStats := m.GetMempoolStats()
		if didCount, ok := mempoolStats["did_count"].(int); ok {
			mempoolDIDCount = int64(didCount)
		}
	}

	stats["indexed_dids"] = indexedDIDs
	stats["mempool_dids"] = mempoolDIDCount
	stats["total_dids"] = indexedDIDs + mempoolDIDCount

	return stats
}

// GetDIDOperations retrieves all operations for a DID (bundles + mempool combined)
func (m *Manager) GetDIDOperations(ctx context.Context, did string, verbose bool) ([]plcclient.PLCOperation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Get bundled operations
	bundledOps, err := m.getDIDOperationsBundledOnly(ctx, did, verbose)
	if err != nil {
		return nil, err
	}

	// Get mempool operations
	mempoolOps, err := m.GetDIDOperationsFromMempool(did)
	if err != nil {
		return nil, err
	}

	if len(mempoolOps) > 0 && verbose {
		m.logger.Printf("DEBUG: Found %d operations in mempool", len(mempoolOps))
	}

	// Combine and sort
	allOps := append(bundledOps, mempoolOps...)

	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
	})

	return allOps, nil
}

// getDIDOperationsBundledOnly retrieves operations from bundles only (private helper)
func (m *Manager) getDIDOperationsBundledOnly(ctx context.Context, did string, verbose bool) ([]plcclient.PLCOperation, error) {
	// Set verbose mode on index
	if m.didIndex != nil {
		m.didIndex.SetVerbose(verbose)
	}

	// Use index if available
	if m.didIndex != nil && m.didIndex.Exists() {
		if verbose {
			m.logger.Printf("DEBUG: Using DID index for lookup")
		}

		locations, err := m.didIndex.GetDIDLocations(did)
		if err != nil {
			return nil, err
		}

		if len(locations) == 0 {
			return []plcclient.PLCOperation{}, nil
		}

		// Filter nullified
		var validLocations []didindex.OpLocation
		for _, loc := range locations {
			if !loc.Nullified {
				validLocations = append(validLocations, loc)
			}
		}

		if verbose {
			m.logger.Printf("DEBUG: Filtered %d valid locations (from %d total)",
				len(validLocations), len(locations))
		}

		// Group by bundle
		bundleMap := make(map[uint16][]uint16)
		for _, loc := range validLocations {
			bundleMap[loc.Bundle] = append(bundleMap[loc.Bundle], loc.Position)
		}

		if verbose {
			m.logger.Printf("DEBUG: Loading from %d bundle(s)", len(bundleMap))
		}

		// Load operations
		var allOps []plcclient.PLCOperation
		for bundleNum, positions := range bundleMap {
			bundle, err := m.LoadBundle(ctx, int(bundleNum))
			if err != nil {
				m.logger.Printf("Warning: failed to load bundle %d: %v", bundleNum, err)
				continue
			}

			for _, pos := range positions {
				if int(pos) < len(bundle.Operations) {
					allOps = append(allOps, bundle.Operations[pos])
				}
			}
		}

		if verbose {
			m.logger.Printf("DEBUG: Loaded %d total operations", len(allOps))
		}

		sort.Slice(allOps, func(i, j int) bool {
			return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
		})

		return allOps, nil
	}

	// Fallback to full scan
	if verbose {
		m.logger.Printf("DEBUG: DID index not available, using full scan")
	}
	m.logger.Printf("Warning: DID index not available, falling back to full scan")

	return m.getDIDOperationsScan(ctx, did)
}

// getDIDOperationsFromMempool retrieves operations for a DID from mempool only
func (m *Manager) GetDIDOperationsFromMempool(did string) ([]plcclient.PLCOperation, error) {
	if m.mempool == nil {
		return []plcclient.PLCOperation{}, nil
	}

	allMempoolOps, err := m.GetMempoolOperations()
	if err != nil {
		return nil, err
	}

	matchingOps := make([]plcclient.PLCOperation, 0, 16)

	for _, op := range allMempoolOps {
		if op.DID == did {
			matchingOps = append(matchingOps, op)
		}
	}

	return matchingOps, nil
}

// getDIDOperationsScan falls back to full scan (private helper)
func (m *Manager) getDIDOperationsScan(ctx context.Context, did string) ([]plcclient.PLCOperation, error) {
	var allOps []plcclient.PLCOperation
	bundles := m.index.GetBundles()

	for _, meta := range bundles {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		bundle, err := m.LoadBundle(ctx, meta.BundleNumber)
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", meta.BundleNumber, err)
			continue
		}

		for _, op := range bundle.Operations {
			if op.DID == did {
				allOps = append(allOps, op)
			}
		}
	}

	sort.Slice(allOps, func(i, j int) bool {
		return allOps[i].CreatedAt.Before(allOps[j].CreatedAt)
	})

	return allOps, nil
}

// GetLatestDIDOperation returns only the most recent non-nullified operation
func (m *Manager) GetLatestDIDOperation(ctx context.Context, did string) (*plcclient.PLCOperation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Check mempool first
	mempoolOps, err := m.GetDIDOperationsFromMempool(did)
	if err == nil && len(mempoolOps) > 0 {
		// Find latest non-nullified in mempool
		for i := len(mempoolOps) - 1; i >= 0; i-- {
			if !mempoolOps[i].IsNullified() {
				return &mempoolOps[i], nil
			}
		}
	}

	// Use index if available
	if m.didIndex != nil && m.didIndex.Exists() {
		locations, err := m.didIndex.GetDIDLocations(did)
		if err != nil {
			return nil, err
		}

		if len(locations) == 0 {
			return nil, fmt.Errorf("DID not found")
		}

		// Find latest non-nullified location
		var latestLoc *didindex.OpLocation
		for i := range locations {
			if locations[i].Nullified {
				continue
			}

			if latestLoc == nil {
				latestLoc = &locations[i]
			} else {
				if locations[i].Bundle > latestLoc.Bundle ||
					(locations[i].Bundle == latestLoc.Bundle && locations[i].Position > latestLoc.Position) {
					latestLoc = &locations[i]
				}
			}
		}

		if latestLoc == nil {
			return nil, fmt.Errorf("no valid operations found (all nullified)")
		}

		// Load operation
		op, err := m.LoadOperation(ctx, int(latestLoc.Bundle), int(latestLoc.Position))
		if err != nil {
			return nil, fmt.Errorf("failed to load operation at bundle %d position %d: %w",
				latestLoc.Bundle, latestLoc.Position, err)
		}

		return op, nil
	}

	// Fallback to full lookup
	ops, err := m.getDIDOperationsBundledOnly(ctx, did, false)
	if err != nil {
		return nil, err
	}

	// Find latest non-nullified
	for i := len(ops) - 1; i >= 0; i-- {
		if !ops[i].IsNullified() {
			return &ops[i], nil
		}
	}

	return nil, fmt.Errorf("no valid operations found")
}

// GetDIDOperationsWithLocations returns operations along with their bundle/position info
func (m *Manager) GetDIDOperationsWithLocations(ctx context.Context, did string, verbose bool) ([]PLCOperationWithLocation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Set verbose mode
	if m.didIndex != nil {
		m.didIndex.SetVerbose(verbose)
	}

	// Use index if available
	if m.didIndex != nil && m.didIndex.Exists() {
		if verbose {
			m.logger.Printf("DEBUG: Using DID index for lookup with locations")
		}
		return m.getDIDOperationsWithLocationsIndexed(ctx, did, verbose)
	}

	// Fallback to scan
	if verbose {
		m.logger.Printf("DEBUG: Using full scan with locations")
	}
	return m.getDIDOperationsWithLocationsScan(ctx, did)
}

// getDIDOperationsWithLocationsIndexed uses index for fast lookup with locations
func (m *Manager) getDIDOperationsWithLocationsIndexed(ctx context.Context, did string, verbose bool) ([]PLCOperationWithLocation, error) {
	locations, err := m.didIndex.GetDIDLocations(did)
	if err != nil {
		return nil, err
	}

	if len(locations) == 0 {
		return []PLCOperationWithLocation{}, nil
	}

	if verbose {
		m.logger.Printf("DEBUG: Found %d locations in index", len(locations))
	}

	// Load operations with their locations preserved
	var results []PLCOperationWithLocation

	// Group by bundle for efficient loading
	bundleMap := make(map[uint16][]didindex.OpLocation)
	for _, loc := range locations {
		bundleMap[loc.Bundle] = append(bundleMap[loc.Bundle], loc)
	}

	if verbose {
		m.logger.Printf("DEBUG: Loading from %d bundle(s)", len(bundleMap))
	}

	for bundleNum, locs := range bundleMap {
		bundle, err := m.LoadBundle(ctx, int(bundleNum))
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", bundleNum, err)
			continue
		}

		for _, loc := range locs {
			if int(loc.Position) >= len(bundle.Operations) {
				continue
			}

			op := bundle.Operations[loc.Position]
			results = append(results, PLCOperationWithLocation{
				Operation: op,
				Bundle:    int(loc.Bundle),
				Position:  int(loc.Position),
			})
		}
	}

	// Sort by time
	sort.Slice(results, func(i, j int) bool {
		return results[i].Operation.CreatedAt.Before(results[j].Operation.CreatedAt)
	})

	if verbose {
		m.logger.Printf("DEBUG: Loaded %d total operations", len(results))
	}

	return results, nil
}

// getDIDOperationsWithLocationsScan falls back to full scan with locations
func (m *Manager) getDIDOperationsWithLocationsScan(ctx context.Context, did string) ([]PLCOperationWithLocation, error) {
	var results []PLCOperationWithLocation
	bundles := m.index.GetBundles()

	for _, meta := range bundles {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		bundle, err := m.LoadBundle(ctx, meta.BundleNumber)
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", meta.BundleNumber, err)
			continue
		}

		for pos, op := range bundle.Operations {
			if op.DID == did {
				results = append(results, PLCOperationWithLocation{
					Operation: op,
					Bundle:    meta.BundleNumber,
					Position:  pos,
				})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Operation.CreatedAt.Before(results[j].Operation.CreatedAt)
	})

	return results, nil
}

// VerifyChain verifies the entire bundle chain
func (m *Manager) VerifyChain(ctx context.Context) (*ChainVerificationResult, error) {
	result := &ChainVerificationResult{
		VerifiedBundles: make([]int, 0),
	}

	bundles := m.index.GetBundles()
	if len(bundles) == 0 {
		result.Valid = true
		return result, nil
	}

	result.ChainLength = len(bundles)

	for i, meta := range bundles {
		// Verify file hash
		vr, err := m.VerifyBundle(ctx, meta.BundleNumber)
		if err != nil || !vr.Valid {
			result.Error = fmt.Sprintf("Bundle %d hash verification failed", meta.BundleNumber)
			result.BrokenAt = meta.BundleNumber
			return result, nil
		}

		// Verify chain link
		if i > 0 {
			prevMeta := bundles[i-1]

			// Check parent reference
			if meta.Parent != prevMeta.Hash {
				result.Error = fmt.Sprintf("Chain broken at bundle %d: parent mismatch", meta.BundleNumber)
				result.BrokenAt = meta.BundleNumber
				return result, nil
			}

			// Verify chain hash calculation
			expectedHash := m.operations.CalculateChainHash(prevMeta.Hash, meta.ContentHash)
			if meta.Hash != expectedHash {
				result.Error = fmt.Sprintf("Chain broken at bundle %d: hash mismatch", meta.BundleNumber)
				result.BrokenAt = meta.BundleNumber
				return result, nil
			}
		}

		result.VerifiedBundles = append(result.VerifiedBundles, meta.BundleNumber)
	}

	result.Valid = true
	return result, nil
}

// FetchNextBundle delegates to sync.Fetcher
func (m *Manager) FetchNextBundle(ctx context.Context, quiet bool) (*Bundle, error) {
	if m.plcClient == nil {
		return nil, fmt.Errorf("PLC client not configured")
	}

	lastBundle := m.index.GetLastBundle()
	nextBundleNum := 1
	var afterTime string
	var prevBoundaryCIDs map[string]bool
	var prevBundleHash string

	if lastBundle != nil {
		nextBundleNum = lastBundle.BundleNumber + 1
		afterTime = lastBundle.EndTime.Format(time.RFC3339Nano)
		prevBundleHash = lastBundle.Hash

		prevBundle, err := m.LoadBundle(ctx, lastBundle.BundleNumber)
		if err == nil {
			_, prevBoundaryCIDs = m.operations.GetBoundaryCIDs(prevBundle.Operations)
		}
	}

	// ✨ Use mempool's last time if available
	if m.mempool.Count() > 0 {
		mempoolLastTime := m.mempool.GetLastTime()
		if mempoolLastTime != "" {
			afterTime = mempoolLastTime
			if !quiet {
				m.logger.Printf("Continuing from mempool cursor: %s (have %d ops)",
					afterTime, m.mempool.Count())
			}
		}
	}

	if !quiet {
		m.logger.Printf("Preparing bundle %06d (mempool: %d ops)...", nextBundleNum, m.mempool.Count())
	}

	// ✨ Fetch operations if needed (FetchToMempool loops internally)
	if m.mempool.Count() < types.BUNDLE_SIZE {
		newOps, err := m.syncer.FetchToMempool(
			ctx,
			afterTime,
			prevBoundaryCIDs,
			types.BUNDLE_SIZE-m.mempool.Count(),
			quiet,
			m.mempool.Count(),
		)

		// Add operations if we got any
		if len(newOps) > 0 {
			added, addErr := m.mempool.Add(newOps)
			if addErr != nil {
				m.mempool.Save()
				return nil, fmt.Errorf("chronological validation failed: %w", addErr)
			}

			if !quiet && added > 0 {
				m.logger.Printf("Added %d new operations (mempool now: %d)", added, m.mempool.Count())
			}
		}

		// If fetch failed AND we don't have enough, return error
		if err != nil && m.mempool.Count() < types.BUNDLE_SIZE {
			m.mempool.Save()
			return nil, err
		}
	}

	// Check if we have enough for a bundle
	if m.mempool.Count() < types.BUNDLE_SIZE {
		m.mempool.Save()
		return nil, fmt.Errorf("insufficient operations: have %d, need %d (no more available)",
			m.mempool.Count(), types.BUNDLE_SIZE)
	}

	// Create bundle
	operations, err := m.mempool.Take(types.BUNDLE_SIZE)
	if err != nil {
		m.mempool.Save()
		return nil, err
	}

	bundle := m.CreateBundle(nextBundleNum, operations, afterTime, prevBundleHash)
	m.mempool.Save()

	return bundle, nil
}

// CloneFromRemote delegates to sync.Cloner
func (m *Manager) CloneFromRemote(ctx context.Context, opts internalsync.CloneOptions) (*internalsync.CloneResult, error) {
	// Delegate to cloner with index update callback
	return m.cloner.Clone(ctx, opts, m.index, m.updateIndexFromRemote)
}

// updateIndexFromRemote updates local index with metadata from remote index
func (m *Manager) updateIndexFromRemote(bundleNumbers []int, remoteMeta map[int]*bundleindex.BundleMetadata, verbose bool) error {
	if len(bundleNumbers) == 0 {
		return nil
	}

	// Add/update bundles in local index using remote metadata
	// Hash verification was already done during download
	for _, num := range bundleNumbers {
		if meta, exists := remoteMeta[num]; exists {
			// Verify the file exists locally
			path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", num))
			if !m.operations.FileExists(path) {
				m.logger.Printf("Warning: bundle %06d not found locally, skipping", num)
				continue
			}

			// Add to index (no need to re-verify hash - already verified during download)
			m.index.AddBundle(meta)

			if verbose {
				m.logger.Printf("Added bundle %06d to index", num)
			}
		}
	}

	// Save index
	return m.SaveIndex()
}

func (m *Manager) CreateBundle(bundleNumber int, operations []plcclient.PLCOperation, cursor string, parent string) *Bundle {
	// Delegate to sync package
	syncBundle := internalsync.CreateBundle(bundleNumber, operations, cursor, parent, m.operations)

	// Convert if needed (or just return directly if types match)
	return &Bundle{
		BundleNumber: syncBundle.BundleNumber,
		StartTime:    syncBundle.StartTime,
		EndTime:      syncBundle.EndTime,
		Operations:   syncBundle.Operations,
		DIDCount:     syncBundle.DIDCount,
		Cursor:       syncBundle.Cursor,
		Parent:       syncBundle.Parent,
		BoundaryCIDs: syncBundle.BoundaryCIDs,
		Compressed:   syncBundle.Compressed,
		CreatedAt:    syncBundle.CreatedAt,
	}
}
