package bundle

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/didindex"
	"tangled.org/atscan.net/plcbundle/internal/handleresolver"
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
	logger     types.Logger
	mempool    *mempool.Mempool
	didIndex   *didindex.Manager

	plcClient      *plcclient.Client
	handleResolver *handleresolver.Client

	syncer *internalsync.Fetcher
	cloner *internalsync.Cloner

	bundleCache  map[int]*Bundle
	cacheMu      sync.RWMutex
	maxCacheSize int

	// Resolver performance tracking
	resolverStats struct {
		sync.Mutex
		totalResolutions int64
		mempoolHits      int64
		bundleHits       int64
		errors           int64

		// Timing (in microseconds)
		totalTime        int64
		totalMempoolTime int64
		totalIndexTime   int64
		totalLoadOpTime  int64

		// Recent timings (circular buffer)
		recentTimes []resolverTiming
		recentIdx   int
		recentSize  int
	}
}

// NewManager creates a new bundle manager
func NewManager(config *Config, plcClient *plcclient.Client) (*Manager, error) {
	if config == nil {
		config = DefaultConfig("./plc_bundles")
	}

	if config.Logger == nil {
		config.Logger = defaultLogger{}
	}

	// CHECK: Don't auto-create if repository doesn't exist
	repoExists := repositoryExists(config.BundleDir)

	if !repoExists && !config.AutoInit {
		return nil, fmt.Errorf(
			"no plcbundle repository found in: %s\n\n"+
				"Initialize a new repository with:\n"+
				"  plcbundle clone <url>     # Clone from remote\n"+
				"  plcbundle sync            # Fetch from PLC directory",
			config.BundleDir,
		)
	}

	// Ensure directory exists (only if repo exists OR AutoInit is enabled)
	if err := os.MkdirAll(config.BundleDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create bundle directory: %w", err)
	}

	// Initialize operations handler
	ops, err := storage.NewOperations(config.Logger, config.Verbose)
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

		if config.Verbose {
			config.Logger.Printf("Loaded index with %d bundles (origin: %s)", index.Count(), index.Origin)
		}

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

	mempool, err := mempool.NewMempool(config.BundleDir, nextBundleNum, minTimestamp, config.Logger, config.Verbose)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize mempool: %w", err)
	}

	// Initialize DID index manager
	didIndex := didindex.NewManager(config.BundleDir, config.Logger)

	// Initialize sync components
	fetcher := internalsync.NewFetcher(plcClient, ops, config.Logger)
	cloner := internalsync.NewCloner(ops, config.BundleDir, config.Logger)

	// Initialize handle resolver if configured
	var handleResolver *handleresolver.Client
	if config.HandleResolverURL != "" {
		handleResolver = handleresolver.NewClient(config.HandleResolverURL)
	}

	m := &Manager{
		config:         config,
		operations:     ops,
		index:          index,
		indexPath:      indexPath,
		logger:         config.Logger,
		mempool:        mempool,
		didIndex:       didIndex, // Updated type
		bundleCache:    make(map[int]*Bundle),
		maxCacheSize:   10,
		syncer:         fetcher,
		cloner:         cloner,
		plcClient:      plcClient,
		handleResolver: handleResolver,
	}
	// Initialize resolver stats
	m.resolverStats.recentSize = 1000
	m.resolverStats.recentTimes = make([]resolverTiming, 1000)

	return m, nil
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
func (m *Manager) loadBundleFromDisk(_ context.Context, bundleNumber int) (*Bundle, error) {
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
func (m *Manager) SaveBundle(ctx context.Context, bundle *Bundle, verbose bool, quiet bool, stats types.BundleProductionStats) (time.Duration, error) {

	totalStart := time.Now()
	if err := bundle.ValidateForSave(); err != nil {
		return 0, fmt.Errorf("bundle validation failed: %w", err)
	}

	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundle.BundleNumber))

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

	// Get origin
	origin := m.index.Origin
	if m.plcClient != nil {
		origin = m.plcClient.GetBaseURL()
	}

	// Get version
	version := "dev"
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}

	// Get hostname
	hostname, _ := os.Hostname()

	// Create BundleInfo
	bundleInfo := &storage.BundleInfo{
		BundleNumber: bundle.BundleNumber,
		Origin:       origin,
		ParentHash:   parent,
		Cursor:       bundle.Cursor,
		CreatedBy:    fmt.Sprintf("plcbundle/%s", version),
		Hostname:     hostname,
	}

	if m.config.Verbose {
		m.logger.Printf("DEBUG: Calling operations.SaveBundle with bundle=%d", bundleInfo.BundleNumber)
	}

	// Save to disk with 3 parameters
	uncompressedHash, compressedHash, uncompressedSize, compressedSize, err := m.operations.SaveBundle(path, bundle.Operations, bundleInfo)
	if err != nil {
		m.logger.Printf("DEBUG: SaveBundle FAILED: %v", err)
		return 0, fmt.Errorf("failed to save bundle: %w", err)
	}

	if m.config.Verbose {
		m.logger.Printf("DEBUG: SaveBundle SUCCESS, setting bundle fields")
	}

	bundle.Hash = m.operations.CalculateChainHash(parent, bundle.ContentHash)
	bundle.ContentHash = uncompressedHash
	bundle.CompressedHash = compressedHash
	bundle.UncompressedSize = uncompressedSize
	bundle.CompressedSize = compressedSize
	bundle.CreatedAt = time.Now().UTC()
	bundle.Hash = m.operations.CalculateChainHash(parent, bundle.ContentHash)

	if m.config.Verbose {
		m.logger.Printf("DEBUG: Adding bundle %d to index", bundle.BundleNumber)
	}

	// Add to index
	m.index.AddBundle(bundle.ToMetadata())

	if m.config.Verbose {
		m.logger.Printf("DEBUG: Index now has %d bundles", m.index.Count())
	}

	// Save index
	if err := m.SaveIndex(); err != nil {
		m.logger.Printf("DEBUG: SaveIndex FAILED: %v", err)
		return 0, fmt.Errorf("failed to save index: %w", err)
	}

	if m.config.Verbose {
		m.logger.Printf("DEBUG: Index saved, last bundle = %d", m.index.GetLastBundle().BundleNumber)
	}

	saveDuration := time.Since(totalStart)

	// Clean up old mempool
	oldMempoolFile := m.mempool.GetFilename()
	if err := m.mempool.Delete(); err != nil && !quiet {
		m.logger.Printf("Warning: failed to delete old mempool %s: %v", oldMempoolFile, err)
	}

	// Create new mempool
	nextBundle := bundle.BundleNumber + 1
	minTimestamp := bundle.EndTime

	newMempool, err := mempool.NewMempool(m.config.BundleDir, nextBundle, minTimestamp, m.logger, m.config.Verbose)
	if err != nil {
		return 0, fmt.Errorf("failed to create new mempool: %w", err)
	}

	m.mempool = newMempool

	// DID index update (if enabled)
	var indexUpdateDuration time.Duration
	if m.didIndex != nil && m.didIndex.Exists() {
		indexUpdateStart := time.Now()
		if err := m.updateDIDIndexForBundle(ctx, bundle); err != nil {
			m.logger.Printf("Warning: failed to update DID index: %v", err)
		} else {
			indexUpdateDuration = time.Since(indexUpdateStart)
			if !quiet && m.config.Verbose {
				m.logger.Printf("  [DID Index] Updated in %s", indexUpdateDuration)
			}
		}
	}

	if !quiet {
		msg := fmt.Sprintf("→ Bundle %06d | %s | fetch: %s (%d reqs)",
			bundle.BundleNumber,
			bundle.Hash[0:7],
			stats.TotalDuration.Round(time.Millisecond),
			stats.TotalFetches,
		)
		if indexUpdateDuration > 0 {
			msg += fmt.Sprintf(" | index: %s", indexUpdateDuration.Round(time.Millisecond))
		}
		msg += fmt.Sprintf(" | %s", formatTimeDistance(time.Since(bundle.EndTime)))
		m.logger.Println(msg)
	}

	if m.config.Verbose {
		m.logger.Printf("DEBUG: Bundle done = %d, finish duration = %s",
			m.index.GetLastBundle().BundleNumber,
			saveDuration.Round(time.Millisecond))
	}

	return indexUpdateDuration, nil
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

// Add to Bundle type to implement BundleData interface
func (b *Bundle) GetBundleNumber() int {
	return b.BundleNumber
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
	// Validate position
	if position < 0 || position >= types.BUNDLE_SIZE {
		return nil, fmt.Errorf("invalid position: %d (must be 0-%d)", position, types.BUNDLE_SIZE-1)
	}

	// Validate bundle exists in index
	_, err := m.index.GetBundle(bundleNumber)
	if err != nil {
		return nil, fmt.Errorf("bundle not in index: %w", err)
	}

	// Build file path
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	if !m.operations.FileExists(path) {
		return nil, fmt.Errorf("bundle file not found: %s", path)
	}

	// Load just the one operation (optimized - decompresses only until position)
	return m.operations.LoadOperationAtPosition(path, position)
}

// LoadOperations loads multiple operations from a bundle efficiently
func (m *Manager) LoadOperations(ctx context.Context, bundleNumber int, positions []int) (map[int]*plcclient.PLCOperation, error) {
	// Build file path
	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNumber))
	if !m.operations.FileExists(path) {
		return nil, fmt.Errorf("bundle file not found: %s", path)
	}

	return m.operations.LoadOperationsAtPositions(path, positions)
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

	// Set verbose mode
	if m.didIndex != nil {
		m.didIndex.SetVerbose(verbose)
	}

	// Get bundled operations from DID index
	bundledOps, err := m.didIndex.GetDIDOperations(ctx, did, m)
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

// GetDIDOperationsFromMempool retrieves operations for a DID from mempool only
func (m *Manager) GetDIDOperationsFromMempool(did string) ([]plcclient.PLCOperation, error) {
	if m.mempool == nil {
		return []plcclient.PLCOperation{}, nil
	}

	// Use direct search - only copies matching operations
	return m.mempool.FindDIDOperations(did), nil
}

// GetLatestDIDOperation returns only the most recent non-nullified operation
func (m *Manager) GetLatestDIDOperation(ctx context.Context, did string) (*plcclient.PLCOperation, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		return nil, err
	}

	// Check mempool first (most recent data)
	mempoolOps, _ := m.GetDIDOperationsFromMempool(did)
	if len(mempoolOps) > 0 {
		for i := len(mempoolOps) - 1; i >= 0; i-- {
			if !mempoolOps[i].IsNullified() {
				return &mempoolOps[i], nil
			}
		}
	}

	// Delegate to DID index for bundled operations
	return m.didIndex.GetLatestDIDOperation(ctx, did, m)
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

	// Delegate to DID index
	results, err := m.didIndex.GetDIDOperationsWithLocations(ctx, did, m)
	if err != nil {
		return nil, err
	}

	// Convert to bundle's type
	bundleResults := make([]PLCOperationWithLocation, len(results))
	for i, r := range results {
		bundleResults[i] = PLCOperationWithLocation{
			Operation: r.Operation,
			Bundle:    r.Bundle,
			Position:  r.Position,
		}
	}

	return bundleResults, nil
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

// FetchNextBundle fetches operations and creates a bundle, looping until caught up
func (m *Manager) FetchNextBundle(ctx context.Context, verbose bool, quiet bool) (*Bundle, types.BundleProductionStats, error) {
	if m.plcClient == nil {
		return nil, types.BundleProductionStats{}, fmt.Errorf("PLC client not configured")
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

		// ALWAYS get boundaries from last bundle initially
		prevBundle, err := m.LoadBundle(ctx, lastBundle.BundleNumber)
		if err == nil {
			_, prevBoundaryCIDs = m.operations.GetBoundaryCIDs(prevBundle.Operations)
			if verbose {
				m.logger.Printf("Loaded %d boundary CIDs from bundle %06d (at %s)",
					len(prevBoundaryCIDs), lastBundle.BundleNumber,
					lastBundle.EndTime.Format(time.RFC3339)[:19])
			}
		}
	}

	// If mempool has operations, update cursor but KEEP boundaries from bundle
	// (mempool operations already had boundary dedup applied when they were added)
	if m.mempool.Count() > 0 {
		mempoolLastTime := m.mempool.GetLastTime()
		if mempoolLastTime != "" {
			if verbose {
				m.logger.Printf("[DEBUG] Mempool has %d ops, resuming from %s",
					m.mempool.Count(), mempoolLastTime[:19])
			}
			afterTime = mempoolLastTime

			// Calculate boundaries from MEMPOOL for next fetch
			mempoolOps, _ := m.GetMempoolOperations()
			if len(mempoolOps) > 0 {
				_, mempoolBoundaries := m.operations.GetBoundaryCIDs(mempoolOps)
				prevBoundaryCIDs = mempoolBoundaries
				if verbose {
					m.logger.Printf("Using %d boundary CIDs from mempool", len(prevBoundaryCIDs))
				}
			}
		}
	}

	if verbose {
		m.logger.Printf("[DEBUG] Preparing bundle %06d (mempool: %d ops)...", nextBundleNum, m.mempool.Count())
		m.logger.Printf("[DEBUG] Starting cursor: %s", afterTime)
	}

	totalFetches := 0
	maxAttempts := 50
	attempt := 0
	caughtUp := false
	attemptStart := time.Now()

	for m.mempool.Count() < types.BUNDLE_SIZE && attempt < maxAttempts {
		attempt++
		needed := types.BUNDLE_SIZE - m.mempool.Count()

		if !quiet && attempt > 1 {
			m.logger.Printf("  Attempt %d: Need %d more ops, cursor: %s",
				attempt, needed, afterTime[:19])
		}

		newOps, fetchCount, err := m.syncer.FetchToMempool(
			ctx,
			afterTime,
			prevBoundaryCIDs,
			needed,
			!verbose,
			m.mempool,
			totalFetches,
		)

		totalFetches += fetchCount

		// Check if we got an incomplete batch
		gotIncompleteBatch := len(newOps) > 0 && len(newOps) < needed && err == nil

		// Update cursor from mempool if we got new ops
		if len(newOps) > 0 && m.mempool.Count() > 0 {
			afterTime = m.mempool.GetLastTime()
		}

		// Stop if caught up or error
		if err != nil || len(newOps) == 0 || gotIncompleteBatch {
			caughtUp = true
			if verbose && totalFetches > 0 {
				m.logger.Printf("DEBUG: Caught up to latest PLC data")
			}
			break
		}

		if m.mempool.Count() >= types.BUNDLE_SIZE {
			break
		}
	}

	totalDuration := time.Since(attemptStart)

	if m.mempool.Count() < types.BUNDLE_SIZE {
		if caughtUp {
			return nil, types.BundleProductionStats{}, fmt.Errorf("insufficient operations: have %d, need %d (caught up to latest PLC data)",
				m.mempool.Count(), types.BUNDLE_SIZE)
		} else {
			return nil, types.BundleProductionStats{}, fmt.Errorf("insufficient operations: have %d, need %d (max attempts reached)",
				m.mempool.Count(), types.BUNDLE_SIZE)
		}
	}

	// Create bundle
	operations, err := m.mempool.Take(types.BUNDLE_SIZE)
	if err != nil {
		return nil, types.BundleProductionStats{}, err
	}

	syncBundle := internalsync.CreateBundle(nextBundleNum, operations, afterTime, prevBundleHash, m.operations)

	bundle := &Bundle{
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

	stats := types.BundleProductionStats{
		TotalFetches:  totalFetches,
		TotalDuration: totalDuration,
		AvgPerFetch:   float64(types.BUNDLE_SIZE) / float64(totalFetches),
		Throughput:    float64(types.BUNDLE_SIZE) / totalDuration.Seconds(),
	}

	return bundle, stats, nil
}

// CloneFromRemote clones bundles from a remote endpoint
func (m *Manager) CloneFromRemote(ctx context.Context, opts internalsync.CloneOptions) (*internalsync.CloneResult, error) {
	// Define index update callback inline
	updateIndexCallback := func(bundleNumbers []int, remoteMeta map[int]*bundleindex.BundleMetadata, verbose bool) error {
		if len(bundleNumbers) == 0 {
			return nil
		}

		// Create file existence checker
		fileExists := func(bundleNum int) bool {
			path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))
			return m.operations.FileExists(path)
		}

		// Update index with remote metadata
		if err := m.index.UpdateFromRemote(bundleNumbers, remoteMeta, fileExists, verbose, m.logger); err != nil {
			return err
		}

		// Save index
		return m.SaveIndex()
	}

	// Delegate to cloner with inline callback
	return m.cloner.Clone(ctx, opts, m.index, updateIndexCallback)
}

// ResolveDID resolves a DID to its current document with detailed timing metrics
func (m *Manager) ResolveDID(ctx context.Context, did string) (*ResolveDIDResult, error) {
	if err := plcclient.ValidateDIDFormat(did); err != nil {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, err
	}

	result := &ResolveDIDResult{}
	totalStart := time.Now()

	// STEP 1: Check mempool first
	mempoolStart := time.Now()
	var latestMempoolOp *plcclient.PLCOperation
	if m.mempool != nil {
		latestMempoolOp = m.mempool.FindLatestDIDOperation(did)
	}
	result.MempoolTime = time.Since(mempoolStart)

	// Early return if found in mempool
	if latestMempoolOp != nil {
		doc, err := plcclient.ResolveDIDDocument(did, []plcclient.PLCOperation{*latestMempoolOp})
		if err != nil {
			atomic.AddInt64(&m.resolverStats.errors, 1)
			return nil, fmt.Errorf("resolution failed: %w", err)
		}

		result.Document = doc
		result.LatestOperation = latestMempoolOp
		result.Source = "mempool"
		result.TotalTime = time.Since(totalStart)

		m.recordResolverTiming(result, nil)
		return result, nil
	}

	// STEP 2: Index lookup
	if m.didIndex == nil || !m.didIndex.Exists() {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, fmt.Errorf("DID index not available - run 'plcbundle index build' to enable DID resolution")
	}

	indexStart := time.Now()
	locations, err := m.didIndex.GetDIDLocations(did)
	result.IndexTime = time.Since(indexStart)

	if err != nil {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, err
	}

	if len(locations) == 0 {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, fmt.Errorf("DID not found")
	}

	// Find latest non-nullified location
	var latestLoc *didindex.OpLocation
	for i := range locations {
		if locations[i].Nullified() {
			continue
		}
		if latestLoc == nil || locations[i].IsAfter(*latestLoc) {
			latestLoc = &locations[i]
		}
	}

	if latestLoc == nil {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, fmt.Errorf("no valid operations (all nullified)")
	}

	// STEP 3: Load operation
	opStart := time.Now()
	op, err := m.LoadOperation(ctx, latestLoc.BundleInt(), latestLoc.PositionInt())
	result.LoadOpTime = time.Since(opStart)

	if err != nil {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, fmt.Errorf("failed to load operation: %w", err)
	}

	result.BundleNumber = latestLoc.BundleInt()
	result.Position = latestLoc.PositionInt()

	// STEP 4: Resolve document
	doc, err := plcclient.ResolveDIDDocument(did, []plcclient.PLCOperation{*op})
	if err != nil {
		atomic.AddInt64(&m.resolverStats.errors, 1)
		return nil, fmt.Errorf("resolution failed: %w", err)
	}

	result.Document = doc
	result.LatestOperation = op
	result.Source = "bundle"
	result.TotalTime = time.Since(totalStart)

	m.recordResolverTiming(result, nil)
	return result, nil
}

// GetLastBundleNumber returns the last bundle number (0 if no bundles)
func (m *Manager) GetLastBundleNumber() int {
	lastBundle := m.index.GetLastBundle()
	if lastBundle == nil {
		return 0
	}
	return lastBundle.BundleNumber
}

// GetMempoolCount returns the number of operations in mempool
func (m *Manager) GetMempoolCount() int {
	return m.mempool.Count()
}

// FetchAndSaveNextBundle fetches and saves next bundle, returns bundle number and index time
func (m *Manager) FetchAndSaveNextBundle(ctx context.Context, verbose bool, quiet bool) (int, *types.BundleProductionStats, error) {
	bundle, stats, err := m.FetchNextBundle(ctx, verbose, quiet)
	if err != nil {
		return 0, nil, err
	}

	indexTime, err := m.SaveBundle(ctx, bundle, verbose, quiet, stats)
	if err != nil {
		return 0, nil, err
	}
	stats.IndexTime = indexTime

	return bundle.BundleNumber, &types.BundleProductionStats{}, nil
}

// RunSyncLoop runs continuous sync loop (delegates to internal/sync)
func (m *Manager) RunSyncLoop(ctx context.Context, config *internalsync.SyncLoopConfig) error {
	// Manager itself implements the SyncManager interface
	return internalsync.RunSyncLoop(ctx, m, config)
}

// RunSyncOnce performs a single sync cycle
func (m *Manager) RunSyncOnce(ctx context.Context, config *internalsync.SyncLoopConfig, verbose bool) (int, error) {
	// Manager itself implements the SyncManager interface
	return internalsync.SyncOnce(ctx, m, config, verbose)
}

// EnsureDIDIndex ensures DID index is built and up-to-date
// Returns true if index was built/rebuilt, false if already up-to-date
func (m *Manager) EnsureDIDIndex(ctx context.Context, progressCallback func(current, total int)) (bool, error) {
	bundleCount := m.index.Count()
	didStats := m.GetDIDIndexStats()

	if bundleCount == 0 {
		return false, nil
	}

	needsBuild := false
	reason := ""

	if !didStats["exists"].(bool) {
		needsBuild = true
		reason = "index does not exist"
	} else {
		// Check version
		if m.didIndex != nil {
			config := m.didIndex.GetConfig()
			if config.Version != didindex.DIDINDEX_VERSION {
				needsBuild = true
				reason = fmt.Sprintf("index version outdated (v%d, need v%d)",
					config.Version, didindex.DIDINDEX_VERSION)
			} else {
				// Check if index is behind bundles
				lastBundle := m.index.GetLastBundle()
				if lastBundle != nil && config.LastBundle < lastBundle.BundleNumber {
					needsBuild = true
					reason = fmt.Sprintf("index is behind (bundle %d, need %d)",
						config.LastBundle, lastBundle.BundleNumber)
				}
			}
		}
	}

	if !needsBuild {
		return false, nil
	}

	// Build index
	m.logger.Printf("Building DID index (%s)", reason)
	m.logger.Printf("This may take several minutes...")

	if err := m.BuildDIDIndex(ctx, progressCallback); err != nil {
		return false, fmt.Errorf("failed to build DID index: %w", err)
	}

	// Verify index consistency
	m.logger.Printf("Verifying index consistency...")
	if err := m.didIndex.VerifyAndRepairIndex(ctx, m); err != nil {
		return false, fmt.Errorf("index verification/repair failed: %w", err)
	}

	return true, nil
}

// Add this helper function at the top of manager.go
func repositoryExists(bundleDir string) bool {
	indexPath := filepath.Join(bundleDir, bundleindex.INDEX_FILE)

	// Check for index file
	if _, err := os.Stat(indexPath); err == nil {
		return true
	}

	// Check for bundle files
	bundleFiles, _ := filepath.Glob(filepath.Join(bundleDir, "*.jsonl.zst"))
	bundleFiles = filterBundleFiles(bundleFiles)

	return len(bundleFiles) > 0
}

// ResolveHandleOrDID resolves input that can be either a handle or DID
// Returns: (did, handleResolveTime, error)
func (m *Manager) ResolveHandleOrDID(ctx context.Context, input string) (string, time.Duration, error) {
	input = strings.TrimSpace(input)

	// Normalize handle format (remove at://, @ prefixes)
	if !strings.HasPrefix(input, "did:") {
		input = strings.TrimPrefix(input, "at://")
		input = strings.TrimPrefix(input, "@")
	}

	// If already a DID, validate and return
	if strings.HasPrefix(input, "did:plc:") {
		if err := plcclient.ValidateDIDFormat(input); err != nil {
			return "", 0, err
		}
		return input, 0, nil // No resolution needed
	}

	// Support did:web too
	if strings.HasPrefix(input, "did:web:") {
		return input, 0, nil
	}

	// It's a handle - need resolver
	if m.handleResolver == nil {
		return "", 0, fmt.Errorf(
			"input '%s' appears to be a handle, but handle resolver is not configured\n\n"+
				"Configure resolver with:\n"+
				"  plcbundle --handle-resolver https://quickdid.smokesignal.tools did resolve %s\n\n"+
				"Or set default in config",
			input, input)
	}

	resolveStart := time.Now()
	if !m.config.Quiet {
		m.logger.Printf("Resolving handle: %s", input)
	}
	did, err := m.handleResolver.ResolveHandle(ctx, input)
	resolveTime := time.Since(resolveStart)

	if err != nil {
		return "", resolveTime, fmt.Errorf("failed to resolve handle '%s': %w", input, err)
	}

	if !m.config.Quiet {
		m.logger.Printf("Resolved: %s → %s (in %s)", input, did, resolveTime)
	}
	return did, resolveTime, nil
}

// GetHandleResolver returns the handle resolver (can be nil)
func (m *Manager) GetHandleResolver() *handleresolver.Client {
	return m.handleResolver
}

// recordResolverTiming records resolver performance metrics
func (m *Manager) recordResolverTiming(result *ResolveDIDResult, _ error) {
	m.resolverStats.Lock()
	defer m.resolverStats.Unlock()

	// Increment counters
	atomic.AddInt64(&m.resolverStats.totalResolutions, 1)

	switch result.Source {
	case "mempool":
		atomic.AddInt64(&m.resolverStats.mempoolHits, 1)
	case "bundle":
		atomic.AddInt64(&m.resolverStats.bundleHits, 1)
	}

	// Record timings
	timing := resolverTiming{
		totalTime:   result.TotalTime.Microseconds(),
		mempoolTime: result.MempoolTime.Microseconds(),
		indexTime:   result.IndexTime.Microseconds(),
		loadOpTime:  result.LoadOpTime.Microseconds(),
		source:      result.Source,
	}

	atomic.AddInt64(&m.resolverStats.totalTime, timing.totalTime)
	atomic.AddInt64(&m.resolverStats.totalMempoolTime, timing.mempoolTime)
	atomic.AddInt64(&m.resolverStats.totalIndexTime, timing.indexTime)
	atomic.AddInt64(&m.resolverStats.totalLoadOpTime, timing.loadOpTime)

	// Add to circular buffer
	m.resolverStats.recentTimes[m.resolverStats.recentIdx] = timing
	m.resolverStats.recentIdx = (m.resolverStats.recentIdx + 1) % m.resolverStats.recentSize
}

// GetResolverStats returns resolver performance statistics
func (m *Manager) GetResolverStats() map[string]interface{} {
	totalResolutions := atomic.LoadInt64(&m.resolverStats.totalResolutions)

	if totalResolutions == 0 {
		return map[string]interface{}{
			"total_resolutions": 0,
		}
	}

	mempoolHits := atomic.LoadInt64(&m.resolverStats.mempoolHits)
	bundleHits := atomic.LoadInt64(&m.resolverStats.bundleHits)
	errors := atomic.LoadInt64(&m.resolverStats.errors)

	totalTime := atomic.LoadInt64(&m.resolverStats.totalTime)
	totalMempoolTime := atomic.LoadInt64(&m.resolverStats.totalMempoolTime)
	totalIndexTime := atomic.LoadInt64(&m.resolverStats.totalIndexTime)
	totalLoadOpTime := atomic.LoadInt64(&m.resolverStats.totalLoadOpTime)

	// Calculate overall averages
	avgTotalMs := float64(totalTime) / float64(totalResolutions) / 1000.0
	avgMempoolMs := float64(totalMempoolTime) / float64(totalResolutions) / 1000.0

	stats := map[string]interface{}{
		"total_resolutions": totalResolutions,
		"mempool_hits":      mempoolHits,
		"bundle_hits":       bundleHits,
		"errors":            errors,
		"success_rate":      float64(totalResolutions-errors) / float64(totalResolutions),
		"mempool_hit_rate":  float64(mempoolHits) / float64(totalResolutions),

		// Overall averages
		"avg_total_time_ms":   avgTotalMs,
		"avg_mempool_time_ms": avgMempoolMs,
	}

	// Only include bundle-specific stats if we have bundle hits
	if bundleHits > 0 {
		avgIndexMs := float64(totalIndexTime) / float64(bundleHits) / 1000.0
		avgLoadMs := float64(totalLoadOpTime) / float64(bundleHits) / 1000.0

		stats["avg_index_time_ms"] = avgIndexMs
		stats["avg_load_op_time_ms"] = avgLoadMs
	}

	// Recent statistics
	m.resolverStats.Lock()
	recentCopy := make([]resolverTiming, m.resolverStats.recentSize)
	copy(recentCopy, m.resolverStats.recentTimes)
	m.resolverStats.Unlock()

	// Filter valid entries
	validRecent := make([]resolverTiming, 0)
	for _, t := range recentCopy {
		if t.totalTime > 0 {
			validRecent = append(validRecent, t)
		}
	}

	if len(validRecent) > 0 {
		// Extract total times for percentiles
		totalTimes := make([]int64, len(validRecent))
		for i, t := range validRecent {
			totalTimes[i] = t.totalTime
		}
		sort.Slice(totalTimes, func(i, j int) bool {
			return totalTimes[i] < totalTimes[j]
		})

		// Calculate recent average
		var recentSum int64
		var recentMempoolSum int64
		var recentIndexSum int64
		var recentLoadSum int64
		recentBundleCount := 0

		for _, t := range validRecent {
			recentSum += t.totalTime
			recentMempoolSum += t.mempoolTime
			if t.source == "bundle" {
				recentIndexSum += t.indexTime
				recentLoadSum += t.loadOpTime
				recentBundleCount++
			}
		}

		stats["recent_avg_total_time_ms"] = float64(recentSum) / float64(len(validRecent)) / 1000.0
		stats["recent_avg_mempool_time_ms"] = float64(recentMempoolSum) / float64(len(validRecent)) / 1000.0

		if recentBundleCount > 0 {
			stats["recent_avg_index_time_ms"] = float64(recentIndexSum) / float64(recentBundleCount) / 1000.0
			stats["recent_avg_load_time_ms"] = float64(recentLoadSum) / float64(recentBundleCount) / 1000.0
		}

		stats["recent_sample_size"] = len(validRecent)

		// Percentiles
		p50idx := len(totalTimes) * 50 / 100
		p95idx := len(totalTimes) * 95 / 100
		p99idx := len(totalTimes) * 99 / 100

		stats["min_total_time_ms"] = float64(totalTimes[0]) / 1000.0
		stats["max_total_time_ms"] = float64(totalTimes[len(totalTimes)-1]) / 1000.0

		if p50idx < len(totalTimes) {
			stats["p50_total_time_ms"] = float64(totalTimes[p50idx]) / 1000.0
		}
		if p95idx < len(totalTimes) {
			stats["p95_total_time_ms"] = float64(totalTimes[p95idx]) / 1000.0
		}
		if p99idx < len(totalTimes) {
			stats["p99_total_time_ms"] = float64(totalTimes[p99idx]) / 1000.0
		}
	}

	return stats
}

// ResetResolverStats resets resolver performance statistics
func (m *Manager) ResetResolverStats() {
	m.resolverStats.Lock()
	defer m.resolverStats.Unlock()

	atomic.StoreInt64(&m.resolverStats.totalResolutions, 0)
	atomic.StoreInt64(&m.resolverStats.mempoolHits, 0)
	atomic.StoreInt64(&m.resolverStats.bundleHits, 0)
	atomic.StoreInt64(&m.resolverStats.errors, 0)
	atomic.StoreInt64(&m.resolverStats.totalTime, 0)
	atomic.StoreInt64(&m.resolverStats.totalMempoolTime, 0)
	atomic.StoreInt64(&m.resolverStats.totalIndexTime, 0)
	atomic.StoreInt64(&m.resolverStats.totalLoadOpTime, 0)

	m.resolverStats.recentTimes = make([]resolverTiming, m.resolverStats.recentSize)
	m.resolverStats.recentIdx = 0
}

func (m *Manager) SetQuiet(quiet bool) {
	m.config.Quiet = quiet
}
