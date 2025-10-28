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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/atscan/plcbundle/plc"
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
	operations *Operations
	index      *Index
	indexPath  string
	plcClient  *plc.Client
	logger     Logger
	mempool    *Mempool
}

// NewManager creates a new bundle manager
func NewManager(config *Config, plcClient *plc.Client) (*Manager, error) {
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
	ops, err := NewOperations(config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize operations: %w", err)
	}

	// Load or create index
	indexPath := filepath.Join(config.BundleDir, INDEX_FILE)
	index, err := LoadIndex(indexPath)

	// Check for bundle files in directory
	bundleFiles, _ := filepath.Glob(filepath.Join(config.BundleDir, "*.jsonl.zst"))
	hasBundleFiles := len(bundleFiles) > 0

	needsRebuild := false

	if err != nil {
		// Index doesn't exist or is invalid
		if hasBundleFiles {
			// We have bundles but no index - need to rebuild
			config.Logger.Printf("No valid index found, but detected %d bundle files", len(bundleFiles))
			needsRebuild = true
		} else {
			// No index and no bundles - create fresh index
			config.Logger.Printf("Creating new index at %s", indexPath)
			index = NewIndex()
			if err := index.Save(indexPath); err != nil {
				return nil, fmt.Errorf("failed to save new index: %w", err)
			}
		}
	} else {
		// Index exists - check if it's complete
		config.Logger.Printf("Loaded index with %d bundles", index.Count())

		// Check if there are bundle files not in the index
		if hasBundleFiles && len(bundleFiles) > index.Count() {
			config.Logger.Printf("Detected %d bundle files but index only has %d entries - rebuilding",
				len(bundleFiles), index.Count())
			needsRebuild = true
		}
	}

	// Perform rebuild if needed (using parallel scan)
	if needsRebuild && config.AutoRebuild {
		config.Logger.Printf("Rebuilding index from %d bundle files...", len(bundleFiles))

		// Create temporary manager for scanning
		tempMgr := &Manager{
			config:     config,
			operations: ops,
			index:      NewIndex(),
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
		index, err = LoadIndex(indexPath)
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
			if meta.ContentHash == "" { // ← Changed from meta.Hash
				missingHashes++
			}
			if i > 0 && meta.Hash == "" { // ← This is now the chain hash
				missingHashes++
			}
		}
		if missingHashes > 0 {
			config.Logger.Printf("⚠️  Warning: %d bundles have missing hashes", missingHashes)
		}
	}

	if index == nil {
		index = NewIndex()
	}

	// Initialize mempool for next bundle
	lastBundle := index.GetLastBundle()
	nextBundleNum := 1
	var minTimestamp time.Time

	if lastBundle != nil {
		nextBundleNum = lastBundle.BundleNumber + 1
		minTimestamp = lastBundle.EndTime
	}

	mempool, err := NewMempool(config.BundleDir, nextBundleNum, minTimestamp, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize mempool: %w", err)
	}

	return &Manager{
		config:     config,
		operations: ops,
		index:      index,
		indexPath:  indexPath,
		plcClient:  plcClient,
		logger:     config.Logger,
		mempool:    mempool,
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
}

// GetIndex returns the current index
func (m *Manager) GetIndex() *Index {
	return m.index
}

// SaveIndex saves the index to disk
func (m *Manager) SaveIndex() error {
	return m.index.Save(m.indexPath)
}

// LoadBundle loads a bundle from disk
func (m *Manager) LoadBundle(ctx context.Context, bundleNumber int) (*Bundle, error) {
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
		Hash:             meta.Hash,        // Chain hash (primary)
		ContentHash:      meta.ContentHash, // Content hash
		Parent:           meta.Parent,      // Parent chain hash
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
func (m *Manager) SaveBundle(ctx context.Context, bundle *Bundle) error {
	if err := bundle.ValidateForSave(); err != nil {
		return fmt.Errorf("bundle validation failed: %w", err)
	}

	path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", bundle.BundleNumber))

	// Save to disk
	uncompressedHash, compressedHash, uncompressedSize, compressedSize, err := m.operations.SaveBundle(path, bundle.Operations)
	if err != nil {
		return fmt.Errorf("failed to save bundle: %w", err)
	}

	// Set content hash and compressed metadata
	bundle.ContentHash = uncompressedHash
	bundle.CompressedHash = compressedHash
	bundle.UncompressedSize = uncompressedSize
	bundle.CompressedSize = compressedSize
	bundle.CreatedAt = time.Now().UTC()

	// Get parent (previous chain hash)
	var parent string
	if bundle.BundleNumber > 1 {
		prevBundle := m.index.GetLastBundle()
		if prevBundle != nil {
			parent = prevBundle.Hash // Get previous chain hash

			m.logger.Printf("Previous bundle %06d: hash=%s, content=%s",
				prevBundle.BundleNumber,
				formatHashPreview(prevBundle.Hash),
				formatHashPreview(prevBundle.ContentHash))
		} else {
			// Try to get specific previous bundle
			if prevMeta, err := m.index.GetBundle(bundle.BundleNumber - 1); err == nil {
				parent = prevMeta.Hash

				m.logger.Printf("Found previous bundle %06d: hash=%s, content=%s",
					prevMeta.BundleNumber,
					formatHashPreview(prevMeta.Hash),
					formatHashPreview(prevMeta.ContentHash))
			}
		}
	}

	bundle.Parent = parent

	// Calculate chain hash (now stored as primary Hash)
	bundle.Hash = m.operations.CalculateChainHash(parent, bundle.ContentHash)

	m.logger.Printf("Bundle %06d: hash=%s, content=%s, parent=%s",
		bundle.BundleNumber,
		formatHashPreview(bundle.Hash),
		formatHashPreview(bundle.ContentHash),
		formatHashPreview(bundle.Parent))

	// Add to index
	m.index.AddBundle(bundle.ToMetadata())

	// Save index
	if err := m.SaveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	m.logger.Printf("Saved bundle %06d (hash: %s)",
		bundle.BundleNumber,
		formatHashPreview(bundle.Hash))

	// IMPORTANT: Clean up old mempool and create new one for next bundle
	oldMempoolFile := m.mempool.GetFilename()
	if err := m.mempool.Delete(); err != nil {
		m.logger.Printf("Warning: failed to delete old mempool %s: %v", oldMempoolFile, err)
	} else {
		m.logger.Printf("Deleted mempool: %s", oldMempoolFile)
	}

	// Create new mempool for next bundle
	nextBundle := bundle.BundleNumber + 1
	minTimestamp := bundle.EndTime

	newMempool, err := NewMempool(m.config.BundleDir, nextBundle, minTimestamp, m.logger)
	if err != nil {
		return fmt.Errorf("failed to create new mempool: %w", err)
	}

	m.mempool = newMempool
	m.logger.Printf("Created new mempool for bundle %06d (min timestamp: %s)",
		nextBundle, minTimestamp.Format(time.RFC3339Nano))

	return nil
}

// formatHashPreview safely formats a hash for display
func formatHashPreview(hash string) string {
	if len(hash) == 0 {
		return "(empty)"
	}
	if len(hash) < 16 {
		return hash
	}
	return hash[:16] + "..."
}

// FetchNextBundle fetches the next bundle from PLC directory
func (m *Manager) FetchNextBundle(ctx context.Context) (*Bundle, error) {
	if m.plcClient == nil {
		return nil, fmt.Errorf("PLC client not configured")
	}

	// Determine next bundle number
	lastBundle := m.index.GetLastBundle()
	nextBundleNum := 1
	var afterTime string
	var prevBoundaryCIDs map[string]bool
	var prevBundleHash string

	if lastBundle != nil {
		nextBundleNum = lastBundle.BundleNumber + 1
		afterTime = lastBundle.EndTime.Format(time.RFC3339Nano)
		prevBundleHash = lastBundle.Hash

		// Try to load boundary CIDs from previous bundle
		prevBundle, err := m.LoadBundle(ctx, lastBundle.BundleNumber)
		if err == nil {
			_, prevBoundaryCIDs = m.operations.GetBoundaryCIDs(prevBundle.Operations)
		}
	}

	m.logger.Printf("Preparing bundle %06d (mempool: %d ops)...", nextBundleNum, m.mempool.Count())

	// Keep fetching until we have enough operations
	for m.mempool.Count() < BUNDLE_SIZE {
		m.logger.Printf("Fetching more operations (have %d/%d)...", m.mempool.Count(), BUNDLE_SIZE)

		err := m.fetchToMempool(ctx, afterTime, prevBoundaryCIDs, BUNDLE_SIZE-m.mempool.Count())
		if err != nil {
			// If we can't fetch more, check if we have enough
			if m.mempool.Count() >= BUNDLE_SIZE {
				break // We have enough now
			}
			// Save current state and return error
			m.mempool.Save()
			return nil, fmt.Errorf("insufficient operations: have %d, need %d", m.mempool.Count(), BUNDLE_SIZE)
		}

		// Check if we made progress
		if m.mempool.Count() < BUNDLE_SIZE {
			// Didn't get enough, but got some - save and return error
			m.mempool.Save()
			return nil, fmt.Errorf("insufficient operations: have %d, need %d (no more available)", m.mempool.Count(), BUNDLE_SIZE)
		}
	}

	// Create bundle from mempool
	m.logger.Printf("Creating bundle %06d from mempool", nextBundleNum)
	operations, err := m.mempool.Take(BUNDLE_SIZE) // ✅ Fixed - handle both return values
	if err != nil {
		m.mempool.Save()
		return nil, fmt.Errorf("failed to take operations from mempool: %w", err)
	}

	bundle := m.operations.CreateBundle(nextBundleNum, operations, afterTime, prevBundleHash)

	// Save mempool state
	if err := m.mempool.Save(); err != nil {
		m.logger.Printf("Warning: failed to save mempool: %v", err)
	}

	m.logger.Printf("✓ Bundle %06d ready (%d ops, mempool: %d remaining)",
		nextBundleNum, len(operations), m.mempool.Count())

	return bundle, nil
}

// fetchToMempool fetches operations and adds them to mempool (returns error if no progress)
func (m *Manager) fetchToMempool(ctx context.Context, afterTime string, prevBoundaryCIDs map[string]bool, target int) error {
	seenCIDs := make(map[string]bool)

	// Mark previous boundary CIDs as seen
	for cid := range prevBoundaryCIDs {
		seenCIDs[cid] = true
	}

	// Use last mempool time if available
	if m.mempool.Count() > 0 {
		afterTime = m.mempool.GetLastTime()
		m.logger.Printf("  Continuing from mempool cursor: %s", afterTime)
	}

	currentAfter := afterTime
	maxFetches := 20
	totalAdded := 0
	startingCount := m.mempool.Count()

	for fetchNum := 0; fetchNum < maxFetches; fetchNum++ {
		// Calculate batch size
		remaining := target - (m.mempool.Count() - startingCount)
		if remaining <= 0 {
			break
		}

		batchSize := 1000
		if remaining < 500 {
			batchSize = 200
		}

		m.logger.Printf("  Fetch #%d: requesting %d operations (mempool: %d)",
			fetchNum+1, batchSize, m.mempool.Count())

		batch, err := m.plcClient.Export(ctx, plc.ExportOptions{
			Count: batchSize,
			After: currentAfter,
		})
		if err != nil {
			m.mempool.Save()
			return fmt.Errorf("export failed: %w", err)
		}

		if len(batch) == 0 {
			m.logger.Printf("  No more operations available from PLC")
			m.mempool.Save()
			if totalAdded > 0 {
				return nil
			}
			return fmt.Errorf("no operations available")
		}

		// Deduplicate
		uniqueOps := make([]plc.PLCOperation, 0)
		for _, op := range batch {
			if !seenCIDs[op.CID] {
				seenCIDs[op.CID] = true
				uniqueOps = append(uniqueOps, op)
			}
		}

		if len(uniqueOps) > 0 {
			// CRITICAL: Add with validation
			added, err := m.mempool.Add(uniqueOps)
			if err != nil {
				// Validation error - save current state and return
				m.mempool.Save()
				return fmt.Errorf("chronological validation failed: %w", err)
			}

			totalAdded += added
			m.logger.Printf("  Added %d new operations (mempool now: %d)", added, m.mempool.Count())

			// Save after each successful addition
			if err := m.mempool.Save(); err != nil {
				m.logger.Printf("  Warning: failed to save mempool: %v", err)
			}
		}

		// Update cursor
		if len(batch) > 0 {
			currentAfter = batch[len(batch)-1].CreatedAt.Format(time.RFC3339Nano)
		}

		// Stop if we got less than requested (caught up)
		if len(batch) < batchSize {
			m.logger.Printf("  Received incomplete batch (%d/%d), caught up to latest", len(batch), batchSize)
			break
		}
	}

	// Final save
	m.mempool.Save()

	if totalAdded > 0 {
		m.logger.Printf("✓ Fetch complete: added %d operations (mempool: %d)", totalAdded, m.mempool.Count())
		return nil
	}

	return fmt.Errorf("no new operations added")
}

// GetMempoolStats returns mempool statistics
func (m *Manager) GetMempoolStats() map[string]interface{} {
	return m.mempool.Stats()
}

// GetMempoolOperations returns all operations currently in mempool
func (m *Manager) GetMempoolOperations() ([]plc.PLCOperation, error) {
	if m.mempool == nil {
		return nil, fmt.Errorf("mempool not initialized")
	}

	// Use Peek to get operations without removing them
	count := m.mempool.Count()
	if count == 0 {
		return []plc.PLCOperation{}, nil
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

	// Verify hash
	valid, actualHash, err := m.operations.VerifyHash(path, meta.CompressedHash)
	if err != nil {
		result.Error = err
		return result, nil
	}

	result.LocalHash = actualHash
	result.HashMatch = valid
	result.Valid = valid

	return result, nil
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

// ScanDirectory scans the bundle directory and rebuilds the index
func (m *Manager) ScanDirectory() (*DirectoryScanResult, error) {
	result := &DirectoryScanResult{
		BundleDir: m.config.BundleDir,
	}

	m.logger.Printf("Scanning directory: %s", m.config.BundleDir)

	// Find all bundle files
	files, err := filepath.Glob(filepath.Join(m.config.BundleDir, "*.jsonl.zst"))
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory: %w", err)
	}

	if len(files) == 0 {
		m.logger.Printf("No bundle files found")
		return result, nil
	}

	// Parse bundle numbers
	var bundleNumbers []int
	for _, file := range files {
		base := filepath.Base(file)
		numStr := strings.TrimSuffix(base, ".jsonl.zst")
		num, err := strconv.Atoi(numStr)
		if err != nil {
			m.logger.Printf("Warning: skipping invalid filename: %s", base)
			continue
		}
		bundleNumbers = append(bundleNumbers, num)
	}

	sort.Ints(bundleNumbers)

	result.BundleCount = len(bundleNumbers)
	if len(bundleNumbers) > 0 {
		result.FirstBundle = bundleNumbers[0]
		result.LastBundle = bundleNumbers[len(bundleNumbers)-1]
	}

	// Find gaps
	if len(bundleNumbers) > 1 {
		for i := result.FirstBundle; i <= result.LastBundle; i++ {
			found := false
			for _, num := range bundleNumbers {
				if num == i {
					found = true
					break
				}
			}
			if !found {
				result.MissingGaps = append(result.MissingGaps, i)
			}
		}
	}

	m.logger.Printf("Found %d bundles (gaps: %d)", result.BundleCount, len(result.MissingGaps))

	// Load each bundle and rebuild index
	var newMetadata []*BundleMetadata
	var totalSize int64

	for _, num := range bundleNumbers {
		path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", num))

		// Load bundle
		ops, err := m.operations.LoadBundle(path)
		if err != nil {
			m.logger.Printf("Warning: failed to load bundle %d: %v", num, err)
			continue
		}

		// Get file size
		size, _ := m.operations.GetFileSize(path)
		totalSize += size

		// Calculate parent
		var parent string
		if num > 1 && len(newMetadata) > 0 {
			prevMeta := newMetadata[len(newMetadata)-1]
			parent = prevMeta.Hash
		}

		// Use the ONE method for metadata calculation
		meta, err := m.operations.CalculateBundleMetadata(num, path, ops, parent)
		if err != nil {
			m.logger.Printf("Warning: failed to calculate metadata for bundle %d: %v", num, err)
			continue
		}

		newMetadata = append(newMetadata, meta)

		m.logger.Printf("  Scanned bundle %06d: %d ops, %d DIDs", num, len(ops), meta.DIDCount)
	}

	result.TotalSize = totalSize

	// Rebuild index
	m.index.Rebuild(newMetadata)

	// Save index
	if err := m.SaveIndex(); err != nil {
		return nil, fmt.Errorf("failed to save index: %w", err)
	}

	result.IndexUpdated = true

	m.logger.Printf("Index rebuilt with %d bundles", len(newMetadata))

	return result, nil
}

// ScanDirectoryParallel scans the bundle directory in parallel and rebuilds the index
func (m *Manager) ScanDirectoryParallel(workers int, progressCallback func(current, total int, bytesProcessed int64)) (*DirectoryScanResult, error) {
	result := &DirectoryScanResult{
		BundleDir: m.config.BundleDir,
	}

	m.logger.Printf("Scanning directory (parallel, %d workers): %s", workers, m.config.BundleDir)

	// Find all bundle files
	files, err := filepath.Glob(filepath.Join(m.config.BundleDir, "*.jsonl.zst"))
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory: %w", err)
	}

	if len(files) == 0 {
		m.logger.Printf("No bundle files found")
		return result, nil
	}

	// Parse bundle numbers
	var bundleNumbers []int
	for _, file := range files {
		base := filepath.Base(file)
		numStr := strings.TrimSuffix(base, ".jsonl.zst")
		num, err := strconv.Atoi(numStr)
		if err != nil {
			m.logger.Printf("Warning: skipping invalid filename: %s", base)
			continue
		}
		bundleNumbers = append(bundleNumbers, num)
	}

	sort.Ints(bundleNumbers)

	result.BundleCount = len(bundleNumbers)
	if len(bundleNumbers) > 0 {
		result.FirstBundle = bundleNumbers[0]
		result.LastBundle = bundleNumbers[len(bundleNumbers)-1]
	}

	// Find gaps
	if len(bundleNumbers) > 1 {
		for i := result.FirstBundle; i <= result.LastBundle; i++ {
			found := false
			for _, num := range bundleNumbers {
				if num == i {
					found = true
					break
				}
			}
			if !found {
				result.MissingGaps = append(result.MissingGaps, i)
			}
		}
	}

	m.logger.Printf("Found %d bundles (gaps: %d)", result.BundleCount, len(result.MissingGaps))

	// Process bundles in parallel
	type bundleResult struct {
		index int
		meta  *BundleMetadata
		err   error
	}

	jobs := make(chan int, len(bundleNumbers))
	results := make(chan bundleResult, len(bundleNumbers))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for num := range jobs {
				path := filepath.Join(m.config.BundleDir, fmt.Sprintf("%06d.jsonl.zst", num))

				// Load and process bundle
				ops, err := m.operations.LoadBundle(path)
				if err != nil {
					results <- bundleResult{index: num, err: err}
					continue
				}

				// Use the FAST method (no chain hash yet)
				meta, err := m.operations.CalculateBundleMetadataFast(num, path, ops)
				if err != nil {
					results <- bundleResult{index: num, err: err}
					continue
				}

				results <- bundleResult{index: num, meta: meta}
			}
		}()
	}

	// Send jobs
	for _, num := range bundleNumbers {
		jobs <- num
	}
	close(jobs)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results (in a map first, then sort)
	metadataMap := make(map[int]*BundleMetadata)
	var totalSize int64
	var totalUncompressed int64
	processed := 0

	for result := range results {
		processed++

		// Update progress WITH bytes
		if progressCallback != nil {
			if result.meta != nil {
				totalUncompressed += result.meta.UncompressedSize
			}
			progressCallback(processed, len(bundleNumbers), totalUncompressed)
		}

		if result.err != nil {
			m.logger.Printf("Warning: failed to process bundle %d: %v", result.index, result.err)
			continue
		}
		metadataMap[result.index] = result.meta
		totalSize += result.meta.CompressedSize
	}

	// Build ordered metadata slice and calculate chain hashes
	var newMetadata []*BundleMetadata
	var parent string // Parent chain hash

	for _, num := range bundleNumbers {
		meta, ok := metadataMap[num]
		if !ok {
			continue // Skip failed bundles
		}

		// Now calculate chain hash (must be done sequentially)
		meta.Hash = m.operations.CalculateChainHash(parent, meta.ContentHash)
		meta.Parent = parent

		newMetadata = append(newMetadata, meta)
		parent = meta.Hash // Store for next iteration
	}

	result.TotalSize = totalSize
	result.TotalUncompressed = totalUncompressed

	// Rebuild index
	m.index.Rebuild(newMetadata)

	// Save index
	if err := m.SaveIndex(); err != nil {
		return nil, fmt.Errorf("failed to save index: %w", err)
	}

	result.IndexUpdated = true

	m.logger.Printf("Index rebuilt with %d bundles", len(newMetadata))

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
func (m *Manager) ExportOperations(ctx context.Context, afterTime time.Time, count int) ([]plc.PLCOperation, error) {
	if count <= 0 {
		count = 1000
	}

	var result []plc.PLCOperation
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

// ScanBundle scans a single bundle file and returns its metadata
func (m *Manager) ScanBundle(path string, bundleNumber int) (*BundleMetadata, error) {
	// Load bundle file
	operations, err := m.operations.LoadBundle(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load bundle: %w", err)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Get parent chain hash from index
	var parent string
	if bundleNumber > 1 {
		if prevMeta, err := m.index.GetBundle(bundleNumber - 1); err == nil {
			parent = prevMeta.Hash
		}
	}

	// Use the ONE method
	return m.operations.CalculateBundleMetadata(bundleNumber, path, operations, parent)
}

// ScanAndIndexBundle scans a bundle file and adds it to the index
func (m *Manager) ScanAndIndexBundle(path string, bundleNumber int) (*BundleMetadata, error) {
	meta, err := m.ScanBundle(path, bundleNumber)
	if err != nil {
		return nil, err
	}

	// Add to index
	m.index.AddBundle(meta)

	// Save index
	if err := m.SaveIndex(); err != nil {
		return nil, fmt.Errorf("failed to save index: %w", err)
	}

	return meta, nil
}

// IsBundleIndexed checks if a bundle is already in the index
func (m *Manager) IsBundleIndexed(bundleNumber int) bool {
	_, err := m.index.GetBundle(bundleNumber)
	return err == nil
}

// RefreshMempool reloads mempool from disk (useful for debugging)
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

	// Get count before clearing
	count := m.mempool.Count()

	// Clear the mempool
	m.mempool.Clear()

	// Save the empty state (this will delete the file since it's empty)
	if err := m.mempool.Save(); err != nil {
		return fmt.Errorf("failed to save mempool: %w", err)
	}

	m.logger.Printf("Cleared %d operations from mempool", count)

	return nil
}

// Add validation method
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

		newIndex, err := LoadIndex(m.indexPath)
		if err != nil {
			return fmt.Errorf("failed to reload index: %w", err)
		}

		m.index = newIndex
		m.logger.Printf("Index reloaded: %d bundles", m.index.Count())
	}

	return nil
}
