package bundle

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	if err != nil {
		config.Logger.Printf("Creating new index at %s", indexPath)
		index = NewIndex()
		if err := index.Save(indexPath); err != nil {
			return nil, fmt.Errorf("failed to save new index: %w", err)
		}
	} else {
		config.Logger.Printf("Loaded index with %d bundles", index.Count())
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
		Hash:             meta.Hash,
		CompressedHash:   meta.CompressedHash,
		CompressedSize:   meta.CompressedSize,
		UncompressedSize: meta.UncompressedSize,
		Cursor:           meta.Cursor,
		PrevBundleHash:   meta.PrevBundleHash,
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

	// Update bundle metadata
	bundle.Hash = uncompressedHash
	bundle.CompressedHash = compressedHash
	bundle.UncompressedSize = uncompressedSize
	bundle.CompressedSize = compressedSize
	bundle.CreatedAt = time.Now().UTC()

	// Add to index
	m.index.AddBundle(bundle.ToMetadata())

	// Save index
	if err := m.SaveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	m.logger.Printf("Saved bundle %06d (hash: %s...)", bundle.BundleNumber, bundle.Hash[:16])

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

	// Verify each bundle
	for i, meta := range bundles {
		// Verify file hash
		vr, err := m.VerifyBundle(ctx, meta.BundleNumber)
		if err != nil {
			result.Error = fmt.Sprintf("Failed to verify bundle %d: %v", meta.BundleNumber, err)
			result.BrokenAt = meta.BundleNumber
			return result, nil
		}

		if !vr.Valid {
			result.Error = fmt.Sprintf("Bundle %d hash verification failed", meta.BundleNumber)
			result.BrokenAt = meta.BundleNumber
			return result, nil
		}

		// Verify chain link (prev_bundle_hash)
		if i > 0 {
			prevMeta := bundles[i-1]
			if meta.PrevBundleHash != prevMeta.Hash {
				result.Error = fmt.Sprintf("Chain broken at bundle %d: prev_hash mismatch", meta.BundleNumber)
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

		// Calculate metadata
		dids := m.operations.ExtractUniqueDIDs(ops)
		jsonlData := m.operations.SerializeJSONL(ops)
		uncompressedHash := m.operations.Hash(jsonlData)
		compressedData, _ := os.ReadFile(path)
		compressedHash := m.operations.Hash(compressedData)

		// Determine cursor (would need previous bundle's end_time in real scenario)
		cursor := ""
		prevHash := ""
		if num > 1 && len(newMetadata) > 0 {
			prevMeta := newMetadata[len(newMetadata)-1]
			cursor = prevMeta.EndTime.Format(time.RFC3339Nano)
			prevHash = prevMeta.Hash
		}

		meta := &BundleMetadata{
			BundleNumber:     num,
			StartTime:        ops[0].CreatedAt,
			EndTime:          ops[len(ops)-1].CreatedAt,
			OperationCount:   len(ops),
			DIDCount:         len(dids),
			Hash:             uncompressedHash,
			CompressedHash:   compressedHash,
			CompressedSize:   size,
			UncompressedSize: int64(len(jsonlData)),
			Cursor:           cursor,
			PrevBundleHash:   prevHash,
			CreatedAt:        time.Now().UTC(),
		}

		newMetadata = append(newMetadata, meta)

		m.logger.Printf("  Scanned bundle %06d: %d ops, %d DIDs", num, len(ops), len(dids))
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

	// Get previous bundle hash from index
	prevHash := ""
	if bundleNumber > 1 {
		if prevMeta, err := m.index.GetBundle(bundleNumber - 1); err == nil {
			prevHash = prevMeta.Hash
		}
	}

	// Calculate metadata
	meta, err := m.calculateBundleMetadata(bundleNumber, path, operations, prevHash)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate metadata: %w", err)
	}

	return meta, nil
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

// calculateBundleMetadata calculates metadata for a bundle (internal helper)
func (m *Manager) calculateBundleMetadata(bundleNumber int, path string, operations []plc.PLCOperation, prevBundleHash string) (*BundleMetadata, error) {
	// Get file info
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Extract unique DIDs
	dids := m.operations.ExtractUniqueDIDs(operations)

	// Calculate sizes and hashes
	jsonlData := m.operations.SerializeJSONL(operations)
	uncompressedSize := int64(len(jsonlData))
	uncompressedHash := m.operations.Hash(jsonlData)

	compressedData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed file: %w", err)
	}
	compressedHash := m.operations.Hash(compressedData)

	// Determine cursor
	cursor := ""
	if bundleNumber > 1 && prevBundleHash != "" {
		cursor = operations[0].CreatedAt.Format(time.RFC3339Nano)
	}

	return &BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        operations[0].CreatedAt,
		EndTime:          operations[len(operations)-1].CreatedAt,
		OperationCount:   len(operations),
		DIDCount:         len(dids),
		Hash:             uncompressedHash,
		CompressedHash:   compressedHash,
		CompressedSize:   info.Size(),
		UncompressedSize: uncompressedSize,
		Cursor:           cursor,
		PrevBundleHash:   prevBundleHash,
		CreatedAt:        time.Now().UTC(),
	}, nil
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
