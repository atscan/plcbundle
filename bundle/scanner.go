package bundle

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
)

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
	files = filterBundleFiles(files)

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
	var newMetadata []*bundleindex.BundleMetadata
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

		// Calculate parent and cursor from previous bundle
		var parent string
		var cursor string
		if num > 1 && len(newMetadata) > 0 {
			prevMeta := newMetadata[len(newMetadata)-1]
			parent = prevMeta.Hash
			cursor = prevMeta.EndTime.Format(time.RFC3339Nano)
		}

		// Use the ONE method for metadata calculation
		meta, err := m.CalculateBundleMetadata(num, path, ops, parent, cursor)
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
	files = filterBundleFiles(files)

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
		meta  *bundleindex.BundleMetadata
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

				// ✅ NEW: Stream metadata WITHOUT loading all operations
				meta, err := m.CalculateMetadataStreaming(num, path)
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

	// Collect results
	metadataMap := make(map[int]*bundleindex.BundleMetadata)
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
	var newMetadata []*bundleindex.BundleMetadata
	var parent string

	for i, num := range bundleNumbers {
		meta, ok := metadataMap[num]
		if !ok {
			continue
		}

		// Set cursor from previous bundle's EndTime
		if i > 0 && len(newMetadata) > 0 {
			prevMeta := newMetadata[len(newMetadata)-1]
			meta.Cursor = prevMeta.EndTime.Format(time.RFC3339Nano)
		}

		// Calculate chain hash (must be done sequentially)
		meta.Hash = m.operations.CalculateChainHash(parent, meta.ContentHash)
		meta.Parent = parent

		newMetadata = append(newMetadata, meta)
		parent = meta.Hash
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

// ScanBundle scans a single bundle file and returns its metadata
func (m *Manager) ScanBundle(path string, bundleNumber int) (*bundleindex.BundleMetadata, error) {
	// Load bundle file
	operations, err := m.operations.LoadBundle(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load bundle: %w", err)
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Get parent chain hash and cursor from previous bundle
	var parent string
	var cursor string
	if bundleNumber > 1 {
		if prevMeta, err := m.index.GetBundle(bundleNumber - 1); err == nil {
			parent = prevMeta.Hash
			cursor = prevMeta.EndTime.Format(time.RFC3339Nano)
		}
	}

	// Use the ONE method
	return m.CalculateBundleMetadata(bundleNumber, path, operations, parent, cursor)
}

// ScanAndIndexBundle scans a bundle file and adds it to the index
func (m *Manager) ScanAndIndexBundle(path string, bundleNumber int) (*bundleindex.BundleMetadata, error) {
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
