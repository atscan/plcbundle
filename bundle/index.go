package bundle

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/goccy/go-json"
)

const (
	// INDEX_FILE is the default index filename
	INDEX_FILE = "plc_bundles.json"

	// INDEX_VERSION is the current index format version
	INDEX_VERSION = "1.0"
)

// Index represents the JSON index file
type Index struct {
	Version    string            `json:"version"`
	LastBundle int               `json:"last_bundle"`
	UpdatedAt  time.Time         `json:"updated_at"`
	TotalSize  int64             `json:"total_size_bytes"`
	Bundles    []*BundleMetadata `json:"bundles"`

	mu sync.RWMutex `json:"-"`
}

// NewIndex creates a new empty index
func NewIndex() *Index {
	return &Index{
		Version:   INDEX_VERSION,
		Bundles:   make([]*BundleMetadata, 0),
		UpdatedAt: time.Now().UTC(),
	}
}

// LoadIndex loads an index from a file
func LoadIndex(path string) (*Index, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file: %w", err)
	}

	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("failed to parse index file: %w", err)
	}

	// Validate version
	if idx.Version != INDEX_VERSION {
		return nil, fmt.Errorf("unsupported index version: %s (expected %s)", idx.Version, INDEX_VERSION)
	}

	return &idx, nil
}

// Save saves the index to a file
func (idx *Index) Save(path string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	// Write atomically (write to temp file, then rename)
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// AddBundle adds a bundle to the index
func (idx *Index) AddBundle(meta *BundleMetadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if bundle already exists
	for i, existing := range idx.Bundles {
		if existing.BundleNumber == meta.BundleNumber {
			// Update existing
			idx.Bundles[i] = meta
			idx.recalculate()
			return
		}
	}

	// Add new bundle
	idx.Bundles = append(idx.Bundles, meta)
	idx.sort()
	idx.recalculate()
}

// GetBundle retrieves a bundle metadata by number
func (idx *Index) GetBundle(bundleNumber int) (*BundleMetadata, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for _, meta := range idx.Bundles {
		if meta.BundleNumber == bundleNumber {
			return meta, nil
		}
	}

	return nil, fmt.Errorf("bundle %d not found in index", bundleNumber)
}

// GetLastBundle returns the metadata of the last bundle
func (idx *Index) GetLastBundle() *BundleMetadata {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.Bundles) == 0 {
		return nil
	}

	return idx.Bundles[len(idx.Bundles)-1]
}

// GetBundles returns all bundle metadata
func (idx *Index) GetBundles() []*BundleMetadata {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Return a copy
	result := make([]*BundleMetadata, len(idx.Bundles))
	copy(result, idx.Bundles)
	return result
}

// GetBundleRange returns bundles in a specific range
func (idx *Index) GetBundleRange(start, end int) []*BundleMetadata {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []*BundleMetadata
	for _, meta := range idx.Bundles {
		if meta.BundleNumber >= start && meta.BundleNumber <= end {
			result = append(result, meta)
		}
	}
	return result
}

// Count returns the number of bundles in the index
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.Bundles)
}

// FindGaps finds missing bundle numbers in the sequence
func (idx *Index) FindGaps() []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.Bundles) == 0 {
		return nil
	}

	var gaps []int
	first := idx.Bundles[0].BundleNumber
	last := idx.Bundles[len(idx.Bundles)-1].BundleNumber

	bundleMap := make(map[int]bool)
	for _, meta := range idx.Bundles {
		bundleMap[meta.BundleNumber] = true
	}

	for i := first; i <= last; i++ {
		if !bundleMap[i] {
			gaps = append(gaps, i)
		}
	}

	return gaps
}

// GetStats returns statistics about the index
func (idx *Index) GetStats() map[string]interface{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.Bundles) == 0 {
		return map[string]interface{}{
			"bundle_count": 0,
			"total_size":   0,
		}
	}

	first := idx.Bundles[0]
	last := idx.Bundles[len(idx.Bundles)-1]

	return map[string]interface{}{
		"bundle_count": len(idx.Bundles),
		"first_bundle": first.BundleNumber,
		"last_bundle":  last.BundleNumber,
		"total_size":   idx.TotalSize,
		"start_time":   first.StartTime,
		"end_time":     last.EndTime,
		"updated_at":   idx.UpdatedAt,
		"gaps":         len(idx.FindGaps()),
	}
}

// sort sorts bundles by bundle number
func (idx *Index) sort() {
	sort.Slice(idx.Bundles, func(i, j int) bool {
		return idx.Bundles[i].BundleNumber < idx.Bundles[j].BundleNumber
	})
}

// recalculate recalculates derived fields (called after modifications)
func (idx *Index) recalculate() {
	if len(idx.Bundles) == 0 {
		idx.LastBundle = 0
		idx.TotalSize = 0
		return
	}

	// Find last bundle
	maxBundle := 0
	totalSize := int64(0)

	for _, meta := range idx.Bundles {
		if meta.BundleNumber > maxBundle {
			maxBundle = meta.BundleNumber
		}
		totalSize += meta.CompressedSize
	}

	idx.LastBundle = maxBundle
	idx.TotalSize = totalSize
}

// Rebuild rebuilds the index from bundle metadata
func (idx *Index) Rebuild(bundles []*BundleMetadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.Bundles = bundles
	idx.sort()
	idx.recalculate()
	idx.UpdatedAt = time.Now().UTC()
}

// Clear clears all bundles from the index
func (idx *Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.Bundles = make([]*BundleMetadata, 0)
	idx.LastBundle = 0
	idx.TotalSize = 0
	idx.UpdatedAt = time.Now().UTC()
}
