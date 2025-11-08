package mempool

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

const MEMPOOL_FILE_PREFIX = "plc_mempool_"

// Mempool stores operations waiting to be bundled
// Operations must be strictly chronological
type Mempool struct {
	operations   []plcclient.PLCOperation
	targetBundle int
	minTimestamp time.Time
	file         string
	mu           sync.RWMutex
	logger       types.Logger
	validated    bool
	dirty        bool

	// Incremental save tracking
	lastSavedLen  int           // How many ops are persisted
	lastSaveTime  time.Time     // Last save timestamp
	saveThreshold int           // Save after N new ops
	saveInterval  time.Duration // Save after duration
}

// NewMempool creates a new mempool for a specific bundle number
func NewMempool(bundleDir string, targetBundle int, minTimestamp time.Time, logger types.Logger) (*Mempool, error) {
	filename := fmt.Sprintf("%s%06d.jsonl", MEMPOOL_FILE_PREFIX, targetBundle)

	m := &Mempool{
		file:          filepath.Join(bundleDir, filename),
		targetBundle:  targetBundle,
		minTimestamp:  minTimestamp,
		operations:    make([]plcclient.PLCOperation, 0),
		logger:        logger,
		validated:     false,
		lastSavedLen:  0,
		lastSaveTime:  time.Now(),
		saveThreshold: 100,
		saveInterval:  15 * time.Second,
	}

	if err := m.Load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load mempool: %w", err)
		}
	}

	return m, nil
}

// Add adds operations to the mempool with strict validation
func (m *Mempool) Add(ops []plcclient.PLCOperation) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(ops) == 0 {
		return 0, nil
	}

	// Build existing CID set
	existingCIDs := make(map[string]bool)
	for _, op := range m.operations {
		existingCIDs[op.CID] = true
	}

	// Validate and add operations
	var newOps []plcclient.PLCOperation
	var lastTime time.Time

	// Start from last operation time if we have any
	if len(m.operations) > 0 {
		lastTime = m.operations[len(m.operations)-1].CreatedAt
	} else {
		lastTime = m.minTimestamp
	}

	for _, op := range ops {
		// Skip duplicates
		if existingCIDs[op.CID] {
			continue
		}

		// CRITICAL: Validate chronological order
		if !op.CreatedAt.After(lastTime) && !op.CreatedAt.Equal(lastTime) {
			return len(newOps), fmt.Errorf(
				"chronological violation: operation %s at %s is not after %s",
				op.CID, op.CreatedAt.Format(time.RFC3339Nano), lastTime.Format(time.RFC3339Nano),
			)
		}

		// Validate operation is after minimum timestamp
		if op.CreatedAt.Before(m.minTimestamp) {
			return len(newOps), fmt.Errorf(
				"operation %s at %s is before minimum timestamp %s (belongs in earlier bundle)",
				op.CID, op.CreatedAt.Format(time.RFC3339Nano), m.minTimestamp.Format(time.RFC3339Nano),
			)
		}

		newOps = append(newOps, op)
		existingCIDs[op.CID] = true
		lastTime = op.CreatedAt
	}

	// Add new operations
	m.operations = append(m.operations, newOps...)
	m.validated = true
	m.dirty = true

	return len(newOps), nil
}

// Validate performs a full chronological validation of all operations
func (m *Mempool) Validate() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.operations) == 0 {
		return nil
	}

	// Check all operations are after minimum timestamp
	for i, op := range m.operations {
		if op.CreatedAt.Before(m.minTimestamp) {
			return fmt.Errorf(
				"operation %d (CID: %s) at %s is before minimum timestamp %s",
				i, op.CID, op.CreatedAt.Format(time.RFC3339Nano), m.minTimestamp.Format(time.RFC3339Nano),
			)
		}
	}

	// Check chronological order
	for i := 1; i < len(m.operations); i++ {
		prev := m.operations[i-1]
		curr := m.operations[i]

		if curr.CreatedAt.Before(prev.CreatedAt) {
			return fmt.Errorf(
				"chronological violation at index %d: %s (%s) is before %s (%s)",
				i, curr.CID, curr.CreatedAt.Format(time.RFC3339Nano),
				prev.CID, prev.CreatedAt.Format(time.RFC3339Nano),
			)
		}
	}

	// Check for duplicate CIDs
	cidSet := make(map[string]int)
	for i, op := range m.operations {
		if prevIdx, exists := cidSet[op.CID]; exists {
			return fmt.Errorf(
				"duplicate CID %s at indices %d and %d",
				op.CID, prevIdx, i,
			)
		}
		cidSet[op.CID] = i
	}

	return nil
}

// Count returns the number of operations in mempool
func (m *Mempool) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.operations)
}

// Take removes and returns up to n operations from the front
func (m *Mempool) Take(n int) ([]plcclient.PLCOperation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate before taking
	if err := m.validateLocked(); err != nil {
		return nil, fmt.Errorf("mempool validation failed: %w", err)
	}

	if n > len(m.operations) {
		n = len(m.operations)
	}

	result := make([]plcclient.PLCOperation, n)
	copy(result, m.operations[:n])

	// Remove taken operations
	m.operations = m.operations[n:]

	return result, nil
}

// validateLocked performs validation with lock already held
func (m *Mempool) validateLocked() error {
	if m.validated {
		return nil
	}

	if len(m.operations) == 0 {
		return nil
	}

	// Check chronological order
	lastTime := m.minTimestamp
	for i, op := range m.operations {
		if op.CreatedAt.Before(lastTime) {
			return fmt.Errorf(
				"chronological violation at index %d: %s is before %s",
				i, op.CreatedAt.Format(time.RFC3339Nano), lastTime.Format(time.RFC3339Nano),
			)
		}
		lastTime = op.CreatedAt
	}

	m.validated = true
	return nil
}

// Peek returns up to n operations without removing them
func (m *Mempool) Peek(n int) []plcclient.PLCOperation {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if n > len(m.operations) {
		n = len(m.operations)
	}

	result := make([]plcclient.PLCOperation, n)
	copy(result, m.operations[:n])

	return result
}

// Clear removes all operations
func (m *Mempool) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operations = make([]plcclient.PLCOperation, 0)
	m.validated = false
}

// ShouldSave checks if threshold/interval is met
func (m *Mempool) ShouldSave() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.dirty {
		return false
	}

	newOps := len(m.operations) - m.lastSavedLen
	timeSinceLastSave := time.Since(m.lastSaveTime)

	return newOps >= m.saveThreshold || timeSinceLastSave >= m.saveInterval
}

// SaveIfNeeded saves only if threshold is met
func (m *Mempool) SaveIfNeeded() error {
	if !m.ShouldSave() {
		return nil
	}
	return m.Save()
}

// Save - always append-only since mempool only grows
func (m *Mempool) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.dirty {
		return nil
	}

	if len(m.operations) == 0 {
		os.Remove(m.file)
		m.lastSavedLen = 0
		m.lastSaveTime = time.Now()
		m.dirty = false
		return nil
	}

	// Validate before saving
	if err := m.validateLocked(); err != nil {
		return fmt.Errorf("mempool validation failed, refusing to save: %w", err)
	}

	// Get only new operations since last save
	newOps := m.operations[m.lastSavedLen:]

	if len(newOps) == 0 {
		// Nothing new to save
		m.dirty = false
		return nil
	}

	// Open for append (or create if first save)
	file, err := os.OpenFile(m.file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open mempool: %w", err)
	}
	defer file.Close()

	// Write only new operations
	writer := bufio.NewWriter(file)
	for _, op := range newOps {
		if len(op.RawJSON) > 0 {
			writer.Write(op.RawJSON)
		} else {
			data, _ := json.Marshal(op)
			writer.Write(data)
		}
		writer.WriteByte('\n')
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush mempool: %w", err)
	}

	// Sync to disk for durability
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync mempool: %w", err)
	}

	m.lastSavedLen = len(m.operations)
	m.lastSaveTime = time.Now()
	m.dirty = false

	return nil
}

// Load reads mempool from disk and validates it
func (m *Mempool) Load() error {
	data, err := os.ReadFile(m.file)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Parse JSONL
	scanner := bufio.NewScanner(bytes.NewReader(data))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	m.operations = make([]plcclient.PLCOperation, 0)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var op plcclient.PLCOperation
		if err := json.Unmarshal(line, &op); err != nil {
			return fmt.Errorf("failed to parse mempool operation: %w", err)
		}

		op.RawJSON = make([]byte, len(line))
		copy(op.RawJSON, line)

		m.operations = append(m.operations, op)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Validate loaded data
	if err := m.validateLocked(); err != nil {
		return fmt.Errorf("loaded mempool failed validation: %w", err)
	}

	// Mark as saved (just loaded from disk)
	m.lastSavedLen = len(m.operations)
	m.lastSaveTime = time.Now()
	m.dirty = false

	if len(m.operations) > 0 {
		m.logger.Printf("Loaded %d operations from mempool for bundle %06d", len(m.operations), m.targetBundle)
	}

	return nil
}

// GetFirstTime returns the created_at of the first operation
func (m *Mempool) GetFirstTime() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.operations) == 0 {
		return ""
	}

	return m.operations[0].CreatedAt.Format(time.RFC3339Nano)
}

// GetLastTime returns the created_at of the last operation
func (m *Mempool) GetLastTime() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.operations) == 0 {
		return ""
	}

	return m.operations[len(m.operations)-1].CreatedAt.Format(time.RFC3339Nano)
}

// GetTargetBundle returns the bundle number this mempool is for
func (m *Mempool) GetTargetBundle() int {
	return m.targetBundle
}

// GetMinTimestamp returns the minimum timestamp for operations
func (m *Mempool) GetMinTimestamp() time.Time {
	return m.minTimestamp
}

// Stats returns mempool statistics
func (m *Mempool) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := len(m.operations)

	stats := map[string]interface{}{
		"count":             count,
		"can_create_bundle": count >= types.BUNDLE_SIZE,
		"target_bundle":     m.targetBundle,
		"min_timestamp":     m.minTimestamp,
		"validated":         m.validated,
	}

	if count > 0 {
		stats["first_time"] = m.operations[0].CreatedAt
		stats["last_time"] = m.operations[len(m.operations)-1].CreatedAt

		// Calculate size and unique DIDs
		totalSize := 0
		didSet := make(map[string]bool)
		for _, op := range m.operations {
			totalSize += len(op.RawJSON)
			didSet[op.DID] = true
		}
		stats["size_bytes"] = totalSize
		stats["did_count"] = len(didSet)
	}

	return stats
}

// Delete removes the mempool file
func (m *Mempool) Delete() error {
	if err := os.Remove(m.file); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete mempool file: %w", err)
	}
	return nil
}

// GetFilename returns the mempool filename
func (m *Mempool) GetFilename() string {
	return filepath.Base(m.file)
}

// FindDIDOperations searches for operations matching a DID (no full copy)
func (m *Mempool) FindDIDOperations(did string) []plcclient.PLCOperation {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.operations) == 0 {
		return []plcclient.PLCOperation{}
	}

	// Pre-allocate with small capacity (most DIDs have 1-5 ops)
	matching := make([]plcclient.PLCOperation, 0, 4)

	for _, op := range m.operations {
		if op.DID == did {
			matching = append(matching, op)
		}
	}

	return matching
}

// FindLatestDIDOperation finds the most recent non-nullified operation for a DID
// Returns nil if not found. Searches backwards for early exit.
func (m *Mempool) FindLatestDIDOperation(did string) *plcclient.PLCOperation {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Search backwards from most recent
	for i := len(m.operations) - 1; i >= 0; i-- {
		if m.operations[i].DID == did {
			if !m.operations[i].IsNullified() {
				// Return pointer to avoid copy
				return &m.operations[i]
			}
		}
	}

	return nil
}
