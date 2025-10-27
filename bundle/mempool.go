package bundle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/atscan/plcbundle/plc"
)

const MEMPOOL_FILE = "plc_mempool.jsonl"

// Mempool stores operations waiting to be bundled
type Mempool struct {
	operations []plc.PLCOperation
	file       string
	mu         sync.RWMutex
	logger     Logger
}

// NewMempool creates a new mempool
func NewMempool(bundleDir string, logger Logger) (*Mempool, error) {
	m := &Mempool{
		file:       filepath.Join(bundleDir, MEMPOOL_FILE),
		operations: make([]plc.PLCOperation, 0),
		logger:     logger,
	}

	// Load existing mempool from disk
	if err := m.Load(); err != nil {
		// If file doesn't exist, that's OK
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load mempool: %w", err)
		}
	}

	return m, nil
}

// Add adds operations to the mempool (deduplicates by CID)
func (m *Mempool) Add(ops []plc.PLCOperation) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build existing CID set
	existingCIDs := make(map[string]bool)
	for _, op := range m.operations {
		existingCIDs[op.CID] = true
	}

	// Add only new operations
	addedCount := 0
	for _, op := range ops {
		if !existingCIDs[op.CID] {
			m.operations = append(m.operations, op)
			existingCIDs[op.CID] = true
			addedCount++
		}
	}

	return addedCount
}

// Count returns the number of operations in mempool
func (m *Mempool) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.operations)
}

// Take removes and returns up to n operations from the front
func (m *Mempool) Take(n int) []plc.PLCOperation {
	m.mu.Lock()
	defer m.mu.Unlock()

	if n > len(m.operations) {
		n = len(m.operations)
	}

	result := make([]plc.PLCOperation, n)
	copy(result, m.operations[:n])

	// Remove taken operations
	m.operations = m.operations[n:]

	return result
}

// Peek returns up to n operations without removing them
func (m *Mempool) Peek(n int) []plc.PLCOperation {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if n > len(m.operations) {
		n = len(m.operations)
	}

	result := make([]plc.PLCOperation, n)
	copy(result, m.operations[:n])

	return result
}

// Clear removes all operations
func (m *Mempool) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operations = make([]plc.PLCOperation, 0)
}

// Save persists mempool to disk
func (m *Mempool) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.operations) == 0 {
		// Remove file if empty
		os.Remove(m.file)
		return nil
	}

	// Serialize to JSONL
	var buf bytes.Buffer
	for _, op := range m.operations {
		if len(op.RawJSON) > 0 {
			buf.Write(op.RawJSON)
		} else {
			data, _ := json.Marshal(op)
			buf.Write(data)
		}
		buf.WriteByte('\n')
	}

	// Write atomically
	tempFile := m.file + ".tmp"
	if err := os.WriteFile(tempFile, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write mempool: %w", err)
	}

	if err := os.Rename(tempFile, m.file); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename mempool file: %w", err)
	}

	return nil
}

// Load reads mempool from disk
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

	m.operations = make([]plc.PLCOperation, 0)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var op plc.PLCOperation
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

	if len(m.operations) > 0 {
		m.logger.Printf("Loaded %d operations from mempool", len(m.operations))
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

	return m.operations[0].CreatedAt.Format("2006-01-02T15:04:05.000Z")
}

// GetLastTime returns the created_at of the last operation
func (m *Mempool) GetLastTime() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.operations) == 0 {
		return ""
	}

	return m.operations[len(m.operations)-1].CreatedAt.Format("2006-01-02T15:04:05.000Z")
}

// Stats returns mempool statistics
func (m *Mempool) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := len(m.operations)

	stats := map[string]interface{}{
		"count":             count,
		"can_create_bundle": count >= BUNDLE_SIZE,
	}

	stats["first_time"] = m.operations[0].CreatedAt
	stats["last_time"] = m.operations[len(m.operations)-1].CreatedAt

	// Calculate size
	totalSize := 0
	for _, op := range m.operations {
		totalSize += len(op.RawJSON)
	}
	stats["size_bytes"] = totalSize

	return stats
}
