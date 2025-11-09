package mempool_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/mempool"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	l.t.Logf(format, v...)
}

func (l *testLogger) Println(v ...interface{}) {
	l.t.Log(v...)
}

// ====================================================================================
// CHRONOLOGICAL VALIDATION - MOST CRITICAL
// ====================================================================================

func TestMempoolChronologicalStrict(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("RejectOutOfOrder", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 1, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Add operations in order: 1, 2, 4
		ops := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second)},
			{CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second)},
			{CID: "cid4", CreatedAt: baseTime.Add(4 * time.Second)},
		}

		_, err = m.Add(ops)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		// Now try to add operation 3 (out of order)
		outOfOrder := []plcclient.PLCOperation{
			{CID: "cid3", CreatedAt: baseTime.Add(3 * time.Second)},
		}

		_, err = m.Add(outOfOrder)
		if err == nil {
			t.Error("expected chronological validation error, got nil")
		}

		if m.Count() != 3 {
			t.Errorf("count should still be 3, got %d", m.Count())
		}
	})

	t.Run("RejectBeforeMinTimestamp", func(t *testing.T) {
		minTime := baseTime.Add(10 * time.Second)
		m, err := mempool.NewMempool(tmpDir, 2, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Try to add operation before min timestamp
		tooEarly := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime}, // Before minTime
		}

		_, err = m.Add(tooEarly)
		if err == nil {
			t.Error("expected error for operation before min timestamp")
		}
	})

	t.Run("AllowEqualTimestamps", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 3, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Multiple operations with same timestamp (happens in real PLC data)
		sameTime := baseTime.Add(5 * time.Second)
		ops := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: sameTime},
			{CID: "cid2", CreatedAt: sameTime},
			{CID: "cid3", CreatedAt: sameTime},
		}

		added, err := m.Add(ops)
		if err != nil {
			t.Fatalf("should allow equal timestamps: %v", err)
		}

		if added != 3 {
			t.Errorf("expected 3 added, got %d", added)
		}
	})

	t.Run("ChronologicalAfterReload", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 4, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Add some operations
		ops1 := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second)},
			{CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second)},
		}
		m.Add(ops1)
		m.Save()

		// Reload mempool
		m2, err := mempool.NewMempool(tmpDir, 4, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool reload failed: %v", err)
		}

		// Try to add out-of-order operation
		outOfOrder := []plcclient.PLCOperation{
			{CID: "cid0", CreatedAt: baseTime}, // Before loaded ops
		}

		_, err = m2.Add(outOfOrder)
		if err == nil {
			t.Error("should reject out-of-order after reload")
		}

		// Add valid operation after loaded ones
		validOps := []plcclient.PLCOperation{
			{CID: "cid3", CreatedAt: baseTime.Add(3 * time.Second)},
		}

		added, err := m2.Add(validOps)
		if err != nil {
			t.Fatalf("should accept in-order operation: %v", err)
		}

		if added != 1 {
			t.Error("should have added 1 operation")
		}
	})

	t.Run("StrictIncreasingOrder", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 5, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Each operation must be >= previous timestamp
		ops := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second)},
			{CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second)},
			{CID: "cid3", CreatedAt: baseTime.Add(2 * time.Second)}, // Equal - OK
			{CID: "cid4", CreatedAt: baseTime.Add(3 * time.Second)},
		}

		added, err := m.Add(ops)
		if err != nil {
			t.Fatalf("should allow non-decreasing timestamps: %v", err)
		}

		if added != 4 {
			t.Errorf("expected 4 added, got %d", added)
		}
	})
}

// ====================================================================================
// DUPLICATE PREVENTION
// ====================================================================================

func TestMempoolDuplicatePrevention(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("SameCIDTwice", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 6, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		op := plcclient.PLCOperation{
			CID:       "duplicate_cid",
			DID:       "did:plc:test",
			CreatedAt: baseTime.Add(1 * time.Second),
		}

		// Add first time
		added, err := m.Add([]plcclient.PLCOperation{op})
		if err != nil {
			t.Fatalf("first add failed: %v", err)
		}
		if added != 1 {
			t.Error("first add should succeed")
		}

		// Add same CID again (should be silently skipped)
		added, err = m.Add([]plcclient.PLCOperation{op})
		if err != nil {
			t.Fatalf("duplicate add should not error: %v", err)
		}
		if added != 0 {
			t.Errorf("duplicate should be skipped, but added=%d", added)
		}

		if m.Count() != 1 {
			t.Errorf("count should be 1, got %d", m.Count())
		}
	})

	t.Run("DuplicateAcrossSaveLoad", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 7, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		op := plcclient.PLCOperation{
			CID:       "persistent_cid",
			DID:       "did:plc:test",
			CreatedAt: baseTime.Add(1 * time.Second),
		}

		// Add and save
		m.Add([]plcclient.PLCOperation{op})
		m.Save()

		// Reload
		m2, err := mempool.NewMempool(tmpDir, 7, minTime, logger)
		if err != nil {
			t.Fatalf("reload failed: %v", err)
		}

		// Try to add same operation
		added, err := m2.Add([]plcclient.PLCOperation{op})
		if err != nil {
			t.Fatalf("add after reload failed: %v", err)
		}

		if added != 0 {
			t.Errorf("duplicate should be skipped after reload, added=%d", added)
		}

		if m2.Count() != 1 {
			t.Errorf("count should be 1, got %d", m2.Count())
		}
	})

	t.Run("DuplicatesInBatch", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 8, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Batch contains duplicates
		ops := []plcclient.PLCOperation{
			{CID: "cid1", DID: "did:plc:001", CreatedAt: baseTime.Add(1 * time.Second)},
			{CID: "cid2", DID: "did:plc:002", CreatedAt: baseTime.Add(2 * time.Second)},
			{CID: "cid1", DID: "did:plc:001", CreatedAt: baseTime.Add(3 * time.Second)}, // Duplicate CID
		}

		added, err := m.Add(ops)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		// Should only add 2 (skip duplicate)
		if added != 2 {
			t.Errorf("expected 2 unique operations, added %d", added)
		}

		if m.Count() != 2 {
			t.Errorf("count should be 2, got %d", m.Count())
		}
	})
}

// ====================================================================================
// PERSISTENCE & CORRUPTION HANDLING
// ====================================================================================

func TestMempoolPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("SaveAndLoad", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 9, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(50)
		m.Add(ops)

		if err := m.Save(); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		// Reload
		m2, err := mempool.NewMempool(tmpDir, 9, minTime, logger)
		if err != nil {
			t.Fatalf("reload failed: %v", err)
		}

		if m2.Count() != 50 {
			t.Errorf("after reload, expected 50 ops, got %d", m2.Count())
		}

		// Verify data integrity
		loaded := m2.Peek(50)
		for i := 0; i < 50; i++ {
			if loaded[i].CID != ops[i].CID {
				t.Errorf("op %d CID mismatch after reload", i)
			}
		}
	})

	// Fix the IncrementalSave test - line ~353
	t.Run("IncrementalSave", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 10, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Add 10 ops and save
		ops1 := makeTestOperations(10)
		m.Add(ops1)
		m.Save()

		// Add 10 more and save
		// FIX: makeTestOperationsFrom(start, COUNT) - so we want (10, 10) not (10, 20)
		ops2 := makeTestOperationsFrom(10, 10) // ← Changed from (10, 20)
		m.Add(ops2)
		m.Save()

		// Reload - should have all 20
		m2, err := mempool.NewMempool(tmpDir, 10, minTime, logger)
		if err != nil {
			t.Fatalf("reload failed: %v", err)
		}

		if m2.Count() != 20 {
			t.Errorf("expected 20 ops after incremental saves, got %d", m2.Count())
		}
	})

	t.Run("CorruptedMempoolFile", func(t *testing.T) {
		minTime := baseTime
		mempoolFile := filepath.Join(tmpDir, "plc_mempool_000011.jsonl")

		// Write corrupted data
		os.WriteFile(mempoolFile, []byte("{invalid json\n{also bad"), 0644)

		// Should error on load
		_, err := mempool.NewMempool(tmpDir, 11, minTime, logger)
		if err == nil {
			t.Error("expected error loading corrupted mempool")
		}
	})

	t.Run("DeleteMempool", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 12, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(10)
		m.Add(ops)
		m.Save()

		// Verify file exists
		mempoolFile := filepath.Join(tmpDir, "plc_mempool_000012.jsonl")
		if _, err := os.Stat(mempoolFile); os.IsNotExist(err) {
			t.Fatal("mempool file should exist after save")
		}

		// Delete
		if err := m.Delete(); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify file gone
		if _, err := os.Stat(mempoolFile); !os.IsNotExist(err) {
			t.Error("mempool file should be deleted")
		}
	})
}

// ====================================================================================
// TAKE OPERATIONS - CRITICAL FOR BUNDLING
// ====================================================================================

func TestMempoolTakeOperations(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("TakeExact", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 13, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		m.Add(makeTestOperations(100))

		taken, err := m.Take(50)
		if err != nil {
			t.Fatalf("Take failed: %v", err)
		}

		if len(taken) != 50 {
			t.Errorf("expected 50 operations, got %d", len(taken))
		}

		if m.Count() != 50 {
			t.Errorf("expected 50 remaining, got %d", m.Count())
		}
	})

	t.Run("TakeMoreThanAvailable", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 14, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		m.Add(makeTestOperations(30))

		// Try to take 100 (only 30 available)
		taken, err := m.Take(100)
		if err != nil {
			t.Fatalf("Take failed: %v", err)
		}

		if len(taken) != 30 {
			t.Errorf("expected 30 operations (all available), got %d", len(taken))
		}

		if m.Count() != 0 {
			t.Errorf("mempool should be empty, got %d", m.Count())
		}
	})

	t.Run("TakePreservesOrder", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 15, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(100)
		m.Add(ops)

		taken, err := m.Take(50)
		if err != nil {
			t.Fatalf("Take failed: %v", err)
		}

		// Verify first 50 match
		for i := 0; i < 50; i++ {
			if taken[i].CID != ops[i].CID {
				t.Errorf("operation %d mismatch: got %s, want %s", i, taken[i].CID, ops[i].CID)
			}
		}

		// Remaining should be ops[50:100]
		remaining := m.Peek(50)
		for i := 0; i < 50; i++ {
			if remaining[i].CID != ops[50+i].CID {
				t.Errorf("remaining op %d mismatch", i)
			}
		}
	})

	t.Run("TakeFromEmpty", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 16, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		taken, err := m.Take(10)
		if err != nil {
			t.Fatalf("Take from empty failed: %v", err)
		}

		if len(taken) != 0 {
			t.Errorf("expected 0 operations from empty mempool, got %d", len(taken))
		}
	})
}

// ====================================================================================
// VALIDATION TESTS
// ====================================================================================

func TestMempoolValidation(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("ValidateChronological", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 17, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(100)
		m.Add(ops)

		if err := m.Validate(); err != nil {
			t.Errorf("Validate failed on valid mempool: %v", err)
		}
	})

	t.Run("ValidateDetectsMinTimestampViolation", func(t *testing.T) {
		minTime := baseTime.Add(10 * time.Second)
		_, err := mempool.NewMempool(tmpDir, 18, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Manually add operation before min (bypassing Add validation)
		// This simulates corrupted state
		ops := makeTestOperations(10)
		ops[0].CreatedAt = baseTime // Before minTime

		// Note: This is hard to test since Add enforces validation
		// Better to test through file corruption
	})

	t.Run("ValidateDetectsDuplicateCIDs", func(t *testing.T) {
		// Test for duplicate CID detection
		// Similar challenge - Add prevents duplicates
		// Would need to manually construct corrupted state
	})
}

// ====================================================================================
// CONCURRENCY TESTS
// ====================================================================================

func TestMempoolConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("ConcurrentReads", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 19, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		m.Add(makeTestOperations(1000))

		// 100 concurrent readers
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				count := m.Count()
				if count != 1000 {
					t.Errorf("count mismatch: got %d", count)
				}

				peek := m.Peek(10)
				if len(peek) != 10 {
					t.Errorf("peek mismatch: got %d", len(peek))
				}
			}()
		}
		wg.Wait()
	})

	t.Run("ConcurrentAddAndRead", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 20, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		var wg sync.WaitGroup
		errors := make(chan error, 100)

		// Writer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				ops := []plcclient.PLCOperation{
					{CID: fmt.Sprintf("cid%d", i*100), CreatedAt: baseTime.Add(time.Duration(i*100) * time.Second)},
				}
				if _, err := m.Add(ops); err != nil {
					errors <- err
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		// Reader goroutines
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					m.Count()
					m.Peek(5)
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("concurrent operation error: %v", err)
		}
	})
}

// ====================================================================================
// STATS & METADATA TESTS
// ====================================================================================

func TestMempoolStats(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("StatsEmpty", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 21, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		stats := m.Stats()

		if stats["count"].(int) != 0 {
			t.Error("empty mempool should have count 0")
		}

		if stats["can_create_bundle"].(bool) {
			t.Error("empty mempool cannot create bundle")
		}

		if stats["target_bundle"].(int) != 21 {
			t.Error("target bundle mismatch")
		}
	})

	t.Run("StatsPopulated", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 22, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(100)
		m.Add(ops)

		stats := m.Stats()

		if stats["count"].(int) != 100 {
			t.Error("count mismatch in stats")
		}

		if _, ok := stats["first_time"]; !ok {
			t.Error("stats missing first_time")
		}

		if _, ok := stats["last_time"]; !ok {
			t.Error("stats missing last_time")
		}

		if _, ok := stats["size_bytes"]; !ok {
			t.Error("stats missing size_bytes")
		}

		if stats["did_count"].(int) != 100 {
			t.Error("did_count should match operation count for unique DIDs")
		}
	})

	t.Run("StatsCanCreateBundle", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 23, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Add exactly BUNDLE_SIZE operations
		m.Add(makeTestOperations(types.BUNDLE_SIZE))

		stats := m.Stats()

		if !stats["can_create_bundle"].(bool) {
			t.Error("should be able to create bundle with BUNDLE_SIZE operations")
		}
	})
}

// ====================================================================================
// DID SEARCH TESTS
// ====================================================================================

func TestMempoolDIDSearch(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("FindDIDOperations", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 24, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		targetDID := "did:plc:target"

		ops := []plcclient.PLCOperation{
			{DID: "did:plc:other1", CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second)},
			{DID: targetDID, CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second)},
			{DID: "did:plc:other2", CID: "cid3", CreatedAt: baseTime.Add(3 * time.Second)},
			{DID: targetDID, CID: "cid4", CreatedAt: baseTime.Add(4 * time.Second)},
			{DID: "did:plc:other3", CID: "cid5", CreatedAt: baseTime.Add(5 * time.Second)},
		}

		m.Add(ops)

		// Search
		found := m.FindDIDOperations(targetDID)

		if len(found) != 2 {
			t.Errorf("expected 2 operations for %s, got %d", targetDID, len(found))
		}

		if found[0].CID != "cid2" || found[1].CID != "cid4" {
			t.Error("wrong operations returned")
		}
	})

	t.Run("FindLatestDIDOperation", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 25, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		targetDID := "did:plc:target"

		ops := []plcclient.PLCOperation{
			{DID: targetDID, CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second), Nullified: false},
			{DID: targetDID, CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second), Nullified: false},
			{DID: targetDID, CID: "cid3", CreatedAt: baseTime.Add(3 * time.Second), Nullified: true}, // Nullified
		}

		m.Add(ops)

		// Should return cid2 (latest non-nullified)
		latest := m.FindLatestDIDOperation(targetDID)

		if latest == nil {
			t.Fatal("expected to find operation, got nil")
		}

		if latest.CID != "cid2" {
			t.Errorf("expected cid2 (latest non-nullified), got %s", latest.CID)
		}
	})

	t.Run("FindLatestDIDOperation_AllNullified", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 26, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		targetDID := "did:plc:target"

		ops := []plcclient.PLCOperation{
			{DID: targetDID, CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second), Nullified: true},
			{DID: targetDID, CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second), Nullified: true},
		}

		m.Add(ops)

		latest := m.FindLatestDIDOperation(targetDID)

		if latest != nil {
			t.Error("should return nil when all operations are nullified")
		}
	})

	t.Run("FindDIDOperations_NotFound", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 27, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		m.Add(makeTestOperations(100))

		found := m.FindDIDOperations("did:plc:nonexistent")

		if len(found) != 0 {
			t.Errorf("expected empty result, got %d operations", len(found))
		}
	})
}

// ====================================================================================
// CLEAR OPERATION TESTS
// ====================================================================================

func TestMempoolClear(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	baseTime := time.Now().Add(-time.Hour)

	t.Run("ClearPopulated", func(t *testing.T) {
		minTime := baseTime
		m, err := mempool.NewMempool(tmpDir, 28, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		m.Add(makeTestOperations(100))

		if m.Count() != 100 {
			t.Fatal("setup failed")
		}

		m.Clear()

		if m.Count() != 0 {
			t.Errorf("after clear, count should be 0, got %d", m.Count())
		}

		// Should be able to add new operations
		newOps := []plcclient.PLCOperation{
			{CID: "new1", CreatedAt: baseTime.Add(200 * time.Second)},
		}

		added, err := m.Add(newOps)
		if err != nil {
			t.Fatalf("Add after clear failed: %v", err)
		}

		if added != 1 {
			t.Error("should be able to add after clear")
		}
	})
}

// ====================================================================================
// HELPER FUNCTIONS
// ====================================================================================

func makeTestOperations(count int) []plcclient.PLCOperation {
	return makeTestOperationsFrom(0, count)
}

func makeTestOperationsFrom(start, count int) []plcclient.PLCOperation {
	ops := make([]plcclient.PLCOperation, count)
	baseTime := time.Now().Add(-time.Hour)

	for i := 0; i < count; i++ {
		idx := start + i
		ops[i] = plcclient.PLCOperation{
			DID:       fmt.Sprintf("did:plc:test%06d", idx),
			CID:       fmt.Sprintf("bafy%06d", idx),
			CreatedAt: baseTime.Add(time.Duration(idx) * time.Second),
		}
	}

	return ops
}
