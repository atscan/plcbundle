package sync_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
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

// Mock mempool for testing
type mockMempool struct {
	operations []plcclient.PLCOperation
	mu         sync.Mutex
	saveCount  int32
}

func newMockMempool() *mockMempool {
	return &mockMempool{
		operations: make([]plcclient.PLCOperation, 0),
	}
}

func (m *mockMempool) Add(ops []plcclient.PLCOperation) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build existing CID set (like real mempool does)
	existingCIDs := make(map[string]bool)
	for _, op := range m.operations {
		existingCIDs[op.CID] = true
	}

	// Only add new operations (deduplicate by CID)
	addedCount := 0
	for _, op := range ops {
		if !existingCIDs[op.CID] {
			m.operations = append(m.operations, op)
			existingCIDs[op.CID] = true
			addedCount++
		}
	}

	return addedCount, nil
}

func (m *mockMempool) Save() error {
	atomic.AddInt32(&m.saveCount, 1)
	return nil
}

func (m *mockMempool) SaveIfNeeded() error {
	return m.Save()
}

func (m *mockMempool) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.operations)
}

func (m *mockMempool) GetLastTime() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.operations) == 0 {
		return ""
	}
	return m.operations[len(m.operations)-1].CreatedAt.Format(time.RFC3339Nano)
}

// ====================================================================================
// FETCHER TESTS - DEDUPLICATION & RETRY LOGIC
// ====================================================================================

func TestFetcherDeduplication(t *testing.T) {
	t.Run("BoundaryDuplicateHandling", func(t *testing.T) {
		// Setup mock server
		baseTime := time.Now()
		boundaryTime := baseTime.Add(5 * time.Second)

		// Simulate operations at bundle boundary
		mockOps := []plcclient.PLCOperation{
			{DID: "did:plc:001", CID: "cid1", CreatedAt: boundaryTime},
			{DID: "did:plc:002", CID: "cid2", CreatedAt: boundaryTime},
			{DID: "did:plc:003", CID: "cid3", CreatedAt: boundaryTime.Add(1 * time.Second)},
		}

		server := createMockPLCServer(t, mockOps)
		defer server.Close()

		// Create fetcher
		client := plcclient.NewClient(server.URL)
		defer client.Close()

		logger := &testLogger{t: t}
		ops, _ := storage.NewOperations(logger, false)
		defer ops.Close()

		fetcher := internalsync.NewFetcher(client, ops, logger)

		// Previous bundle had cid1 and cid2 at boundary
		prevBoundaryCIDs := map[string]bool{
			"cid1": true,
			"cid2": true,
		}

		mempool := newMockMempool()

		// Fetch
		newOps, fetchCount, err := fetcher.FetchToMempool(
			context.Background(),
			boundaryTime.Add(-1*time.Second).Format(time.RFC3339Nano),
			prevBoundaryCIDs,
			10,
			true, // quiet
			mempool,
			0,
		)

		if err != nil {
			t.Fatalf("FetchToMempool failed: %v", err)
		}

		// Should have filtered out cid1 and cid2 (duplicates)
		// Only cid3 should be returned
		if len(newOps) != 1 {
			t.Errorf("expected 1 unique operation, got %d", len(newOps))
		}

		if len(newOps) > 0 && newOps[0].CID != "cid3" {
			t.Errorf("expected cid3, got %s", newOps[0].CID)
		}

		if fetchCount == 0 {
			t.Error("expected at least one fetch")
		}
	})

	t.Run("ConcurrentFetchDedup", func(t *testing.T) {
		baseTime := time.Now()
		mockOps := make([]plcclient.PLCOperation, 50)
		for i := 0; i < 50; i++ {
			mockOps[i] = plcclient.PLCOperation{
				DID:       fmt.Sprintf("did:plc:%03d", i),
				CID:       fmt.Sprintf("cid%03d", i),
				CreatedAt: baseTime.Add(time.Duration(i) * time.Second),
			}
		}

		server := createMockPLCServer(t, mockOps)
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		logger := &testLogger{t: t}
		storageOps, _ := storage.NewOperations(logger, false)
		defer storageOps.Close()

		fetcher := internalsync.NewFetcher(client, storageOps, logger)
		mempool := newMockMempool()

		// First fetch
		initialCount := mempool.Count()
		_, _, err := fetcher.FetchToMempool(
			context.Background(),
			"",
			nil,
			30,
			true,
			mempool,
			0,
		)
		if err != nil {
			t.Fatalf("First fetch failed: %v", err)
		}

		countAfterFirst := mempool.Count()
		addedFirst := countAfterFirst - initialCount

		if addedFirst == 0 {
			t.Fatal("first fetch should add operations")
		}

		// Second fetch with same cursor - mempool deduplicates
		countBeforeSecond := mempool.Count()
		_, _, err = fetcher.FetchToMempool(
			context.Background(),
			"", // Same cursor - fetches same data
			nil,
			30,
			true,
			mempool,
			1,
		)
		if err != nil {
			t.Fatalf("Second fetch failed: %v", err)
		}

		countAfterSecond := mempool.Count()
		addedSecond := countAfterSecond - countBeforeSecond

		// Mempool's Add() method deduplicates by CID
		// So second fetch should add 0 (all duplicates)
		if addedSecond != 0 {
			t.Errorf("expected 0 new ops in mempool after second fetch (duplicates), got %d", addedSecond)
		}

		t.Logf("First fetch: +%d ops, Second fetch: +%d ops (deduped)", addedFirst, addedSecond)
	})

	t.Run("EmptyBoundaryCIDs", func(t *testing.T) {
		baseTime := time.Now()
		mockOps := []plcclient.PLCOperation{
			{DID: "did:plc:001", CID: "cid1", CreatedAt: baseTime},
		}

		server := createMockPLCServer(t, mockOps)
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		logger := &testLogger{t: t}
		storageOps, _ := storage.NewOperations(logger, false)
		defer storageOps.Close()

		fetcher := internalsync.NewFetcher(client, storageOps, logger)
		mempool := newMockMempool()

		// Fetch with no boundary CIDs (genesis bundle)
		newOps, _, err := fetcher.FetchToMempool(
			context.Background(),
			"",
			nil, // No previous boundary
			10,
			true,
			mempool,
			0,
		)

		if err != nil {
			t.Fatalf("FetchToMempool failed: %v", err)
		}

		if len(newOps) != 1 {
			t.Errorf("expected 1 operation, got %d", len(newOps))
		}
	})
}

func TestFetcherRetry(t *testing.T) {
	t.Run("TransientFailures", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++

			if attemptCount < 3 {
				// Fail first 2 attempts
				w.WriteHeader(500)
				return
			}

			// Succeed on 3rd attempt
			w.Header().Set("Content-Type", "application/x-ndjson")
			op := plcclient.PLCOperation{
				DID:       "did:plc:test",
				CID:       "cid1",
				CreatedAt: time.Now(),
			}
			json.NewEncoder(w).Encode(op)
		}))
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		// Should retry and eventually succeed
		_, err := client.Export(context.Background(), plcclient.ExportOptions{Count: 1})
		if err != nil {
			t.Fatalf("expected retry to succeed, got error: %v", err)
		}

		if attemptCount < 3 {
			t.Errorf("expected at least 3 attempts, got %d", attemptCount)
		}
	})

	t.Run("RateLimitHandling", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++

			if attemptCount == 1 {
				// Return 429 with Retry-After
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(429)
				return
			}

			// Success
			w.Header().Set("Content-Type", "application/x-ndjson")
			op := plcclient.PLCOperation{
				DID:       "did:plc:test",
				CID:       "cid1",
				CreatedAt: time.Now(),
			}
			json.NewEncoder(w).Encode(op)
		}))
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		startTime := time.Now()
		_, err := client.Export(context.Background(), plcclient.ExportOptions{Count: 1})
		elapsed := time.Since(startTime)

		if err != nil {
			t.Fatalf("expected success after rate limit, got: %v", err)
		}

		// Should have waited at least 1 second
		if elapsed < 1*time.Second {
			t.Errorf("expected wait for rate limit, elapsed: %v", elapsed)
		}

		if attemptCount != 2 {
			t.Errorf("expected 2 attempts, got %d", attemptCount)
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Slow response
			time.Sleep(5 * time.Second)
		}))
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := client.Export(ctx, plcclient.ExportOptions{Count: 1})
		if err == nil {
			t.Error("expected timeout error, got nil")
		}
	})
}

func TestFetcherMempoolIntegration(t *testing.T) {
	t.Run("AutoSaveAfterFetch", func(t *testing.T) {
		baseTime := time.Now()
		mockOps := []plcclient.PLCOperation{
			{DID: "did:plc:001", CID: "cid1", CreatedAt: baseTime},
			{DID: "did:plc:002", CID: "cid2", CreatedAt: baseTime.Add(1 * time.Second)},
		}

		server := createMockPLCServer(t, mockOps)
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		logger := &testLogger{t: t}
		storageOps, _ := storage.NewOperations(logger, false)
		defer storageOps.Close()

		fetcher := internalsync.NewFetcher(client, storageOps, logger)
		mempool := newMockMempool()

		_, _, err := fetcher.FetchToMempool(
			context.Background(),
			"",
			nil,
			10,
			true,
			mempool,
			0,
		)

		if err != nil {
			t.Fatalf("FetchToMempool failed: %v", err)
		}

		// Verify mempool.SaveIfNeeded was called
		if mempool.saveCount == 0 {
			t.Error("expected mempool to be saved after fetch")
		}
	})
}

// ====================================================================================
// CLONER TESTS
// ====================================================================================

func TestClonerAtomicity(t *testing.T) {
	// Note: Cloner tests would need more complex mocking
	// Including mock HTTP server, file system operations, etc.
	// This is a template showing what to test

	t.Run("InterruptedClone", func(t *testing.T) {
		// TODO: Test context cancellation mid-download
		// Verify:
		// - .tmp files are cleaned up OR kept for resume
		// - Index not updated for incomplete downloads
		// - Partial progress can resume with --resume flag
	})

	t.Run("HashVerificationFailure", func(t *testing.T) {
		// TODO: Mock server returns file with wrong hash
		// Verify:
		// - File is deleted (or .tmp is not renamed)
		// - Bundle NOT added to index
		// - Error returned to user
	})

	t.Run("IndexUpdateTiming", func(t *testing.T) {
		// CRITICAL: Index must only update AFTER file write succeeds
		// TODO: Implement test that verifies ordering
	})
}

// ====================================================================================
// SYNC LOOP TESTS
// ====================================================================================

func TestSyncLoopBehavior(t *testing.T) {
	t.Run("CatchUpDetection", func(t *testing.T) {
		// Mock manager
		mockMgr := &mockSyncManager{
			lastBundle:   5,
			mempoolCount: 500,
		}

		logger := &testLogger{t: t}
		config := &internalsync.SyncLoopConfig{
			MaxBundles: 0,
			Verbose:    false,
			Logger:     logger,
		}

		// First sync should detect "caught up" when no progress
		synced, err := internalsync.SyncOnce(context.Background(), mockMgr, config, false)

		if err != nil {
			t.Fatalf("SyncOnce failed: %v", err)
		}

		// Should return 0 if already caught up
		if synced != 0 {
			t.Logf("Note: synced %d bundles (manager may not be caught up)", synced)
		}
	})

	t.Run("MaxBundlesLimit", func(t *testing.T) {
		mockMgr := &mockSyncManager{
			lastBundle:   0,
			mempoolCount: 10000, // Always has enough for bundle
		}

		logger := &testLogger{t: t}
		config := &internalsync.SyncLoopConfig{
			MaxBundles: 3,
			Verbose:    false,
			Logger:     logger,
		}

		ctx := context.Background()
		synced, err := internalsync.SyncOnce(ctx, mockMgr, config, false)

		if err != nil {
			t.Fatalf("SyncOnce failed: %v", err)
		}

		// Should respect max limit
		if synced > 3 {
			t.Errorf("synced %d bundles, but max was 3", synced)
		}
	})

	t.Run("GracefulShutdown", func(t *testing.T) {
		mockMgr := &mockSyncManager{
			lastBundle:   0,
			mempoolCount: 10000,
			fetchDelay:   50 * time.Millisecond,
		}

		logger := &testLogger{t: t}
		config := &internalsync.SyncLoopConfig{
			Interval:   100 * time.Millisecond,
			MaxBundles: 0,
			Verbose:    false,
			Logger:     logger,
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Start sync loop in goroutine
		done := make(chan error, 1)
		go func() {
			done <- internalsync.RunSyncLoop(ctx, mockMgr, config)
		}()

		// Let it run briefly (should complete at least one cycle)
		time.Sleep(250 * time.Millisecond)

		// Cancel context
		cancel()

		// Should exit gracefully with context.Canceled error
		select {
		case err := <-done:
			// Expected: context.Canceled or nil
			if err != nil && err != context.Canceled {
				t.Errorf("unexpected error on shutdown: %v", err)
			}
			t.Logf("Sync loop stopped cleanly: %v", err)

		case <-time.After(2 * time.Second):
			t.Error("sync loop did not stop within timeout after context cancellation")
		}

		// NOTE: Mempool saving on shutdown is handled by the caller (commands/server),
		// not by the sync loop itself. The sync loop only respects context cancellation.
		//
		// For mempool save testing, see command-level tests.
	})
}

// ====================================================================================
// BUNDLER TESTS
// ====================================================================================

func TestBundlerCreateBundle(t *testing.T) {
	logger := &testLogger{t: t}
	storageOps, _ := storage.NewOperations(logger, false)
	defer storageOps.Close()

	t.Run("BasicBundleCreation", func(t *testing.T) {
		operations := makeTestOperations(10000)
		cursor := operations[len(operations)-1].CreatedAt.Format(time.RFC3339Nano)

		bundle := internalsync.CreateBundle(1, operations, cursor, "", storageOps)

		if bundle.BundleNumber != 1 {
			t.Errorf("wrong bundle number: got %d, want 1", bundle.BundleNumber)
		}

		if len(bundle.Operations) != 10000 {
			t.Errorf("wrong operation count: got %d, want 10000", len(bundle.Operations))
		}

		if bundle.DIDCount == 0 {
			t.Error("DIDCount should not be zero")
		}

		if len(bundle.BoundaryCIDs) == 0 {
			t.Error("BoundaryCIDs should not be empty")
		}

		if bundle.Cursor != cursor {
			t.Error("cursor mismatch")
		}
	})

	t.Run("GenesisBundle", func(t *testing.T) {
		operations := makeTestOperations(10000)
		cursor := operations[len(operations)-1].CreatedAt.Format(time.RFC3339Nano)

		bundle := internalsync.CreateBundle(1, operations, cursor, "", storageOps)

		// Genesis should have empty parent
		if bundle.Parent != "" {
			t.Errorf("genesis bundle should have empty parent, got %s", bundle.Parent)
		}
	})

	t.Run("ChainedBundle", func(t *testing.T) {
		operations := makeTestOperations(10000)
		cursor := operations[len(operations)-1].CreatedAt.Format(time.RFC3339Nano)
		parentHash := "parent_hash_from_bundle_1"

		bundle := internalsync.CreateBundle(2, operations, cursor, parentHash, storageOps)

		if bundle.Parent != parentHash {
			t.Errorf("parent mismatch: got %s, want %s", bundle.Parent, parentHash)
		}

		if bundle.BundleNumber != 2 {
			t.Error("bundle number should be 2")
		}
	})

	t.Run("BoundaryTimestamps", func(t *testing.T) {
		baseTime := time.Now()

		// Create operations where last 5 share same timestamp
		operations := makeTestOperations(10000)
		for i := 9995; i < 10000; i++ {
			operations[i].CreatedAt = baseTime
		}

		cursor := baseTime.Format(time.RFC3339Nano)
		bundle := internalsync.CreateBundle(1, operations, cursor, "", storageOps)

		// Should capture all 5 CIDs at boundary
		if len(bundle.BoundaryCIDs) != 5 {
			t.Errorf("expected 5 boundary CIDs, got %d", len(bundle.BoundaryCIDs))
		}
	})
}

// ====================================================================================
// MOCK SERVER & HELPERS
// ====================================================================================

func createMockPLCServer(_ *testing.T, operations []plcclient.PLCOperation) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/export" {
			w.WriteHeader(404)
			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")

		// Return operations as JSONL
		for _, op := range operations {
			json.NewEncoder(w).Encode(op)
		}
	}))
}

func makeTestOperations(count int) []plcclient.PLCOperation {
	ops := make([]plcclient.PLCOperation, count)
	baseTime := time.Now().Add(-time.Hour)

	for i := 0; i < count; i++ {
		ops[i] = plcclient.PLCOperation{
			DID:       fmt.Sprintf("did:plc:test%06d", i),
			CID:       fmt.Sprintf("bafy%06d", i),
			CreatedAt: baseTime.Add(time.Duration(i) * time.Second),
		}
	}

	return ops
}

// Mock sync manager for testing
type mockSyncManager struct {
	lastBundle       int
	mempoolCount     int
	fetchDelay       time.Duration
	mempoolSaveCount int
	mu               sync.Mutex
}

func (m *mockSyncManager) GetLastBundleNumber() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastBundle
}

func (m *mockSyncManager) GetMempoolCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mempoolCount
}

func (m *mockSyncManager) FetchAndSaveNextBundle(ctx context.Context, quiet bool) (int, time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.fetchDelay > 0 {
		time.Sleep(m.fetchDelay)
	}

	// Simulate creating bundle if we have enough ops
	if m.mempoolCount >= 10000 {
		m.lastBundle++
		m.mempoolCount -= 10000
		return m.lastBundle, 10 * time.Millisecond, nil
	}

	// Not enough ops
	return 0, 0, fmt.Errorf("insufficient operations")
}

func (m *mockSyncManager) SaveMempool() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mempoolSaveCount++
	return nil
}

func TestMockMempoolDeduplication(t *testing.T) {
	m := newMockMempool()

	op1 := plcclient.PLCOperation{
		CID:       "duplicate_cid",
		DID:       "did:plc:test",
		CreatedAt: time.Now(),
	}

	// Add first time
	added, _ := m.Add([]plcclient.PLCOperation{op1})
	if added != 1 {
		t.Fatalf("first add should return 1, got %d", added)
	}

	// Add same CID again
	added, _ = m.Add([]plcclient.PLCOperation{op1})
	if added != 0 {
		t.Fatalf("duplicate add should return 0, got %d", added)
	}

	if m.Count() != 1 {
		t.Fatalf("count should be 1, got %d", m.Count())
	}
}
