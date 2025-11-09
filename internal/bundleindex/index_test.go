package bundleindex_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
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
// INDEX CREATION & BASIC OPERATIONS
// ====================================================================================

func TestIndexCreation(t *testing.T) {
	t.Run("NewIndex", func(t *testing.T) {
		idx := bundleindex.NewIndex("https://plc.directory")

		if idx == nil {
			t.Fatal("NewIndex returned nil")
		}

		if idx.Version != types.INDEX_VERSION {
			t.Errorf("version mismatch: got %s, want %s", idx.Version, types.INDEX_VERSION)
		}

		if idx.Origin != "https://plc.directory" {
			t.Errorf("origin mismatch: got %s", idx.Origin)
		}

		if idx.Count() != 0 {
			t.Error("new index should be empty")
		}
	})

	t.Run("NewIndex_EmptyOrigin", func(t *testing.T) {
		idx := bundleindex.NewIndex("")

		if idx.Origin != "" {
			t.Error("should allow empty origin")
		}
	})
}

func TestIndexAddBundle(t *testing.T) {
	t.Run("AddSingleBundle", func(t *testing.T) {
		idx := bundleindex.NewIndex("test-origin")

		meta := &bundleindex.BundleMetadata{
			BundleNumber:     1,
			StartTime:        time.Now(),
			EndTime:          time.Now().Add(time.Hour),
			OperationCount:   types.BUNDLE_SIZE,
			DIDCount:         1000,
			Hash:             "hash123",
			ContentHash:      "content123",
			CompressedHash:   "compressed123",
			CompressedSize:   1024,
			UncompressedSize: 5120,
		}

		idx.AddBundle(meta)

		if idx.Count() != 1 {
			t.Errorf("count should be 1, got %d", idx.Count())
		}

		retrieved, err := idx.GetBundle(1)
		if err != nil {
			t.Fatalf("GetBundle failed: %v", err)
		}

		if retrieved.Hash != "hash123" {
			t.Error("hash mismatch after retrieval")
		}
	})

	t.Run("AddMultipleBundles_AutoSort", func(t *testing.T) {
		idx := bundleindex.NewIndex("test-origin")

		// Add bundles out of order: 3, 1, 2
		for _, num := range []int{3, 1, 2} {
			meta := &bundleindex.BundleMetadata{
				BundleNumber:   num,
				StartTime:      time.Now(),
				EndTime:        time.Now().Add(time.Hour),
				OperationCount: types.BUNDLE_SIZE,
			}
			idx.AddBundle(meta)
		}

		bundles := idx.GetBundles()

		// Should be sorted: 1, 2, 3
		if bundles[0].BundleNumber != 1 {
			t.Error("bundles not sorted")
		}
		if bundles[1].BundleNumber != 2 {
			t.Error("bundles not sorted")
		}
		if bundles[2].BundleNumber != 3 {
			t.Error("bundles not sorted")
		}
	})

	t.Run("UpdateExistingBundle", func(t *testing.T) {
		idx := bundleindex.NewIndex("test-origin")

		original := &bundleindex.BundleMetadata{
			BundleNumber:   1,
			Hash:           "original_hash",
			StartTime:      time.Now(),
			EndTime:        time.Now().Add(time.Hour),
			OperationCount: types.BUNDLE_SIZE,
		}

		idx.AddBundle(original)

		// Add again with different hash (update)
		updated := &bundleindex.BundleMetadata{
			BundleNumber:   1,
			Hash:           "updated_hash",
			StartTime:      time.Now(),
			EndTime:        time.Now().Add(time.Hour),
			OperationCount: types.BUNDLE_SIZE,
		}

		idx.AddBundle(updated)

		// Should have only 1 bundle (updated, not duplicated)
		if idx.Count() != 1 {
			t.Errorf("should have 1 bundle after update, got %d", idx.Count())
		}

		retrieved, _ := idx.GetBundle(1)
		if retrieved.Hash != "updated_hash" {
			t.Error("bundle was not updated")
		}
	})
}

// ====================================================================================
// SAVE & LOAD TESTS
// ====================================================================================

func TestIndexPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("SaveAndLoad", func(t *testing.T) {
		indexPath := filepath.Join(tmpDir, "test_index.json")

		// Create and populate index
		idx := bundleindex.NewIndex("https://plc.directory")

		for i := 1; i <= 5; i++ {
			meta := &bundleindex.BundleMetadata{
				BundleNumber:     i,
				StartTime:        time.Now().Add(time.Duration(i-1) * time.Hour),
				EndTime:          time.Now().Add(time.Duration(i) * time.Hour),
				OperationCount:   types.BUNDLE_SIZE,
				DIDCount:         1000 * i,
				Hash:             fmt.Sprintf("hash%d", i),
				ContentHash:      fmt.Sprintf("content%d", i),
				CompressedHash:   fmt.Sprintf("compressed%d", i),
				CompressedSize:   int64(1024 * i),
				UncompressedSize: int64(5120 * i),
			}
			idx.AddBundle(meta)
		}

		// Save
		if err := idx.Save(indexPath); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		// Verify file exists
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			t.Fatal("index file not created")
		}

		// Load
		loaded, err := bundleindex.LoadIndex(indexPath)
		if err != nil {
			t.Fatalf("LoadIndex failed: %v", err)
		}

		// Verify data integrity
		if loaded.Count() != 5 {
			t.Errorf("loaded count mismatch: got %d, want 5", loaded.Count())
		}

		if loaded.Origin != "https://plc.directory" {
			t.Error("origin not preserved")
		}

		if loaded.LastBundle != 5 {
			t.Error("LastBundle not calculated correctly")
		}

		// Verify specific bundle
		bundle3, err := loaded.GetBundle(3)
		if err != nil {
			t.Fatalf("GetBundle(3) failed: %v", err)
		}

		if bundle3.Hash != "hash3" {
			t.Error("bundle data not preserved")
		}
	})

	t.Run("AtomicSave", func(t *testing.T) {
		indexPath := filepath.Join(tmpDir, "atomic_test.json")

		idx := bundleindex.NewIndex("test")
		idx.AddBundle(&bundleindex.BundleMetadata{
			BundleNumber:   1,
			StartTime:      time.Now(),
			EndTime:        time.Now(),
			OperationCount: types.BUNDLE_SIZE,
		})

		idx.Save(indexPath)

		// Verify no .tmp file left behind
		tmpPath := indexPath + ".tmp"
		if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
			t.Error("temporary file should not exist after successful save")
		}
	})

	t.Run("LoadInvalidVersion", func(t *testing.T) {
		indexPath := filepath.Join(tmpDir, "invalid_version.json")

		// Write index with wrong version
		invalidData := `{"version":"99.99","origin":"test","bundles":[]}`
		os.WriteFile(indexPath, []byte(invalidData), 0644)

		_, err := bundleindex.LoadIndex(indexPath)
		if err == nil {
			t.Error("should reject index with invalid version")
		}
	})

	t.Run("LoadCorruptedJSON", func(t *testing.T) {
		indexPath := filepath.Join(tmpDir, "corrupted.json")

		os.WriteFile(indexPath, []byte("{invalid json"), 0644)

		_, err := bundleindex.LoadIndex(indexPath)
		if err == nil {
			t.Error("should reject corrupted JSON")
		}
	})
}

// ====================================================================================
// QUERY OPERATIONS
// ====================================================================================

func TestIndexQueries(t *testing.T) {
	idx := bundleindex.NewIndex("test")

	// Populate with bundles
	for i := 1; i <= 10; i++ {
		meta := &bundleindex.BundleMetadata{
			BundleNumber:   i,
			StartTime:      time.Now().Add(time.Duration(i-1) * time.Hour),
			EndTime:        time.Now().Add(time.Duration(i) * time.Hour),
			OperationCount: types.BUNDLE_SIZE,
			CompressedSize: int64(i * 1000),
		}
		idx.AddBundle(meta)
	}

	t.Run("GetBundle", func(t *testing.T) {
		meta, err := idx.GetBundle(5)
		if err != nil {
			t.Fatalf("GetBundle failed: %v", err)
		}

		if meta.BundleNumber != 5 {
			t.Error("wrong bundle returned")
		}
	})

	t.Run("GetBundle_NotFound", func(t *testing.T) {
		_, err := idx.GetBundle(999)
		if err == nil {
			t.Error("should return error for nonexistent bundle")
		}
	})

	t.Run("GetLastBundle", func(t *testing.T) {
		last := idx.GetLastBundle()

		if last == nil {
			t.Fatal("GetLastBundle returned nil")
		}

		if last.BundleNumber != 10 {
			t.Errorf("last bundle should be 10, got %d", last.BundleNumber)
		}
	})

	t.Run("GetLastBundle_Empty", func(t *testing.T) {
		emptyIdx := bundleindex.NewIndex("test")

		last := emptyIdx.GetLastBundle()

		if last != nil {
			t.Error("empty index should return nil for GetLastBundle")
		}
	})

	t.Run("GetBundleRange", func(t *testing.T) {
		bundles := idx.GetBundleRange(3, 7)

		if len(bundles) != 5 {
			t.Errorf("expected 5 bundles, got %d", len(bundles))
		}

		if bundles[0].BundleNumber != 3 || bundles[4].BundleNumber != 7 {
			t.Error("range boundaries incorrect")
		}
	})

	t.Run("GetBundleRange_OutOfBounds", func(t *testing.T) {
		bundles := idx.GetBundleRange(100, 200)

		if len(bundles) != 0 {
			t.Errorf("expected 0 bundles for out-of-range query, got %d", len(bundles))
		}
	})

	t.Run("GetBundles_ReturnsShallowCopy", func(t *testing.T) {
		bundles1 := idx.GetBundles()
		bundles2 := idx.GetBundles()

		// Should be different slices
		if &bundles1[0] == &bundles2[0] {
			t.Error("GetBundles should return copy, not same slice")
		}

		// But same data
		if bundles1[0].BundleNumber != bundles2[0].BundleNumber {
			t.Error("bundle data should be same")
		}
	})
}

// ====================================================================================
// GAP DETECTION - CRITICAL FOR INTEGRITY
// ====================================================================================

func TestIndexFindGaps(t *testing.T) {
	t.Run("NoGaps", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		for i := 1; i <= 10; i++ {
			idx.AddBundle(createTestMetadata(i))
		}

		gaps := idx.FindGaps()

		if len(gaps) != 0 {
			t.Errorf("expected no gaps, found %d: %v", len(gaps), gaps)
		}
	})

	t.Run("SingleGap", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		// Add bundles 1, 2, 4, 5 (missing 3)
		for _, num := range []int{1, 2, 4, 5} {
			idx.AddBundle(createTestMetadata(num))
		}

		gaps := idx.FindGaps()

		if len(gaps) != 1 {
			t.Errorf("expected 1 gap, got %d", len(gaps))
		}

		if len(gaps) > 0 && gaps[0] != 3 {
			t.Errorf("expected gap at 3, got %d", gaps[0])
		}
	})

	t.Run("MultipleGaps", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		// Add bundles 1, 2, 5, 6, 9, 10 (missing 3, 4, 7, 8)
		for _, num := range []int{1, 2, 5, 6, 9, 10} {
			idx.AddBundle(createTestMetadata(num))
		}

		gaps := idx.FindGaps()

		expectedGaps := []int{3, 4, 7, 8}
		if len(gaps) != len(expectedGaps) {
			t.Errorf("expected %d gaps, got %d", len(expectedGaps), len(gaps))
		}

		for i, expected := range expectedGaps {
			if gaps[i] != expected {
				t.Errorf("gap %d: got %d, want %d", i, gaps[i], expected)
			}
		}
	})

	t.Run("FindGaps_EmptyIndex", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		gaps := idx.FindGaps()

		if len(gaps) > 0 {
			t.Error("empty index should have no gaps")
		}
	})

	t.Run("FindGaps_NonSequentialStart", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		// Start at bundle 100
		for i := 100; i <= 105; i++ {
			idx.AddBundle(createTestMetadata(i))
		}

		gaps := idx.FindGaps()

		// No gaps between 100-105
		if len(gaps) != 0 {
			t.Errorf("expected no gaps, got %d", len(gaps))
		}
	})
}

// ====================================================================================
// STATISTICS & DERIVED FIELDS
// ====================================================================================

func TestIndexStatistics(t *testing.T) {
	idx := bundleindex.NewIndex("test")

	t.Run("StatsEmpty", func(t *testing.T) {
		stats := idx.GetStats()

		if stats["bundle_count"].(int) != 0 {
			t.Error("empty index should have count 0")
		}
	})

	t.Run("StatsPopulated", func(t *testing.T) {
		totalSize := int64(0)
		totalUncompressed := int64(0)

		for i := 1; i <= 5; i++ {
			meta := &bundleindex.BundleMetadata{
				BundleNumber:     i,
				StartTime:        time.Now().Add(time.Duration(i-1) * time.Hour),
				EndTime:          time.Now().Add(time.Duration(i) * time.Hour),
				OperationCount:   types.BUNDLE_SIZE,
				CompressedSize:   int64(1000 * i),
				UncompressedSize: int64(5000 * i),
			}
			idx.AddBundle(meta)
			totalSize += meta.CompressedSize
			totalUncompressed += meta.UncompressedSize
		}

		stats := idx.GetStats()

		if stats["bundle_count"].(int) != 5 {
			t.Error("bundle count mismatch")
		}

		if stats["first_bundle"].(int) != 1 {
			t.Error("first_bundle mismatch")
		}

		if stats["last_bundle"].(int) != 5 {
			t.Error("last_bundle mismatch")
		}

		if stats["total_size"].(int64) != totalSize {
			t.Errorf("total_size mismatch: got %d, want %d", stats["total_size"].(int64), totalSize)
		}

		if stats["total_uncompressed_size"].(int64) != totalUncompressed {
			t.Error("total_uncompressed_size mismatch")
		}

		if _, ok := stats["start_time"]; !ok {
			t.Error("stats missing start_time")
		}

		if _, ok := stats["end_time"]; !ok {
			t.Error("stats missing end_time")
		}

		if stats["gaps"].(int) != 0 {
			t.Error("should have no gaps")
		}
	})

	t.Run("StatsRecalculateAfterAdd", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		idx.AddBundle(&bundleindex.BundleMetadata{
			BundleNumber:   1,
			StartTime:      time.Now(),
			EndTime:        time.Now(),
			OperationCount: types.BUNDLE_SIZE,
			CompressedSize: 1000,
		})

		stats1 := idx.GetStats()
		size1 := stats1["total_size"].(int64)

		// Add another bundle
		idx.AddBundle(&bundleindex.BundleMetadata{
			BundleNumber:   2,
			StartTime:      time.Now(),
			EndTime:        time.Now(),
			OperationCount: types.BUNDLE_SIZE,
			CompressedSize: 2000,
		})

		stats2 := idx.GetStats()
		size2 := stats2["total_size"].(int64)

		if size2 != size1+2000 {
			t.Errorf("total_size not recalculated: got %d, want %d", size2, size1+2000)
		}

		if stats2["last_bundle"].(int) != 2 {
			t.Error("last_bundle not recalculated")
		}
	})
}

// ====================================================================================
// REBUILD OPERATION
// ====================================================================================

func TestIndexRebuild(t *testing.T) {
	t.Run("RebuildFromMetadata", func(t *testing.T) {
		idx := bundleindex.NewIndex("original")

		// Add some bundles
		for i := 1; i <= 3; i++ {
			idx.AddBundle(createTestMetadata(i))
		}

		if idx.Count() != 3 {
			t.Fatal("setup failed")
		}

		// Create new metadata for rebuild
		newMetadata := []*bundleindex.BundleMetadata{
			createTestMetadata(1),
			createTestMetadata(2),
			createTestMetadata(5),
			createTestMetadata(6),
		}

		// Rebuild
		idx.Rebuild(newMetadata)

		// Should now have 4 bundles
		if idx.Count() != 4 {
			t.Errorf("after rebuild, expected 4 bundles, got %d", idx.Count())
		}

		// Should have new bundles 5, 6
		if _, err := idx.GetBundle(5); err != nil {
			t.Error("should have bundle 5 after rebuild")
		}

		// Should not have bundle 3
		if _, err := idx.GetBundle(3); err == nil {
			t.Error("should not have bundle 3 after rebuild")
		}

		// Origin should be preserved
		if idx.Origin != "original" {
			t.Error("origin should be preserved during rebuild")
		}
	})

	t.Run("RebuildAutoSorts", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		// Rebuild with unsorted data
		unsorted := []*bundleindex.BundleMetadata{
			createTestMetadata(5),
			createTestMetadata(2),
			createTestMetadata(8),
			createTestMetadata(1),
		}

		idx.Rebuild(unsorted)

		bundles := idx.GetBundles()

		// Should be sorted
		for i := 0; i < len(bundles)-1; i++ {
			if bundles[i].BundleNumber >= bundles[i+1].BundleNumber {
				t.Error("bundles not sorted after rebuild")
			}
		}
	})
}

// ====================================================================================
// CLEAR OPERATION
// ====================================================================================

func TestIndexClear(t *testing.T) {
	idx := bundleindex.NewIndex("test")

	// Populate
	for i := 1; i <= 10; i++ {
		idx.AddBundle(createTestMetadata(i))
	}

	if idx.Count() != 10 {
		t.Fatal("setup failed")
	}

	// Clear
	idx.Clear()

	if idx.Count() != 0 {
		t.Error("count should be 0 after clear")
	}

	if idx.LastBundle != 0 {
		t.Error("LastBundle should be 0 after clear")
	}

	if idx.TotalSize != 0 {
		t.Error("TotalSize should be 0 after clear")
	}

	// Should be able to add after clear
	idx.AddBundle(createTestMetadata(1))

	if idx.Count() != 1 {
		t.Error("should be able to add after clear")
	}
}

// ====================================================================================
// CONCURRENCY TESTS
// ====================================================================================

func TestIndexConcurrency(t *testing.T) {
	t.Run("ConcurrentReads", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		// Populate
		for i := 1; i <= 100; i++ {
			idx.AddBundle(createTestMetadata(i))
		}

		// 100 concurrent readers
		var wg sync.WaitGroup
		errors := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Various read operations
				idx.Count()
				idx.GetLastBundle()
				idx.GetBundles()
				idx.FindGaps()
				idx.GetStats()

				if _, err := idx.GetBundle(id%100 + 1); err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("concurrent read error: %v", err)
		}
	})

	t.Run("ConcurrentReadsDuringSave", func(t *testing.T) {
		tmpDir := t.TempDir()
		indexPath := filepath.Join(tmpDir, "concurrent.json")

		idx := bundleindex.NewIndex("test")

		for i := 1; i <= 50; i++ {
			idx.AddBundle(createTestMetadata(i))
		}

		var wg sync.WaitGroup

		// Saver goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				idx.Save(indexPath)
				time.Sleep(10 * time.Millisecond)
			}
		}()

		// Reader goroutines
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					idx.Count()
					idx.GetBundles()
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		wg.Wait()
	})
}

// ====================================================================================
// REMOTE UPDATE TESTS (FOR CLONING)
// ====================================================================================

func TestIndexUpdateFromRemote(t *testing.T) {

	t.Run("UpdateFromRemote_Basic", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		// Local has bundles 1-3
		for i := 1; i <= 3; i++ {
			idx.AddBundle(createTestMetadata(i))
		}

		// Remote has bundles 1-5
		remoteMeta := make(map[int]*bundleindex.BundleMetadata)
		for i := 1; i <= 5; i++ {
			remoteMeta[i] = createTestMetadata(i)
		}

		bundlesToUpdate := []int{4, 5}

		// Mock file existence (4 and 5 exist)
		fileExists := func(bundleNum int) bool {
			return bundleNum == 4 || bundleNum == 5
		}

		logger := &testLogger{t: &testing.T{}}

		err := idx.UpdateFromRemote(bundlesToUpdate, remoteMeta, fileExists, false, logger)
		if err != nil {
			t.Fatalf("UpdateFromRemote failed: %v", err)
		}

		// Should now have 5 bundles
		if idx.Count() != 5 {
			t.Errorf("expected 5 bundles after update, got %d", idx.Count())
		}
	})

	t.Run("UpdateFromRemote_SkipsMissingFiles", func(t *testing.T) {
		idx := bundleindex.NewIndex("test")

		remoteMeta := map[int]*bundleindex.BundleMetadata{
			1: createTestMetadata(1),
			2: createTestMetadata(2),
		}

		bundlesToUpdate := []int{1, 2}

		// Only bundle 1 exists locally
		fileExists := func(bundleNum int) bool {
			return bundleNum == 1
		}

		logger := &testLogger{t: &testing.T{}}

		err := idx.UpdateFromRemote(bundlesToUpdate, remoteMeta, fileExists, false, logger)
		if err != nil {
			t.Fatalf("UpdateFromRemote failed: %v", err)
		}

		// Should only have bundle 1
		if idx.Count() != 1 {
			t.Errorf("expected 1 bundle, got %d", idx.Count())
		}

		if _, err := idx.GetBundle(2); err == nil {
			t.Error("should not have bundle 2 (file missing)")
		}
	})
}

// ====================================================================================
// HELPER FUNCTIONS
// ====================================================================================

func createTestMetadata(bundleNum int) *bundleindex.BundleMetadata {
	return &bundleindex.BundleMetadata{
		BundleNumber:     bundleNum,
		StartTime:        time.Now().Add(time.Duration(bundleNum-1) * time.Hour),
		EndTime:          time.Now().Add(time.Duration(bundleNum) * time.Hour),
		OperationCount:   types.BUNDLE_SIZE,
		DIDCount:         1000,
		Hash:             fmt.Sprintf("hash%d", bundleNum),
		ContentHash:      fmt.Sprintf("content%d", bundleNum),
		Parent:           fmt.Sprintf("parent%d", bundleNum-1),
		CompressedHash:   fmt.Sprintf("compressed%d", bundleNum),
		CompressedSize:   int64(1000 * bundleNum),
		UncompressedSize: int64(5000 * bundleNum),
		CreatedAt:        time.Now(),
	}
}
