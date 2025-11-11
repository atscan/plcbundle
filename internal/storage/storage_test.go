package storage_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
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
// COMPRESSION TESTS
// ====================================================================================

func TestStorageCompression(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("RoundTripCompression", func(t *testing.T) {
		tests := []struct {
			name  string
			count int
		}{
			{"Empty", 0},
			{"Single", 1},
			{"Small", 10},
			{"Medium", 100},
			{"Large", 1000},
			{"FullBundle", 10000},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.count == 0 {
					return // Skip empty for now
				}

				original := makeTestOperations(tt.count)
				path := filepath.Join(tmpDir, tt.name+".jsonl.zst")

				// Save
				_, _, _, _, err := ops.SaveBundle(path, original, nil)
				if err != nil {
					t.Fatalf("SaveBundle failed: %v", err)
				}

				// Load
				loaded, err := ops.LoadBundle(path)
				if err != nil {
					t.Fatalf("LoadBundle failed: %v", err)
				}

				// Verify count
				if len(loaded) != len(original) {
					t.Errorf("count mismatch: got %d, want %d", len(loaded), len(original))
				}

				// Verify each operation
				for i := range original {
					if loaded[i].DID != original[i].DID {
						t.Errorf("op %d DID mismatch: got %s, want %s", i, loaded[i].DID, original[i].DID)
					}
					if loaded[i].CID != original[i].CID {
						t.Errorf("op %d CID mismatch: got %s, want %s", i, loaded[i].CID, original[i].CID)
					}
					if !loaded[i].CreatedAt.Equal(original[i].CreatedAt) {
						t.Errorf("op %d timestamp mismatch", i)
					}
				}
			})
		}
	})

	t.Run("CompressionRatio", func(t *testing.T) {
		operations := makeTestOperations(10000)
		path := filepath.Join(tmpDir, "compression_test.jsonl.zst")

		_, _, uncompSize, compSize, err := ops.SaveBundle(path, operations, nil)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		if compSize >= uncompSize {
			t.Errorf("compression failed: compressed=%d >= uncompressed=%d", compSize, uncompSize)
		}

		ratio := float64(uncompSize) / float64(compSize)
		if ratio < 2.0 {
			t.Errorf("poor compression ratio: %.2fx (expected > 2.0x)", ratio)
		}

		t.Logf("Compression ratio: %.2fx (%d → %d bytes)", ratio, uncompSize, compSize)
	})

	t.Run("CompressedDataIntegrity", func(t *testing.T) {
		operations := makeTestOperations(100)
		path := filepath.Join(tmpDir, "integrity_test.jsonl.zst")

		contentHash, compHash, _, _, err := ops.SaveBundle(path, operations, nil)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		// Recalculate hashes
		calcCompHash, _, calcContentHash, _, err := ops.CalculateFileHashes(path)
		if err != nil {
			t.Fatalf("CalculateFileHashes failed: %v", err)
		}

		if calcCompHash != compHash {
			t.Errorf("compressed hash mismatch: got %s, want %s", calcCompHash, compHash)
		}

		if calcContentHash != contentHash {
			t.Errorf("content hash mismatch: got %s, want %s", calcContentHash, contentHash)
		}
	})
}

// ====================================================================================
// HASHING TESTS - CRITICAL FOR CHAIN INTEGRITY
// ====================================================================================

func TestStorageHashing(t *testing.T) {
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("HashDeterminism", func(t *testing.T) {
		data := []byte("test data for hashing")

		// Calculate hash multiple times
		hashes := make([]string, 100)
		for i := 0; i < 100; i++ {
			hashes[i] = ops.Hash(data)
		}

		// All should be identical
		firstHash := hashes[0]
		for i, h := range hashes {
			if h != firstHash {
				t.Errorf("hash %d differs: got %s, want %s", i, h, firstHash)
			}
		}

		// Verify it's actually a valid SHA256 hex (64 chars)
		if len(firstHash) != 64 {
			t.Errorf("invalid hash length: got %d, want 64", len(firstHash))
		}
	})

	t.Run("ChainHashCalculation", func(t *testing.T) {
		contentHash := "abc123def456"

		// Genesis bundle (no parent)
		genesisHash := ops.CalculateChainHash("", contentHash)
		expectedGenesis := ops.Hash([]byte("plcbundle:genesis:" + contentHash))
		if genesisHash != expectedGenesis {
			t.Errorf("genesis hash mismatch: got %s, want %s", genesisHash, expectedGenesis)
		}

		// Second bundle (has parent)
		parentHash := genesisHash
		childHash := ops.CalculateChainHash(parentHash, contentHash)
		expectedChild := ops.Hash([]byte(parentHash + ":" + contentHash))
		if childHash != expectedChild {
			t.Errorf("child hash mismatch: got %s, want %s", childHash, expectedChild)
		}

		// Chain continues
		grandchildHash := ops.CalculateChainHash(childHash, contentHash)
		expectedGrandchild := ops.Hash([]byte(childHash + ":" + contentHash))
		if grandchildHash != expectedGrandchild {
			t.Errorf("grandchild hash mismatch")
		}
	})

	t.Run("HashSensitivity", func(t *testing.T) {
		// Small changes should produce completely different hashes
		data1 := []byte("test data")
		data2 := []byte("test datb")  // Changed one char
		data3 := []byte("test data ") // Added space

		hash1 := ops.Hash(data1)
		hash2 := ops.Hash(data2)
		hash3 := ops.Hash(data3)

		if hash1 == hash2 {
			t.Error("different data produced same hash (collision!)")
		}
		if hash1 == hash3 {
			t.Error("different data produced same hash (collision!)")
		}
	})

	t.Run("EmptyDataHash", func(t *testing.T) {
		hash := ops.Hash([]byte{})
		if len(hash) != 64 {
			t.Errorf("empty data hash invalid length: %d", len(hash))
		}
		// SHA256 of empty string is known constant
		// e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
		expected := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if hash != expected {
			t.Errorf("empty data hash mismatch: got %s, want %s", hash, expected)
		}
	})
}

// ====================================================================================
// CONCURRENCY TESTS - CRITICAL FOR PRODUCTION
// ====================================================================================

func TestStorageConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("ParallelBundleReads", func(t *testing.T) {
		// Create test bundle
		operations := makeTestOperations(10000)
		path := filepath.Join(tmpDir, "parallel_test.jsonl.zst")
		_, _, _, _, err := ops.SaveBundle(path, operations, nil)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		// Read from 100 goroutines simultaneously
		var wg sync.WaitGroup
		errors := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				loaded, err := ops.LoadBundle(path)
				if err != nil {
					errors <- err
					return
				}
				if len(loaded) != 10000 {
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

	t.Run("LoadOperationAtPositionConcurrency", func(t *testing.T) {
		// Critical test - this is heavily used by DID lookups
		operations := makeTestOperations(10000)
		path := filepath.Join(tmpDir, "position_test.jsonl.zst")
		_, _, _, _, err := ops.SaveBundle(path, operations, nil)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		// 200 concurrent position reads
		var wg sync.WaitGroup
		errors := make(chan error, 200)

		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func(position int) {
				defer wg.Done()
				op, err := ops.LoadOperationAtPosition(path, position%10000)
				if err != nil {
					errors <- err
					return
				}
				if op == nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("concurrent position read error: %v", err)
		}
	})

	t.Run("ConcurrentHashVerification", func(t *testing.T) {
		operations := makeTestOperations(1000)
		path := filepath.Join(tmpDir, "verify_test.jsonl.zst")
		_, compHash, _, _, err := ops.SaveBundle(path, operations, nil)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				valid, _, err := ops.VerifyHash(path, compHash)
				if err != nil {
					t.Errorf("VerifyHash failed: %v", err)
				}
				if !valid {
					t.Error("hash verification failed")
				}
			}()
		}
		wg.Wait()
	})
}

// ====================================================================================
// EDGE CASES & ERROR HANDLING
// ====================================================================================

func TestStorageEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("CorruptedZstdFile", func(t *testing.T) {
		path := filepath.Join(tmpDir, "corrupted.jsonl.zst")
		// Write invalid zstd data
		os.WriteFile(path, []byte("this is not valid zstd data"), 0644)

		_, err := ops.LoadBundle(path)
		if err == nil {
			t.Error("expected error loading corrupted file, got nil")
		}
	})

	t.Run("TruncatedFile", func(t *testing.T) {
		operations := makeTestOperations(100)
		path := filepath.Join(tmpDir, "truncated.jsonl.zst")
		ops.SaveBundle(path, operations, nil)

		// Read and truncate
		data, _ := os.ReadFile(path)
		os.WriteFile(path, data[:len(data)/2], 0644)

		_, err := ops.LoadBundle(path)
		if err == nil {
			t.Error("expected error loading truncated file, got nil")
		}
	})

	t.Run("InvalidJSONL", func(t *testing.T) {
		path := filepath.Join(tmpDir, "invalid.jsonl.zst")
		invalidData := []byte("{invalid json}\n{also invalid}")

		// Manually compress invalid data
		operations := makeTestOperations(10)
		ops.SaveBundle(path, operations, nil) // Create valid file first

		// Now corrupt it with invalid JSON
		// This is hard to test properly since SaveBundle enforces valid data
		// Better to test ParseJSONL directly
		_, err := ops.ParseJSONL(invalidData)
		if err == nil {
			t.Error("expected error parsing invalid JSONL, got nil")
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		_, err := ops.LoadBundle("/nonexistent/path/file.jsonl.zst")
		if err == nil {
			t.Error("expected error loading nonexistent file, got nil")
		}
	})

	t.Run("InvalidPosition", func(t *testing.T) {
		operations := makeTestOperations(100)
		path := filepath.Join(tmpDir, "position_test.jsonl.zst")
		ops.SaveBundle(path, operations, nil)

		// Negative position
		_, err := ops.LoadOperationAtPosition(path, -1)
		if err == nil {
			t.Error("expected error for negative position")
		}

		// Position beyond file
		_, err = ops.LoadOperationAtPosition(path, 10000)
		if err == nil {
			t.Error("expected error for position beyond file")
		}
	})
}

// ====================================================================================
// BOUNDARY CONDITIONS - CRITICAL FOR BUNDLE CHAINING
// ====================================================================================

func TestStorageBoundaryConditions(t *testing.T) {
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("GetBoundaryCIDs_SingleOperation", func(t *testing.T) {
		baseTime := time.Now()
		operations := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime},
		}

		boundaryTime, cids := ops.GetBoundaryCIDs(operations)

		if !boundaryTime.Equal(baseTime) {
			t.Error("boundary time mismatch")
		}
		if len(cids) != 1 {
			t.Errorf("expected 1 boundary CID, got %d", len(cids))
		}
		if !cids["cid1"] {
			t.Error("expected cid1 in boundary set")
		}
	})

	t.Run("GetBoundaryCIDs_MultipleSameTimestamp", func(t *testing.T) {
		// CRITICAL: Operations with identical timestamps (happens in real data)
		baseTime := time.Now()
		operations := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime.Add(-2 * time.Second)},
			{CID: "cid2", CreatedAt: baseTime.Add(-1 * time.Second)},
			{CID: "cid3", CreatedAt: baseTime}, // Last timestamp
			{CID: "cid4", CreatedAt: baseTime}, // Same as cid3
			{CID: "cid5", CreatedAt: baseTime}, // Same as cid3
		}

		boundaryTime, cids := ops.GetBoundaryCIDs(operations)

		if !boundaryTime.Equal(baseTime) {
			t.Error("boundary time should be last operation time")
		}

		// Should return ALL CIDs with the last timestamp
		if len(cids) != 3 {
			t.Errorf("expected 3 boundary CIDs, got %d", len(cids))
		}

		for _, expectedCID := range []string{"cid3", "cid4", "cid5"} {
			if !cids[expectedCID] {
				t.Errorf("expected %s in boundary set", expectedCID)
			}
		}

		// Earlier CIDs should NOT be in set
		if cids["cid1"] || cids["cid2"] {
			t.Error("earlier CIDs should not be in boundary set")
		}
	})

	t.Run("GetBoundaryCIDs_AllSameTimestamp", func(t *testing.T) {
		baseTime := time.Now()
		operations := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime},
			{CID: "cid2", CreatedAt: baseTime},
			{CID: "cid3", CreatedAt: baseTime},
		}

		_, cids := ops.GetBoundaryCIDs(operations)

		if len(cids) != 3 {
			t.Errorf("expected all 3 CIDs, got %d", len(cids))
		}
	})

	t.Run("GetBoundaryCIDs_EmptyOperations", func(t *testing.T) {
		operations := []plcclient.PLCOperation{}
		boundaryTime, cids := ops.GetBoundaryCIDs(operations)

		if !boundaryTime.IsZero() {
			t.Error("expected zero time for empty operations")
		}
		if len(cids) > 0 {
			t.Error("expected nil or empty CID set")
		}
	})

	t.Run("StripBoundaryDuplicates_ActualDuplication", func(t *testing.T) {
		// CRITICAL: This prevents duplicate operations across bundle boundaries
		baseTime := time.Now()
		boundaryTimestamp := baseTime.Format(time.RFC3339Nano)

		prevBoundaryCIDs := map[string]bool{
			"cid3": true,
			"cid4": true,
		}

		operations := []plcclient.PLCOperation{
			{CID: "cid3", CreatedAt: baseTime},                      // Duplicate - should be stripped
			{CID: "cid4", CreatedAt: baseTime},                      // Duplicate - should be stripped
			{CID: "cid5", CreatedAt: baseTime},                      // New - should be kept
			{CID: "cid6", CreatedAt: baseTime.Add(1 * time.Second)}, // After boundary - kept
		}

		result := ops.StripBoundaryDuplicates(operations, boundaryTimestamp, prevBoundaryCIDs)

		if len(result) != 2 {
			t.Errorf("expected 2 operations after stripping, got %d", len(result))
		}

		if result[0].CID != "cid5" {
			t.Errorf("expected cid5 first, got %s", result[0].CID)
		}
		if result[1].CID != "cid6" {
			t.Errorf("expected cid6 second, got %s", result[1].CID)
		}
	})

	t.Run("StripBoundaryDuplicates_NoDuplicates", func(t *testing.T) {
		baseTime := time.Now()
		boundaryTimestamp := baseTime.Format(time.RFC3339Nano)

		prevBoundaryCIDs := map[string]bool{
			"old_cid": true,
		}

		operations := []plcclient.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime.Add(1 * time.Second)},
			{CID: "cid2", CreatedAt: baseTime.Add(2 * time.Second)},
		}

		result := ops.StripBoundaryDuplicates(operations, boundaryTimestamp, prevBoundaryCIDs)

		if len(result) != 2 {
			t.Errorf("expected 2 operations, got %d", len(result))
		}
	})

	t.Run("StripBoundaryDuplicates_EmptyPrevious", func(t *testing.T) {
		baseTime := time.Now()
		operations := makeTestOperations(10)

		result := ops.StripBoundaryDuplicates(operations, baseTime.Format(time.RFC3339Nano), nil)

		if len(result) != len(operations) {
			t.Error("should not strip anything with no previous boundary CIDs")
		}
	})
}

// ====================================================================================
// SERIALIZATION TESTS
// ====================================================================================

func TestStorageSerialization(t *testing.T) {
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("SerializeJSONL_PreservesRawJSON", func(t *testing.T) {
		rawJSON := []byte(`{"did":"did:plc:test","cid":"bafytest","createdAt":"2024-01-01T00:00:00.000Z"}`)
		op := plcclient.PLCOperation{
			DID:       "did:plc:test",
			CID:       "bafytest",
			CreatedAt: time.Now(),
			RawJSON:   rawJSON,
		}

		result := ops.SerializeJSONL([]plcclient.PLCOperation{op})

		// Should use RawJSON directly
		if !containsBytes(result, rawJSON) {
			t.Error("SerializeJSONL did not preserve RawJSON")
		}
	})

	t.Run("SerializeJSONL_MarshalsFallback", func(t *testing.T) {
		op := plcclient.PLCOperation{
			DID:       "did:plc:test",
			CID:       "bafytest",
			CreatedAt: time.Now(),
			// No RawJSON - should marshal
		}

		result := ops.SerializeJSONL([]plcclient.PLCOperation{op})

		if len(result) == 0 {
			t.Error("SerializeJSONL returned empty result")
		}

		// Should contain the DID
		if !containsBytes(result, []byte("did:plc:test")) {
			t.Error("serialized data missing DID")
		}
	})

	t.Run("ParseJSONL_RoundTrip", func(t *testing.T) {
		original := makeTestOperations(100)
		data := ops.SerializeJSONL(original)

		parsed, err := ops.ParseJSONL(data)
		if err != nil {
			t.Fatalf("ParseJSONL failed: %v", err)
		}

		if len(parsed) != len(original) {
			t.Errorf("count mismatch: got %d, want %d", len(parsed), len(original))
		}

		// Verify RawJSON is populated
		for i, op := range parsed {
			if len(op.RawJSON) == 0 {
				t.Errorf("operation %d missing RawJSON", i)
			}
		}
	})
}

// ====================================================================================
// UTILITY FUNCTION TESTS
// ====================================================================================

func TestStorageUtilities(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("ExtractUniqueDIDs", func(t *testing.T) {
		operations := []plcclient.PLCOperation{
			{DID: "did:plc:aaa"},
			{DID: "did:plc:bbb"},
			{DID: "did:plc:aaa"}, // Duplicate
			{DID: "did:plc:ccc"},
			{DID: "did:plc:bbb"}, // Duplicate
			{DID: "did:plc:aaa"}, // Duplicate
		}

		dids := ops.ExtractUniqueDIDs(operations)

		if len(dids) != 3 {
			t.Errorf("expected 3 unique DIDs, got %d", len(dids))
		}

		// Verify all expected DIDs present
		didSet := make(map[string]bool)
		for _, did := range dids {
			didSet[did] = true
		}

		for _, expectedDID := range []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"} {
			if !didSet[expectedDID] {
				t.Errorf("missing expected DID: %s", expectedDID)
			}
		}
	})

	t.Run("ExtractUniqueDIDs_Empty", func(t *testing.T) {
		dids := ops.ExtractUniqueDIDs([]plcclient.PLCOperation{})
		if len(dids) != 0 {
			t.Error("expected empty result for empty input")
		}
	})

	t.Run("FileExists", func(t *testing.T) {
		existingFile := filepath.Join(tmpDir, "exists.txt")
		os.WriteFile(existingFile, []byte("test"), 0644)

		if !ops.FileExists(existingFile) {
			t.Error("FileExists returned false for existing file")
		}

		if ops.FileExists(filepath.Join(tmpDir, "nonexistent.txt")) {
			t.Error("FileExists returned true for nonexistent file")
		}
	})

	t.Run("GetFileSize", func(t *testing.T) {
		testFile := filepath.Join(tmpDir, "size_test.txt")
		testData := []byte("exactly 12 b")
		os.WriteFile(testFile, testData, 0644)

		size, err := ops.GetFileSize(testFile)
		if err != nil {
			t.Fatalf("GetFileSize failed: %v", err)
		}

		if size != int64(len(testData)) {
			t.Errorf("size mismatch: got %d, want %d", size, len(testData))
		}
	})
}

// ====================================================================================
// STREAMING TESTS
// ====================================================================================

func TestStorageStreaming(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}
	ops, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("StreamRaw", func(t *testing.T) {
		operations := makeTestOperations(100)
		path := filepath.Join(tmpDir, "stream_raw.jsonl.zst")
		_, _, _, _, err := ops.SaveBundle(path, operations, nil)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		reader, err := ops.StreamRaw(path)
		if err != nil {
			t.Fatalf("StreamRaw failed: %v", err)
		}
		defer reader.Close()

		// Read all data
		data := make([]byte, 1024*1024)
		n, err := reader.Read(data)
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("Read failed: %v", err)
		}

		if n == 0 {
			t.Error("StreamRaw returned no data")
		}
	})

	t.Run("StreamDecompressed", func(t *testing.T) {
		operations := makeTestOperations(100)
		path := filepath.Join(tmpDir, "stream_decomp.jsonl.zst")
		ops.SaveBundle(path, operations, nil)

		reader, err := ops.StreamDecompressed(path)
		if err != nil {
			t.Fatalf("StreamDecompressed failed: %v", err)
		}
		defer reader.Close()

		// Count JSONL lines
		scanner := bufio.NewScanner(reader)
		lineCount := 0
		for scanner.Scan() {
			lineCount++
		}

		if lineCount != 100 {
			t.Errorf("expected 100 lines, got %d", lineCount)
		}
	})
}

// ====================================================================================
// PERFORMANCE / BENCHMARK TESTS
// ====================================================================================

func BenchmarkStorageOperations(b *testing.B) {
	tmpDir := b.TempDir()
	logger := &testLogger{t: &testing.T{}}
	ops, _ := storage.NewOperations(logger)
	defer ops.Close()

	operations := makeTestOperations(10000)

	b.Run("SaveBundle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.jsonl.zst", i))
			ops.SaveBundle(path, operations, nil)
		}
	})

	// Create bundle for read benchmarks
	testPath := filepath.Join(tmpDir, "bench_read.jsonl.zst")
	ops.SaveBundle(testPath, operations, nil)

	b.Run("LoadBundle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ops.LoadBundle(testPath)
		}
	})

	b.Run("LoadOperationAtPosition", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ops.LoadOperationAtPosition(testPath, i%10000)
		}
	})

	b.Run("Hash", func(b *testing.B) {
		data := ops.SerializeJSONL(operations)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ops.Hash(data)
		}
	})

	b.Run("SerializeJSONL", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ops.SerializeJSONL(operations)
		}
	})
}

// ====================================================================================
// HELPER FUNCTIONS
// ====================================================================================

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

func containsBytes(haystack, needle []byte) bool {
	return bytes.Contains(haystack, needle)
}
