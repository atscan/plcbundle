package bundle_test

import (
	"path/filepath"
	"testing"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/plc"
)

// TestIndex tests index operations
func TestIndex(t *testing.T) {
	t.Run("CreateNewIndex", func(t *testing.T) {
		idx := bundle.NewIndex()
		if idx == nil {
			t.Fatal("NewIndex returned nil")
		}
		if idx.Version != bundle.INDEX_VERSION {
			t.Errorf("expected version %s, got %s", bundle.INDEX_VERSION, idx.Version)
		}
		if idx.Count() != 0 {
			t.Errorf("expected empty index, got count %d", idx.Count())
		}
	})

	t.Run("AddBundle", func(t *testing.T) {
		idx := bundle.NewIndex()
		meta := &bundle.BundleMetadata{
			BundleNumber:   1,
			StartTime:      time.Now(),
			EndTime:        time.Now().Add(time.Hour),
			OperationCount: bundle.BUNDLE_SIZE,
			DIDCount:       1000,
			Hash:           "abc123",
			CompressedHash: "def456",
		}

		idx.AddBundle(meta)

		if idx.Count() != 1 {
			t.Errorf("expected count 1, got %d", idx.Count())
		}

		retrieved, err := idx.GetBundle(1)
		if err != nil {
			t.Fatalf("GetBundle failed: %v", err)
		}
		if retrieved.Hash != meta.Hash {
			t.Errorf("expected hash %s, got %s", meta.Hash, retrieved.Hash)
		}
	})

	t.Run("SaveAndLoad", func(t *testing.T) {
		tmpDir := t.TempDir()
		indexPath := filepath.Join(tmpDir, "test_index.json")

		// Create and save
		idx := bundle.NewIndex()
		idx.AddBundle(&bundle.BundleMetadata{
			BundleNumber:   1,
			StartTime:      time.Now(),
			EndTime:        time.Now().Add(time.Hour),
			OperationCount: bundle.BUNDLE_SIZE,
			Hash:           "test123",
		})

		if err := idx.Save(indexPath); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		// Load
		loaded, err := bundle.LoadIndex(indexPath)
		if err != nil {
			t.Fatalf("LoadIndex failed: %v", err)
		}

		if loaded.Count() != 1 {
			t.Errorf("expected count 1, got %d", loaded.Count())
		}
	})

	t.Run("GetBundleRange", func(t *testing.T) {
		idx := bundle.NewIndex()
		for i := 1; i <= 5; i++ {
			idx.AddBundle(&bundle.BundleMetadata{
				BundleNumber:   i,
				StartTime:      time.Now(),
				EndTime:        time.Now().Add(time.Hour),
				OperationCount: bundle.BUNDLE_SIZE,
			})
		}

		bundles := idx.GetBundleRange(2, 4)
		if len(bundles) != 3 {
			t.Errorf("expected 3 bundles, got %d", len(bundles))
		}
		if bundles[0].BundleNumber != 2 || bundles[2].BundleNumber != 4 {
			t.Errorf("unexpected bundle range")
		}
	})

	t.Run("FindGaps", func(t *testing.T) {
		idx := bundle.NewIndex()
		// Add bundles 1, 2, 4, 5 (missing 3)
		for _, num := range []int{1, 2, 4, 5} {
			idx.AddBundle(&bundle.BundleMetadata{
				BundleNumber:   num,
				StartTime:      time.Now(),
				EndTime:        time.Now().Add(time.Hour),
				OperationCount: bundle.BUNDLE_SIZE,
			})
		}

		gaps := idx.FindGaps()
		if len(gaps) != 1 {
			t.Errorf("expected 1 gap, got %d", len(gaps))
		}
		if len(gaps) > 0 && gaps[0] != 3 {
			t.Errorf("expected gap at 3, got %d", gaps[0])
		}
	})
}

// TestBundle tests bundle operations
func TestBundle(t *testing.T) {
	t.Run("ValidateForSave", func(t *testing.T) {
		tests := []struct {
			name    string
			bundle  *bundle.Bundle
			wantErr bool
		}{
			{
				name: "valid bundle",
				bundle: &bundle.Bundle{
					BundleNumber: 1,
					StartTime:    time.Now(),
					EndTime:      time.Now().Add(time.Hour),
					Operations:   makeTestOperations(bundle.BUNDLE_SIZE),
				},
				wantErr: false,
			},
			{
				name: "invalid bundle number",
				bundle: &bundle.Bundle{
					BundleNumber: 0,
					Operations:   makeTestOperations(bundle.BUNDLE_SIZE),
				},
				wantErr: true,
			},
			{
				name: "wrong operation count",
				bundle: &bundle.Bundle{
					BundleNumber: 1,
					Operations:   makeTestOperations(100),
				},
				wantErr: true,
			},
			{
				name: "start after end",
				bundle: &bundle.Bundle{
					BundleNumber: 1,
					StartTime:    time.Now().Add(time.Hour),
					EndTime:      time.Now(),
					Operations:   makeTestOperations(bundle.BUNDLE_SIZE),
				},
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := tt.bundle.ValidateForSave()
				if (err != nil) != tt.wantErr {
					t.Errorf("ValidateForSave() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("CompressionRatio", func(t *testing.T) {
		b := &bundle.Bundle{
			CompressedSize:   1000,
			UncompressedSize: 5000,
		}
		ratio := b.CompressionRatio()
		if ratio != 5.0 {
			t.Errorf("expected ratio 5.0, got %f", ratio)
		}
	})
}

// TestMempool tests mempool operations
func TestMempool(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}

	t.Run("CreateAndAdd", func(t *testing.T) {
		minTime := time.Now().Add(-time.Hour)
		m, err := bundle.NewMempool(tmpDir, 1, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(100)
		added, err := m.Add(ops)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if added != 100 {
			t.Errorf("expected 100 added, got %d", added)
		}
		if m.Count() != 100 {
			t.Errorf("expected count 100, got %d", m.Count())
		}
	})

	t.Run("ChronologicalValidation", func(t *testing.T) {
		minTime := time.Now().Add(-time.Hour)
		m, err := bundle.NewMempool(tmpDir, 2, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		// Add operations in order
		ops := makeTestOperations(10)
		_, err = m.Add(ops)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		// Try to add operation before last one (should fail)
		oldOp := []plc.PLCOperation{
			{
				DID:       "did:plc:old",
				CID:       "old123",
				CreatedAt: time.Now().Add(-2 * time.Hour),
			},
		}
		_, err = m.Add(oldOp)
		if err == nil {
			t.Error("expected chronological validation error")
		}
	})

	t.Run("TakeOperations", func(t *testing.T) {
		minTime := time.Now().Add(-time.Hour)
		m, err := bundle.NewMempool(tmpDir, 3, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(100)
		m.Add(ops)

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

	t.Run("SaveAndLoad", func(t *testing.T) {
		minTime := time.Now().Add(-time.Hour)
		m, err := bundle.NewMempool(tmpDir, 4, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(50)
		m.Add(ops)

		if err := m.Save(); err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		// Create new mempool and load
		m2, err := bundle.NewMempool(tmpDir, 4, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		if m2.Count() != 50 {
			t.Errorf("expected 50 operations after load, got %d", m2.Count())
		}
	})

	t.Run("Validate", func(t *testing.T) {
		minTime := time.Now().Add(-time.Hour)
		m, err := bundle.NewMempool(tmpDir, 5, minTime, logger)
		if err != nil {
			t.Fatalf("NewMempool failed: %v", err)
		}

		ops := makeTestOperations(10)
		m.Add(ops)

		if err := m.Validate(); err != nil {
			t.Errorf("Validate failed: %v", err)
		}
	})
}

// TestOperations tests low-level operations
func TestOperations(t *testing.T) {
	tmpDir := t.TempDir()
	logger := &testLogger{t: t}

	ops, err := bundle.NewOperations(logger)
	if err != nil {
		t.Fatalf("NewOperations failed: %v", err)
	}
	defer ops.Close()

	t.Run("SerializeJSONL", func(t *testing.T) {
		operations := makeTestOperations(10)
		data := ops.SerializeJSONL(operations)
		if len(data) == 0 {
			t.Error("SerializeJSONL returned empty data")
		}
	})

	t.Run("Hash", func(t *testing.T) {
		data := []byte("test data")
		hash := ops.Hash(data)
		if len(hash) != 64 { // SHA256 hex = 64 chars
			t.Errorf("expected hash length 64, got %d", len(hash))
		}

		// Same data should produce same hash
		hash2 := ops.Hash(data)
		if hash != hash2 {
			t.Error("same data produced different hashes")
		}
	})

	t.Run("SaveAndLoadBundle", func(t *testing.T) {
		operations := makeTestOperations(bundle.BUNDLE_SIZE)
		path := filepath.Join(tmpDir, "test_bundle.jsonl.zst")

		// Save
		uncompHash, compHash, uncompSize, compSize, err := ops.SaveBundle(path, operations)
		if err != nil {
			t.Fatalf("SaveBundle failed: %v", err)
		}

		if uncompHash == "" || compHash == "" {
			t.Error("empty hashes returned")
		}
		if uncompSize == 0 || compSize == 0 {
			t.Error("zero sizes returned")
		}
		if compSize >= uncompSize {
			t.Error("compressed size should be smaller than uncompressed")
		}

		// Load
		loaded, err := ops.LoadBundle(path)
		if err != nil {
			t.Fatalf("LoadBundle failed: %v", err)
		}

		if len(loaded) != len(operations) {
			t.Errorf("expected %d operations, got %d", len(operations), len(loaded))
		}
	})

	t.Run("ExtractUniqueDIDs", func(t *testing.T) {
		operations := []plc.PLCOperation{
			{DID: "did:plc:1"},
			{DID: "did:plc:2"},
			{DID: "did:plc:1"}, // duplicate
			{DID: "did:plc:3"},
		}

		dids := ops.ExtractUniqueDIDs(operations)
		if len(dids) != 3 {
			t.Errorf("expected 3 unique DIDs, got %d", len(dids))
		}
	})

	t.Run("GetBoundaryCIDs", func(t *testing.T) {
		baseTime := time.Now()
		operations := []plc.PLCOperation{
			{CID: "cid1", CreatedAt: baseTime},
			{CID: "cid2", CreatedAt: baseTime.Add(time.Second)},
			{CID: "cid3", CreatedAt: baseTime.Add(2 * time.Second)},
			{CID: "cid4", CreatedAt: baseTime.Add(2 * time.Second)}, // same as cid3
			{CID: "cid5", CreatedAt: baseTime.Add(2 * time.Second)}, // same as cid3
		}

		boundaryTime, cids := ops.GetBoundaryCIDs(operations)
		if !boundaryTime.Equal(baseTime.Add(2 * time.Second)) {
			t.Error("unexpected boundary time")
		}
		if len(cids) != 3 { // cid3, cid4, cid5
			t.Errorf("expected 3 boundary CIDs, got %d", len(cids))
		}
	})
}

// Helper functions

func makeTestOperations(count int) []plc.PLCOperation {
	ops := make([]plc.PLCOperation, count)
	baseTime := time.Now().Add(-time.Hour)

	for i := 0; i < count; i++ {
		ops[i] = plc.PLCOperation{
			DID:       "did:plc:test" + string(rune(i)),
			CID:       "bafytest" + string(rune(i)),
			CreatedAt: baseTime.Add(time.Duration(i) * time.Second),
			/*Operation: map[string]interface{}{
				"type": "create",
			},*/
		}
	}

	return ops
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	l.t.Logf(format, v...)
}

func (l *testLogger) Println(v ...interface{}) {
	l.t.Log(v...)
}
