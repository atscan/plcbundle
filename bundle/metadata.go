package bundle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
)

// CalculateBundleMetadata calculates complete metadata for a bundle
func (m *Manager) CalculateBundleMetadata(bundleNumber int, path string, operations []plcclient.PLCOperation, parent string, cursor string) (*bundleindex.BundleMetadata, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Get file info
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Extract unique DIDs
	dids := m.operations.ExtractUniqueDIDs(operations)

	// Serialize to JSONL and calculate content hash
	jsonlData := m.operations.SerializeJSONL(operations)
	contentSize := int64(len(jsonlData))
	contentHash := m.operations.Hash(jsonlData)

	// Read compressed file and calculate compressed hash
	compressedData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed file: %w", err)
	}
	compressedHash := m.operations.Hash(compressedData)
	compressedSize := info.Size()

	// Calculate chain hash
	chainHash := m.operations.CalculateChainHash(parent, contentHash)

	return &bundleindex.BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        operations[0].CreatedAt,
		EndTime:          operations[len(operations)-1].CreatedAt,
		OperationCount:   len(operations),
		DIDCount:         len(dids),
		Hash:             chainHash,
		ContentHash:      contentHash,
		Parent:           parent,
		CompressedHash:   compressedHash,
		CompressedSize:   compressedSize,
		UncompressedSize: contentSize,
		Cursor:           cursor,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

// CalculateBundleMetadataFast calculates metadata quickly without chain hash
func (m *Manager) CalculateBundleMetadataFast(bundleNumber int, path string, operations []plcclient.PLCOperation, cursor string) (*bundleindex.BundleMetadata, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Calculate hashes efficiently
	compressedHash, compressedSize, contentHash, contentSize, err := m.operations.CalculateFileHashes(path)
	if err != nil {
		return nil, err
	}

	// Extract unique DIDs
	dids := m.operations.ExtractUniqueDIDs(operations)

	return &bundleindex.BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        operations[0].CreatedAt,
		EndTime:          operations[len(operations)-1].CreatedAt,
		OperationCount:   len(operations),
		DIDCount:         len(dids),
		Hash:             "",
		ContentHash:      contentHash,
		Parent:           "",
		CompressedHash:   compressedHash,
		CompressedSize:   compressedSize,
		UncompressedSize: contentSize,
		Cursor:           cursor,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

// CalculateMetadataStreaming calculates metadata by streaming (NO full load)
func (m *Manager) CalculateMetadataStreaming(bundleNumber int, path string) (*bundleindex.BundleMetadata, error) {
	// STEP 1: Stream to get times + counts (minimal memory)
	opCount, didCount, startTime, endTime, err := m.streamBundleInfo(path)
	if err != nil {
		return nil, err
	}

	// STEP 2: Calculate hashes from file
	compressedHash, compressedSize, contentHash, contentSize, err := m.operations.CalculateFileHashes(path)
	if err != nil {
		return nil, err
	}

	return &bundleindex.BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        startTime,
		EndTime:          endTime,
		OperationCount:   opCount,
		DIDCount:         didCount,
		Hash:             "", // Calculated later in sequential phase
		ContentHash:      contentHash,
		Parent:           "", // Calculated later
		CompressedHash:   compressedHash,
		CompressedSize:   compressedSize,
		UncompressedSize: contentSize,
		Cursor:           "",
		CreatedAt:        time.Now().UTC(),
	}, nil
}

// streamBundleInfo extracts metadata by streaming (minimal memory)
func (m *Manager) streamBundleInfo(path string) (opCount, didCount int, startTime, endTime time.Time, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, time.Time{}, time.Time{}, err
	}
	defer file.Close()

	// Use abstracted reader from storage package
	reader, err := storage.NewStreamingReader(file)
	if err != nil {
		return 0, 0, time.Time{}, time.Time{}, fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	didSet := make(map[string]bool)
	lineNum := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Only parse minimal fields (DID + time)
		var op struct {
			DID       string    `json:"did"`
			CreatedAt time.Time `json:"createdAt"`
		}

		if err := json.Unmarshal(line, &op); err != nil {
			continue
		}

		if lineNum == 0 {
			startTime = op.CreatedAt
		}
		endTime = op.CreatedAt

		didSet[op.DID] = true
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		return 0, 0, time.Time{}, time.Time{}, err
	}

	return lineNum, len(didSet), startTime, endTime, nil
}
