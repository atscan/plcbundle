package bundle

import (
	"fmt"
	"os"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/plcclient"
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
