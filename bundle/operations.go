package bundle

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	gozstd "github.com/DataDog/zstd"
	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/plc"
)

// Operations handles low-level bundle file operations
type Operations struct {
	logger Logger
}

func NewOperations(logger Logger) (*Operations, error) {
	return &Operations{logger: logger}, nil
}

func (op *Operations) Close() {
	// Nothing to close
}

// ========================================
// CORE SERIALIZATION (JSONL)
// ========================================

// SerializeJSONL serializes operations to newline-delimited JSON
// This is the ONE method everyone should use for serialization
func (op *Operations) SerializeJSONL(operations []plc.PLCOperation) []byte {
	var buf bytes.Buffer

	for _, operation := range operations {
		// Use RawJSON if available (preserves exact format)
		if len(operation.RawJSON) > 0 {
			buf.Write(operation.RawJSON)
		} else {
			// Fallback to marshaling
			data, _ := json.Marshal(operation)
			buf.Write(data)
		}
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

// ParseJSONL parses newline-delimited JSON into operations
// This is the ONE method everyone should use for deserialization
func (op *Operations) ParseJSONL(data []byte) ([]plc.PLCOperation, error) {
	var operations []plc.PLCOperation
	scanner := bufio.NewScanner(bytes.NewReader(data))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var operation plc.PLCOperation
		// Use sonic instead of json.Unmarshal
		if err := json.Unmarshal(line, &operation); err != nil {
			return nil, fmt.Errorf("failed to parse line: %w", err)
		}

		operation.RawJSON = make([]byte, len(line))
		copy(operation.RawJSON, line)
		operations = append(operations, operation)
	}

	return operations, nil
}

// ========================================
// FILE OPERATIONS (uses JSONL + compression)
// ========================================

// LoadBundle loads a compressed bundle
func (op *Operations) LoadBundle(path string) ([]plc.PLCOperation, error) {
	compressed, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	decompressed, err := gozstd.Decompress(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %w", err)
	}

	return op.ParseJSONL(decompressed)
}

// SaveBundle saves operations to disk (compressed)
// Returns: contentHash, compressedHash, contentSize, compressedSize, error
func (op *Operations) SaveBundle(path string, operations []plc.PLCOperation) (string, string, int64, int64, error) {
	jsonlData := op.SerializeJSONL(operations)
	contentSize := int64(len(jsonlData))
	contentHash := op.Hash(jsonlData)

	// DataDog zstd.Compress returns ([]byte, error)
	compressed, err := gozstd.Compress(nil, jsonlData)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("failed to compress: %w", err)
	}

	compressedSize := int64(len(compressed))
	compressedHash := op.Hash(compressed)

	if err := os.WriteFile(path, compressed, 0644); err != nil {
		return "", "", 0, 0, fmt.Errorf("failed to write file: %w", err)
	}

	return contentHash, compressedHash, contentSize, compressedSize, nil
}

// ========================================
// STREAMING
// ========================================

// StreamRaw returns a reader for the raw compressed bundle file
func (op *Operations) StreamRaw(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle: %w", err)
	}
	return file, nil
}

// StreamDecompressed returns a reader for decompressed bundle data
func (op *Operations) StreamDecompressed(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle: %w", err)
	}

	// Create zstd reader using DataDog's package
	reader := gozstd.NewReader(file)

	// Return a wrapper that closes both the reader and file
	return &decompressedReader{
		reader: reader,
		file:   file,
	}, nil
}

// decompressedReader wraps a zstd decoder and underlying file
type decompressedReader struct {
	reader io.ReadCloser
	file   *os.File
}

func (dr *decompressedReader) Read(p []byte) (int, error) {
	return dr.reader.Read(p)
}

func (dr *decompressedReader) Close() error {
	dr.reader.Close()
	return dr.file.Close()
}

// ========================================
// HASHING
// ========================================

// Hash computes SHA256 hash of data
func (op *Operations) Hash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// CalculateChainHash calculates the cumulative chain hash
func (op *Operations) CalculateChainHash(parent string, contentHash string) string {
	var data string
	if parent == "" {
		// Genesis bundle (first bundle)
		data = "plcbundle:genesis:" + contentHash
	} else {
		// Subsequent bundles - chain parent hash with current content
		data = parent + ":" + contentHash
	}
	return op.Hash([]byte(data))
}

// CalculateFileHashes calculates both content and compressed hashes efficiently
func (op *Operations) CalculateFileHashes(path string) (compressedHash string, compressedSize int64, contentHash string, contentSize int64, err error) {
	// Read compressed file
	compressedData, err := os.ReadFile(path)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("failed to read file: %w", err)
	}

	// Calculate compressed hash
	compressedHash = op.Hash(compressedData)
	compressedSize = int64(len(compressedData))

	// Decompress with DataDog zstd
	decompressed, err := gozstd.Decompress(nil, compressedData)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("failed to decompress: %w", err)
	}

	// Calculate content hash
	contentHash = op.Hash(decompressed)
	contentSize = int64(len(decompressed))

	return compressedHash, compressedSize, contentHash, contentSize, nil
}

// VerifyHash verifies the hash of a bundle file
func (op *Operations) VerifyHash(path string, expectedHash string) (bool, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, "", fmt.Errorf("failed to read file: %w", err)
	}

	actualHash := op.Hash(data)
	return actualHash == expectedHash, actualHash, nil
}

// ========================================
// UTILITY FUNCTIONS
// ========================================

// FileExists checks if a file exists
func (op *Operations) FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// GetFileSize returns the size of a file
func (op *Operations) GetFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ExtractUniqueDIDs extracts unique DIDs from operations
func (op *Operations) ExtractUniqueDIDs(operations []plc.PLCOperation) []string {
	didSet := make(map[string]bool)
	for _, operation := range operations {
		didSet[operation.DID] = true
	}

	dids := make([]string, 0, len(didSet))
	for did := range didSet {
		dids = append(dids, did)
	}

	return dids
}

// GetBoundaryCIDs returns CIDs that share the same timestamp as the last operation
func (op *Operations) GetBoundaryCIDs(operations []plc.PLCOperation) (time.Time, map[string]bool) {
	if len(operations) == 0 {
		return time.Time{}, nil
	}

	lastOp := operations[len(operations)-1]
	boundaryTime := lastOp.CreatedAt
	cidSet := make(map[string]bool)

	// Walk backwards from the end
	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i]
		if op.CreatedAt.Equal(boundaryTime) {
			cidSet[op.CID] = true
		} else {
			break
		}
	}

	return boundaryTime, cidSet
}

// StripBoundaryDuplicates removes operations that are in prevBoundaryCIDs
func (op *Operations) StripBoundaryDuplicates(operations []plc.PLCOperation, boundaryTimestamp string, prevBoundaryCIDs map[string]bool) []plc.PLCOperation {
	if len(operations) == 0 || len(prevBoundaryCIDs) == 0 {
		return operations
	}

	boundaryTime, err := time.Parse(time.RFC3339Nano, boundaryTimestamp)
	if err != nil {
		return operations
	}

	startIdx := 0
	for startIdx < len(operations) {
		op := operations[startIdx]

		if op.CreatedAt.After(boundaryTime) {
			break
		}

		if op.CreatedAt.Equal(boundaryTime) && prevBoundaryCIDs[op.CID] {
			startIdx++
			continue
		}

		break
	}

	return operations[startIdx:]
}

// CreateBundle creates a complete bundle structure from operations
func (op *Operations) CreateBundle(bundleNumber int, operations []plc.PLCOperation, cursor string, parent string) *Bundle {
	if len(operations) != BUNDLE_SIZE {
		op.logger.Printf("Warning: bundle has %d operations, expected %d", len(operations), BUNDLE_SIZE)
	}

	dids := op.ExtractUniqueDIDs(operations)
	_, boundaryCIDs := op.GetBoundaryCIDs(operations)

	// Convert boundary CIDs map to slice
	cidSlice := make([]string, 0, len(boundaryCIDs))
	for cid := range boundaryCIDs {
		cidSlice = append(cidSlice, cid)
	}

	bundle := &Bundle{
		BundleNumber: bundleNumber,
		StartTime:    operations[0].CreatedAt,
		EndTime:      operations[len(operations)-1].CreatedAt,
		Operations:   operations,
		DIDCount:     len(dids),
		Cursor:       cursor,
		Parent:       parent,
		BoundaryCIDs: cidSlice,
		Compressed:   true,
		CreatedAt:    time.Now().UTC(),
	}

	return bundle
}

// ========================================
// METADATA CALCULATION
// ========================================

// CalculateBundleMetadata calculates complete metadata for a bundle
// This is the ONE method everyone should use for metadata calculation
func (op *Operations) CalculateBundleMetadata(bundleNumber int, path string, operations []plc.PLCOperation, parent string, cursor string) (*BundleMetadata, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Get file info
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Extract unique DIDs
	dids := op.ExtractUniqueDIDs(operations)

	// Serialize to JSONL and calculate content hash
	jsonlData := op.SerializeJSONL(operations)
	contentSize := int64(len(jsonlData))
	contentHash := op.Hash(jsonlData)

	// Read compressed file and calculate compressed hash
	compressedData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed file: %w", err)
	}
	compressedHash := op.Hash(compressedData)
	compressedSize := info.Size()

	// Calculate chain hash
	chainHash := op.CalculateChainHash(parent, contentHash)

	return &BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        operations[0].CreatedAt,
		EndTime:          operations[len(operations)-1].CreatedAt,
		OperationCount:   len(operations),
		DIDCount:         len(dids),
		Hash:             chainHash,   // Chain hash (primary)
		ContentHash:      contentHash, // Content hash
		Parent:           parent,      // Parent chain hash
		CompressedHash:   compressedHash,
		CompressedSize:   compressedSize,
		UncompressedSize: contentSize,
		Cursor:           cursor,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

// CalculateBundleMetadataFast calculates metadata quickly without chain hash
// Used during parallel scanning - chain hash calculated later sequentially
func (op *Operations) CalculateBundleMetadataFast(bundleNumber int, path string, operations []plc.PLCOperation, cursor string) (*BundleMetadata, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Calculate hashes efficiently (read file once)
	compressedHash, compressedSize, contentHash, contentSize, err := op.CalculateFileHashes(path)
	if err != nil {
		return nil, err
	}

	// Extract unique DIDs
	dids := op.ExtractUniqueDIDs(operations)

	// Note: Hash, Parent, and Cursor are set to empty - will be calculated later sequentially
	return &BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        operations[0].CreatedAt,
		EndTime:          operations[len(operations)-1].CreatedAt,
		OperationCount:   len(operations),
		DIDCount:         len(dids),
		Hash:             "",          // Chain hash - calculated later
		ContentHash:      contentHash, // Content hash
		Parent:           "",          // Parent - set later
		CompressedHash:   compressedHash,
		CompressedSize:   compressedSize,
		UncompressedSize: contentSize,
		Cursor:           cursor,
		CreatedAt:        time.Now().UTC(),
	}, nil
}
