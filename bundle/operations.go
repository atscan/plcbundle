package bundle

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/atscan/plcbundle/plc"
	"github.com/klauspost/compress/zstd"
)

// Operations handles low-level bundle file operations
type Operations struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	logger  Logger
}

// NewOperations creates a new Operations handler with default compression
func NewOperations(logger Logger) (*Operations, error) {
	// Always use default compression (level 3 - good balance)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &Operations{
		encoder: encoder,
		decoder: decoder,
		logger:  logger,
	}, nil
}

// Close cleans up resources
func (op *Operations) Close() {
	if op.encoder != nil {
		op.encoder.Close()
	}
	if op.decoder != nil {
		op.decoder.Close()
	}
}

// LoadBundle loads a bundle from disk
func (op *Operations) LoadBundle(path string) ([]plc.PLCOperation, error) {
	compressed, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Decompress
	decompressed, err := op.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %w", err)
	}

	// Parse JSONL
	operations, err := op.parseJSONL(decompressed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSONL: %w", err)
	}

	return operations, nil
}

// SaveBundle saves a bundle to disk
func (op *Operations) SaveBundle(path string, operations []plc.PLCOperation) (uncompressedHash, compressedHash string, uncompressedSize, compressedSize int64, err error) {
	// Serialize to JSONL
	jsonlData := op.SerializeJSONL(operations)
	uncompressedSize = int64(len(jsonlData))
	uncompressedHash = op.Hash(jsonlData)

	// Compress
	compressed := op.encoder.EncodeAll(jsonlData, nil)
	compressedSize = int64(len(compressed))
	compressedHash = op.Hash(compressed)

	// Write to file
	if err := os.WriteFile(path, compressed, 0644); err != nil {
		return "", "", 0, 0, fmt.Errorf("failed to write file: %w", err)
	}

	return uncompressedHash, compressedHash, uncompressedSize, compressedSize, nil
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

// GetFileSize returns the size of a file
func (op *Operations) GetFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// FileExists checks if a file exists
func (op *Operations) FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// parseJSONL parses newline-delimited JSON
func (op *Operations) parseJSONL(data []byte) ([]plc.PLCOperation, error) {
	var operations []plc.PLCOperation
	scanner := bufio.NewScanner(bytes.NewReader(data))

	// Set a large buffer for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		if len(line) == 0 {
			continue
		}

		var operation plc.PLCOperation
		if err := json.Unmarshal(line, &operation); err != nil {
			return nil, fmt.Errorf("failed to parse line %d: %w", lineNum, err)
		}

		// Store raw JSON
		operation.RawJSON = make([]byte, len(line))
		copy(operation.RawJSON, line)

		operations = append(operations, operation)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return operations, nil
}

// serializeJSONL serializes operations to newline-delimited JSON
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

// hash computes SHA256 hash of data
func (op *Operations) Hash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
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
func (op *Operations) CreateBundle(bundleNumber int, operations []plc.PLCOperation, cursor string, prevBundleHash string) *Bundle {
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
		BundleNumber:   bundleNumber,
		StartTime:      operations[0].CreatedAt,
		EndTime:        operations[len(operations)-1].CreatedAt,
		Operations:     operations,
		DIDCount:       len(dids),
		Cursor:         cursor,
		PrevBundleHash: prevBundleHash,
		BoundaryCIDs:   cidSlice,
		Compressed:     true,
		CreatedAt:      time.Now().UTC(),
	}

	return bundle
}

// CalculateMetadataFromFile calculates complete metadata from a bundle file
func (op *Operations) CalculateMetadataFromFile(path string, bundleNumber int, prevBundleHash string) (*BundleMetadata, error) {
	// Load operations
	operations, err := op.LoadBundle(path)
	if err != nil {
		return nil, err
	}

	if len(operations) == 0 {
		return nil, fmt.Errorf("bundle is empty")
	}

	// Calculate metadata
	return op.CalculateMetadata(bundleNumber, path, operations, prevBundleHash)
}

// CalculateMetadata calculates metadata from loaded operations
func (op *Operations) CalculateMetadata(bundleNumber int, path string, operations []plc.PLCOperation, prevBundleHash string) (*BundleMetadata, error) {
	// Get file info
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	// Extract unique DIDs
	dids := op.ExtractUniqueDIDs(operations)

	// Calculate sizes and hashes
	jsonlData := op.SerializeJSONL(operations)
	uncompressedSize := int64(len(jsonlData))
	uncompressedHash := op.Hash(jsonlData)

	compressedData, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	compressedHash := op.Hash(compressedData)

	// Determine cursor from previous bundle
	cursor := ""
	if bundleNumber > 1 && prevBundleHash != "" {
		cursor = operations[0].CreatedAt.Format(time.RFC3339Nano)
	}

	return &BundleMetadata{
		BundleNumber:     bundleNumber,
		StartTime:        operations[0].CreatedAt,
		EndTime:          operations[len(operations)-1].CreatedAt,
		OperationCount:   len(operations),
		DIDCount:         len(dids),
		Hash:             uncompressedHash,
		CompressedHash:   compressedHash,
		CompressedSize:   info.Size(),
		UncompressedSize: uncompressedSize,
		Cursor:           cursor,
		PrevBundleHash:   prevBundleHash,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

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

	// Create a new decoder for this stream
	decoder, err := zstd.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create decompressor: %w", err)
	}

	// Return a wrapper that closes both the decoder and file
	return &decompressedReader{
		decoder: decoder,
		file:    file,
	}, nil
}

// decompressedReader wraps a zstd decoder and underlying file
type decompressedReader struct {
	decoder *zstd.Decoder
	file    *os.File
}

func (dr *decompressedReader) Read(p []byte) (int, error) {
	return dr.decoder.Read(p)
}

func (dr *decompressedReader) Close() error {
	dr.decoder.Close()
	return dr.file.Close()
}
