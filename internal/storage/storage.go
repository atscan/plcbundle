package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	gozstd "github.com/DataDog/zstd"
	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/plcclient" // ONLY import plcclient, NOT bundle
)

// Operations handles low-level bundle file operations
type Operations struct {
	logger Logger
}

// Logger interface
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
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
func (op *Operations) SerializeJSONL(operations []plcclient.PLCOperation) []byte {
	var buf bytes.Buffer

	for _, operation := range operations {
		if len(operation.RawJSON) > 0 {
			buf.Write(operation.RawJSON)
		} else {
			data, _ := json.Marshal(operation)
			buf.Write(data)
		}
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

// ParseJSONL parses newline-delimited JSON into operations
func (op *Operations) ParseJSONL(data []byte) ([]plcclient.PLCOperation, error) {
	var operations []plcclient.PLCOperation
	scanner := bufio.NewScanner(bytes.NewReader(data))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var operation plcclient.PLCOperation
		if err := json.UnmarshalNoEscape(line, &operation); err != nil {
			return nil, fmt.Errorf("failed to parse line: %w", err)
		}

		operation.RawJSON = make([]byte, len(line))
		copy(operation.RawJSON, line)
		operations = append(operations, operation)
	}

	return operations, nil
}

// ========================================
// FILE OPERATIONS
// ========================================

// LoadBundle loads a compressed bundle
func (op *Operations) LoadBundle(path string) ([]plcclient.PLCOperation, error) {
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
func (op *Operations) SaveBundle(path string, operations []plcclient.PLCOperation) (string, string, int64, int64, error) {
	jsonlData := op.SerializeJSONL(operations)
	contentSize := int64(len(jsonlData))
	contentHash := op.Hash(jsonlData)

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

// Pool for scanner buffers
var scannerBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64*1024)
		return &buf
	},
}

// LoadOperationAtPosition loads a single operation from a bundle
func (op *Operations) LoadOperationAtPosition(path string, position int) (*plcclient.PLCOperation, error) {
	if position < 0 {
		return nil, fmt.Errorf("invalid position: %d", position)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := gozstd.NewReader(file)
	defer reader.Close()

	bufPtr := scannerBufPool.Get().(*[]byte)
	defer scannerBufPool.Put(bufPtr)

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(*bufPtr, 512*1024)

	lineNum := 0
	for scanner.Scan() {
		if lineNum == position {
			line := scanner.Bytes()

			var operation plcclient.PLCOperation
			if err := json.UnmarshalNoEscape(line, &operation); err != nil {
				return nil, fmt.Errorf("failed to parse operation at position %d: %w", position, err)
			}

			operation.RawJSON = make([]byte, len(line))
			copy(operation.RawJSON, line)

			return &operation, nil
		}
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return nil, fmt.Errorf("position %d not found", position)
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

	reader := gozstd.NewReader(file)

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
		data = "plcbundle:genesis:" + contentHash
	} else {
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

	compressedHash = op.Hash(compressedData)
	compressedSize = int64(len(compressedData))

	decompressed, err := gozstd.Decompress(nil, compressedData)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("failed to decompress: %w", err)
	}

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
func (op *Operations) ExtractUniqueDIDs(operations []plcclient.PLCOperation) []string {
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
func (op *Operations) GetBoundaryCIDs(operations []plcclient.PLCOperation) (time.Time, map[string]bool) {
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
func (op *Operations) StripBoundaryDuplicates(operations []plcclient.PLCOperation, boundaryTimestamp string, prevBoundaryCIDs map[string]bool) []plcclient.PLCOperation {
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

// LoadOperationsAtPositions loads multiple operations from a bundle in one pass
func (op *Operations) LoadOperationsAtPositions(path string, positions []int) (map[int]*plcclient.PLCOperation, error) {
	if len(positions) == 0 {
		return make(map[int]*plcclient.PLCOperation), nil
	}

	// Create position set for fast lookup
	posSet := make(map[int]bool)
	maxPos := 0
	for _, pos := range positions {
		if pos < 0 {
			continue
		}
		posSet[pos] = true
		if pos > maxPos {
			maxPos = pos
		}
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := gozstd.NewReader(file)
	defer reader.Close()

	bufPtr := scannerBufPool.Get().(*[]byte)
	defer scannerBufPool.Put(bufPtr)

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(*bufPtr, 512*1024)

	results := make(map[int]*plcclient.PLCOperation)
	lineNum := 0

	for scanner.Scan() {
		// Early exit if we found everything
		if len(results) == len(posSet) {
			break
		}

		// Only parse if this position is requested
		if posSet[lineNum] {
			line := scanner.Bytes()
			var operation plcclient.PLCOperation
			if err := json.UnmarshalNoEscape(line, &operation); err != nil {
				return nil, fmt.Errorf("failed to parse operation at position %d: %w", lineNum, err)
			}

			operation.RawJSON = make([]byte, len(line))
			copy(operation.RawJSON, line)
			results[lineNum] = &operation
		}

		lineNum++

		// Early exit if we passed the max position we need
		if lineNum > maxPos {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return results, nil
}
