package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
)

const (
	MetadataFormatVersion = 1
)

// BundleMetadata - Self-describing bundle (content-focused, not container)
type BundleMetadata struct {
	// === Format Info ===
	Format string `json:"format"` // "plcbundle-v1"

	// === Bundle Identity ===
	Origin       string `json:"origin"`        // Source PLC directory URL
	BundleNumber int    `json:"bundle_number"` // Sequential bundle number

	// === Content Integrity ===
	ContentHash string `json:"content_hash"`          // SHA256 of uncompressed JSONL content
	ParentHash  string `json:"parent_hash,omitempty"` // Hash of previous bundle (chain)

	// === Content Description ===
	OperationCount int       `json:"operation_count"` // Always 10000 for complete bundles
	DIDCount       int       `json:"did_count"`       // Unique DIDs in this bundle
	StartTime      time.Time `json:"start_time"`      // First operation timestamp
	EndTime        time.Time `json:"end_time"`        // Last operation timestamp

	// === Frame Structure (for random access) ===
	FrameCount   int     `json:"frame_count"`   // Number of zstd frames (usually 100)
	FrameSize    int     `json:"frame_size"`    // Operations per frame (100)
	FrameOffsets []int64 `json:"frame_offsets"` // Byte offsets of each frame

	// === Optional Context ===
	Cursor string `json:"cursor,omitempty"` // PLC export cursor for this bundle
	Notes  string `json:"notes,omitempty"`  // Optional description

	// === Creation Provenance ===
	CreatedAt     time.Time `json:"created_at"`                // When bundle was created
	CreatedBy     string    `json:"created_by"`                // "plcbundle/v1.2.3"
	CreatedByHost string    `json:"created_by_host,omitempty"` // Optional: hostname that created it
}

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

// BundleInfo contains info needed to create metadata
type BundleInfo struct {
	BundleNumber int
	Origin       string
	ParentHash   string
	Cursor       string
	CreatedBy    string // "plcbundle/v1.2.3"
	Hostname     string // Optional
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
// FILE OPERATIONS (using zstd abstraction)
// ========================================

// SaveBundle saves operations with metadata containing RELATIVE frame offsets
func (op *Operations) SaveBundle(path string, operations []plcclient.PLCOperation, bundleInfo *BundleInfo) (string, string, int64, int64, error) {
	// 1. Calculate content
	jsonlData := op.SerializeJSONL(operations)
	contentSize := int64(len(jsonlData))
	contentHash := op.Hash(jsonlData)
	dids := op.ExtractUniqueDIDs(operations)

	hostnameHash := ""
	if bundleInfo.Hostname != "" {
		hostnameHash = op.Hash([]byte(bundleInfo.Hostname))[:16] // First 16 chars (64 bits)
	}

	// 2. Compress all frames
	compressedFrames := make([][]byte, 0)

	for i := 0; i < len(operations); i += FrameSize {
		end := i + FrameSize
		if end > len(operations) {
			end = len(operations)
		}
		opChunk := operations[i:end]
		chunkJsonlData := op.SerializeJSONL(opChunk)

		compressedChunk, err := CompressFrame(chunkJsonlData)
		if err != nil {
			return "", "", 0, 0, fmt.Errorf("failed to compress frame: %w", err)
		}

		compressedFrames = append(compressedFrames, compressedChunk)
	}

	// 3. ✅ Calculate RELATIVE offsets (relative to first data frame)
	relativeOffsets := make([]int64, len(compressedFrames)+1)
	relativeOffsets[0] = 0

	cumulative := int64(0)
	for i, frame := range compressedFrames {
		cumulative += int64(len(frame))
		relativeOffsets[i+1] = cumulative
	}

	// 4. ✅ Build metadata with RELATIVE offsets
	metadata := &BundleMetadata{
		Format:         fmt.Sprintf("plcbundle-v%d", MetadataFormatVersion),
		BundleNumber:   bundleInfo.BundleNumber,
		Origin:         bundleInfo.Origin,
		CreatedAt:      time.Now().UTC(),
		CreatedBy:      bundleInfo.CreatedBy,
		CreatedByHost:  hostnameHash,
		ContentHash:    contentHash,
		ParentHash:     bundleInfo.ParentHash,
		OperationCount: len(operations),
		DIDCount:       len(dids),
		FrameCount:     len(compressedFrames),
		FrameSize:      FrameSize,
		Cursor:         bundleInfo.Cursor,
		FrameOffsets:   relativeOffsets, // ✅ RELATIVE to data start!
	}

	if len(operations) > 0 {
		metadata.StartTime = operations[0].CreatedAt
		metadata.EndTime = operations[len(operations)-1].CreatedAt
	}

	// 5. Write final file
	finalFile, err := os.Create(path)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		finalFile.Close()
		if err != nil {
			os.Remove(path)
		}
	}()

	// Write metadata frame
	if _, err := WriteMetadataFrame(finalFile, metadata); err != nil {
		return "", "", 0, 0, fmt.Errorf("failed to write metadata: %w", err)
	}

	// Write all data frames
	for _, frame := range compressedFrames {
		if _, err := finalFile.Write(frame); err != nil {
			return "", "", 0, 0, fmt.Errorf("failed to write frame: %w", err)
		}
	}

	finalFile.Sync()
	finalFile.Close()

	// 6. Hash
	compressedData, err := os.ReadFile(path)
	if err != nil {
		return "", "", 0, 0, err
	}
	compressedHash := op.Hash(compressedData)

	os.Remove(path + ".idx")

	return contentHash, compressedHash, contentSize, int64(len(compressedData)), nil
}

// LoadBundle loads a compressed bundle
func (op *Operations) LoadBundle(path string) ([]plcclient.PLCOperation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// ✅ Use abstracted streaming reader
	reader, err := NewStreamingReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

	// Read all decompressed data from all frames
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %w", err)
	}

	// Parse JSONL
	return op.ParseJSONL(decompressed)
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

	// ✅ Use abstracted reader
	reader, err := NewStreamingReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return &decompressedReader{
		reader: reader,
		file:   file,
	}, nil
}

// decompressedReader wraps a zstd decoder and underlying file
type decompressedReader struct {
	reader StreamReader
	file   *os.File
}

func (dr *decompressedReader) Read(p []byte) (int, error) {
	return dr.reader.Read(p)
}

func (dr *decompressedReader) Close() error {
	dr.reader.Release()
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

	// ✅ Use abstracted decompression
	decompressed, err := DecompressAll(compressedData)
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

// Pool for scanner buffers
var scannerBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64*1024)
		return &buf
	},
}

// ========================================
// POSITION-BASED LOADING (with frame index)
// ========================================

// LoadOperationAtPosition loads a single operation from a bundle
func (op *Operations) LoadOperationAtPosition(path string, position int) (*plcclient.PLCOperation, error) {
	if position < 0 {
		return nil, fmt.Errorf("invalid position: %d", position)
	}

	// ✅ Try multiple sources for frame index (no goto!)
	frameOffsets, err := op.loadFrameIndex(path)
	if err != nil {
		// No frame index available - use legacy full scan
		if op.logger != nil {
			op.logger.Printf("No frame index found for %s, using legacy scan", filepath.Base(path))
		}
		return op.loadOperationAtPositionLegacy(path, position)
	}

	// We have frame index - use it for fast random access
	return op.loadOperationFromFrame(path, position, frameOffsets)
}

// loadFrameIndex loads frame offsets and converts to absolute positions
func (op *Operations) loadFrameIndex(path string) ([]int64, error) {
	// Try embedded metadata first
	meta, err := ExtractMetadataFromFile(path)
	if err == nil && len(meta.FrameOffsets) > 0 {
		// ✅ Convert relative offsets to absolute
		// First, get metadata frame size by re-reading
		file, _ := os.Open(path)
		if file != nil {
			defer file.Close()

			// Read metadata frame to find where data starts
			magic, data, readErr := ReadSkippableFrame(file)
			if readErr == nil && magic == SkippableMagicMetadata {
				// Metadata frame size = 4 (magic) + 4 (size) + len(data)
				metadataFrameSize := int64(8 + len(data))

				// Convert relative to absolute
				absoluteOffsets := make([]int64, len(meta.FrameOffsets))
				for i, relOffset := range meta.FrameOffsets {
					absoluteOffsets[i] = metadataFrameSize + relOffset
				}

				return absoluteOffsets, nil
			}
		}
	}

	// Fallback to external .idx file
	indexPath := path + ".idx"
	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("no frame index available: %w", err)
	}

	var offsets []int64
	if err := json.Unmarshal(indexData, &offsets); err != nil {
		return nil, fmt.Errorf("invalid frame index: %w", err)
	}

	return offsets, nil
}

// loadOperationFromFrame loads operation using frame index
func (op *Operations) loadOperationFromFrame(path string, position int, frameOffsets []int64) (*plcclient.PLCOperation, error) {
	frameIndex := position / FrameSize
	lineInFrame := position % FrameSize

	if frameIndex >= len(frameOffsets)-1 {
		return nil, fmt.Errorf("position %d out of bounds (frame %d, total frames %d)",
			position, frameIndex, len(frameOffsets)-1)
	}

	startOffset := frameOffsets[frameIndex]
	endOffset := frameOffsets[frameIndex+1]
	frameLength := endOffset - startOffset

	// ✅ DEBUG
	if op.logger != nil {
		op.logger.Printf("DEBUG: Frame %d: offset %d-%d, length %d bytes",
			frameIndex, startOffset, endOffset, frameLength)
	}

	if frameLength <= 0 || frameLength > 10*1024*1024 {
		return nil, fmt.Errorf("invalid frame length: %d (offsets: %d-%d)",
			frameLength, startOffset, endOffset)
	}

	bundleFile, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle: %w", err)
	}
	defer bundleFile.Close()

	compressedFrame := make([]byte, frameLength)
	n, err := bundleFile.ReadAt(compressedFrame, startOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read frame %d (offset %d, length %d): %w",
			frameIndex, startOffset, frameLength, err)
	}

	if op.logger != nil {
		op.logger.Printf("DEBUG: Read %d bytes from offset %d", n, startOffset)
	}

	// Decompress
	decompressed, err := DecompressFrame(compressedFrame)
	if err != nil {
		// ✅ DEBUG: Show first few bytes to diagnose
		if op.logger != nil {
			preview := compressedFrame
			if len(preview) > 16 {
				preview = preview[:16]
			}
			op.logger.Printf("DEBUG: Failed frame data (first 16 bytes): % x", preview)
		}
		return nil, fmt.Errorf("failed to decompress frame %d: %w", frameIndex, err)
	}

	// Scan to find the line
	scanner := bufio.NewScanner(bytes.NewReader(decompressed))
	lineNum := 0

	for scanner.Scan() {
		if lineNum == lineInFrame {
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
		return nil, fmt.Errorf("scanner error on frame %d: %w", frameIndex, err)
	}

	return nil, fmt.Errorf("position %d not found in frame %d", position, frameIndex)
}

// loadOperationAtPositionLegacy loads operation from old single-frame bundles
func (op *Operations) loadOperationAtPositionLegacy(path string, position int) (*plcclient.PLCOperation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// ✅ Use abstracted streaming reader
	reader, err := NewStreamingReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 512*1024)
	scanner.Buffer(buf, 1024*1024)

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

	return nil, fmt.Errorf("position %d not found in bundle", position)
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

	// ✅ Use abstracted streaming reader
	reader, err := NewStreamingReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

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

		// Early exit if we passed the max position
		if lineNum > maxPos {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return results, nil
}

// CalculateMetadataWithoutLoading calculates metadata by streaming (no full load)
func (op *Operations) CalculateMetadataWithoutLoading(path string) (opCount int, didCount int, startTime, endTime time.Time, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, time.Time{}, time.Time{}, err
	}
	defer file.Close()

	// ✅ Use abstracted reader
	reader, err := NewStreamingReader(file)
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

		// Only parse minimal fields needed for metadata
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

	return lineNum, len(didSet), startTime, endTime, scanner.Err()
}

// ExtractBundleMetadata extracts metadata from bundle file without decompressing
func (op *Operations) ExtractBundleMetadata(path string) (*BundleMetadata, error) {
	meta, err := ExtractMetadataFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to extract metadata: %w", err)
	}
	return meta, nil
}

// LoadBundleWithMetadata loads bundle and returns both data and embedded metadata
func (op *Operations) LoadBundleWithMetadata(path string) ([]plcclient.PLCOperation, *BundleMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 1. Try to read metadata frame first
	meta, err := ReadMetadataFrame(file)
	if err != nil {
		// No metadata frame - fall back to regular load
		file.Seek(0, io.SeekStart) // Reset to beginning
		ops, err := op.loadFromReader(file)
		return ops, nil, err
	}

	// 2. Read compressed data (file position is now after metadata frame)
	ops, err := op.loadFromReader(file)
	if err != nil {
		return nil, nil, err
	}

	return ops, meta, nil
}

// loadFromReader loads operations from a reader (internal helper)
func (op *Operations) loadFromReader(r io.Reader) ([]plcclient.PLCOperation, error) {
	reader, err := NewStreamingReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %w", err)
	}

	return op.ParseJSONL(decompressed)
}
