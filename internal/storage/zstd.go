package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/valyala/gozstd"
)

// ============================================================================
// ZSTD COMPRESSION ABSTRACTION LAYER
// ============================================================================

const (
	CompressionLevel = 1
	FrameSize        = 100

	SkippableMagicMetadata = 0x184D2A50
)

// ============================================================================
// SKIPPABLE FRAME FUNCTIONS
// ============================================================================

// WriteSkippableFrame writes a skippable frame with the given data
func WriteSkippableFrame(w io.Writer, magicNumber uint32, data []byte) (int64, error) {
	frameSize := uint32(len(data))

	// Write magic number (little-endian)
	if err := binary.Write(w, binary.LittleEndian, magicNumber); err != nil {
		return 0, err
	}

	// Write frame size (little-endian)
	if err := binary.Write(w, binary.LittleEndian, frameSize); err != nil {
		return 0, err
	}

	// Write data
	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	totalBytes := int64(4 + 4 + n) // magic + size + data
	return totalBytes, nil
}

// ReadSkippableFrame with debug
func (ops *Operations) ReadSkippableFrame(r io.Reader) (uint32, []byte, error) {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return 0, nil, fmt.Errorf("failed to read magic: %w", err)
	}

	if magic < 0x184D2A50 || magic > 0x184D2A5F {
		return 0, nil, fmt.Errorf("not a skippable frame: magic=0x%08X (expected 0x184D2A50-0x184D2A5F)", magic)
	}

	var frameSize uint32
	if err := binary.Read(r, binary.LittleEndian, &frameSize); err != nil {
		return 0, nil, fmt.Errorf("failed to read frame size: %w", err)
	}

	data := make([]byte, frameSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, fmt.Errorf("failed to read frame data: %w", err)
	}

	return magic, data, nil
}

// WriteMetadataFrame writes bundle metadata as skippable frame (compact JSON)
func (op *Operations) WriteMetadataFrame(w io.Writer, meta *BundleMetadata) (int64, error) {
	jsonData, err := json.Marshal(meta)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return WriteSkippableFrame(w, SkippableMagicMetadata, jsonData)
}

// ReadMetadataFrame reads bundle metadata from skippable frame
func (ops *Operations) ReadMetadataFrame(r io.Reader) (*BundleMetadata, error) {
	magic, data, err := ops.ReadSkippableFrame(r)
	if err != nil {
		return nil, err
	}

	if magic != SkippableMagicMetadata {
		return nil, fmt.Errorf("unexpected skippable frame magic: 0x%08X (expected 0x%08X)",
			magic, SkippableMagicMetadata)
	}

	var meta BundleMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &meta, nil
}

// ExtractMetadataFromFile reads metadata without decompressing
func (ops *Operations) ExtractMetadataFromFile(path string) (*BundleMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Check first bytes
	header := make([]byte, 8)
	if _, err := file.Read(header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Seek back to start
	file.Seek(0, io.SeekStart)

	meta, err := ops.ReadMetadataFrame(file)
	if err != nil {
		return nil, fmt.Errorf("no metadata frame found: %w", err)
	}

	return meta, nil
}

// ExtractFrameIndexFromFile now just reads from metadata
func (ops *Operations) ExtractFrameIndexFromFile(path string) ([]int64, error) {
	meta, err := ops.ExtractMetadataFromFile(path)
	if err != nil {
		return nil, err
	}

	if len(meta.FrameOffsets) == 0 {
		return nil, fmt.Errorf("metadata has no frame offsets")
	}

	return meta.FrameOffsets, nil
}

// DebugFrameOffsets extracts and displays frame offset information
func (ops *Operations) DebugFrameOffsets(path string) error {
	meta, err := ops.ExtractMetadataFromFile(path)
	if err != nil {
		return fmt.Errorf("failed to extract metadata: %w", err)
	}

	fmt.Printf("Frame Offset Debug for: %s\n\n", filepath.Base(path))
	fmt.Printf("Metadata:\n")
	fmt.Printf("  Bundle: %d\n", meta.BundleNumber)
	fmt.Printf("  Frames: %d\n", meta.FrameCount)
	fmt.Printf("  Frame size: %d ops\n", meta.FrameSize)
	fmt.Printf("  Total ops: %d\n", meta.OperationCount)

	fmt.Printf("\nFrame Offsets (%d total):\n", len(meta.FrameOffsets))
	for i, offset := range meta.FrameOffsets {
		if i < len(meta.FrameOffsets)-1 {
			nextOffset := meta.FrameOffsets[i+1]
			frameSize := nextOffset - offset
			fmt.Printf("  Frame %3d: offset %10d, size %10d bytes\n", i, offset, frameSize)
		} else {
			fmt.Printf("  End mark: offset %10d\n", offset)
		}
	}

	// Try to verify first frame
	fmt.Printf("\nVerifying first frame...\n")
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if len(meta.FrameOffsets) < 2 {
		return fmt.Errorf("not enough frame offsets")
	}

	startOffset := meta.FrameOffsets[0]
	endOffset := meta.FrameOffsets[1]
	frameLength := endOffset - startOffset

	fmt.Printf("  Start: %d, End: %d, Length: %d\n", startOffset, endOffset, frameLength)

	compressedFrame := make([]byte, frameLength)
	_, err = file.ReadAt(compressedFrame, startOffset)
	if err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	decompressed, err := DecompressFrame(compressedFrame)
	if err != nil {
		return fmt.Errorf("failed to decompress: %w", err)
	}

	fmt.Printf("  ✓ Decompressed: %d bytes\n", len(decompressed))

	// Count lines
	lines := bytes.Count(decompressed, []byte("\n"))
	fmt.Printf("  ✓ Lines: %d\n", lines)

	return nil
}

// ============================================================================
// COMPRESSION/DECOMPRESSION
// ============================================================================

func CompressFrame(data []byte) ([]byte, error) {
	compressed := gozstd.CompressLevel(nil, data, CompressionLevel)
	return compressed, nil
}

func DecompressAll(compressed []byte) ([]byte, error) {
	decompressed, err := gozstd.Decompress(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}
	return decompressed, nil
}

func DecompressFrame(compressedFrame []byte) ([]byte, error) {
	return gozstd.Decompress(nil, compressedFrame)
}

func NewStreamingReader(r io.Reader) (StreamReader, error) {
	reader := gozstd.NewReader(r)
	return &gozstdReader{reader: reader}, nil
}

func NewStreamingWriter(w io.Writer) (StreamWriter, error) {
	writer := gozstd.NewWriterLevel(w, CompressionLevel)
	return &gozstdWriter{writer: writer}, nil
}

// ============================================================================
// INTERFACES
// ============================================================================

type StreamReader interface {
	io.Reader
	io.WriterTo
	Release()
}

type StreamWriter interface {
	io.Writer
	io.Closer
	Flush() error
	Release()
}

// ============================================================================
// WRAPPER TYPES
// ============================================================================

type gozstdReader struct {
	reader *gozstd.Reader
}

func (r *gozstdReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *gozstdReader) WriteTo(w io.Writer) (int64, error) {
	return r.reader.WriteTo(w)
}

func (r *gozstdReader) Release() {
	r.reader.Release()
}

type gozstdWriter struct {
	writer *gozstd.Writer
}

func (w *gozstdWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *gozstdWriter) Close() error {
	return w.writer.Close()
}

func (w *gozstdWriter) Flush() error {
	return w.writer.Flush()
}

func (w *gozstdWriter) Release() {
	w.writer.Release()
}
