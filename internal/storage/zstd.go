package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/valyala/gozstd"
)

// ============================================================================
// ZSTD COMPRESSION ABSTRACTION LAYER
// ============================================================================

const (
	CompressionLevel = 3
	FrameSize        = 100

	// Skippable frame magic numbers (0x184D2A50 to 0x184D2A5F)
	// We use 0x184D2A50 for bundle metadata
	SkippableMagicMetadata = 0x184D2A50
)

// BundleMetadata is stored in skippable frame at start of bundle file
type BundleMetadata struct {
	Version      int    `json:"version"` // Metadata format version
	BundleNumber int    `json:"bundle_number"`
	Origin       string `json:"origin,omitempty"`

	// Hashes
	ContentHash    string `json:"content_hash"`
	CompressedHash string `json:"compressed_hash"`
	ParentHash     string `json:"parent_hash,omitempty"`

	// Sizes
	UncompressedSize int64 `json:"uncompressed_size"`
	CompressedSize   int64 `json:"compressed_size"`

	// Timestamps
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	CreatedAt time.Time `json:"created_at"`

	// Counts
	OperationCount int `json:"operation_count"`
	DIDCount       int `json:"did_count"`
	FrameCount     int `json:"frame_count"`

	// Additional info
	Cursor string `json:"cursor,omitempty"`
}

// ============================================================================
// SKIPPABLE FRAME FUNCTIONS
// ============================================================================

// WriteSkippableFrame writes a skippable frame with the given data
// Returns the number of bytes written
func WriteSkippableFrame(w io.Writer, magicNumber uint32, data []byte) (int64, error) {
	// Skippable frame format:
	// [4 bytes] Magic Number (0x184D2A5X)
	// [4 bytes] Frame Size (little-endian uint32)
	// [N bytes] Frame Data

	frameSize := uint32(len(data))

	// Write magic number
	if err := binary.Write(w, binary.LittleEndian, magicNumber); err != nil {
		return 0, err
	}

	// Write frame size
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

// ReadSkippableFrame reads a skippable frame from the reader
// Returns the magic number and data, or error if not a skippable frame
func ReadSkippableFrame(r io.Reader) (uint32, []byte, error) {
	// Read magic number
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return 0, nil, err
	}

	// Verify it's a skippable frame (0x184D2A50 to 0x184D2A5F)
	if magic < 0x184D2A50 || magic > 0x184D2A5F {
		return 0, nil, fmt.Errorf("not a skippable frame: magic=0x%08X", magic)
	}

	// Read frame size
	var frameSize uint32
	if err := binary.Read(r, binary.LittleEndian, &frameSize); err != nil {
		return 0, nil, err
	}

	// Read frame data
	data := make([]byte, frameSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, err
	}

	return magic, data, nil
}

// WriteMetadataFrame writes bundle metadata as a skippable frame
func WriteMetadataFrame(w io.Writer, meta *BundleMetadata) (int64, error) {
	// Serialize metadata to JSON
	jsonData, err := json.Marshal(meta)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write as skippable frame
	return WriteSkippableFrame(w, SkippableMagicMetadata, jsonData)
}

// ReadMetadataFrame reads bundle metadata from skippable frame
func ReadMetadataFrame(r io.Reader) (*BundleMetadata, error) {
	magic, data, err := ReadSkippableFrame(r)
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

// ExtractMetadataFromFile reads just the metadata without decompressing the bundle
func ExtractMetadataFromFile(path string) (*BundleMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Try to read skippable frame at start
	meta, err := ReadMetadataFrame(file)
	if err != nil {
		return nil, fmt.Errorf("no metadata frame found: %w", err)
	}

	return meta, nil
}

// ============================================================================
// COMPRESSION/DECOMPRESSION
// ============================================================================

// CompressFrame compresses a single chunk of data into a zstd frame
func CompressFrame(data []byte) ([]byte, error) {
	compressed := gozstd.Compress(nil, data)
	return compressed, nil
}

// DecompressAll decompresses all frames in the compressed data
func DecompressAll(compressed []byte) ([]byte, error) {
	decompressed, err := gozstd.Decompress(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}
	return decompressed, nil
}

// DecompressFrame decompresses a single frame
func DecompressFrame(compressedFrame []byte) ([]byte, error) {
	return gozstd.Decompress(nil, compressedFrame)
}

// NewStreamingReader creates a streaming decompressor
func NewStreamingReader(r io.Reader) (StreamReader, error) {
	reader := gozstd.NewReader(r)
	return &gozstdReader{reader: reader}, nil
}

// NewStreamingWriter creates a streaming compressor at default level
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
