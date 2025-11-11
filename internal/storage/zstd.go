package storage

import (
	"fmt"
	"io"

	"github.com/valyala/gozstd"
)

// ============================================================================
// ZSTD COMPRESSION ABSTRACTION LAYER
// ============================================================================
// This file provides a clean interface for zstd operations.
// Swap implementations by changing the functions in this file.

const (
	// CompressionLevel is the default compression level
	CompressionLevel = 2 // Default from zstd

	// FrameSize is the number of operations per frame
	FrameSize = 100
)

// CompressFrame compresses a single chunk of data into a zstd frame
// with proper content size headers for multi-frame concatenation
func CompressFrame(data []byte) ([]byte, error) {
	// ✅ valyala/gozstd.Compress creates proper frames with content size
	compressed := gozstd.Compress(nil, data)
	return compressed, nil
}

// DecompressAll decompresses all frames in the compressed data
func DecompressAll(compressed []byte) ([]byte, error) {
	// ✅ valyala/gozstd.Decompress handles multi-frame
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
// Returns a reader that must be released with Release()
func NewStreamingReader(r io.Reader) (StreamReader, error) {
	reader := gozstd.NewReader(r)
	return &gozstdReader{reader: reader}, nil
}

// NewStreamingWriter creates a streaming compressor at default level
// Returns a writer that must be closed with Close() then released with Release()
func NewStreamingWriter(w io.Writer) (StreamWriter, error) {
	writer := gozstd.NewWriterLevel(w, CompressionLevel)
	return &gozstdWriter{writer: writer}, nil
}

// ============================================================================
// INTERFACES (for abstraction)
// ============================================================================

// StreamReader is a streaming decompression reader
type StreamReader interface {
	io.Reader
	io.WriterTo
	Release()
}

// StreamWriter is a streaming compression writer
type StreamWriter interface {
	io.Writer
	io.Closer
	Flush() error
	Release()
}

// ============================================================================
// WRAPPER TYPES (valyala/gozstd specific)
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
