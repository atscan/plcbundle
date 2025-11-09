package types_test

import (
	"bytes"
	"fmt"
	"testing"

	"tangled.org/atscan.net/plcbundle/internal/types"
)

// ====================================================================================
// CONSTANT VALIDATION TESTS
// ====================================================================================

func TestConstants(t *testing.T) {
	t.Run("BundleSize", func(t *testing.T) {
		if types.BUNDLE_SIZE != 10000 {
			t.Errorf("BUNDLE_SIZE = %d, want 10000", types.BUNDLE_SIZE)
		}

		// Ensure it's a reasonable size
		if types.BUNDLE_SIZE < 1000 {
			t.Error("BUNDLE_SIZE too small")
		}

		if types.BUNDLE_SIZE > 100000 {
			t.Error("BUNDLE_SIZE too large")
		}
	})

	t.Run("IndexFile", func(t *testing.T) {
		if types.INDEX_FILE != "plc_bundles.json" {
			t.Errorf("INDEX_FILE = %s, want plc_bundles.json", types.INDEX_FILE)
		}

		// Should be a valid filename
		if types.INDEX_FILE == "" {
			t.Error("INDEX_FILE should not be empty")
		}

		// Should have .json extension
		if len(types.INDEX_FILE) < 5 || types.INDEX_FILE[len(types.INDEX_FILE)-5:] != ".json" {
			t.Error("INDEX_FILE should have .json extension")
		}
	})

	t.Run("IndexVersion", func(t *testing.T) {
		if types.INDEX_VERSION != "1.0" {
			t.Errorf("INDEX_VERSION = %s, want 1.0", types.INDEX_VERSION)
		}

		// Should follow semantic versioning format (at least major.minor)
		if len(types.INDEX_VERSION) < 3 {
			t.Error("INDEX_VERSION should follow semantic versioning")
		}
	})
}

// ====================================================================================
// LOGGER INTERFACE COMPLIANCE TESTS
// ====================================================================================

func TestLoggerInterface(t *testing.T) {
	t.Run("MockLoggerImplementsInterface", func(t *testing.T) {
		var logger types.Logger = &mockLogger{}

		// Should compile and not panic
		logger.Printf("test %s", "message")
		logger.Println("test", "message")
	})

	t.Run("BufferedLoggerImplementation", func(t *testing.T) {
		buf := &bytes.Buffer{}
		logger := &bufferedLogger{buf: buf}

		// Cast to interface
		var _ types.Logger = logger

		logger.Printf("formatted %s %d", "message", 42)
		logger.Println("plain", "message")

		output := buf.String()

		if !containsString(output, "formatted message 42") {
			t.Error("Printf output not captured")
		}

		if !containsString(output, "plain message") {
			t.Error("Println output not captured")
		}
	})

	t.Run("NullLoggerImplementation", func(t *testing.T) {
		// Logger that discards all output
		logger := &nullLogger{}

		// Should not panic
		var _ types.Logger = logger
		logger.Printf("test %s", "ignored")
		logger.Println("also", "ignored")
	})

	t.Run("MultiLoggerImplementation", func(t *testing.T) {
		// Logger that writes to multiple destinations
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}

		logger := &multiLogger{
			loggers: []types.Logger{
				&bufferedLogger{buf: buf1},
				&bufferedLogger{buf: buf2},
			},
		}

		var _ types.Logger = logger

		logger.Printf("test %s", "message")

		// Both buffers should have the message
		if !containsString(buf1.String(), "test message") {
			t.Error("first logger didn't receive message")
		}

		if !containsString(buf2.String(), "test message") {
			t.Error("second logger didn't receive message")
		}
	})
}

// ====================================================================================
// CONSTANT USAGE IN CALCULATIONS
// ====================================================================================

func TestConstantUsage(t *testing.T) {
	t.Run("GlobalPositionCalculation", func(t *testing.T) {
		// Global position = bundleNumber * BUNDLE_SIZE + position
		bundleNumber := 42
		position := 1337

		globalPos := bundleNumber*types.BUNDLE_SIZE + position
		expected := 420000 + 1337

		if globalPos != expected {
			t.Errorf("global position calculation incorrect: got %d, want %d", globalPos, expected)
		}
	})

	t.Run("BundleFromGlobalPosition", func(t *testing.T) {
		globalPos := 88410345

		bundleNumber := globalPos / types.BUNDLE_SIZE
		position := globalPos % types.BUNDLE_SIZE

		if bundleNumber != 8841 {
			t.Errorf("bundle calculation wrong: got %d, want 8841", bundleNumber)
		}

		if position != 345 {
			t.Errorf("position calculation wrong: got %d, want 345", position)
		}
	})

	t.Run("OperationCountPerBundle", func(t *testing.T) {
		// Each bundle should have exactly BUNDLE_SIZE operations
		bundleCount := 100
		totalOps := bundleCount * types.BUNDLE_SIZE

		if totalOps != 1000000 {
			t.Errorf("total ops calculation: got %d, want 1000000", totalOps)
		}
	})
}

// ====================================================================================
// HELPER IMPLEMENTATIONS
// ====================================================================================

type mockLogger struct{}

func (l *mockLogger) Printf(format string, v ...interface{}) {}
func (l *mockLogger) Println(v ...interface{})               {}

type bufferedLogger struct {
	buf *bytes.Buffer
}

func (l *bufferedLogger) Printf(format string, v ...interface{}) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

func (l *bufferedLogger) Println(v ...interface{}) {
	fmt.Fprintln(l.buf, v...)
}

type nullLogger struct{}

func (l *nullLogger) Printf(format string, v ...interface{}) {}
func (l *nullLogger) Println(v ...interface{})               {}

type multiLogger struct {
	loggers []types.Logger
}

func (l *multiLogger) Printf(format string, v ...interface{}) {
	for _, logger := range l.loggers {
		logger.Printf(format, v...)
	}
}

func (l *multiLogger) Println(v ...interface{}) {
	for _, logger := range l.loggers {
		logger.Println(v...)
	}
}

func containsString(haystack, needle string) bool {
	return bytes.Contains([]byte(haystack), []byte(needle))
}
