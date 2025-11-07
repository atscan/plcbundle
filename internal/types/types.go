package types

// Logger is a simple logging interface used throughout plcbundle
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

const (
	// BUNDLE_SIZE is the standard number of operations per bundle
	BUNDLE_SIZE = 10000

	// INDEX_FILE is the default index filename
	INDEX_FILE = "plc_bundles.json"

	// INDEX_VERSION is the current index format version
	INDEX_VERSION = "1.0"
)
