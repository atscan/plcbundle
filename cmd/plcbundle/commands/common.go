package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/didindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	internalsync "tangled.org/atscan.net/plcbundle/internal/sync"
)

// BundleManager interface (for testing/mocking)
type BundleManager interface {
	Close()
	GetIndex() *bundleindex.Index
	LoadBundle(ctx context.Context, bundleNumber int) (*bundle.Bundle, error)
	VerifyBundle(ctx context.Context, bundleNumber int) (*bundle.VerificationResult, error)
	VerifyChain(ctx context.Context) (*bundle.ChainVerificationResult, error)
	GetInfo() map[string]interface{}
	GetMempoolStats() map[string]interface{}
	GetMempoolOperations() ([]plcclient.PLCOperation, error)
	ValidateMempool() error
	RefreshMempool() error
	ClearMempool() error
	FetchNextBundle(ctx context.Context, quiet bool) (*bundle.Bundle, error)
	SaveBundle(ctx context.Context, b *bundle.Bundle, quiet bool) (time.Duration, error)
	SaveIndex() error
	GetDIDIndexStats() map[string]interface{}
	GetDIDIndex() *didindex.Manager
	BuildDIDIndex(ctx context.Context, progress func(int, int)) error
	GetDIDOperations(ctx context.Context, did string, verbose bool) ([]plcclient.PLCOperation, error)
	GetDIDOperationsWithLocations(ctx context.Context, did string, verbose bool) ([]bundle.PLCOperationWithLocation, error)
	GetDIDOperationsFromMempool(did string) ([]plcclient.PLCOperation, error)
	GetLatestDIDOperation(ctx context.Context, did string) (*plcclient.PLCOperation, error)
	LoadOperation(ctx context.Context, bundleNum, position int) (*plcclient.PLCOperation, error)
	LoadOperations(ctx context.Context, bundleNumber int, positions []int) (map[int]*plcclient.PLCOperation, error)
	CloneFromRemote(ctx context.Context, opts internalsync.CloneOptions) (*internalsync.CloneResult, error)
	ResolveDID(ctx context.Context, did string) (*bundle.ResolveDIDResult, error)
	RunSyncOnce(ctx context.Context, config *internalsync.SyncLoopConfig, verbose bool) (int, error)
	RunSyncLoop(ctx context.Context, config *internalsync.SyncLoopConfig) error
	GetBundleIndex() didindex.BundleIndexProvider
	ScanDirectoryParallel(workers int, progressCallback func(current, total int, bytesProcessed int64)) (*bundle.DirectoryScanResult, error)
	LoadBundleForDIDIndex(ctx context.Context, bundleNumber int) (*didindex.BundleData, error)
	ResolveHandleOrDID(ctx context.Context, input string) (string, time.Duration, error)
}

// PLCOperationWithLocation wraps operation with location info
type PLCOperationWithLocation = bundle.PLCOperationWithLocation

// ============================================================================
// MANAGER OPTIONS STRUCT
// ============================================================================

// ManagerOptions configures manager creation
type ManagerOptions struct {
	Cmd               *cobra.Command // Optional: for reading --dir flag
	Dir               string         // Optional: explicit directory (overrides Cmd flag and cwd)
	PLCURL            string         // Optional: PLC directory URL
	HandleResolverURL string         // Optional: Handle resolver URL (XRPC)
	AutoInit          bool           // Optional: allow creating new empty repository (default: false)
}

// ============================================================================
// SINGLE UNIFIED getManager METHOD
// ============================================================================

// getManager creates or opens a bundle manager
// Pass nil for default options (read-only, current directory, no PLC client)
//
// Examples:
//
//	getManager(nil)                                          // All defaults
//	getManager(&ManagerOptions{Cmd: cmd})                    // Use --dir flag
//	getManager(&ManagerOptions{PLCURL: url})                 // Add PLC client
//	getManager(&ManagerOptions{AutoInit: true})              // Allow creating repo
//	getManager(&ManagerOptions{Dir: "/path", AutoInit: true}) // Explicit dir + create
func getManager(opts *ManagerOptions) (*bundle.Manager, string, error) {

	// Silence usage for operational errors
	if opts.Cmd != nil {
		opts.Cmd.SilenceUsage = true
	}

	// Use defaults if nil
	if opts == nil {
		opts = &ManagerOptions{}
	}

	// Determine directory (priority: explicit Dir > Cmd flag > current working dir)
	var dir string

	if opts.Dir != "" {
		// Explicit directory provided
		dir = opts.Dir
	} else if opts.Cmd != nil {
		// Try to get from command --dir flag
		dir, _ = opts.Cmd.Root().PersistentFlags().GetString("dir")
	}

	// Fallback to current working directory
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil, "", err
		}
	}

	// Convert to absolute path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, "", fmt.Errorf("invalid directory path: %w", err)
	}

	// Create config
	config := bundle.DefaultConfig(absDir)
	config.AutoInit = opts.AutoInit

	// Check BOTH global AND local verbose flags
	if opts.Cmd != nil {
		globalVerbose, _ := opts.Cmd.Root().PersistentFlags().GetBool("verbose")
		localVerbose, _ := opts.Cmd.Flags().GetBool("verbose")

		// Use OR logic: verbose if EITHER flag is set
		config.Verbose = globalVerbose || localVerbose
	}

	// Create PLC client if URL provided
	var client *plcclient.Client
	if opts.PLCURL != "" {
		// Build user agent with version
		userAgent := fmt.Sprintf("plcbundle/%s",
			GetVersion())

		client = plcclient.NewClient(
			opts.PLCURL,
			plcclient.WithUserAgent(userAgent),
		)
	}

	// Set handle resolver URL from flag or option
	handleResolverURL := opts.HandleResolverURL
	if handleResolverURL == "" && opts.Cmd != nil {
		handleResolverURL, _ = opts.Cmd.Root().PersistentFlags().GetString("handle-resolver")
	}
	// Only override default if explicitly provided
	if handleResolverURL != "" {
		config.HandleResolverURL = handleResolverURL
	}

	// Create manager
	mgr, err := bundle.NewManager(config, client)
	if err != nil {
		return nil, "", err
	}

	return mgr, absDir, nil
}

// parseBundleRange parses bundle range string
func parseBundleRange(rangeStr string) (start, end int, err error) {
	if !strings.Contains(rangeStr, "-") {
		var num int
		_, err = fmt.Sscanf(rangeStr, "%d", &num)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid bundle number: %w", err)
		}
		return num, num, nil
	}

	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format (expected: N or start-end)")
	}

	_, err = fmt.Sscanf(parts[0], "%d", &start)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start: %w", err)
	}

	_, err = fmt.Sscanf(parts[1], "%d", &end)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end: %w", err)
	}

	if start > end {
		return 0, 0, fmt.Errorf("start must be <= end")
	}

	return start, end, nil
}

// Formatting helpers

func formatBytes(bytes int64) string {
	if bytes < 0 {
		return fmt.Sprintf("-%s", formatBytes(-bytes))
	}

	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0f seconds", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1f minutes", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	days := d.Hours() / 24
	if days < 30 {
		return fmt.Sprintf("%.1f days", days)
	}
	if days < 365 {
		return fmt.Sprintf("%.1f months", days/30)
	}
	return fmt.Sprintf("%.1f years", days/365)
}

func formatNumber(n int) string {
	s := fmt.Sprintf("%d", n)
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

// commandLogger adapts to types.Logger
type commandLogger struct {
	quiet bool
}

func (l *commandLogger) Printf(format string, v ...interface{}) {
	if !l.quiet {
		fmt.Fprintf(os.Stderr, format+"\n", v...)
	}
}

func (l *commandLogger) Println(v ...interface{}) {
	if !l.quiet {
		fmt.Fprintln(os.Stderr, v...)
	}
}

// formatCount formats count with color coding
func formatCount(count int) string {
	if count == 0 {
		return "\033[32m0 ✓\033[0m"
	}
	return fmt.Sprintf("\033[33m%d ⚠️\033[0m", count)
}

// formatCountCritical formats count with critical color coding
func formatCountCritical(count int) string {
	if count == 0 {
		return "\033[32m0 ✓\033[0m"
	}
	return fmt.Sprintf("\033[31m%d ✗\033[0m", count)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
