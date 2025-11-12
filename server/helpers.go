package server

import (
	"fmt"
	"net/http"
	"time"

	"tangled.org/atscan.net/plcbundle/bundle"
)

// getScheme determines the HTTP scheme
func getScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}

	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}

	if r.Header.Get("X-Forwarded-Ssl") == "on" {
		return "https"
	}

	return "http"
}

// getWSScheme determines the WebSocket scheme
func getWSScheme(r *http.Request) string {
	if getScheme(r) == "https" {
		return "wss"
	}
	return "ws"
}

// getBaseURL returns the base URL for HTTP
func getBaseURL(r *http.Request) string {
	scheme := getScheme(r)
	host := r.Host
	return fmt.Sprintf("%s://%s", scheme, host)
}

// getWSURL returns the base URL for WebSocket
func getWSURL(r *http.Request) string {
	scheme := getWSScheme(r)
	host := r.Host
	return fmt.Sprintf("%s://%s", scheme, host)
}

// formatNumber formats numbers with thousand separators
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

func setDIDDocumentHeaders(
	w http.ResponseWriter,
	_ *http.Request,
	did string,
	resolvedHandle string,
	result *bundle.ResolveDIDResult,
	handleResolveTime time.Duration,
) {
	// === Identity ===
	w.Header().Set("X-DID", did)

	if resolvedHandle != "" {
		w.Header().Set("X-Handle-Resolved", resolvedHandle)
		w.Header().Set("X-Handle-Resolution-Time-Ms",
			fmt.Sprintf("%.3f", float64(handleResolveTime.Microseconds())/1000.0))
		w.Header().Set("X-Request-Type", "handle")
	} else {
		w.Header().Set("X-Request-Type", "did")
	}

	// === Resolution Source & Location ===
	w.Header().Set("X-Resolution-Source", result.Source)

	if result.Source == "bundle" {
		w.Header().Set("X-Bundle-Number", fmt.Sprintf("%d", result.BundleNumber))
		w.Header().Set("X-Bundle-Position", fmt.Sprintf("%d", result.Position))
		globalPos := (result.BundleNumber * 10000) + result.Position
		w.Header().Set("X-Global-Position", fmt.Sprintf("%d", globalPos))
		w.Header().Set("X-Pointer", fmt.Sprintf("%d:%d", result.BundleNumber, result.Position))
	} else {
		w.Header().Set("X-Mempool", "true")
	}

	// === Operation Metadata (from result.LatestOperation) ===
	if result.LatestOperation != nil {
		op := result.LatestOperation

		w.Header().Set("X-Operation-CID", op.CID)
		w.Header().Set("X-Operation-Created", op.CreatedAt.Format(time.RFC3339))

		opAge := time.Since(op.CreatedAt)
		w.Header().Set("X-Operation-Age-Seconds", fmt.Sprintf("%d", int(opAge.Seconds())))

		if len(op.RawJSON) > 0 {
			w.Header().Set("X-Operation-Size", fmt.Sprintf("%d", len(op.RawJSON)))
		}

		// Nullification status
		if op.IsNullified() {
			w.Header().Set("X-Operation-Nullified", "true")
			if nullCID := op.GetNullifyingCID(); nullCID != "" {
				w.Header().Set("X-Operation-Nullified-By", nullCID)
			}
		} else {
			w.Header().Set("X-Operation-Nullified", "false")
		}

		// Standard HTTP headers
		w.Header().Set("Last-Modified", op.CreatedAt.UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, op.CID))
	}

	// === Performance Metrics ===
	totalTime := handleResolveTime + result.TotalTime
	w.Header().Set("X-Resolution-Time-Ms",
		fmt.Sprintf("%.3f", float64(totalTime.Microseconds())/1000.0))
	w.Header().Set("X-Mempool-Time-Ms",
		fmt.Sprintf("%.3f", float64(result.MempoolTime.Microseconds())/1000.0))

	if result.Source == "bundle" {
		w.Header().Set("X-Index-Time-Ms",
			fmt.Sprintf("%.3f", float64(result.IndexTime.Microseconds())/1000.0))
		w.Header().Set("X-Load-Time-Ms",
			fmt.Sprintf("%.3f", float64(result.LoadOpTime.Microseconds())/1000.0))
	}

	// === Caching Strategy ===
	if result.Source == "bundle" {
		// Bundled data: cache 5min, stale-while-revalidate 10min
		w.Header().Set("Cache-Control",
			"public, max-age=300, stale-while-revalidate=600, stale-if-error=3600")
	} else {
		// Mempool data: cache 1min, stale-while-revalidate 5min
		w.Header().Set("Cache-Control",
			"public, max-age=60, stale-while-revalidate=300, stale-if-error=600")
	}

	w.Header().Set("Vary", "Accept, If-None-Match")
}
