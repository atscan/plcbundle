package server

import (
	"fmt"
	"net/http"
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
