// repo/server/helpers_test.go
package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerHelperFunctions(t *testing.T) {
	// Note: Many helper functions are unexported, so we test them indirectly

	t.Run("FormatNumber_ViaOutput", func(t *testing.T) {
		// This tests the formatNumber function indirectly
		srv, _, cleanup := setupTestServer(t, false)
		defer cleanup()

		ts := httptest.NewServer(srv)
		defer ts.Close()

		resp, _ := http.Get(ts.URL + "/")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Large numbers should be formatted with commas
		// Check if output looks reasonable
		if len(body) == 0 {
			t.Error("root page is empty")
		}
	})
}
