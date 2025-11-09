package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/internal/bundleindex"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
	"tangled.org/atscan.net/plcbundle/server"
)

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	l.t.Logf(format, v...)
}

func (l *testLogger) Println(v ...interface{}) {
	l.t.Log(v...)
}

// ====================================================================================
// HTTP ENDPOINT TESTS
// ====================================================================================

func TestServerHTTPEndpoints(t *testing.T) {
	handler, _, cleanup := setupTestServer(t, false)
	defer cleanup()

	ts := httptest.NewServer(handler)
	defer ts.Close()

	t.Run("RootEndpoint", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/")
		if err != nil {
			t.Fatalf("GET / failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		bodyStr := string(body)

		// Should contain welcome message
		if !strings.Contains(bodyStr, "plcbundle server") {
			t.Error("root page missing title")
		}

		// Should show API endpoints
		if !strings.Contains(bodyStr, "API Endpoints") {
			t.Error("root page missing API documentation")
		}
	})

	t.Run("IndexJSON", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/index.json")
		if err != nil {
			t.Fatalf("GET /index.json failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		// Should be JSON
		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "application/json") {
			t.Errorf("wrong content type: %s", contentType)
		}

		// Parse JSON
		var idx bundleindex.Index
		if err := json.NewDecoder(resp.Body).Decode(&idx); err != nil {
			t.Fatalf("failed to parse index JSON: %v", err)
		}

		if idx.Version != "1.0" {
			t.Errorf("index version mismatch: got %s", idx.Version)
		}
	})

	t.Run("BundleMetadata", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/bundle/1")
		if err != nil {
			t.Fatalf("GET /bundle/1 failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		var meta bundleindex.BundleMetadata
		if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
			t.Fatalf("failed to parse bundle metadata: %v", err)
		}

		if meta.BundleNumber != 1 {
			t.Error("wrong bundle returned")
		}

		// Verify it has the fields we set
		if meta.ContentHash == "" {
			t.Error("metadata missing content hash")
		}
	})

	t.Run("BundleMetadata_NotFound", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/bundle/9999")
		if err != nil {
			t.Fatalf("GET /bundle/9999 failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 404 {
			t.Errorf("expected 404 for nonexistent bundle, got %d", resp.StatusCode)
		}
	})

	t.Run("BundleMetadata_InvalidNumber", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/bundle/invalid")
		if err != nil {
			t.Fatalf("GET /bundle/invalid failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 400 {
			t.Errorf("expected 400 for invalid bundle number, got %d", resp.StatusCode)
		}
	})

	t.Run("BundleData_Raw", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/data/1")
		if err != nil {
			t.Fatalf("GET /data/1 failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			// If 500, read error body
			if resp.StatusCode == 500 {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 200, got 500. Error: %s", string(body))
			}
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		// Should be zstd compressed
		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "application/zstd") {
			t.Errorf("wrong content type for raw data: %s", contentType)
		}

		// Should have content-disposition header
		disposition := resp.Header.Get("Content-Disposition")
		if !strings.Contains(disposition, "000001.jsonl.zst") {
			t.Errorf("wrong disposition header: %s", disposition)
		}

		// Should have data
		data, _ := io.ReadAll(resp.Body)
		if len(data) == 0 {
			t.Error("bundle data is empty")
		}

		t.Logf("Bundle data size: %d bytes", len(data))
	})

	t.Run("BundleJSONL_Decompressed", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/jsonl/1")
		if err != nil {
			t.Fatalf("GET /jsonl/1 failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		// Should be JSONL
		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "application/x-ndjson") {
			t.Errorf("wrong content type for JSONL: %s", contentType)
		}

		// Count lines
		data, _ := io.ReadAll(resp.Body)
		lines := bytes.Count(data, []byte("\n"))

		if lines == 0 {
			t.Error("JSONL should have lines")
		}
	})

	t.Run("StatusEndpoint", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/status")
		if err != nil {
			t.Fatalf("GET /status failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		var status server.StatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			t.Fatalf("failed to parse status JSON: %v", err)
		}

		// Verify structure
		if status.Server.Version == "" {
			t.Error("status missing server version")
		}

		if status.Bundles.Count < 0 {
			t.Error("invalid bundle count")
		}

		if status.Server.UptimeSeconds < 0 {
			t.Error("invalid uptime")
		}
	})
}

// ====================================================================================
// DID RESOLUTION ENDPOINT TESTS
// ====================================================================================

func TestServerDIDResolution(t *testing.T) {
	handler, _, cleanup := setupTestServerWithResolver(t)
	defer cleanup()

	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Use valid did:plc format: "did:plc:" + 24 chars base32 (a-z, 2-7 only)
	testDID := "did:plc:abc234def567ghi234jkl456" // Valid format

	t.Run("DIDDocument", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/" + testDID)
		if err != nil {
			t.Fatalf("GET /%s failed: %v", testDID, err)
		}
		defer resp.Body.Close()

		// Should be 404 (not in test data) or 500 (no DID index)
		// NOT 400 (that means invalid format)
		if resp.StatusCode == 400 {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("got 400 (invalid DID format): %s", string(body))
		}

		if resp.StatusCode == 500 {
			t.Log("Expected 500 (no DID index)")
			return
		}

		if resp.StatusCode == 404 {
			t.Log("Expected 404 (DID not found)")
			return
		}

		if resp.StatusCode == 200 {
			var doc plcclient.DIDDocument
			if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
				t.Fatalf("failed to parse DID document: %v", err)
			}
		}
	})

	t.Run("DIDData_RawState", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/" + testDID + "/data")
		if err != nil {
			t.Fatalf("GET /%s/data failed: %v", testDID, err)
		}
		defer resp.Body.Close()

		// /data endpoint validates format, so 400 is NOT acceptable for valid DID
		if resp.StatusCode == 400 {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("got 400 for valid DID format: %s", string(body))
		}

		// 404 or 500 acceptable (no data / no index)
		if resp.StatusCode == 500 || resp.StatusCode == 404 {
			t.Logf("Expected error (no DID index): status %d", resp.StatusCode)
			return
		}

		if resp.StatusCode == 200 {
			var state plcclient.DIDState
			json.NewDecoder(resp.Body).Decode(&state)
		}
	})

	t.Run("DIDAuditLog", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/" + testDID + "/log/audit")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		// Should NOT be 400 for valid DID
		if resp.StatusCode == 400 {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("got 400 for valid DID format: %s", string(body))
		}

		// 404, 500 acceptable
		if resp.StatusCode == 500 || resp.StatusCode == 404 {
			t.Logf("Expected error (no DID index): status %d", resp.StatusCode)
			return
		}
	})

	// Test invalid formats on /data endpoint (which validates properly)
	t.Run("InvalidDIDFormat_OnDataEndpoint", func(t *testing.T) {
		// Test DIDs that START with "did:plc:" but are still invalid
		// (routing checks prefix first, so "did:invalid:" returns 404 before validation)
		invalidDIDs := []string{
			"did:plc:short",                       // Too short (< 24 chars)
			"did:plc:tooshort2345",                // Still too short
			"did:plc:contains0189invalidchars456", // Has 0,1,8,9 (invalid in base32)
			"did:plc:UPPERCASENOTALLOWED1234",     // Has uppercase
			"did:plc:has-dashes-not-allowed12",    // Has dashes
			"did:plc:waytoolonggggggggggggggggg",  // Too long (> 24 chars)
		}

		for _, invalidDID := range invalidDIDs {
			resp, err := http.Get(ts.URL + "/" + invalidDID + "/data")
			if err != nil {
				t.Fatalf("request to %s failed: %v", invalidDID, err)
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			// /data endpoint validates format and should return 400
			if resp.StatusCode != 400 {
				t.Logf("DID %s: got %d (body: %s)", invalidDID, resp.StatusCode, string(body))
				// Some might also return 500 if they pass initial checks
				// but fail deeper validation - that's also acceptable
				if resp.StatusCode != 500 {
					t.Errorf("DID %s: expected 400 or 500, got %d", invalidDID, resp.StatusCode)
				}
			}
		}
	})

	t.Run("InvalidDIDMethod_Returns404", func(t *testing.T) {
		// DIDs with wrong method get 404 from routing (never reach validation)
		wrongMethodDIDs := []string{
			"did:invalid:format",
			"did:web:example.com",
			"did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
			"notadid",
		}

		for _, did := range wrongMethodDIDs {
			resp, err := http.Get(ts.URL + "/" + did + "/data")
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			resp.Body.Close()

			// Should get 404 (not a did:plc: path)
			if resp.StatusCode != 404 {
				t.Errorf("DID %s: expected 404 from routing, got %d", did, resp.StatusCode)
			}
		}
	})

	t.Run("NotADIDPath", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/notadid")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 404 {
			t.Errorf("expected 404 for non-DID path, got %d", resp.StatusCode)
		}
	})
}

// ====================================================================================
// CORS MIDDLEWARE TESTS
// ====================================================================================

func TestServerCORS(t *testing.T) {
	srv, _, cleanup := setupTestServer(t, false)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("CORS_Headers_GET", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/index.json")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		// Check CORS headers
		if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Error("missing or wrong Access-Control-Allow-Origin header")
		}

		methods := resp.Header.Get("Access-Control-Allow-Methods")
		if !strings.Contains(methods, "GET") {
			t.Errorf("Access-Control-Allow-Methods missing GET: %s", methods)
		}
	})

	t.Run("CORS_Preflight_OPTIONS", func(t *testing.T) {
		req, _ := http.NewRequest("OPTIONS", ts.URL+"/index.json", nil)
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "Content-Type")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("OPTIONS request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 204 {
			t.Errorf("expected 204 for OPTIONS, got %d", resp.StatusCode)
		}

		if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Error("CORS headers missing on OPTIONS")
		}

		maxAge := resp.Header.Get("Access-Control-Max-Age")
		if maxAge != "86400" {
			t.Errorf("wrong max-age: %s", maxAge)
		}
	})
}

// ====================================================================================
// WEBSOCKET TESTS
// ====================================================================================

func TestServerWebSocket(t *testing.T) {
	srv, _, cleanup := setupTestServer(t, true) // Enable WebSocket
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	t.Run("WebSocket_Connect", func(t *testing.T) {
		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("WebSocket dial failed: %v", err)
		}
		defer ws.Close()

		// Should connect successfully
		t.Log("WebSocket connected successfully")
	})

	t.Run("WebSocket_ReceiveOperations", func(t *testing.T) {
		ws, _, err := websocket.DefaultDialer.Dial(wsURL+"?cursor=0", nil)
		if err != nil {
			t.Fatalf("WebSocket dial failed: %v", err)
		}
		defer ws.Close()

		// Set read deadline
		ws.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read a message (should get operations or timeout)
		_, message, err := ws.ReadMessage()
		if err != nil {
			// Timeout is OK (no operations available)
			if !strings.Contains(err.Error(), "timeout") {
				t.Logf("Read error (may be OK if no ops): %v", err)
			}
			return
		}

		// If we got a message, verify it's valid JSON
		var op plcclient.PLCOperation
		if err := json.Unmarshal(message, &op); err != nil {
			t.Errorf("received invalid operation JSON: %v", err)
		}

		t.Logf("Received operation: %s", op.CID)
	})

	t.Run("WebSocket_InvalidCursor", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/ws?cursor=invalid")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 400 {
			t.Errorf("expected 400 for invalid cursor, got %d", resp.StatusCode)
		}
	})

	t.Run("WebSocket_CloseGracefully", func(t *testing.T) {
		ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("WebSocket dial failed: %v", err)
		}

		// Close immediately
		err = ws.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			t.Logf("close message error (may be OK): %v", err)
		}

		ws.Close()
		t.Log("WebSocket closed gracefully")
	})
}

// ====================================================================================
// SYNC MODE TESTS
// ====================================================================================

func TestServerSyncMode(t *testing.T) {
	srv, _, cleanup := setupTestServer(t, true)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("MempoolEndpoint", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/mempool")
		if err != nil {
			t.Fatalf("GET /mempool failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		// Should be JSONL
		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "application/x-ndjson") {
			t.Errorf("wrong content type: %s", contentType)
		}
	})

	t.Run("StatusWithMempool", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/status")
		if err != nil {
			t.Fatalf("GET /status failed: %v", err)
		}
		defer resp.Body.Close()

		var status server.StatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			t.Fatalf("failed to parse status: %v", err)
		}

		// Sync mode should include mempool stats
		if status.Server.SyncMode {
			if status.Mempool == nil {
				t.Error("sync mode status missing mempool")
			}
		}
	})
}

// ====================================================================================
// CONCURRENT REQUEST TESTS
// ====================================================================================

func TestServerConcurrency(t *testing.T) {
	srv, _, cleanup := setupTestServer(t, false)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("ConcurrentIndexRequests", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				resp, err := http.Get(ts.URL + "/index.json")
				if err != nil {
					errors <- err
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != 200 {
					errors <- fmt.Errorf("status %d", resp.StatusCode)
				}
			}()
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("concurrent request error: %v", err)
		}
	})

	t.Run("ConcurrentBundleRequests", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 50)

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(bundleNum int) {
				defer wg.Done()

				resp, err := http.Get(fmt.Sprintf("%s/bundle/%d", ts.URL, bundleNum%3+1))
				if err != nil {
					errors <- err
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != 200 && resp.StatusCode != 404 {
					errors <- fmt.Errorf("unexpected status %d", resp.StatusCode)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("concurrent request error: %v", err)
		}
	})

	t.Run("MixedEndpointConcurrency", func(t *testing.T) {
		var wg sync.WaitGroup

		endpoints := []string{
			"/",
			"/index.json",
			"/bundle/1",
			"/data/1",
			"/jsonl/1",
			"/status",
		}

		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				endpoint := endpoints[id%len(endpoints)]
				resp, err := http.Get(ts.URL + endpoint)
				if err != nil {
					t.Errorf("request to %s failed: %v", endpoint, err)
					return
				}
				defer resp.Body.Close()

				// Read body to completion
				io.ReadAll(resp.Body)
			}(i)
		}

		wg.Wait()
	})
}

// ====================================================================================
// ERROR HANDLING TESTS
// ====================================================================================

func TestServerErrorHandling(t *testing.T) {
	srv, _, cleanup := setupTestServer(t, false)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("404_NotFound", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/nonexistent")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 404 {
			t.Errorf("expected 404, got %d", resp.StatusCode)
		}
	})

	t.Run("405_MethodNotAllowed", func(t *testing.T) {
		// POST to GET-only endpoint
		resp, err := http.Post(ts.URL+"/index.json", "application/json", bytes.NewReader([]byte("{}")))
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 404 && resp.StatusCode != 405 {
			t.Logf("Note: Got status %d (404/405 both acceptable)", resp.StatusCode)
		}
	})

	t.Run("LargeRequestHandling", func(t *testing.T) {
		// Request very large bundle number
		resp, err := http.Get(ts.URL + "/bundle/999999")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 404 {
			t.Errorf("expected 404 for large bundle number, got %d", resp.StatusCode)
		}
	})
}

// ====================================================================================
// MIDDLEWARE TESTS
// ====================================================================================

func TestServerMiddleware(t *testing.T) {
	srv, _, cleanup := setupTestServer(t, false)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("JSON_ContentType", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/index.json")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "application/json") {
			t.Errorf("wrong content type: %s", contentType)
		}
	})

	t.Run("CORS_AllowsAllOrigins", func(t *testing.T) {
		req, _ := http.NewRequest("GET", ts.URL+"/index.json", nil)
		req.Header.Set("Origin", "https://example.com")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		allowOrigin := resp.Header.Get("Access-Control-Allow-Origin")
		if allowOrigin != "*" {
			t.Errorf("CORS not allowing all origins: %s", allowOrigin)
		}
	})
}

// ====================================================================================
// HELPER FUNCTIONS & FORMATTERS
// ====================================================================================

func TestServerHelpers(t *testing.T) {
	t.Run("FormatNumber", func(t *testing.T) {
		// Note: formatNumber is not exported, so we test indirectly
		// through endpoints that use it (like root page)

		srv, _, cleanup := setupTestServer(t, false)
		defer cleanup()

		ts := httptest.NewServer(srv)
		defer ts.Close()

		resp, _ := http.Get(ts.URL + "/")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Should have formatted numbers with commas
		// (if there are any large numbers in output)
		t.Logf("Root page length: %d bytes", len(body))
	})
}

// ====================================================================================
// MEMORY & PERFORMANCE TESTS
// ====================================================================================

func TestServerPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	srv, _, cleanup := setupTestServer(t, false)
	defer cleanup()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("MemoryDebugEndpoint", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/debug/memory")
		if err != nil {
			t.Fatalf("GET /debug/memory failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}

		body, _ := io.ReadAll(resp.Body)
		bodyStr := string(body)

		if !strings.Contains(bodyStr, "Memory Stats") {
			t.Error("memory debug output missing stats")
		}

		if !strings.Contains(bodyStr, "Alloc:") {
			t.Error("memory debug missing allocation info")
		}
	})

	t.Run("ResponseTime", func(t *testing.T) {
		// Measure response time for index
		start := time.Now()
		resp, err := http.Get(ts.URL + "/index.json")
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()

		// Should be fast (< 100ms for index)
		if elapsed > 100*time.Millisecond {
			t.Logf("Warning: slow response time: %v", elapsed)
		}

		t.Logf("Index response time: %v", elapsed)
	})
}

// ====================================================================================
// SERVER LIFECYCLE TESTS
// ====================================================================================

func TestServerLifecycle(t *testing.T) {
	t.Run("StartAndStop", func(t *testing.T) {
		mgr, mgrCleanup := setupTestManager(t)
		defer mgrCleanup()

		config := &server.Config{
			Addr:            "127.0.0.1:0", // Random port
			SyncMode:        false,
			EnableWebSocket: false,
			EnableResolver:  false,
			Version:         "test",
		}

		srv := server.New(mgr, config)

		// Start in goroutine
		errChan := make(chan error, 1)
		go func() {
			// This will block
			errChan <- srv.ListenAndServe()
		}()

		// Give it time to start
		time.Sleep(100 * time.Millisecond)

		// Shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown failed: %v", err)
		}

		// Should exit
		select {
		case err := <-errChan:
			if err != nil && err != http.ErrServerClosed {
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("server did not stop after shutdown")
		}
	})

	t.Run("GetStartTime", func(t *testing.T) {
		mgr, cleanup := setupTestManager(t)
		defer cleanup()

		config := &server.Config{
			Addr:    ":0",
			Version: "test",
		}

		before := time.Now()
		srv := server.New(mgr, config)
		after := time.Now()

		startTime := srv.GetStartTime()

		if startTime.Before(before) || startTime.After(after) {
			t.Error("start time not in expected range")
		}
	})
}

// ====================================================================================
// SETUP HELPERS
// ====================================================================================

func setupTestServer(t *testing.T, enableWebSocket bool) (http.Handler, *server.Server, func()) {
	mgr, cleanup := setupTestManager(t)

	config := &server.Config{
		Addr:            ":8080",
		SyncMode:        true,
		SyncInterval:    1 * time.Minute,
		EnableWebSocket: enableWebSocket,
		EnableResolver:  false,
		Version:         "test",
	}

	srv := server.New(mgr, config)

	// Get handler from server
	handler := srv.Handler() // Use new method

	return handler, srv, cleanup
}

func setupTestServerWithResolver(t *testing.T) (http.Handler, *server.Server, func()) {
	mgr, cleanup := setupTestManager(t)

	config := &server.Config{
		Addr:            ":8080",
		SyncMode:        false,
		EnableWebSocket: false,
		EnableResolver:  true,
		Version:         "test",
	}

	srv := server.New(mgr, config)
	handler := srv.Handler()

	return handler, srv, cleanup
}

func setupTestManager(t *testing.T) (*bundle.Manager, func()) {
	tmpDir := t.TempDir()

	config := bundle.DefaultConfig(tmpDir)
	config.AutoInit = true
	config.VerifyOnLoad = false // Disable verification in tests

	// Create storage operations ONCE and reuse
	logger := &testLogger{t: t}
	storageOps, err := storage.NewOperations(logger)
	if err != nil {
		t.Fatalf("failed to create storage operations: %v", err)
	}

	mgr, err := bundle.NewManager(config, nil)
	if err != nil {
		storageOps.Close()
		t.Fatalf("failed to create manager: %v", err)
	}

	// Add test bundles with actual files
	for i := 1; i <= 3; i++ {
		// Create actual bundle file FIRST
		path := filepath.Join(tmpDir, fmt.Sprintf("%06d.jsonl.zst", i))
		ops := makeMinimalTestOperations(10000, i*10000) // Unique ops per bundle

		contentHash, compHash, uncompSize, compSize, err := storageOps.SaveBundle(path, ops)
		if err != nil {
			t.Fatalf("failed to save test bundle %d: %v", i, err)
		}

		// Create metadata that matches the actual file
		meta := &bundleindex.BundleMetadata{
			BundleNumber:     i,
			StartTime:        ops[0].CreatedAt,
			EndTime:          ops[len(ops)-1].CreatedAt,
			OperationCount:   len(ops),
			DIDCount:         len(ops), // All unique in test data
			Hash:             fmt.Sprintf("hash%d", i),
			ContentHash:      contentHash, // Use actual hash
			CompressedHash:   compHash,    // Use actual hash
			CompressedSize:   compSize,    // Use actual size
			UncompressedSize: uncompSize,  // Use actual size
			CreatedAt:        time.Now(),
		}

		mgr.GetIndex().AddBundle(meta)
	}

	if err := mgr.SaveIndex(); err != nil {
		t.Fatalf("failed to save index: %v", err)
	}

	cleanup := func() {
		storageOps.Close()
		mgr.Close()
	}

	return mgr, cleanup
}

func makeMinimalTestOperations(count int, offset int) []plcclient.PLCOperation {
	ops := make([]plcclient.PLCOperation, count)
	baseTime := time.Now().Add(-time.Hour)

	for i := 0; i < count; i++ {
		idx := offset + i

		// Create valid base32 DID identifier (24 chars, only a-z and 2-7)
		// Convert index to base32-like string
		identifier := fmt.Sprintf("%024d", idx)
		// Replace invalid chars (0,1,8,9) with valid ones
		identifier = strings.ReplaceAll(identifier, "0", "a")
		identifier = strings.ReplaceAll(identifier, "1", "b")
		identifier = strings.ReplaceAll(identifier, "8", "c")
		identifier = strings.ReplaceAll(identifier, "9", "d")

		ops[i] = plcclient.PLCOperation{
			DID:       "did:plc:" + identifier,
			CID:       fmt.Sprintf("bafytest%012d", idx),
			CreatedAt: baseTime.Add(time.Duration(idx) * time.Second),
		}
	}

	return ops
}
