package plcclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/storage"
)

// TestPLCOperation tests operation parsing and methods
func TestPLCOperation(t *testing.T) {
	t.Run("IsNullified", func(t *testing.T) {
		tests := []struct {
			name      string
			nullified interface{}
			want      bool
		}{
			{"nil", nil, false},
			{"false", false, false},
			{"true", true, true},
			{"empty string", "", false},
			{"non-empty string", "cid123", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				op := plcclient.PLCOperation{Nullified: tt.nullified}
				if got := op.IsNullified(); got != tt.want {
					t.Errorf("IsNullified() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("GetNullifyingCID", func(t *testing.T) {
		op := plcclient.PLCOperation{Nullified: "bafytest123"}
		if cid := op.GetNullifyingCID(); cid != "bafytest123" {
			t.Errorf("expected 'bafytest123', got '%s'", cid)
		}

		op2 := plcclient.PLCOperation{Nullified: true}
		if cid := op2.GetNullifyingCID(); cid != "" {
			t.Errorf("expected empty string, got '%s'", cid)
		}
	})

	t.Run("JSONParsing", func(t *testing.T) {
		jsonData := `{
			"did": "did:plc:test123",
			"cid": "bafytest",
			"createdAt": "2024-01-01T12:00:00.000Z",
			"operation": {"type": "create"},
			"nullified": false
		}`

		var op plcclient.PLCOperation
		if err := json.Unmarshal([]byte(jsonData), &op); err != nil {
			t.Fatalf("failed to parse operation: %v", err)
		}

		if op.DID != "did:plc:test123" {
			t.Errorf("unexpected DID: %s", op.DID)
		}
		if op.CID != "bafytest" {
			t.Errorf("unexpected CID: %s", op.CID)
		}
	})
}

// TestClient tests PLC client operations
func TestClient(t *testing.T) {
	t.Run("Export", func(t *testing.T) {
		// Create mock server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/export" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}

			// Check query parameters
			count := r.URL.Query().Get("count")
			if count != "100" {
				t.Errorf("unexpected count: %s", count)
			}

			// Return mock JSONL data
			w.Header().Set("Content-Type", "application/x-ndjson")
			for i := 0; i < 10; i++ {
				op := plcclient.PLCOperation{
					DID:       "did:plc:test" + string(rune(i)),
					CID:       "bafytest" + string(rune(i)),
					CreatedAt: time.Now(),
					//Operation: map[string]interface{}{"type": "create"},
				}
				json.NewEncoder(w).Encode(op)
			}
		}))
		defer server.Close()

		// Create client
		client := plcclient.NewClient(server.URL)
		defer client.Close()

		// Test export
		ctx := context.Background()
		ops, err := client.Export(ctx, plcclient.ExportOptions{
			Count: 100,
		})
		if err != nil {
			t.Fatalf("Export failed: %v", err)
		}

		if len(ops) != 10 {
			t.Errorf("expected 10 operations, got %d", len(ops))
		}

		// Check that RawJSON is preserved
		if len(ops[0].RawJSON) == 0 {
			t.Error("RawJSON not preserved")
		}
	})

	t.Run("RateLimitRetry", func(t *testing.T) {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			if attempts < 2 {
				// Return 429 on first attempt
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			// Success on second attempt
			w.Header().Set("Content-Type", "application/x-ndjson")
			op := plcclient.PLCOperation{DID: "did:plc:test", CID: "bafytest", CreatedAt: time.Now()}
			json.NewEncoder(w).Encode(op)
		}))
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		ctx := context.Background()
		ops, err := client.Export(ctx, plcclient.ExportOptions{Count: 1})
		if err != nil {
			t.Fatalf("Export failed after retry: %v", err)
		}

		if len(ops) != 1 {
			t.Errorf("expected 1 operation, got %d", len(ops))
		}

		if attempts < 2 {
			t.Errorf("expected at least 2 attempts, got %d", attempts)
		}
	})

	t.Run("GetDID", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/did:plc:test123" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}

			doc := plcclient.DIDDocument{
				Context: []string{"https://www.w3.org/ns/did/v1"},
				ID:      "did:plc:test123",
			}
			json.NewEncoder(w).Encode(doc)
		}))
		defer server.Close()

		client := plcclient.NewClient(server.URL)
		defer client.Close()

		ctx := context.Background()
		doc, err := client.GetDID(ctx, "did:plc:test123")
		if err != nil {
			t.Fatalf("GetDID failed: %v", err)
		}

		if doc.ID != "did:plc:test123" {
			t.Errorf("unexpected DID: %s", doc.ID)
		}
	})
}

// TestRateLimiter tests rate limiting functionality
func TestRateLimiter(t *testing.T) {
	t.Run("BasicRateLimit", func(t *testing.T) {
		// 10 requests per second
		rl := plcclient.NewRateLimiter(10, time.Second)
		defer rl.Stop()

		ctx := context.Background()

		// First 10 should be fast
		start := time.Now()
		for i := 0; i < 10; i++ {
			if err := rl.Wait(ctx); err != nil {
				t.Fatalf("Wait failed: %v", err)
			}
		}
		elapsed := time.Since(start)

		// Should be very fast (less than 100ms)
		if elapsed > 100*time.Millisecond {
			t.Errorf("expected fast execution, took %v", elapsed)
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		rl := plcclient.NewRateLimiter(1, time.Minute) // Very slow rate
		defer rl.Stop()

		// Consume the one available token
		ctx := context.Background()
		rl.Wait(ctx)

		// Try to wait with cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := rl.Wait(cancelCtx)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

// Benchmark tests
func BenchmarkSerializeJSONL(b *testing.B) {
	ops := make([]plcclient.PLCOperation, 10000)
	for i := 0; i < 10000; i++ {
		ops[i] = plcclient.PLCOperation{
			DID:       "did:plc:test",
			CID:       "bafytest",
			CreatedAt: time.Now(),
			//Operation: map[string]interface{}{"type": "create"},
		}
	}

	logger := &benchLogger{}
	operations, _ := storage.NewOperations(logger, false)
	defer operations.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = operations.SerializeJSONL(ops)
	}
}

type benchLogger struct{}

func (l *benchLogger) Printf(format string, v ...interface{}) {}
func (l *benchLogger) Println(v ...interface{})               {}
