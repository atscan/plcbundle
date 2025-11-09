package server_test

import (
	"encoding/json"
	"testing"
	"time"

	"tangled.org/atscan.net/plcbundle/server"
)

func TestServerResponseTypes(t *testing.T) {
	t.Run("StatusResponse_JSON", func(t *testing.T) {
		response := server.StatusResponse{
			Server: server.ServerStatus{
				Version:             "1.0.0",
				UptimeSeconds:       3600,
				SyncMode:            true,
				SyncIntervalSeconds: 60,
				WebSocketEnabled:    true,
				Origin:              "https://plc.directory",
			},
			Bundles: server.BundleStatus{
				Count:            100,
				FirstBundle:      1,
				LastBundle:       100,
				TotalSize:        1024000,
				UncompressedSize: 5120000,
				CompressionRatio: 5.0,
				TotalOperations:  1000000,
				AvgOpsPerHour:    10000,
				UpdatedAt:        time.Now(),
				HeadAgeSeconds:   30,
				RootHash:         "root_hash",
				HeadHash:         "head_hash",
				Gaps:             0,
				HasGaps:          false,
			},
		}

		// Should marshal to JSON
		data, err := json.Marshal(response)
		if err != nil {
			t.Fatalf("failed to marshal StatusResponse: %v", err)
		}

		// Should unmarshal back
		var decoded server.StatusResponse
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal StatusResponse: %v", err)
		}

		// Verify round-trip
		if decoded.Server.Version != "1.0.0" {
			t.Error("version not preserved")
		}

		if decoded.Bundles.Count != 100 {
			t.Error("bundle count not preserved")
		}
	})

	t.Run("MempoolStatus_JSON", func(t *testing.T) {
		status := server.MempoolStatus{
			Count:                500,
			TargetBundle:         42,
			CanCreateBundle:      false,
			MinTimestamp:         time.Now(),
			Validated:            true,
			ProgressPercent:      5.0,
			BundleSize:           10000,
			OperationsNeeded:     9500,
			FirstTime:            time.Now().Add(-time.Hour),
			LastTime:             time.Now(),
			TimespanSeconds:      3600,
			LastOpAgeSeconds:     10,
			EtaNextBundleSeconds: 1800,
		}

		data, err := json.Marshal(status)
		if err != nil {
			t.Fatalf("failed to marshal MempoolStatus: %v", err)
		}

		var decoded server.MempoolStatus
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal MempoolStatus: %v", err)
		}

		if decoded.Count != 500 {
			t.Error("count not preserved")
		}

		if decoded.ProgressPercent != 5.0 {
			t.Error("progress not preserved")
		}
	})

	t.Run("BundleStatus_WithGaps", func(t *testing.T) {
		status := server.BundleStatus{
			Count:      100,
			Gaps:       3,
			HasGaps:    true,
			GapNumbers: []int{5, 23, 67},
		}

		data, err := json.Marshal(status)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var decoded server.BundleStatus
		json.Unmarshal(data, &decoded)

		if !decoded.HasGaps {
			t.Error("HasGaps flag not preserved")
		}

		if len(decoded.GapNumbers) != 3 {
			t.Error("gap numbers not preserved")
		}
	})
}
