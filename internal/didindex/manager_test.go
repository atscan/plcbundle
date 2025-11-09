package didindex_test

import (
	"testing"

	"tangled.org/atscan.net/plcbundle/internal/didindex"
)

func TestOpLocationPacking(t *testing.T) {
	tests := []struct {
		bundle    uint16
		position  uint16
		nullified bool
	}{
		{1, 0, false},
		{1, 9999, false},
		{100, 5000, true},
		{65535, 9999, true}, // Max values
	}

	for _, tt := range tests {
		loc := didindex.NewOpLocation(tt.bundle, tt.position, tt.nullified)

		// Test unpacking
		if loc.Bundle() != tt.bundle {
			t.Errorf("Bundle mismatch: got %d, want %d", loc.Bundle(), tt.bundle)
		}
		if loc.Position() != tt.position {
			t.Errorf("Position mismatch: got %d, want %d", loc.Position(), tt.position)
		}
		if loc.Nullified() != tt.nullified {
			t.Errorf("Nullified mismatch: got %v, want %v", loc.Nullified(), tt.nullified)
		}

		// Test global position
		expectedGlobal := uint32(tt.bundle)*10000 + uint32(tt.position)
		if loc.GlobalPosition() != expectedGlobal {
			t.Errorf("Global position mismatch: got %d, want %d",
				loc.GlobalPosition(), expectedGlobal)
		}
	}
}

func TestOpLocationComparison(t *testing.T) {
	loc1 := didindex.NewOpLocation(100, 50, false) // 1,000,050
	loc2 := didindex.NewOpLocation(100, 51, false) // 1,000,051
	loc3 := didindex.NewOpLocation(200, 30, false) // 2,000,030

	if !loc1.Less(loc2) {
		t.Error("Expected loc1 < loc2")
	}
	if !loc2.Less(loc3) {
		t.Error("Expected loc2 < loc3")
	}
	if loc3.Less(loc1) {
		t.Error("Expected loc3 > loc1")
	}
}
