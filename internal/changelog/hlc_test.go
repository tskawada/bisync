package changelog

import (
	"strings"
	"testing"
	"time"
)

// --- FormatHLC / ParseHLC ---

func TestFormatAndParseHLC_roundTrip(t *testing.T) {
	ts := time.Date(2026, 4, 26, 12, 34, 56, 789_000_000, time.UTC)
	s := FormatHLC(ts, 0x0000_0042)
	if !strings.HasSuffix(s, ":00000042") {
		t.Errorf("expected hex suffix :00000042, got %q", s)
	}

	phys, logical, err := ParseHLC(s)
	if err != nil {
		t.Fatalf("ParseHLC: %v", err)
	}
	if !phys.Equal(ts) {
		t.Errorf("physical mismatch: got %v want %v", phys, ts)
	}
	if logical != 0x42 {
		t.Errorf("logical mismatch: got %d want %d", logical, 0x42)
	}
}

func TestParseHLC_invalidFormat(t *testing.T) {
	cases := []string{"", "nocooon", "2026-04-26T12:00:00Z"}
	for _, s := range cases {
		if _, _, err := ParseHLC(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

// --- CompareHLC ---

func TestCompareHLC_physicalDominates(t *testing.T) {
	earlier := "2026-04-26T10:00:00Z:000000ff"
	later := "2026-04-26T11:00:00Z:00000000"
	if CompareHLC(earlier, later) >= 0 {
		t.Error("earlier physical should be less than later physical")
	}
	if CompareHLC(later, earlier) <= 0 {
		t.Error("later physical should be greater")
	}
}

func TestCompareHLC_logicalBreaksTie(t *testing.T) {
	base := "2026-04-26T12:00:00.000000000Z"
	a := base + ":00000001"
	b := base + ":00000002"
	if CompareHLC(a, b) >= 0 {
		t.Error("lower logical should compare less")
	}
}

func TestCompareHLC_equal(t *testing.T) {
	s := "2026-04-26T12:00:00.000000000Z:00000001"
	if CompareHLC(s, s) != 0 {
		t.Error("same HLC should compare equal")
	}
}

// --- HLC.Now monotonicity ---

func TestHLCNow_isMonotonic(t *testing.T) {
	prev := HLCNow()
	for i := 0; i < 100; i++ {
		next := HLCNow()
		if CompareHLC(next, prev) < 0 {
			t.Errorf("HLC went backwards: prev=%s next=%s", prev, next)
		}
		prev = next
	}
}

func TestHLCNow_logicalIncrements(t *testing.T) {
	// When called rapidly, the physical part stays the same and logical increments.
	// At minimum, two calls must produce ordered values.
	a := HLCNow()
	b := HLCNow()
	if CompareHLC(b, a) < 0 {
		t.Errorf("second call must be >= first: a=%s b=%s", a, b)
	}
}

// --- HLCReceive ---

func TestHLCReceive_advancesOnNewerRemote(t *testing.T) {
	// Give a remote HLC far in the future so that the global clock must advance.
	future := FormatHLC(time.Now().UTC().Add(10*time.Second), 0)
	result, err := HLCReceive(future)
	if err != nil {
		t.Fatalf("HLCReceive: %v", err)
	}
	if CompareHLC(result, future) < 0 {
		t.Errorf("result %s should be >= received %s", result, future)
	}
}

func TestHLCReceive_invalidInput(t *testing.T) {
	if _, err := HLCReceive("garbage"); err == nil {
		t.Error("expected error for invalid HLC")
	}
}
