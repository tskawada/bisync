package changelog

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HLC implements a Hybrid Logical Clock.
// Format: {RFC3339Nano}:{8-digit hex logical counter}
// Example: 2026-04-26T12:34:56.789012345Z:00000001
type HLC struct {
	mu       sync.Mutex
	physical time.Time
	logical  uint32
}

var globalHLC = &HLC{}

// Now returns the current global HLC timestamp string.
func HLCNow() string {
	return globalHLC.tick(time.Now().UTC())
}

// Receive updates the global HLC upon receiving a remote timestamp and returns
// the updated local HLC string.
func HLCReceive(received string) (string, error) {
	rPhys, rLog, err := ParseHLC(received)
	if err != nil {
		return "", err
	}
	return globalHLC.receive(time.Now().UTC(), rPhys, rLog), nil
}

// CompareHLC returns -1, 0, or 1 when a < b, a == b, a > b.
func CompareHLC(a, b string) int {
	ap, al, err1 := ParseHLC(a)
	bp, bl, err2 := ParseHLC(b)
	if err1 != nil || err2 != nil {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}
	if ap.Before(bp) {
		return -1
	} else if ap.After(bp) {
		return 1
	}
	if al < bl {
		return -1
	} else if al > bl {
		return 1
	}
	return 0
}

func (h *HLC) tick(now time.Time) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if now.After(h.physical) {
		h.physical = now
		h.logical = 0
	} else {
		h.logical++
	}
	return FormatHLC(h.physical, h.logical)
}

func (h *HLC) receive(now, rPhys time.Time, rLog uint32) string {
	h.mu.Lock()
	defer h.mu.Unlock()

	maxPhys := maxTime(now, h.physical, rPhys)

	switch {
	case maxPhys.Equal(h.physical) && maxPhys.Equal(rPhys):
		h.logical = max32(h.logical, rLog) + 1
	case maxPhys.Equal(h.physical):
		h.logical++
	case maxPhys.Equal(rPhys):
		h.logical = rLog + 1
	default:
		h.logical = 0
	}
	h.physical = maxPhys
	return FormatHLC(h.physical, h.logical)
}

// FormatHLC encodes a physical time + logical counter into HLC string form.
func FormatHLC(t time.Time, logical uint32) string {
	return fmt.Sprintf("%s:%08x", t.UTC().Format(time.RFC3339Nano), logical)
}

// ParseHLC decodes an HLC string into its components.
func ParseHLC(s string) (time.Time, uint32, error) {
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return time.Time{}, 0, fmt.Errorf("invalid HLC %q: missing separator", s)
	}
	phys, err := time.Parse(time.RFC3339Nano, s[:idx])
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid HLC physical time in %q: %w", s, err)
	}
	logVal, err := strconv.ParseUint(s[idx+1:], 16, 32)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid HLC logical counter in %q: %w", s, err)
	}
	return phys, uint32(logVal), nil
}

func maxTime(a, b, c time.Time) time.Time {
	m := a
	if b.After(m) {
		m = b
	}
	if c.After(m) {
		m = c
	}
	return m
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
