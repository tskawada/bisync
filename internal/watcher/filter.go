package watcher

import (
	"path/filepath"
	"strings"

	"github.com/gobwas/glob"
)

// Filter matches file paths against a set of glob exclude patterns.
type Filter struct {
	patterns []glob.Glob
}

// NewFilter compiles the given glob patterns.
func NewFilter(patterns []string) (*Filter, error) {
	f := &Filter{}
	for _, p := range patterns {
		g, err := glob.Compile(p, '/')
		if err != nil {
			return nil, err
		}
		f.patterns = append(f.patterns, g)
	}
	return f, nil
}

// Excluded reports whether the given relative path matches any exclude pattern.
// It checks the full path, the basename, and every directory component so that
// a pattern like ".Trash-*" also excludes files inside ".Trash-1000/".
func (f *Filter) Excluded(relPath string) bool {
	base := filepath.Base(relPath)
	for _, g := range f.patterns {
		if g.Match(relPath) || g.Match(base) {
			return true
		}
	}
	// Check directory components (e.g. ".Trash-1000/file" → check ".Trash-1000").
	parts := strings.Split(relPath, string(filepath.Separator))
	for i := 1; i < len(parts); i++ {
		prefix := strings.Join(parts[:i], string(filepath.Separator))
		for _, g := range f.patterns {
			if g.Match(prefix) || g.Match(parts[i-1]) {
				return true
			}
		}
	}
	return false
}
