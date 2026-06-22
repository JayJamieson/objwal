package duckdbq

import (
	"context"
	"path"
	"strconv"
	"strings"
)

// VisibleHigh derives the highest finalized record sequence from the read
// location by listing files (via DuckDB's glob, so it works for a local path or
// s3:// alike) and parsing the part-<first>-<last> range encoded in each name.
// It returns (0, false, nil) when no finalized files exist. An out-of-process
// query can call this to learn the window upper bound without the Sink.
func (e *Engine) VisibleHigh(ctx context.Context) (uint64, bool, error) {
	files, err := e.listFiles(ctx)
	if err != nil {
		return 0, false, err
	}
	var high uint64
	var have bool
	for _, f := range files {
		_, last, ok := parsePartName(path.Base(f))
		if !ok {
			continue
		}
		if !have || last > high {
			high, have = last, true
		}
	}
	return high, have, nil
}

// listFiles returns the file paths matching ReadGlob. DuckDB's glob table
// function returns zero rows (not an error) when nothing matches.
func (e *Engine) listFiles(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT file FROM glob("+sqlString(e.cfg.ReadGlob)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// parsePartName parses "part-<first>-<last><ext>" into its sequence bounds.
func parsePartName(base string) (first, last uint64, ok bool) {
	if !strings.HasPrefix(base, "part-") {
		return 0, 0, false
	}
	if dot := strings.LastIndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	mid := strings.TrimPrefix(base, "part-")
	dash := strings.IndexByte(mid, '-')
	if dash < 0 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseUint(mid[:dash], 10, 64)
	last, err2 := strconv.ParseUint(mid[dash+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return first, last, true
}
