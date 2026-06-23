package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

const maxCellWidth = 48

// renderer draws query results to the console, clearing in place on a TTY (unless
// -no-clear) or appending a separator otherwise.
type renderer struct {
	c     *Config
	isTTY bool
	mode  string
}

func newRenderer(c *Config) *renderer {
	fi, _ := os.Stdout.Stat()
	isTTY := fi != nil && fi.Mode()&os.ModeCharDevice != 0
	mode := "cumulative"
	if c.Incremental {
		mode = "incremental"
	}
	return &renderer{c: c, isTTY: isTTY, mode: mode}
}

// render draws one result frame: a header, the rows, and a context footer. lo/hi
// are the inclusive sequence window the rows cover.
func (r *renderer) render(rows *sql.Rows, lo, hi uint64) error {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	cells, n, err := scanAll(rows, len(cols))
	if err != nil {
		return err
	}

	r.frameStart()
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(cols, "\t"))
	for _, row := range cells {
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	tw.Flush()

	window := fmt.Sprintf("[%d, %d]", lo, hi)
	fmt.Printf("\n- %s  window=%s  rows=%d  %s -\n", r.mode, window, n, time.Now().Format("15:04:05"))
	return rows.Err()
}

// renderWaiting shows a holding frame while no finalized files exist yet.
func (r *renderer) renderWaiting(wm uint64) {
	r.frameStart()
	fmt.Printf("waiting for data…  (%s, watermark=%d, %s)\n", r.mode, wm, time.Now().Format("15:04:05"))
}

// frameStart clears the screen in place on a TTY, else prints a separator.
func (r *renderer) frameStart() {
	if r.isTTY && !r.c.NoClear {
		fmt.Print("\033[H\033[2J")
		return
	}
	fmt.Printf("--- refresh @ %s ---\n", time.Now().Format("15:04:05"))
}

// scanAll reads every row into string cells, truncating long values.
func scanAll(rows *sql.Rows, ncol int) ([][]string, int, error) {
	raw := make([]sql.RawBytes, ncol)
	dest := make([]any, ncol)
	for i := range dest {
		dest[i] = &raw[i]
	}
	var out [][]string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, 0, err
		}
		cells := make([]string, ncol)
		for i, rb := range raw {
			cells[i] = truncate(string(rb))
		}
		out = append(out, cells)
	}
	return out, len(out), rows.Err()
}

func truncate(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > maxCellWidth {
		return s[:maxCellWidth-1] + "…"
	}
	return s
}
