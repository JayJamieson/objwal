package duckdbq

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestDuckDBSmoke(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT 40 + 2").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("got %d, want 42", n)
	}
}
