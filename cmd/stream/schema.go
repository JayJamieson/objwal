package main

import (
	"encoding/json"

	"github.com/JayJamieson/objwal/wal"
)

// Review is the demo row. It carries both json (WAL payload wire form) and
// parquet (sink column) tags. seq is the primary key the query layer prunes and
// dedups on; it is NOT part of the JSON payload - the WAL assigns it per record
// and decodeReview copies it in from rec.Sequence.
type Review struct {
	Seq         uint64 `json:"-"         parquet:"seq"`
	Index       int64  `json:"index"     parquet:"index"`
	ReviewID    string `json:"review_id" parquet:"review_id"`
	ProductName string `json:"product"   parquet:"product_name"`
	Rating      int64  `json:"rating"    parquet:"rating"`
	ReviewText  string `json:"text"      parquet:"review_text"`
}

// encodeReview frames a row as the WAL record payload (producer side). Seq is
// not encoded; the WAL assigns it on append.
func encodeReview(r Review) ([]byte, error) { return json.Marshal(r) }

// decodeReview is the querystream supplier seam (consumer side): unmarshal the
// payload, then stamp the WAL-assigned sequence as the primary key.
func decodeReview(rec wal.Record) (Review, error) {
	var r Review
	if err := json.Unmarshal(rec.Data, &r); err != nil {
		return Review{}, err
	}
	r.Seq = rec.Sequence
	return r, nil
}
