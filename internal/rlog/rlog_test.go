package rlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func sample() []Record {
	return []Record{
		{ArrivalUnixNano: 1000, IntendedUnixNano: 900, Channel: "acme.md-1", Seq: 1, Mid: "01J8ZK3V9GXXH"},
		{ArrivalUnixNano: 2000, IntendedUnixNano: 1800, Channel: "acme.md-1", Seq: 2, Mid: "01J8ZK3VABCDE"},
		{ArrivalUnixNano: 2500, IntendedUnixNano: 2400, Channel: "acme.md-2", Seq: 1, Mid: "01J8ZK3VFGHIJ"},
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub-0.rlog")
	w, err := Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for _, r := range sample() {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d records, want 3", len(got))
	}
	for i, r := range sample() {
		if got[i] != r {
			t.Errorf("record %d = %+v, want %+v", i, got[i], r)
		}
	}
}

func TestTruncatedTailReturnsCompleteRecordsAndError(t *testing.T) {
	// A crash mid-append leaves a partial final frame. The reader must return
	// every complete record AND a non-nil error — the checker decides what a
	// truncated log means for the verdict; silently dropping the tail would
	// forge completeness.
	path := filepath.Join(t.TempDir(), "sub-0.rlog")
	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range sample() {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-7], 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(path)
	if err == nil {
		t.Fatal("ReadAll returned nil error on a truncated log")
	}
	if len(got) != 2 {
		t.Fatalf("read %d complete records from truncated log, want 2", len(got))
	}
}

func TestEmptyLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.rlog")
	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadAll(path)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty log: got %d records, err %v", len(got), err)
	}
}

func TestCorruptHeaderRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.rlog")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0xFF}, 32), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAll(path); err == nil {
		t.Fatal("ReadAll accepted a file that is not a receive log")
	}
}
