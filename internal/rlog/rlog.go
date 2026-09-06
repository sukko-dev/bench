// Package rlog is the subscriber's append-only receive log: one binary file
// per subscriber connection, written with no in-run aggregation so the hot
// receive path does nothing but append. The checker and latency analysis read
// the logs after the run.
//
// Format: an 8-byte magic header, then length-prefixed frames
// (u32 body length, body = two i64 timestamps, u64 seq, and two
// length-prefixed strings). A partial trailing frame (crash mid-append) is
// reported as an error alongside every complete record — never silently
// dropped, because a truncated log must not forge completeness.
package rlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var magic = [8]byte{'S', 'K', 'B', 'E', 'N', 'C', 'H', '1'}

// ErrTruncated marks a log whose final frame is incomplete.
var ErrTruncated = errors.New("receive log has a truncated trailing frame")

// Record is one received message.
type Record struct {
	ArrivalUnixNano  int64
	IntendedUnixNano int64
	Channel          string
	Seq              uint64
	Mid              string
}

// Writer appends records to a receive log.
type Writer struct {
	f  *os.File
	bw *bufio.Writer
}

// Create opens a new receive log at path, truncating any existing file.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create receive log: %w", err)
	}
	bw := bufio.NewWriterSize(f, 1<<16)
	if _, err := bw.Write(magic[:]); err != nil {
		f.Close() // best-effort; the write error is the failure that matters
		return nil, fmt.Errorf("write receive log header: %w", err)
	}
	return &Writer{f: f, bw: bw}, nil
}

// Append writes one record.
func (w *Writer) Append(r Record) error {
	body := make([]byte, 0, 8+8+8+4+len(r.Channel)+4+len(r.Mid))
	body = binary.LittleEndian.AppendUint64(body, uint64(r.ArrivalUnixNano))
	body = binary.LittleEndian.AppendUint64(body, uint64(r.IntendedUnixNano))
	body = binary.LittleEndian.AppendUint64(body, r.Seq)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(r.Channel)))
	body = append(body, r.Channel...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(r.Mid)))
	body = append(body, r.Mid...)

	var frameLen [4]byte
	binary.LittleEndian.PutUint32(frameLen[:], uint32(len(body)))
	if _, err := w.bw.Write(frameLen[:]); err != nil {
		return fmt.Errorf("append receive log frame: %w", err)
	}
	if _, err := w.bw.Write(body); err != nil {
		return fmt.Errorf("append receive log frame body: %w", err)
	}
	return nil
}

// Close flushes and closes the log. The flush error is checked — a silently
// unflushed tail would read as message loss.
func (w *Writer) Close() error {
	if err := w.bw.Flush(); err != nil {
		w.f.Close() // best-effort; the flush error is the failure that matters
		return fmt.Errorf("flush receive log: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("close receive log: %w", err)
	}
	return nil
}

// ReadAll returns every complete record in the log. A partial trailing frame
// yields the complete records plus ErrTruncated.
func ReadAll(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open receive log: %w", err)
	}
	defer f.Close() // read path; the data either parsed or errored already

	br := bufio.NewReaderSize(f, 1<<16)
	var head [8]byte
	if _, err := io.ReadFull(br, head[:]); err != nil {
		return nil, fmt.Errorf("read receive log header: %w", err)
	}
	if head != magic {
		return nil, errors.New("not a receive log (bad magic)")
	}

	var records []Record
	for {
		var frameLen [4]byte
		if _, err := io.ReadFull(br, frameLen[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return records, nil // clean end
			}
			return records, fmt.Errorf("%w: %v", ErrTruncated, err)
		}
		body := make([]byte, binary.LittleEndian.Uint32(frameLen[:]))
		if _, err := io.ReadFull(br, body); err != nil {
			return records, fmt.Errorf("%w: %v", ErrTruncated, err)
		}
		r, err := decodeBody(body)
		if err != nil {
			return records, fmt.Errorf("%w: %v", ErrTruncated, err)
		}
		records = append(records, r)
	}
}

func decodeBody(body []byte) (Record, error) {
	const fixed = 8 + 8 + 8 + 4
	if len(body) < fixed {
		return Record{}, errors.New("frame body too short")
	}
	r := Record{
		ArrivalUnixNano:  int64(binary.LittleEndian.Uint64(body[0:8])),
		IntendedUnixNano: int64(binary.LittleEndian.Uint64(body[8:16])),
		Seq:              binary.LittleEndian.Uint64(body[16:24]),
	}
	rest := body[24:]
	chLen := int(binary.LittleEndian.Uint32(rest[:4]))
	rest = rest[4:]
	if len(rest) < chLen+4 {
		return Record{}, errors.New("frame channel field overruns body")
	}
	r.Channel = string(rest[:chLen])
	rest = rest[chLen:]
	midLen := int(binary.LittleEndian.Uint32(rest[:4]))
	rest = rest[4:]
	if len(rest) != midLen {
		return Record{}, errors.New("frame mid field length mismatch")
	}
	r.Mid = string(rest)
	return r, nil
}
