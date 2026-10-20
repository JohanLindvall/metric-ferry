package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

type inputStream struct {
	decoder *zstd.Decoder
	file    *os.File
}

func openInput(c config) (*inputStream, error) {
	f := os.Stdin
	if c.input != "-" {
		var err error
		f, err = os.Open(c.input)
		if err != nil {
			return nil, err
		}
	}
	d, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(c.decoderWorkers), zstd.WithDecoderMaxMemory(256<<20))
	if err != nil {
		if f != os.Stdin {
			f.Close()
		}
		return nil, err
	}
	return &inputStream{decoder: d, file: f}, nil
}

func (in *inputStream) Close() {
	in.decoder.Close()
	if in.file != os.Stdin {
		in.file.Close()
	}
}

func scanner(r io.Reader, maxLine int) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, min(256<<10, maxLine)), maxLine)
	return s
}

func timestampScale(unit string) (int64, error) {
	switch unit {
	case "s":
		return 1e9, nil
	case "ms":
		return 1e6, nil
	case "us":
		return 1e3, nil
	case "ns":
		return 1, nil
	default:
		return 0, fmt.Errorf("unknown timestamp unit %q; use s, ms, us, or ns", unit)
	}
}

// Parse decimal timestamps exactly. Parsing an epoch timestamp through float64
// loses sub-microsecond precision and can shift millisecond boundaries.
func parseTimestamp(text []byte, scale int64) (int64, error) {
	s := string(text)
	if s == "" {
		return 0, fmt.Errorf("missing timestamp")
	}
	negative := s[0] == '-'
	if negative || s[0] == '+' {
		s = s[1:]
	}
	if strings.ContainsAny(s, "eE") {
		r, ok := new(big.Rat).SetString(string(text))
		if !ok {
			return 0, fmt.Errorf("invalid timestamp %q", text)
		}
		r.Mul(r, new(big.Rat).SetInt64(scale))
		if !r.IsInt() || !r.Num().IsInt64() {
			return 0, fmt.Errorf("timestamp %q is outside nanosecond precision/range", text)
		}
		return r.Num().Int64(), nil
	}
	whole, fraction, hasDot := strings.Cut(s, ".")
	if whole == "" && (!hasDot || fraction == "") {
		return 0, fmt.Errorf("invalid timestamp %q", text)
	}
	var n uint64
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	for _, ch := range []byte(whole) {
		if ch < '0' || ch > '9' || n > (limit-uint64(ch-'0'))/10 {
			return 0, fmt.Errorf("invalid or overflowing timestamp %q", text)
		}
		n = n*10 + uint64(ch-'0')
	}
	if n > limit/uint64(scale) {
		return 0, fmt.Errorf("timestamp %q overflows nanosecond range", text)
	}
	n *= uint64(scale)
	place := uint64(scale)
	for _, ch := range []byte(fraction) {
		place /= 10
		if ch < '0' || ch > '9' || (place == 0 && ch != '0') {
			return 0, fmt.Errorf("invalid timestamp or sub-nanosecond precision: %q", text)
		}
		add := uint64(ch-'0') * place
		if n > limit-add {
			return 0, fmt.Errorf("timestamp %q overflows nanosecond range", text)
		}
		n += add
	}
	if negative {
		return -int64(n), nil
	}
	return int64(n), nil
}

func addTimestamp(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, fmt.Errorf("rebased timestamp overflows nanosecond range")
	}
	return a + b, nil
}

func subtractTimestamp(a, b int64) (int64, error) {
	if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
		return 0, fmt.Errorf("timestamp shift overflows duration range")
	}
	return a - b, nil
}

func parseTarget(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("-rebase must be now or an RFC3339 timestamp: %w", err)
	}
	ns := t.UnixNano()
	if !time.Unix(0, ns).Equal(t) {
		return 0, fmt.Errorf("rebase target is outside nanosecond timestamp range")
	}
	return ns, nil
}

func targetNow(s string) (int64, error) {
	if s == "now" {
		return time.Now().UnixNano(), nil
	}
	return parseTarget(s)
}

// End rebasing needs a full pass: the last physical line need not be the newest.
func latestTimestamp(ctx context.Context, c config) (int64, error) {
	in, err := openInput(c)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	scale, _ := timestampScale(c.timestampUnit)
	s := scanner(in.decoder, c.maxLineBytes)
	var latest int64 = math.MinInt64
	var count, line int64
	for s.Scan() {
		line++
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		raw := trimSpace(s.Bytes())
		if len(raw) == 0 || raw[0] == '#' {
			continue
		}
		_, _, ts, _, err := splitSample(raw, c.dropExemplars)
		if err != nil {
			return 0, fmt.Errorf("line %d: %w", line, err)
		}
		ns, err := parseTimestamp(ts, scale)
		if err != nil {
			return 0, fmt.Errorf("line %d: %w", line, err)
		}
		latest = max(latest, ns)
		count++
		if c.limit > 0 && count == c.limit {
			break
		}
	}
	if err := s.Err(); err != nil {
		return 0, fmt.Errorf("read zstd during rebase scan: %w", err)
	}
	if count == 0 {
		return 0, fmt.Errorf("input has no samples")
	}
	return latest, nil
}

func formatTimestamp(ns int64) string {
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
}

func parseValue(b []byte) (float64, error) {
	v, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid sample value %q: %w", b, err)
	}
	return v, nil
}
