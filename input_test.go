package main

import (
	"math"
	"testing"
)

func TestTimestampPrecision(t *testing.T) {
	tests := []struct {
		text, unit string
		want       int64
	}{
		{"1791625283.101", "s", 1791625283101000000},
		{"1791625283.999999999", "s", 1791625283999999999},
		{"1791625283101", "ms", 1791625283101000000},
		{"1791625283101000", "us", 1791625283101000000},
		{"1791625283101000000", "ns", 1791625283101000000},
		{"1.25e3", "s", 1250000000000},
		{"-.000000001", "s", -1},
		{"-9223372036.854775808", "s", math.MinInt64},
		{"9223372036.854775807", "s", math.MaxInt64},
		{"0.0000000010", "s", 1},
	}
	for _, tt := range tests {
		t.Run(tt.text+tt.unit, func(t *testing.T) {
			scale, _ := timestampScale(tt.unit)
			got, err := parseTimestamp([]byte(tt.text), scale)
			if err != nil || got != tt.want {
				t.Fatalf("got %d, %v; want %d", got, err, tt.want)
			}
		})
	}
	for _, invalid := range []string{"", "NaN", "Inf", "1e999", "9223372036.854775808", "-9223372036.854775809", "1.0000000001", "1.2.3", "--1", ".", "1/2", "1e-10"} {
		if _, err := parseTimestamp([]byte(invalid), 1e9); err == nil {
			t.Errorf("accepted invalid timestamp %q", invalid)
		}
	}
	if unixMillis(-1) != -1 || unixMillis(-1000001) != -2 {
		t.Fatal("negative timestamps must round down to milliseconds")
	}
	if _, err := addTimestamp(math.MaxInt64, 1); err == nil {
		t.Fatal("accepted timestamp overflow")
	}
	if _, err := subtractTimestamp(1, math.MinInt64); err == nil {
		t.Fatal("accepted duration overflow")
	}
}
