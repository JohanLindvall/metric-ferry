package main

import (
	"io"
	"testing"
)

func TestInvalidConfigurationFailsBeforeImport(t *testing.T) {
	for _, args := range [][]string{
		{"-input", "file"},
		{"-dry-run"},
		{"-input", "-", "-dry-run", "-rebase", "now"},
		{"-input", "file", "-dry-run", "-rebase", "now", "-shift", "1h"},
		{"-input", "file", "-dry-run", "-workers", "0"},
		{"-input", "file", "-dry-run", "-timestamp-unit", "auto"},
		{"-input", "file", "-dry-run", "-otlp-url", "grpc://localhost:4317"},
		{"-input", "file", "-dry-run", "-otlp-url", "http://user:password@localhost:4318/v1/metrics"},
	} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Errorf("accepted invalid arguments %v", args)
		}
	}
	for _, header := range []string{
		"Content-Encoding=gzip", "X-Prometheus-Remote-Write-Version=bad",
		"Authorization=Bearer abc\r\nOther: header", "Invalid Name=value", "X-Test=invalid\x00value",
	} {
		if _, err := headers([]string{header}); err == nil {
			t.Errorf("accepted invalid header %q", header)
		}
	}
	h, err := headers([]string{"Authorization=Bearer abc", "X-Scope-OrgID=tenant"})
	if err != nil || h.Get("Authorization") != "Bearer abc" || h.Get("X-Scope-OrgID") != "tenant" {
		t.Fatalf("valid authentication headers rejected: %v", err)
	}
}
