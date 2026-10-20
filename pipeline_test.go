package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/prompb"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func archive(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metrics.om.zst")
	w, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := os.WriteFile(path, w.EncodeAll([]byte(text), nil), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func quietLogger() *log.Logger { return log.New(io.Discard, "", 0) }

func decodeProm(t *testing.T, r *http.Request) *prompb.WriteRequest {
	t.Helper()
	if r.Header.Get("Content-Encoding") != "snappy" || r.Header.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" ||
		r.Header.Get("Content-Type") != "application/x-protobuf" {
		t.Error("invalid remote write headers")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
		return nil
	}
	body, err = snappy.Decode(nil, body)
	if err != nil {
		t.Error(err)
		return nil
	}
	req := new(prompb.WriteRequest)
	if err := req.Unmarshal(body); err != nil {
		t.Error(err)
		return nil
	}
	return req
}

func decodeOTLP(t *testing.T, r *http.Request) *collectorpb.ExportMetricsServiceRequest {
	t.Helper()
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
			return nil
		}
		defer gr.Close()
		reader = gr
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Error(err)
		return nil
	}
	req := new(collectorpb.ExportMetricsServiceRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		t.Error(err)
		return nil
	}
	return req
}

func TestImportBothOrderedRebasedAndRetried(t *testing.T) {
	var input strings.Builder
	input.WriteString("# TYPE calls counter\n# TYPE temp gauge\n")
	for tick := 0; tick < 20; tick++ {
		for id := 0; id < 8; id++ {
			for _, metric := range []string{"calls_total", "temp"} {
				fmt.Fprintf(&input, "%s{service_name=\"app\",k8s_pod_name=\"p%d\",http_response_status_code=\"200\",job=\"app\",instance=\"%d\"} %d %d.123456789\n",
					metric, id, id, tick+1, 1700000000+tick)
			}
		}
	}
	input.WriteString("# EOF\n")
	var mu sync.Mutex
	promLast, otlpLast := make(map[string]int64), make(map[string]uint64)
	var promCount, otlpCount int
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/prom":
			req := decodeProm(t, r)
			if req == nil {
				w.WriteHeader(400)
				return
			}
			if r.Header.Get("Authorization") != "Bearer test" {
				t.Error("missing configured authentication header")
			}
			if attempts.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(503)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, series := range req.Timeseries {
				if !sort.SliceIsSorted(series.Labels, func(i, j int) bool { return series.Labels[i].Name < series.Labels[j].Name }) {
					t.Error("remote write labels are not sorted")
				}
				key := labelKey(series.Labels)
				for _, sample := range series.Samples {
					if old := promLast[key]; old != 0 && sample.Timestamp <= old {
						t.Errorf("remote write ordering violation: %d <= %d", sample.Timestamp, old)
					}
					promLast[key] = sample.Timestamp
					promCount++
				}
			}
			w.WriteHeader(204)
		case "/otel":
			if r.Header.Get("Content-Encoding") != "gzip" {
				t.Error("expected compressed OTLP")
			}
			req := decodeOTLP(t, r)
			if req == nil {
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, resource := range req.ResourceMetrics {
				attrs := attrsMap(resource.Resource.Attributes)
				if attrs["service.name"] != "app" || attrs["k8s.pod.name"] == "" {
					t.Errorf("incorrect OTLP resource: %v", attrs)
				}
				for _, scope := range resource.ScopeMetrics {
					for _, metric := range scope.Metrics {
						var points []*metricspb.NumberDataPoint
						if metric.Name == "calls_total" {
							sum := metric.GetSum()
							if sum == nil || !sum.IsMonotonic || sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
								t.Error("counter not encoded as a cumulative monotonic sum")
								continue
							}
							points = sum.DataPoints
						} else {
							points = metric.GetGauge().DataPoints
						}
						for _, point := range points {
							data := attrsMap(point.Attributes)
							if data["http.response.status_code"] != "200" || data["k8s.pod.name"] != "" {
								t.Errorf("incorrect OTLP data attributes: %v", data)
							}
							key := attrs["k8s.pod.name"] + metric.Name
							if old := otlpLast[key]; old != 0 && point.TimeUnixNano <= old {
								t.Errorf("OTLP ordering violation: %d <= %d", point.TimeUnixNano, old)
							}
							otlpLast[key] = point.TimeUnixNano
							otlpCount++
						}
					}
				}
			}
			w.Header().Set("Content-Type", "application/x-protobuf")
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	c := testConfig(t, "-prometheus-url", server.URL+"/prom", "-otlp-url", server.URL+"/otel",
		"-workers", "4", "-batch-size", "17", "-rebase", "2026-01-01T00:00:00Z", "-rebase-anchor", "end",
		"-prometheus-header", "Authorization=Bearer test")
	c.input, c.dryRun = archive(t, input.String()), false
	stats, err := run(context.Background(), c, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if promCount != 320 || otlpCount != 320 || stats.retries.Load() != 1 || len(promLast) != 16 || len(otlpLast) != 16 {
		t.Fatalf("lost samples: prom=%d otlp=%d retries=%d", promCount, otlpCount, stats.retries.Load())
	}
	target, _ := parseTarget(c.rebase)
	if stats.maxTimestamp != target || stats.minTimestamp != target-int64(19*time.Second) {
		t.Fatalf("rebase distorted relative time: %d to %d", stats.minTimestamp, stats.maxTimestamp)
	}
	for _, ts := range otlpLast {
		if ts != uint64(target) {
			t.Fatalf("wrong OTLP timestamp %d", ts)
		}
	}
	for _, ts := range promLast {
		if ts != unixMillis(target) {
			t.Fatalf("wrong remote write timestamp %d", ts)
		}
	}
}

func TestImportRejectsBadInput(t *testing.T) {
	tests := []struct{ name, text, errorText string }{
		{"out of order", "x 1 2\nx 2 1\n", "out-of-order"},
		{"conflicting values", "x 1 1\nx 2 1\n", "conflicting"},
		{"millisecond collision", "x 1 1.000000001\nx 2 1.000000002\n", "millisecond"},
		{"missing timestamp", "x 1\n", "explicit timestamps"},
		{"negative OTLP", "x 1 -1\n", "Unix epoch"},
		{"trailing content", "x 1 1\n# EOF\nx 2 2\n", "after OpenMetrics"},
		{"empty", "# EOF\n", "no samples"},
		{"bad sample", "x invalid 1\n", "invalid sample"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig(t)
			c.input = archive(t, tt.text)
			_, err := run(context.Background(), c, quietLogger())
			if err == nil || !strings.Contains(err.Error(), tt.errorText) {
				t.Fatalf("got %v, want error containing %q", err, tt.errorText)
			}
		})
	}
}

func TestFirstRebaseDuplicatesAndChecksum(t *testing.T) {
	c := testConfig(t, "-rebase", "2026-01-01T00:00:00Z", "-rebase-anchor", "first")
	c.input = archive(t, "x 1 1700000000.123456789\nx 1 1700000000.123456789\nx 2 1700000001.123456789\n")
	stats, err := run(context.Background(), c, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if stats.samples.Load() != 3 || stats.duplicates.Load() != 1 || stats.otlp.Load() != 2 || stats.prometheus.Load() != 2 {
		t.Fatalf("duplicate handling lost samples: %#v", stats)
	}
	target, _ := parseTarget(c.rebase)
	if stats.minTimestamp != target || stats.maxTimestamp != target+1e9 {
		t.Fatal("first rebasing lost precision")
	}
	b, err := os.ReadFile(c.input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.input, b[:len(b)-2], 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(context.Background(), c, quietLogger()); err == nil {
		t.Fatal("accepted a truncated zstd archive")
	}
}

func TestHTTPPartialSuccessAndFatalError(t *testing.T) {
	for _, mode := range []string{"partial", "bad-request", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var attempts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				switch mode {
				case "partial":
					body, _ := proto.Marshal(&collectorpb.ExportMetricsServiceResponse{
						PartialSuccess: &collectorpb.ExportMetricsPartialSuccess{RejectedDataPoints: 1, ErrorMessage: "bad data"},
					})
					w.Write(body)
				case "bad-request":
					w.WriteHeader(400)
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
				}
			}))
			defer server.Close()
			c := testConfig(t, "-otlp-url", server.URL)
			c.input, c.dryRun = archive(t, "x 1 1700000000\n"), false
			if _, err := run(context.Background(), c, quietLogger()); err == nil {
				t.Fatal("receiver failure reported as success")
			}
			if attempts.Load() != 1 {
				t.Fatalf("retried a permanent error %d times", attempts.Load())
			}
		})
	}
}

func TestHTTP413SplitsWithoutLosingOrder(t *testing.T) {
	var count, calls int
	var last int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		req := decodeProm(t, r)
		if req == nil {
			return
		}
		samples := req.Timeseries[0].Samples
		if len(samples) > 2 {
			w.WriteHeader(413)
			return
		}
		for _, sample := range samples {
			if sample.Timestamp <= last {
				t.Error("split batch changed sample order")
			}
			last = sample.Timestamp
			count++
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	c := testConfig(t, "-prometheus-url", server.URL, "-workers", "1", "-batch-size", "10")
	c.input, c.dryRun = archive(t, "x 1 1\nx 2 2\nx 3 3\nx 4 4\nx 5 5\n"), false
	if _, err := run(context.Background(), c, quietLogger()); err != nil {
		t.Fatal(err)
	}
	if count != 5 || calls != 5 {
		t.Fatalf("split requests: count=%d calls=%d", count, calls)
	}
}

func TestCancellationInterruptsRetry(t *testing.T) {
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(503)
		received <- struct{}{}
	}))
	defer server.Close()
	c := testConfig(t, "-prometheus-url", server.URL, "-workers", "1")
	c.input, c.dryRun = archive(t, "x 1 1\n"), false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := run(ctx, c, quietLogger())
		done <- err
	}()
	<-received
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancellation reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not interrupt retry wait")
	}
}

func BenchmarkCachedParse(b *testing.B) {
	c := config{seriesLimit: 100, inferCounters: true}
	p := newParser(c, nil)
	line := []byte(`requests_total{deployment_environment="prod",instance="host:80",job="namespace/service",k8s_cluster_name="cluster",k8s_namespace_name="namespace",k8s_pod_name="pod",service_instance_id="host:80",service_name="service",service_namespace="namespace"} 123.5 1791625283.101`)
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	for b.Loop() {
		prefix, value, timestamp, _, _ := splitSample(line, false)
		if _, err := p.get(prefix); err != nil {
			b.Fatal(err)
		}
		parseValue(value)
		parseTimestamp(timestamp, 1e9)
	}
}
