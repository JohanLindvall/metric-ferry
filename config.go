package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

type stringsFlag []string

func (s *stringsFlag) String() string { return strings.Join(*s, ", ") }
func (s *stringsFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

type config struct {
	input, prometheusURL, otlpURL                  string
	timestampUnit, rebase, rebaseAnchor            string
	otlpCompression, attributeMode, mappingFile    string
	workers, batchSize, batchBytes, decoderWorkers int
	seriesLimit, maxLineBytes, retries             int
	limit                                          int64
	shift, timeout, progress                       time.Duration
	dryRun, inferCounters, dropExemplars           bool
	promHeaders, otlpHeaders                       stringsFlag
	renames, resources, attributes                 stringsFlag
}

func parseConfig(args []string, output io.Writer) (config, error) {
	var c config
	f := flag.NewFlagSet("metric-ferry", flag.ContinueOnError)
	f.SetOutput(output)
	f.StringVar(&c.input, "input", "", "zstd-compressed OpenMetrics file (or - for stdin)")
	f.StringVar(&c.prometheusURL, "prometheus-url", "", "full remote-write URL, e.g. http://localhost:9090/api/v1/write")
	f.StringVar(&c.otlpURL, "otlp-url", "", "full OTLP HTTP/protobuf URL, e.g. http://localhost:4318/v1/metrics")
	f.IntVar(&c.workers, "workers", min(4, runtime.GOMAXPROCS(0)), "ordered export shards (parallel requests per destination)")
	f.IntVar(&c.batchSize, "batch-size", 10000, "maximum samples per shard request")
	f.IntVar(&c.batchBytes, "batch-bytes", 4<<20, "maximum uncompressed protobuf request bytes")
	f.IntVar(&c.decoderWorkers, "zstd-workers", min(4, runtime.GOMAXPROCS(0)), "klauspost zstd decoder concurrency")
	f.IntVar(&c.seriesLimit, "series-limit", 200000, "maximum cached series and distinct text label sets; fail if exceeded")
	f.IntVar(&c.maxLineBytes, "max-line-bytes", 4<<20, "maximum decompressed line size")
	f.IntVar(&c.retries, "retries", 8, "maximum retries per request after the initial attempt")
	f.Int64Var(&c.limit, "limit", 0, "stop after this many input samples (0 reads the entire archive)")
	f.DurationVar(&c.timeout, "timeout", 30*time.Second, "HTTP timeout per attempt")
	f.DurationVar(&c.progress, "progress", 5*time.Second, "progress report interval (0 disables)")
	f.StringVar(&c.timestampUnit, "timestamp-unit", "s", "input timestamp unit: s (OpenMetrics), ms (Prometheus text), us, ns")
	f.DurationVar(&c.shift, "shift", 0, "shift every timestamp by a fixed duration, e.g. 240h or -1h")
	f.StringVar(&c.rebase, "rebase", "", "move the anchor to now or an RFC3339 timestamp, preserving time differences")
	f.StringVar(&c.rebaseAnchor, "rebase-anchor", "end", "end (latest timestamp; two passes) or first (first sample; one pass)")
	f.BoolVar(&c.dryRun, "dry-run", false, "decode, validate, map, batch and encode without sending HTTP requests")
	f.BoolVar(&c.inferCounters, "infer-counters", true, "treat untyped metrics ending in _total as OTLP cumulative counters")
	f.BoolVar(&c.dropExemplars, "drop-exemplars", false, "explicitly discard exemplars (otherwise reject them)")
	f.StringVar(&c.otlpCompression, "otlp-compression", "gzip", "OTLP request compression: gzip or none")
	f.StringVar(&c.attributeMode, "attribute-mode", "semantic", "OTLP key mapping: semantic (known keys), all (all underscores to dots), none")
	f.StringVar(&c.mappingFile, "mapping-file", "", "JSON file containing rename, resource, and datapoint rules")
	f.Var(&c.promHeaders, "prometheus-header", "HTTP header Name=Value (repeatable; use an environment variable for secrets)")
	f.Var(&c.otlpHeaders, "otlp-header", "HTTP header Name=Value (repeatable)")
	f.Var(&c.renames, "rename", "OTLP source_label=attribute.key override (repeatable)")
	f.Var(&c.resources, "resource-attribute", "promote a final OTLP attribute key to the resource (repeatable)")
	f.Var(&c.attributes, "datapoint-attribute", "keep a final OTLP attribute key on data points (repeatable)")
	f.Usage = func() {
		fmt.Fprintln(output, "Usage: metric-ferry -input metrics.om.zst [options]\n\nSend to Prometheus remote write, OTLP HTTP/protobuf, or both.\nWithout a destination, -dry-run encodes both protocols.")
		f.PrintDefaults()
	}
	if err := f.Parse(args); err != nil {
		return c, err
	}
	if f.NArg() != 0 {
		return c, fmt.Errorf("unexpected positional arguments: %v", f.Args())
	}
	if c.input == "" {
		return c, errors.New("-input is required")
	}
	if c.prometheusURL == "" && c.otlpURL == "" && !c.dryRun {
		return c, errors.New("set -prometheus-url, -otlp-url, or -dry-run")
	}
	for _, endpoint := range []string{c.prometheusURL, c.otlpURL} {
		if endpoint == "" {
			continue
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" || u.User != nil {
			return c, errors.New("destinations must be absolute http(s) URLs without credentials or fragments; use headers for authentication")
		}
	}
	if c.workers < 1 || c.batchSize < 1 || c.batchBytes < 1024 || c.decoderWorkers < 1 ||
		c.seriesLimit < 1 || c.maxLineBytes < 1024 || c.retries < 0 || c.limit < 0 ||
		c.timeout <= 0 || c.progress < 0 {
		return c, errors.New("invalid limits: counts must be positive, byte limits >= 1024, retries/limit/progress nonnegative, timeout positive")
	}
	if _, err := timestampScale(c.timestampUnit); err != nil {
		return c, err
	}
	if c.rebase != "" && c.shift != 0 {
		return c, errors.New("-rebase and -shift are mutually exclusive")
	}
	if c.rebase != "" && c.rebase != "now" {
		if _, err := parseTarget(c.rebase); err != nil {
			return c, err
		}
	}
	if c.rebaseAnchor != "end" && c.rebaseAnchor != "first" {
		return c, errors.New("-rebase-anchor must be end or first")
	}
	if c.input == "-" && c.rebase != "" && c.rebaseAnchor == "end" {
		return c, errors.New("rebasing the end requires a seekable file; use -rebase-anchor=first or -shift with stdin")
	}
	if c.otlpCompression != "gzip" && c.otlpCompression != "none" {
		return c, errors.New("-otlp-compression must be gzip or none")
	}
	if c.attributeMode != "semantic" && c.attributeMode != "all" && c.attributeMode != "none" {
		return c, errors.New("-attribute-mode must be semantic, all, or none")
	}
	return c, nil
}

func (c config) destinations() (prom, otlp bool) {
	prom, otlp = c.prometheusURL != "", c.otlpURL != ""
	if c.dryRun && !prom && !otlp {
		return true, true
	}
	return
}

func headers(values []string) (http.Header, error) {
	h := make(http.Header)
	for _, v := range values {
		k, value, ok := strings.Cut(v, "=")
		k = http.CanonicalHeaderKey(strings.TrimSpace(k))
		if !ok || k == "" || strings.ContainsAny(k+value, "\r\n") {
			return nil, errors.New("headers must be Name=Value without newlines")
		}
		for _, b := range []byte(value) {
			if b < 0x20 && b != '\t' || b == 0x7f {
				return nil, errors.New("HTTP header values cannot contain control characters")
			}
		}
		for _, ch := range k {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", ch)) {
				return nil, errors.New("invalid HTTP header name")
			}
		}
		switch k {
		case "Content-Type", "Content-Encoding", "Content-Length", "User-Agent", "X-Prometheus-Remote-Write-Version", "Host", "Transfer-Encoding":
			return nil, fmt.Errorf("header %s is controlled by the importer", k)
		}
		h.Add(k, strings.TrimSpace(value))
	}
	return h, nil
}
