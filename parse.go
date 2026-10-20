package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/prompb"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

type metricMetadata struct {
	kind, help, unit string
}

// Definitions are immutable after construction, except for timestamp validation
// fields owned exclusively by the reader goroutine. Workers never read those.
type series struct {
	name, key, resourceKey string
	labels                 []prompb.Label
	resource, attributes   []*commonpb.KeyValue
	metadata               metricMetadata
	counter                bool
	shard                  uint64
	estimatedBytes         int
	lastNS, lastMS         int64
	lastValue              uint64
	seen                   bool
}

type point struct {
	series    *series
	timestamp int64
	value     float64
}

type parser struct {
	raw         map[string]*series
	canonical   map[string]*series
	otelIDs     map[string]string
	metadata    map[string]metricMetadata
	mapper      *attributeMapper
	maxSeries   int
	infer       bool
	scalarTypes map[string]bool
}

func newParser(c config, mapper *attributeMapper) *parser {
	return &parser{
		raw: make(map[string]*series), canonical: make(map[string]*series),
		otelIDs: make(map[string]string), metadata: make(map[string]metricMetadata),
		mapper: mapper, maxSeries: c.seriesLimit, infer: c.inferCounters,
		scalarTypes: make(map[string]bool),
	}
}

func isSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }

func trimSpace(b []byte) []byte {
	for len(b) > 0 && isSpace(b[0]) {
		b = b[1:]
	}
	for len(b) > 0 && isSpace(b[len(b)-1]) {
		b = b[:len(b)-1]
	}
	return b
}

func lastToken(b []byte) (rest, token []byte) {
	b = trimSpace(b)
	i := len(b)
	for i > 0 && !isSpace(b[i-1]) {
		i--
	}
	return trimSpace(b[:i]), b[i:]
}

// Split from the end so the hot path does not scan the (often very long) label
// set. A map lookup on string(prefix) does not allocate in Go.
func splitSample(line []byte, dropExemplars bool) (prefix, value, timestamp []byte, dropped bool, err error) {
	if bytes.IndexByte(line, '#') >= 0 {
		quoted, escaped := false, false
		for i, b := range line {
			if escaped {
				escaped = false
				continue
			}
			if quoted && b == '\\' {
				escaped = true
			} else if b == '"' {
				quoted = !quoted
			} else if b == '#' && !quoted {
				if !dropExemplars {
					return nil, nil, nil, false, errors.New("exemplars are not supported; use -drop-exemplars to explicitly discard them")
				}
				line, dropped = trimSpace(line[:i]), true
				break
			}
		}
	}
	rest, timestamp := lastToken(line)
	prefix, value = lastToken(rest)
	if len(prefix) == 0 || len(value) == 0 || len(timestamp) == 0 {
		return nil, nil, nil, dropped, errors.New("expected metric{labels} value timestamp; explicit timestamps are required")
	}
	return prefix, value, timestamp, dropped, nil
}

func nameStart(b byte, metric bool) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b == '_' || metric && b == ':'
}

func takeName(s string, metric bool) (name, rest string, err error) {
	if s == "" {
		return "", "", errors.New("missing name")
	}
	if s[0] == '"' {
		return takeQuoted(s)
	}
	if !nameStart(s[0], metric) {
		return "", "", fmt.Errorf("invalid name at %q", s)
	}
	i := 1
	for i < len(s) && (nameStart(s[i], metric) || s[i] >= '0' && s[i] <= '9') {
		i++
	}
	return s[:i], s[i:], nil
}

func takeQuoted(s string) (string, string, error) {
	if s == "" || s[0] != '"' {
		return "", "", errors.New("label values must be quoted")
	}
	var out strings.Builder
	start := 1
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '"':
			if out.Len() == 0 {
				return s[start:i], s[i+1:], nil
			}
			out.WriteString(s[start:i])
			return out.String(), s[i+1:], nil
		case '\\':
			out.WriteString(s[start:i])
			i++
			if i >= len(s) {
				return "", "", errors.New("unfinished label escape")
			}
			switch s[i] {
			case '\\', '"':
				out.WriteByte(s[i])
			case 'n':
				out.WriteByte('\n')
			default:
				return "", "", fmt.Errorf("invalid label escape \\%c", s[i])
			}
			start = i + 1
		}
	}
	return "", "", errors.New("unterminated quoted string")
}

func parseLabels(raw string) (string, []prompb.Label, error) {
	if !utf8.ValidString(raw) {
		return "", nil, errors.New("metric/labels contain invalid UTF-8")
	}
	name, rest, err := takeName(raw, true)
	if err != nil {
		return "", nil, fmt.Errorf("invalid metric name: %w", err)
	}
	if name == "" {
		return "", nil, errors.New("empty metric name")
	}
	labels := []prompb.Label{{Name: "__name__", Value: name}}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return name, labels, nil
	}
	if rest[0] != '{' {
		return "", nil, errors.New("unexpected characters after metric name")
	}
	rest = strings.TrimSpace(rest[1:])
	seen := make(map[string]bool)
	for rest != "" && rest[0] != '}' {
		var key, value string
		key, rest, err = takeName(rest, false)
		if err != nil || key == "" {
			return "", nil, errors.New("invalid label name")
		}
		if key == "__name__" || seen[key] {
			return "", nil, fmt.Errorf("duplicate or reserved label %q", key)
		}
		seen[key] = true
		rest = strings.TrimSpace(rest)
		if rest == "" || rest[0] != '=' {
			return "", nil, fmt.Errorf("missing = after label %q", key)
		}
		value, rest, err = takeQuoted(strings.TrimSpace(rest[1:]))
		if err != nil {
			return "", nil, err
		}
		// Empty labels are equivalent to absent labels in Prometheus.
		if value != "" {
			labels = append(labels, prompb.Label{Name: key, Value: value})
		}
		rest = strings.TrimSpace(rest)
		if rest != "" && rest[0] == '}' {
			break
		}
		if rest == "" || rest[0] != ',' {
			return "", nil, errors.New("expected comma or closing brace after label value")
		}
		rest = strings.TrimSpace(rest[1:])
	}
	if rest != "}" {
		return "", nil, errors.New("missing closing brace or trailing characters in label set")
	}
	slices.SortFunc(labels, func(a, b prompb.Label) int { return strings.Compare(a.Name, b.Name) })
	return name, labels, nil
}

func appendKey(buf []byte, k, v string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(k)))
	buf = append(buf, k...)
	buf = binary.AppendUvarint(buf, uint64(len(v)))
	return append(buf, v...)
}

func labelKey(labels []prompb.Label) string {
	var b []byte
	for _, l := range labels {
		b = appendKey(b, l.Name, l.Value)
	}
	return string(b)
}

func (p *parser) get(prefix []byte) (*series, error) {
	if s := p.raw[string(prefix)]; s != nil {
		return s, nil
	}
	if len(p.raw) >= p.maxSeries {
		return nil, fmt.Errorf("series cache limit %d reached; increase -series-limit (cache includes alternate text label orderings)", p.maxSeries)
	}
	raw := string(prefix)
	name, labels, err := parseLabels(raw)
	if err != nil {
		return nil, err
	}
	key := labelKey(labels)
	if s := p.canonical[key]; s != nil {
		p.raw[raw] = s
		return s, nil
	}
	meta, found := p.metadata[name]
	if !found && strings.HasSuffix(name, "_total") {
		meta, found = p.metadata[strings.TrimSuffix(name, "_total")]
	}
	s := &series{name: name, key: key, labels: labels, metadata: meta,
		counter:        meta.kind == "counter" || !found && p.infer && strings.HasSuffix(name, "_total"),
		estimatedBytes: len(raw) + 48}
	s.shard = xxhash.Sum64String(key)
	if p.mapper != nil {
		s.resource, s.attributes, err = p.mapper.mapLabels(labels)
		if err != nil {
			return nil, fmt.Errorf("metric %s: %w", name, err)
		}
		s.resourceKey = attributeKey(s.resource)
		id := string(appendKey(appendKey(nil, name, s.resourceKey), "", attributeKey(s.attributes)))
		if original, ok := p.otelIDs[id]; ok && original != key {
			return nil, fmt.Errorf("attribute mapping merges distinct source series for metric %s; use -rename or -datapoint-attribute to preserve identity", name)
		}
		p.otelIDs[id] = key
	}
	p.raw[raw], p.canonical[key] = s, s
	return s, nil
}

func (p *parser) comment(line []byte) error {
	s := strings.TrimSpace(string(line[1:]))
	kind, rest, _ := strings.Cut(s, " ")
	if kind != "TYPE" && kind != "HELP" && kind != "UNIT" {
		return nil
	}
	name, value, err := takeName(strings.TrimSpace(rest), true)
	if err != nil {
		return err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("empty %s metadata", kind)
	}
	meta := p.metadata[name]
	switch kind {
	case "TYPE":
		switch value {
		case "counter", "gauge", "histogram", "gaugehistogram", "summary", "info", "stateset", "untyped", "unknown":
		default:
			return fmt.Errorf("unsupported TYPE %q", value)
		}
		if value == "histogram" || value == "gaugehistogram" || value == "summary" {
			p.scalarTypes[name] = true
		}
		meta.kind = value
	case "HELP":
		meta.help = strings.NewReplacer("\\n", "\n", "\\\\", "\\").Replace(value)
	case "UNIT":
		meta.unit = ucumUnit(value)
	}
	if meta == p.metadata[name] {
		return nil
	}
	// Do not mutate definitions already being read by export workers.
	for _, existing := range p.canonical {
		if existing.name == name || existing.name == name+"_total" {
			return fmt.Errorf("%s metadata for %s changed after samples; put metadata before samples", kind, name)
		}
	}
	p.metadata[name] = meta
	return nil
}

func ucumUnit(s string) string {
	units := map[string]string{
		"seconds": "s", "milliseconds": "ms", "microseconds": "us", "nanoseconds": "ns",
		"minutes": "min", "hours": "h", "days": "d", "bytes": "By", "kilobytes": "kBy",
		"megabytes": "MBy", "gigabytes": "GBy", "terabytes": "TBy",
		"kibibytes": "KiBy", "mebibytes": "MiBy", "gibibytes": "GiBy", "tebibytes": "TiBy",
		"meters": "m", "volts": "V", "amperes": "A", "joules": "J", "watts": "W",
		"grams": "g", "celsius": "Cel", "hertz": "Hz", "percent": "%",
	}
	if unit, ok := units[s]; ok {
		return unit
	}
	return s
}
