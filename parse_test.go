package main

import (
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func testConfig(t *testing.T, extra ...string) config {
	t.Helper()
	c, err := parseConfig(append([]string{"-input", "unused.zst", "-dry-run", "-progress", "0"}, extra...), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseEscapesAndCanonicalIdentity(t *testing.T) {
	p := newParser(testConfig(t), nil)
	first, err := p.get([]byte(`metric{z="quote: \" slash: \\ newline: \n } # value",a="a b",empty=""}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.get([]byte(`metric{a="a b",z="quote: \" slash: \\ newline: \n } # value"}`))
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(p.canonical) != 1 {
		t.Fatal("label reordering/empty labels changed the series identity")
	}
	want := []prompb.Label{
		{Name: "__name__", Value: "metric"}, {Name: "a", Value: "a b"},
		{Name: "z", Value: "quote: \" slash: \\ newline: \n } # value"},
	}
	if !slices.EqualFunc(first.labels, want, func(a, b prompb.Label) bool {
		return a.Name == b.Name && a.Value == b.Value
	}) {
		t.Fatalf("got %#v", first.labels)
	}
	line := []byte(`metric{a="} # value"} 1.5 1700000000.000000001`)
	prefix, value, ts, _, err := splitSample(line, false)
	if err != nil || string(prefix) != `metric{a="} # value"}` || string(value) != "1.5" || string(ts) != "1700000000.000000001" {
		t.Fatalf("split failed: %q %q %q %v", prefix, value, ts, err)
	}
	for _, input := range []string{
		`metric{a="x",a="y"}`, `metric{a="x"} tail`, `metric{a="bad\t"}`,
		`metric{a=x}`, `metric{__name__="x"}`, `metric{a="unfinished}`, `metric{a="x"`,
	} {
		if _, _, err := parseLabels(input); err == nil {
			t.Errorf("accepted invalid label set %s", input)
		}
	}
}

func TestExemplarsRequireExplicitDiscard(t *testing.T) {
	line := []byte(`requests_total{a="x"} 2 1700000000 # {trace_id="abc"} 1 1700000000`)
	if _, _, _, _, err := splitSample(line, false); err == nil {
		t.Fatal("silently discarded an exemplar")
	}
	prefix, value, ts, dropped, err := splitSample(line, true)
	if err != nil || !dropped || string(prefix) != `requests_total{a="x"}` || string(value) != "2" || string(ts) != "1700000000" {
		t.Fatalf("wrong exemplar stripping: %q %q %q %v", prefix, value, ts, err)
	}
}

func attrsMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string)
	for _, a := range attrs {
		m[a.Key] = a.Value.GetStringValue()
	}
	return m
}

func TestSemanticMapping(t *testing.T) {
	c := testConfig(t, "-rename", "platform_product_name=platform.product.name", "-resource-attribute", "platform.product.name")
	mapper, err := newMapper(c)
	if err != nil {
		t.Fatal(err)
	}
	p := newParser(c, mapper)
	s, err := p.get([]byte(`requests_total{service_name="orders",service_namespace="shop",k8s_pod_name="pod",deployment_environment="prod",deployment_environment_name="prod",cloud_availability_zone="a",http_response_status_code="200",cache_type="hot",platform_product_name="shop",job="other/service",instance="host:80"}`))
	if err != nil {
		t.Fatal(err)
	}
	r, a := attrsMap(s.resource), attrsMap(s.attributes)
	if r["service.name"] != "orders" || r["service.namespace"] != "shop" || r["service.instance.id"] != "host:80" ||
		r["deployment.environment.name"] != "prod" || r["k8s.pod.name"] != "pod" || r["cloud.availability_zone"] != "a" ||
		r["platform.product.name"] != "shop" {
		t.Fatalf("incorrect resource attributes: %#v", r)
	}
	if a["http.response.status_code"] != "200" || a["cache_type"] != "hot" || a["job"] != "other/service" ||
		a["instance"] != "host:80" || a["service.name"] != "" {
		t.Fatalf("incorrect datapoint attributes: %#v", a)
	}
	if _, err := p.get([]byte(`bad{deployment_environment="a",deployment_environment_name="b"}`)); err == nil {
		t.Fatal("mapping collision should fail")
	}
	if _, err := p.get([]byte(`x{deployment_environment="prod"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.get([]byte(`x{deployment_environment_name="prod"}`)); err == nil {
		t.Fatal("distinct source series merged silently")
	}
}

func TestMappingOverridesAndFallback(t *testing.T) {
	c := testConfig(t, "-attribute-mode", "all", "-datapoint-attribute", "k8s.pod.name")
	m, err := newMapper(c)
	if err != nil {
		t.Fatal(err)
	}
	p := newParser(c, m)
	s, err := p.get([]byte(`x{job="namespace/app",instance="i",cache_type="x",k8s_pod_name="pod",cloud_availability_zone="zone"}`))
	if err != nil {
		t.Fatal(err)
	}
	r, a := attrsMap(s.resource), attrsMap(s.attributes)
	if a["cache.type"] != "x" || a["k8s.pod.name"] != "pod" || r["k8s.pod.name"] != "" ||
		r["service.namespace"] != "namespace" || r["service.name"] != "app" || r["cloud.availability_zone"] != "zone" {
		t.Fatalf("overrides failed: resource=%v data=%v", r, a)
	}
}

func TestMetadataAndCacheLimit(t *testing.T) {
	c := testConfig(t, "-series-limit", "2", "-infer-counters=false")
	p := newParser(c, nil)
	for _, line := range []string{"# TYPE requests counter", "# HELP requests Number of requests", "# UNIT requests seconds"} {
		if err := p.comment([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := p.get([]byte("requests_total"))
	if err != nil || !s.counter || s.metadata.unit != "s" || s.metadata.help != "Number of requests" {
		t.Fatalf("metadata not applied: %#v %v", s, err)
	}
	if err := p.comment([]byte("# TYPE requests gauge")); err == nil {
		t.Fatal("accepted changing metric type while workers use it")
	}
	if _, err := p.get([]byte("other")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.get([]byte("third")); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("series cache not bounded: %v", err)
	}
}

func FuzzSampleParsing(f *testing.F) {
	for _, seed := range []string{`m{a="b"} 1 1700000000`, `m{a="x\\\" # }"} NaN 0`, `m 1 0`, `m{a=""} 1 1`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		prefix, value, timestamp, _, err := splitSample([]byte(text), false)
		if err != nil {
			return
		}
		parseLabels(string(prefix))
		parseValue(value)
		parseTimestamp(timestamp, 1e9)
	})
}
