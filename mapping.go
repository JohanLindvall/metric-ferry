package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/prometheus/prometheus/prompb"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// Explicitly restore namespace separators, including conventions that retain
// underscores inside a component (cloud.availability_zone, http.response.status_code).
// Unknown application labels stay unchanged unless configured by the user.
var resourceKeys = strings.Fields(`
service.name service.namespace service.instance.id service.version
deployment.environment.name
cloud.provider cloud.account.id cloud.region cloud.availability_zone cloud.platform cloud.resource_id
host.id host.name host.type host.arch host.image.id host.image.name host.image.version host.ip
container.id container.name container.runtime container.image.name container.image.id
container.image.repo_digests container.image.tags
k8s.cluster.name k8s.cluster.uid k8s.namespace.name
k8s.node.name k8s.node.uid k8s.pod.name k8s.pod.uid k8s.pod.ip
k8s.container.name k8s.container.restart_count
k8s.deployment.name k8s.deployment.uid k8s.replicaset.name k8s.replicaset.uid
k8s.statefulset.name k8s.statefulset.uid k8s.daemonset.name k8s.daemonset.uid
k8s.job.name k8s.job.uid k8s.cronjob.name k8s.cronjob.uid
process.pid process.parent_pid process.executable.name process.executable.path
process.command process.command_line process.owner
process.runtime.name process.runtime.version process.runtime.description
os.type os.name os.version os.description os.build.id
telemetry.sdk.name telemetry.sdk.language telemetry.sdk.version telemetry.distro.name telemetry.distro.version
faas.name faas.id faas.version faas.instance faas.max_memory
`)

var datapointKeys = strings.Fields(`
http.request.method http.response.status_code http.route http.request.resend_count
http.request.body.size http.response.body.size http.server.request.duration
server.address server.port client.address client.port
network.transport network.type network.protocol.name network.protocol.version
network.local.address network.local.port network.peer.address network.peer.port
url.scheme url.path url.query url.full
error.type db.system.name db.namespace db.collection.name db.operation.name
db.query.text db.response.status_code
rpc.system rpc.service rpc.method rpc.grpc.status_code
messaging.system messaging.destination.name messaging.destination.template
messaging.operation.name messaging.operation.type messaging.consumer.group.name
messaging.kafka.partition messaging.kafka.message.key
thread.id thread.name
`)

type mappingRules struct {
	Rename    map[string]string `json:"rename"`
	Resource  []string          `json:"resource"`
	Datapoint []string          `json:"datapoint"`
}

type attributeMapper struct {
	mode      string
	rename    map[string]string
	resource  map[string]bool
	placement map[string]bool
}

func newMapper(c config) (*attributeMapper, error) {
	m := &attributeMapper{
		mode: c.attributeMode, rename: make(map[string]string),
		resource: make(map[string]bool), placement: make(map[string]bool),
	}
	for _, key := range resourceKeys {
		m.resource[key] = true
	}
	if c.attributeMode != "none" {
		for _, key := range append(slices.Clone(resourceKeys), datapointKeys...) {
			m.rename[strings.ReplaceAll(key, ".", "_")] = key
		}
		m.rename["deployment_environment"] = "deployment.environment.name"
		m.rename["deployment.environment"] = "deployment.environment.name"
	}
	var rules mappingRules
	if c.mappingFile != "" {
		f, err := os.Open(c.mappingFile)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		dec := json.NewDecoder(io.LimitReader(f, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rules); err != nil {
			return nil, fmt.Errorf("mapping file: %w", err)
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			return nil, errors.New("mapping file must contain exactly one JSON object")
		}
	}
	for source, target := range rules.Rename {
		if source == "" || target == "" || source == "__name__" || target == "__name__" {
			return nil, errors.New("mapping keys must be nonempty and cannot be __name__")
		}
		m.rename[source] = target
	}
	for _, rule := range c.renames {
		source, target, ok := strings.Cut(rule, "=")
		if !ok || source == "" || target == "" || source == "__name__" || target == "__name__" {
			return nil, fmt.Errorf("-rename must be source=target with nonempty, nonreserved names")
		}
		m.rename[source] = target
	}
	for _, key := range append(rules.Resource, c.resources...) {
		if key == "" {
			return nil, errors.New("empty resource attribute rule")
		}
		m.placement[key] = true
	}
	for _, key := range append(rules.Datapoint, c.attributes...) {
		if key == "" {
			return nil, errors.New("empty datapoint attribute rule")
		}
		if promote, exists := m.placement[key]; exists && promote {
			return nil, fmt.Errorf("%s cannot be forced onto both resources and data points", key)
		}
		m.placement[key] = false
	}
	return m, nil
}

func (m *attributeMapper) mapLabels(labels []prompb.Label) (resource, attributes []*commonpb.KeyValue, err error) {
	values := make(map[string]string, len(labels))
	for _, label := range labels {
		if label.Name == "__name__" {
			continue
		}
		key := label.Name
		if renamed, ok := m.rename[key]; ok {
			key = renamed
		} else if m.mode == "all" {
			key = strings.ReplaceAll(key, "_", ".")
		}
		if old, exists := values[key]; exists && old != label.Value {
			return nil, nil, fmt.Errorf("labels map to %q with conflicting values; supply explicit -rename rules", key)
		}
		values[key] = label.Value
	}
	// Prometheus job/instance identify a service when explicit OTel labels are
	// absent. Keep the original labels as dimensions so distinct source series
	// cannot be silently merged by resource fallback.
	if m.mode != "none" {
		if job := values["job"]; job != "" && values["service.name"] == "" {
			namespace, name, hasNamespace := strings.Cut(job, "/")
			if hasNamespace {
				values["service.name"] = name
				if values["service.namespace"] == "" {
					values["service.namespace"] = namespace
				}
			} else {
				values["service.name"] = job
			}
		}
		if instance := values["instance"]; instance != "" && values["service.instance.id"] == "" {
			values["service.instance.id"] = instance
		}
	}
	for key, value := range values {
		if value == "" {
			continue
		}
		attr := &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
		isResource, explicit := m.placement[key]
		if !explicit {
			isResource = m.resource[key]
		}
		if isResource {
			resource = append(resource, attr)
		} else {
			attributes = append(attributes, attr)
		}
	}
	compare := func(a, b *commonpb.KeyValue) int { return strings.Compare(a.Key, b.Key) }
	slices.SortFunc(resource, compare)
	slices.SortFunc(attributes, compare)
	return resource, attributes, nil
}

func attributeKey(attrs []*commonpb.KeyValue) string {
	var buf []byte
	for _, a := range attrs {
		buf = appendKey(buf, a.Key, a.Value.GetStringValue())
	}
	return string(buf)
}
