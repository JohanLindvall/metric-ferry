package main

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/s2"
	"github.com/prometheus/prometheus/prompb"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

const version = "0.1.0"

type encoder struct {
	protocol    string
	compression string
	raw, snappy []byte
	gzipBuffer  bytes.Buffer
	gzipWriter  *gzip.Writer
	numbers     []metricspb.NumberDataPoint
	values      []metricspb.NumberDataPoint_AsDouble
}

func (e *encoder) encode(points []point) ([]byte, int, error) {
	var err error
	switch e.protocol {
	case "prometheus":
		req := prometheusRequest(points)
		size := req.Size()
		if cap(e.raw) < size {
			e.raw = make([]byte, size)
		}
		e.raw = e.raw[:size]
		_, err = req.MarshalToSizedBuffer(e.raw)
	case "otlp":
		req := e.otlpRequest(points)
		e.raw, err = proto.MarshalOptions{}.MarshalAppend(e.raw[:0], req)
	default:
		return nil, 0, fmt.Errorf("unknown protocol %s", e.protocol)
	}
	if err != nil {
		return nil, 0, err
	}
	rawSize := len(e.raw)
	if e.protocol == "prometheus" {
		// EncodeSnappy emits the Snappy BLOCK format required by remote write.
		e.snappy = s2.EncodeSnappy(e.snappy[:0], e.raw)
		return e.snappy, rawSize, nil
	}
	if e.compression == "none" {
		return e.raw, rawSize, nil
	}
	e.gzipBuffer.Reset()
	if e.gzipWriter == nil {
		e.gzipWriter, err = gzip.NewWriterLevel(&e.gzipBuffer, gzip.BestSpeed)
		if err != nil {
			return nil, 0, err
		}
	} else {
		e.gzipWriter.Reset(&e.gzipBuffer)
	}
	if _, err := e.gzipWriter.Write(e.raw); err != nil {
		return nil, 0, err
	}
	if err := e.gzipWriter.Close(); err != nil {
		return nil, 0, err
	}
	return e.gzipBuffer.Bytes(), rawSize, nil
}

func prometheusRequest(points []point) *prompb.WriteRequest {
	req := &prompb.WriteRequest{}
	index := make(map[*series]int)
	for _, p := range points {
		i, ok := index[p.series]
		if !ok {
			i = len(req.Timeseries)
			index[p.series] = i
			req.Timeseries = append(req.Timeseries, prompb.TimeSeries{Labels: p.series.labels})
		}
		s := &req.Timeseries[i]
		ms := unixMillis(p.timestamp)
		// Equal values at the same millisecond have already been validated.
		if len(s.Samples) > 0 && s.Samples[len(s.Samples)-1].Timestamp == ms {
			continue
		}
		s.Samples = append(s.Samples, prompb.Sample{Value: p.value, Timestamp: ms})
	}
	return req
}

func unixMillis(ns int64) int64 {
	ms := ns / 1e6
	if ns < 0 && ns%1e6 != 0 {
		ms--
	}
	return ms
}

type resourceGroup struct {
	resource *metricspb.ResourceMetrics
	metrics  map[string]*metricspb.Metric
}

func (e *encoder) otlpRequest(points []point) *collectorpb.ExportMetricsServiceRequest {
	req := &collectorpb.ExportMetricsServiceRequest{}
	resources := make(map[string]*resourceGroup)
	// Reuse the two largest collections of protobuf objects between batches.
	if cap(e.numbers) < len(points) {
		e.numbers = make([]metricspb.NumberDataPoint, len(points))
		e.values = make([]metricspb.NumberDataPoint_AsDouble, len(points))
	} else {
		e.numbers = e.numbers[:len(points)]
		e.values = e.values[:len(points)]
	}
	for i, p := range points {
		s := p.series
		group := resources[s.resourceKey]
		if group == nil {
			group = &resourceGroup{
				resource: &metricspb.ResourceMetrics{
					Resource: &resourcepb.Resource{Attributes: s.resource},
					ScopeMetrics: []*metricspb.ScopeMetrics{{
						Scope: &commonpb.InstrumentationScope{Name: "github.com/JohanLindvall/metric-ferry", Version: version},
					}},
				},
				metrics: make(map[string]*metricspb.Metric),
			}
			resources[s.resourceKey] = group
			req.ResourceMetrics = append(req.ResourceMetrics, group.resource)
		}
		metric := group.metrics[s.name]
		if metric == nil {
			metric = &metricspb.Metric{Name: s.name, Description: s.metadata.help, Unit: s.metadata.unit}
			if s.counter {
				metric.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					IsMonotonic:            true,
				}}
			} else {
				metric.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}
			}
			group.metrics[s.name] = metric
			scope := group.resource.ScopeMetrics[0]
			scope.Metrics = append(scope.Metrics, metric)
		}
		e.values[i].AsDouble = p.value
		e.numbers[i] = metricspb.NumberDataPoint{
			Attributes: s.attributes, TimeUnixNano: uint64(p.timestamp), Value: &e.values[i],
		}
		if s.counter {
			metric.GetSum().DataPoints = append(metric.GetSum().DataPoints, &e.numbers[i])
		} else {
			metric.GetGauge().DataPoints = append(metric.GetGauge().DataPoints, &e.numbers[i])
		}
	}
	return req
}
