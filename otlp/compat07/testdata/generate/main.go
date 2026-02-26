// Generator creates .binpb test fixtures from real OTLP v0.7.0 proto types.
// These fixtures are used by compat07 tests to verify conversion of 0.7 data
// that arrives as protobuf unknown fields when deserialized with 1.x types.
//
// Run: go run . (from compat07/testdata/generate/)
// Output: ../int_gauge.binpb, ../int_sum_delta_monotonic.binpb, etc.
package main

import (
	"log"
	"os"
	"path/filepath"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

const (
	startNano uint64 = 1_000_000_000_000_000_000 // 2001-09-09T01:46:40Z
	endNano   uint64 = 1_000_000_060_000_000_000 // 60 seconds later
)

func main() {
	outDir := filepath.Join("..")

	fixtures := map[string]*collectormetrics.ExportMetricsServiceRequest{
		"int_gauge.binpb":                    intGaugeRequest(),
		"int_sum_delta_monotonic.binpb":      intSumRequest(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA, true),
		"int_sum_cumulative_nonmonotonic.binpb": intSumRequest(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, false),
		"int_histogram.binpb":                intHistogramRequest(),
		"int_histogram_zero_buckets.binpb":   intHistogramZeroBucketsRequest(),
		"mixed_07_1x.binpb":                  mixedRequest(),
		"labels_only.binpb":                  labelsOnlyRequest(),
		"histogram_with_labels.binpb":        histogramWithLabelsRequest(),
		"summary_with_labels.binpb":          summaryWithLabelsRequest(),
	}

	for name, req := range fixtures {
		data, err := proto.Marshal(req)
		if err != nil {
			log.Fatalf("marshal %s: %v", name, err)
		}
		path := filepath.Join(outDir, name)
		if err := os.WriteFile(path, data, 0644); err != nil {
			log.Fatalf("write %s: %v", name, err)
		}
		log.Printf("wrote %s (%d bytes)", path, len(data))
	}
}

// wrapMetrics wraps metrics in a full ExportMetricsServiceRequest with
// a resource and instrumentation library.
func wrapMetrics(metrics ...*metricspb.Metric) *collectormetrics.ExportMetricsServiceRequest {
	return &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key:   "service.name",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}},
						},
					},
				},
				InstrumentationLibraryMetrics: []*metricspb.InstrumentationLibraryMetrics{
					{
						InstrumentationLibrary: &commonpb.InstrumentationLibrary{
							Name:    "test-library",
							Version: "0.7.0",
						},
						Metrics: metrics,
					},
				},
			},
		},
	}
}

func testLabels() []*commonpb.StringKeyValue {
	return []*commonpb.StringKeyValue{
		{Key: "host", Value: "server-01"},
		{Key: "region", Value: "us-east-1"},
	}
}

func testExemplar() *metricspb.IntExemplar {
	return &metricspb.IntExemplar{
		FilteredLabels: []*commonpb.StringKeyValue{
			{Key: "thread", Value: "main"},
		},
		TimeUnixNano: endNano,
		Value:        42,
		SpanId:       []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		TraceId:      []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
	}
}

// intGaugeRequest creates an IntGauge metric with labels and an exemplar.
func intGaugeRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name:        "system.cpu.utilization",
		Description: "CPU utilization",
		Unit:        "%",
		Data: &metricspb.Metric_IntGauge{
			IntGauge: &metricspb.IntGauge{
				DataPoints: []*metricspb.IntDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Value:             72,
						Exemplars:         []*metricspb.IntExemplar{testExemplar()},
					},
				},
			},
		},
	})
}

// intSumRequest creates an IntSum metric with the given temporality and monotonicity.
func intSumRequest(temporality metricspb.AggregationTemporality, monotonic bool) *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name:        "http.server.request_count",
		Description: "Total HTTP requests",
		Unit:        "1",
		Data: &metricspb.Metric_IntSum{
			IntSum: &metricspb.IntSum{
				DataPoints: []*metricspb.IntDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Value:             1523,
					},
				},
				AggregationTemporality: temporality,
				IsMonotonic:            monotonic,
			},
		},
	})
}

// intHistogramRequest creates an IntHistogram with buckets and an exemplar.
func intHistogramRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name:        "http.server.duration",
		Description: "HTTP request duration",
		Unit:        "ms",
		Data: &metricspb.Metric_IntHistogram{
			IntHistogram: &metricspb.IntHistogram{
				DataPoints: []*metricspb.IntHistogramDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Count:             100,
						Sum:               15234,
						BucketCounts:      []uint64{10, 25, 30, 20, 10, 5},
						ExplicitBounds:    []float64{10, 50, 100, 200, 500},
						Exemplars:         []*metricspb.IntExemplar{testExemplar()},
					},
				},
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			},
		},
	})
}

// intHistogramZeroBucketsRequest creates an IntHistogram with zero buckets (edge case).
func intHistogramZeroBucketsRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name:        "http.server.duration.sparse",
		Description: "Sparse histogram",
		Unit:        "ms",
		Data: &metricspb.Metric_IntHistogram{
			IntHistogram: &metricspb.IntHistogram{
				DataPoints: []*metricspb.IntHistogramDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Count:             0,
						Sum:               0,
						// No bucket_counts or explicit_bounds
					},
				},
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			},
		},
	})
}

// mixedRequest creates a request with both a 0.7 IntGauge and a 1.x-era DoubleGauge.
// This tests that the converter handles mixed payloads correctly (AC4).
func mixedRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(
		// 0.7-era metric
		&metricspb.Metric{
			Name: "cpu.int_gauge",
			Data: &metricspb.Metric_IntGauge{
				IntGauge: &metricspb.IntGauge{
					DataPoints: []*metricspb.IntDataPoint{
						{
							Labels:       testLabels(),
							TimeUnixNano: endNano,
							Value:        50,
						},
					},
				},
			},
		},
		// 1.x-era metric (DoubleGauge existed in both 0.7 and 1.x, field 5)
		&metricspb.Metric{
			Name: "cpu.double_gauge",
			Data: &metricspb.Metric_DoubleGauge{
				DoubleGauge: &metricspb.DoubleGauge{
					DataPoints: []*metricspb.DoubleDataPoint{
						{
							Labels:       testLabels(),
							TimeUnixNano: endNano,
							Value:        75.5,
						},
					},
				},
			},
		},
	)
}

// labelsOnlyRequest creates a 1.x-type DoubleSum metric but with StringKeyValue
// labels (field 1) on the data points. When deserialized with upstream 1.x types,
// the DoubleSum → Sum mapping works (field 7), but the labels go to unknown fields
// since the upstream removed field 1 from NumberDataPoint.
// This tests the labels-to-attributes conversion path (AC2) independent of the
// metric type conversion path (AC1).
func labelsOnlyRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name: "http.server.active_requests",
		Unit: "1",
		Data: &metricspb.Metric_DoubleSum{
			DoubleSum: &metricspb.DoubleSum{
				DataPoints: []*metricspb.DoubleDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Value:             42.0,
					},
				},
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				IsMonotonic:            true,
			},
		},
	})
}

// histogramWithLabelsRequest creates a 1.x-type DoubleHistogram metric but with
// StringKeyValue labels (field 1) on the data points. When deserialized with
// upstream 1.x types, DoubleHistogram → Histogram (field 9), but the labels go
// to unknown fields since v1.9.0 removed field 1 from HistogramDataPoint.
// This tests AC2.2 (labels on HistogramDataPoint) independently.
func histogramWithLabelsRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name: "http.server.duration.double",
		Unit: "ms",
		Data: &metricspb.Metric_DoubleHistogram{
			DoubleHistogram: &metricspb.DoubleHistogram{
				DataPoints: []*metricspb.DoubleHistogramDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Count:             50,
						Sum:               2500.0,
						BucketCounts:      []uint64{5, 15, 20, 10},
						ExplicitBounds:    []float64{10, 50, 100},
					},
				},
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			},
		},
	})
}

// summaryWithLabelsRequest creates a DoubleSummary metric with StringKeyValue labels
// (field 1) on the DoubleSummaryDataPoint. DoubleSummary existed in both 0.7 and 1.x
// (field 12 on Metric), so the metric type is recognized, but labels go to
// unknown fields since v1.9.0 removed field 1 from SummaryDataPoint.
// This tests AC2.3 (labels on SummaryDataPoint) independently.
func summaryWithLabelsRequest() *collectormetrics.ExportMetricsServiceRequest {
	return wrapMetrics(&metricspb.Metric{
		Name: "http.server.duration.summary",
		Unit: "ms",
		Data: &metricspb.Metric_DoubleSummary{
			DoubleSummary: &metricspb.DoubleSummary{
				DataPoints: []*metricspb.DoubleSummaryDataPoint{
					{
						Labels:            testLabels(),
						StartTimeUnixNano: startNano,
						TimeUnixNano:      endNano,
						Count:             100,
						Sum:               5000.0,
						QuantileValues: []*metricspb.DoubleSummaryDataPoint_ValueAtQuantile{
							{Quantile: 0.5, Value: 45.0},
							{Quantile: 0.99, Value: 120.0},
						},
					},
				},
			},
		},
	})
}
