package compat07

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// cloneMetrics deep-copies a metric slice so mutations from conversion
// don't affect subsequent benchmark iterations.
func cloneMetrics(metrics []*metricspb.Metric) []*metricspb.Metric {
	result := make([]*metricspb.Metric, len(metrics))
	for i, m := range metrics {
		result[i] = proto.Clone(m).(*metricspb.Metric)
	}
	return result
}

// pure1xMetrics returns a single-element slice with a clean 1.x Gauge metric.
func pure1xMetrics() []*metricspb.Metric {
	return []*metricspb.Metric{
		{
			Name: "bench_gauge",
			Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{
						{
							Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
							Attributes: []*commonpb.KeyValue{
								{
									Key:   "env",
									Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "prod"}},
								},
							},
						},
					},
				},
			},
		},
	}
}

func BenchmarkHasField(b *testing.B) {
	// Use int_gauge fixture: metric has unknown field 4 (IntGauge).
	metrics := loadFixture(b, "int_gauge.binpb")
	raw := metrics[0].ProtoReflect().GetUnknown()

	b.Run("no_match", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			hasField(raw, 99)
		}
	})
	b.Run("match", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			hasField(raw, 4)
		}
	})
}

func BenchmarkHas07Data_Pure1x(b *testing.B) {
	metrics := pure1xMetrics()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Has07Data(metrics)
	}
}

func BenchmarkDetectAndConvertMetrics_1xPassthrough(b *testing.B) {
	metrics := pure1xMetrics()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DetectAndConvertMetrics(metrics)
	}
}

func BenchmarkDetectAndConvertMetrics_IntGauge(b *testing.B) {
	metrics := loadFixture(b, "int_gauge.binpb")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DetectAndConvertMetrics(metrics)
	}
}

func BenchmarkDetectAndConvertMetrics_IntSum(b *testing.B) {
	metrics := loadFixture(b, "int_sum_delta_monotonic.binpb")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DetectAndConvertMetrics(metrics)
	}
}

func BenchmarkDetectAndConvertMetrics_IntHistogram(b *testing.B) {
	metrics := loadFixture(b, "int_histogram.binpb")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DetectAndConvertMetrics(metrics)
	}
}

func BenchmarkDetectAndConvertMetrics_LabelsOnly(b *testing.B) {
	// Labels conversion mutates data points in place, so clone per iteration.
	original := loadFixture(b, "labels_only.binpb")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		metrics := cloneMetrics(original)
		b.StartTimer()
		DetectAndConvertMetrics(metrics)
	}
}

func BenchmarkDetectAndConvertMetrics_Mixed(b *testing.B) {
	// Mixed payload has 1.x metrics with labels that get mutated.
	original := loadFixture(b, "mixed_07_1x.binpb")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		metrics := cloneMetrics(original)
		b.StartTimer()
		DetectAndConvertMetrics(metrics)
	}
}
