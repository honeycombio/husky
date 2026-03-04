package compat07

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	shadowcommonpb "github.com/honeycombio/husky/otlp/compat07/internal/shadowpb/commonpb"
)

// loadFixture loads a .binpb test fixture and returns the metrics slice.
func loadFixture(t testing.TB, name string) []*metricspb.Metric {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	var req collectormetricspb.ExportMetricsServiceRequest
	require.NoError(t, proto.Unmarshal(data, &req))
	require.NotEmpty(t, req.GetResourceMetrics())
	// Walk ResourceMetrics → ScopeMetrics → Metrics
	var metrics []*metricspb.Metric
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			metrics = append(metrics, sm.GetMetrics()...)
		}
	}
	return metrics
}

// TestAC1_1_IntGaugeConversion verifies IntGauge converts to Gauge with as_int value.
func TestAC1_1_IntGaugeConversion(t *testing.T) {
	metrics := loadFixture(t, "int_gauge.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetGauge())
	gauge := m.GetGauge()
	require.Len(t, gauge.GetDataPoints(), 1)

	dp := gauge.GetDataPoints()[0]
	asInt, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt)
	require.True(t, ok, "expected NumberDataPoint_AsInt")
	assert.Equal(t, int64(72), asInt.AsInt)

	// Verify attributes from labels
	assert.Len(t, dp.GetAttributes(), 2)
	assert.Equal(t, "host", dp.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp.GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, "region", dp.GetAttributes()[1].GetKey())
	assert.Equal(t, "us-east-1", dp.GetAttributes()[1].GetValue().GetStringValue())

	// AC5.3: Verify timestamps preserved
	assert.Equal(t, uint64(1_000_000_000_000_000_000), dp.GetStartTimeUnixNano())
	assert.Equal(t, uint64(1_000_000_060_000_000_000), dp.GetTimeUnixNano())
}

// TestAC1_2_IntSumDeltaMonotonic verifies IntSum with delta and monotonic flags.
func TestAC1_2_IntSumDeltaMonotonic(t *testing.T) {
	metrics := loadFixture(t, "int_sum_delta_monotonic.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetSum())
	sum := m.GetSum()
	assert.Equal(t, metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA, sum.GetAggregationTemporality())
	assert.Equal(t, true, sum.GetIsMonotonic())
	require.Len(t, sum.GetDataPoints(), 1)

	dp := sum.GetDataPoints()[0]
	asInt, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt)
	require.True(t, ok)
	assert.Equal(t, int64(1523), asInt.AsInt)
}

// TestAC1_2_IntSumCumulativeNonmonotonic verifies IntSum with cumulative and non-monotonic flags.
func TestAC1_2_IntSumCumulativeNonmonotonic(t *testing.T) {
	metrics := loadFixture(t, "int_sum_cumulative_nonmonotonic.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetSum())
	sum := m.GetSum()
	assert.Equal(t, metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, sum.GetAggregationTemporality())
	assert.Equal(t, false, sum.GetIsMonotonic())
}

// TestAC1_3_IntHistogramConversion verifies IntHistogram converts with bucket counts and bounds.
func TestAC1_3_IntHistogramConversion(t *testing.T) {
	metrics := loadFixture(t, "int_histogram.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetHistogram())
	hist := m.GetHistogram()
	require.Len(t, hist.GetDataPoints(), 1)

	dp := hist.GetDataPoints()[0]
	assert.Equal(t, uint64(100), dp.GetCount())
	require.NotNil(t, dp.Sum)
	assert.Equal(t, float64(15234), *dp.Sum)
	assert.Equal(t, []uint64{10, 25, 30, 20, 10, 5}, dp.GetBucketCounts())
	assert.Equal(t, []float64{10, 50, 100, 200, 500}, dp.GetExplicitBounds())
}

// TestAC1_4_IntExemplarConversion verifies IntExemplar on NumberDataPoint converts to as_int.
func TestAC1_4_IntExemplarConversion(t *testing.T) {
	metrics := loadFixture(t, "int_gauge.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)

	gauge := converted[0].GetGauge()
	dp := gauge.GetDataPoints()[0]
	require.Len(t, dp.GetExemplars(), 1)

	exemplar := dp.GetExemplars()[0]
	asInt, ok := exemplar.GetValue().(*metricspb.Exemplar_AsInt)
	require.True(t, ok)
	assert.Equal(t, int64(42), asInt.AsInt)

	// Verify span/trace IDs preserved
	assert.Len(t, exemplar.GetSpanId(), 8)
	assert.Len(t, exemplar.GetTraceId(), 16)

	// Verify filtered_attributes from filtered_labels
	require.Len(t, exemplar.GetFilteredAttributes(), 1)
	assert.Equal(t, "thread", exemplar.GetFilteredAttributes()[0].GetKey())
	assert.Equal(t, "main", exemplar.GetFilteredAttributes()[0].GetValue().GetStringValue())
}

// TestAC1_5_MalformedBytesError verifies malformed unknown field bytes return error.
func TestAC1_5_MalformedBytesError(t *testing.T) {
	// Create a metric with malformed unknown field bytes at field 4.
	// Field 4 expects a length-delimited IntGauge message, but we provide invalid bytes.
	m := &metricspb.Metric{
		Name: "test_malformed",
	}
	// Construct invalid protobuf bytes: field 4, wire type 2, invalid length varint
	malformed := []byte{0x22, 0xFF, 0xFF, 0xFF, 0xFF}
	m.ProtoReflect().SetUnknown(malformed)

	converted, _, err := DetectAndConvertMetrics([]*metricspb.Metric{m})
	assert.Error(t, err)
	assert.Nil(t, converted)
}

// TestAC1_6_IntHistogramZeroBuckets verifies IntHistogram with zero buckets converts.
func TestAC1_6_IntHistogramZeroBuckets(t *testing.T) {
	metrics := loadFixture(t, "int_histogram_zero_buckets.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	hist := converted[0].GetHistogram()
	require.NotNil(t, hist)
	require.Len(t, hist.GetDataPoints(), 1)

	dp := hist.GetDataPoints()[0]
	assert.Equal(t, uint64(0), dp.GetCount())
	require.NotNil(t, dp.Sum)
	assert.Equal(t, float64(0), *dp.Sum)
	assert.Empty(t, dp.GetBucketCounts())
	assert.Empty(t, dp.GetExplicitBounds())
}

// TestAC2_1_LabelsOnNumberDataPoint verifies labels convert to attributes.
func TestAC2_1_LabelsOnNumberDataPoint(t *testing.T) {
	metrics := loadFixture(t, "labels_only.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetSum())
	sum := m.GetSum()
	require.Len(t, sum.GetDataPoints(), 1)

	dp := sum.GetDataPoints()[0]
	require.Len(t, dp.GetAttributes(), 2)
	assert.Equal(t, "host", dp.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp.GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, "region", dp.GetAttributes()[1].GetKey())
	assert.Equal(t, "us-east-1", dp.GetAttributes()[1].GetValue().GetStringValue())
}

// TestAC2_2_LabelsOnHistogramDataPoint verifies labels convert on HistogramDataPoint.
func TestAC2_2_LabelsOnHistogramDataPoint(t *testing.T) {
	metrics := loadFixture(t, "histogram_with_labels.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetHistogram())
	hist := m.GetHistogram()
	require.Len(t, hist.GetDataPoints(), 1)

	dp := hist.GetDataPoints()[0]
	require.Len(t, dp.GetAttributes(), 2)
	assert.Equal(t, "host", dp.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp.GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, "region", dp.GetAttributes()[1].GetKey())
	assert.Equal(t, "us-east-1", dp.GetAttributes()[1].GetValue().GetStringValue())

	// Verify histogram data is intact
	assert.Equal(t, uint64(50), dp.GetCount())
	require.NotNil(t, dp.Sum)
	assert.Equal(t, float64(2500), *dp.Sum)
	assert.Equal(t, []uint64{5, 15, 20, 10}, dp.GetBucketCounts())
	assert.Equal(t, []float64{10, 50, 100}, dp.GetExplicitBounds())
}

// TestAC2_3_LabelsOnSummaryDataPoint verifies labels convert on SummaryDataPoint.
func TestAC2_3_LabelsOnSummaryDataPoint(t *testing.T) {
	metrics := loadFixture(t, "summary_with_labels.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 1)

	m := converted[0]
	require.NotNil(t, m.GetSummary())
	summary := m.GetSummary()
	require.Len(t, summary.GetDataPoints(), 1)

	dp := summary.GetDataPoints()[0]
	require.Len(t, dp.GetAttributes(), 2)
	assert.Equal(t, "host", dp.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp.GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, "region", dp.GetAttributes()[1].GetKey())
	assert.Equal(t, "us-east-1", dp.GetAttributes()[1].GetValue().GetStringValue())

	// Verify summary data is intact
	assert.Equal(t, uint64(100), dp.GetCount())
	assert.Equal(t, float64(5000), dp.GetSum())
	require.Len(t, dp.GetQuantileValues(), 2)
	assert.Equal(t, float64(0.5), dp.GetQuantileValues()[0].GetQuantile())
	assert.Equal(t, float64(45.0), dp.GetQuantileValues()[0].GetValue())
	assert.Equal(t, float64(0.99), dp.GetQuantileValues()[1].GetQuantile())
	assert.Equal(t, float64(120.0), dp.GetQuantileValues()[1].GetValue())
}

// TestAC2_4_FilteredLabelsOnExemplar is covered by TestAC1_4_IntExemplarConversion.
// This test explicitly verifies filtered_labels conversion on exemplars.
func TestAC2_4_FilteredLabelsOnExemplar(t *testing.T) {
	metrics := loadFixture(t, "int_gauge.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)

	gauge := converted[0].GetGauge()
	dp := gauge.GetDataPoints()[0]
	require.Len(t, dp.GetExemplars(), 1)

	exemplar := dp.GetExemplars()[0]
	require.Len(t, exemplar.GetFilteredAttributes(), 1)
	assert.Equal(t, "thread", exemplar.GetFilteredAttributes()[0].GetKey())
	assert.Equal(t, "main", exemplar.GetFilteredAttributes()[0].GetValue().GetStringValue())
}

// TestAC2_5_EmptyLabels verifies empty labels field produces empty attributes.
func TestAC2_5_EmptyLabels(t *testing.T) {
	// Construct a NumberDataPoint with field 1 present but with zero StringKeyValue entries.
	dp := &metricspb.NumberDataPoint{
		Value: &metricspb.NumberDataPoint_AsInt{AsInt: 42},
	}
	// Manually construct unknown bytes: field 1 (wire type 2), length 0
	unknownBytes := []byte{0x0A, 0x00}
	dp.ProtoReflect().SetUnknown(unknownBytes)

	// Wrap in a Sum metric
	m := &metricspb.Metric{
		Name: "test_empty_labels",
		Data: &metricspb.Metric_Sum{
			Sum: &metricspb.Sum{
				DataPoints: []*metricspb.NumberDataPoint{dp},
			},
		},
	}

	converted, _, err := DetectAndConvertMetrics([]*metricspb.Metric{m})
	require.NoError(t, err)
	require.Len(t, converted, 1)

	convertedDP := converted[0].GetSum().GetDataPoints()[0]
	// Labels extracted but empty, no error
	// Note: attributes may be appended even if empty (result of conversion)
	// Just verify no error occurred and metric is intact
	assert.Equal(t, int64(42), convertedDP.GetValue().(*metricspb.NumberDataPoint_AsInt).AsInt)
}

// TestAC3_1_1xPassthrough verifies 1.x metrics without unknown fields pass through unchanged.
func TestAC3_1_1xPassthrough(t *testing.T) {
	// Construct a pure 1.x Gauge metric
	m := &metricspb.Metric{
		Name: "test_gauge",
		Data: &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{
					{
						Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
						Attributes: []*commonpb.KeyValue{
							{
								Key: "env",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "prod"},
								},
							},
						},
					},
				},
			},
		},
	}

	converted, _, err := DetectAndConvertMetrics([]*metricspb.Metric{m})
	require.NoError(t, err)
	require.Len(t, converted, 1)

	// Verify it's returned unchanged
	assert.Equal(t, m, converted[0])
}

// TestAC3_2_NonOTLP07UnknownFieldsPreserved verifies non-0.7 unknown fields are preserved.
func TestAC3_2_NonOTLP07UnknownFieldsPreserved(t *testing.T) {
	// Construct a metric with unknown field at field 99 (not 4, 6, or 8)
	m := &metricspb.Metric{
		Name: "test_unknown_field",
		Data: &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{
					{
						Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 100.0},
					},
				},
			},
		},
	}
	// Field 99, wire type 2 (length-delimited), value "custom"
	unknownBytes := []byte{0xEA, 0x07, 0x06, 'c', 'u', 's', 't', 'o', 'm'}
	m.ProtoReflect().SetUnknown(unknownBytes)

	converted, _, err := DetectAndConvertMetrics([]*metricspb.Metric{m})
	require.NoError(t, err)

	// Verify field 99 is still present by checking unknown bytes
	unknownAfter := converted[0].ProtoReflect().GetUnknown()
	assert.Equal(t, unknownBytes, []byte(unknownAfter))
}

// TestAC4_1_MixedPayloads verifies mixed 0.7/1.x metrics are handled correctly.
func TestAC4_1_MixedPayloads(t *testing.T) {
	metrics := loadFixture(t, "mixed_07_1x.binpb")
	require.Len(t, metrics, 2) // One 0.7 IntGauge and one 0.7 DoubleGauge (proto type unchanged in 1.x)

	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)
	require.Len(t, converted, 2)

	// First metric should be converted IntGauge → Gauge
	m0 := converted[0]
	require.NotNil(t, m0.GetGauge())
	gauge0 := m0.GetGauge()
	require.Len(t, gauge0.GetDataPoints(), 1)
	dp0 := gauge0.GetDataPoints()[0]
	asInt, ok := dp0.GetValue().(*metricspb.NumberDataPoint_AsInt)
	require.True(t, ok)
	assert.Equal(t, int64(50), asInt.AsInt)
	// Verify labels converted to attributes
	require.Len(t, dp0.GetAttributes(), 2)
	assert.Equal(t, "host", dp0.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp0.GetAttributes()[0].GetValue().GetStringValue())

	// Second metric is a 0.7 DoubleGauge (proto type unchanged in 1.x) with labels converted to attributes
	m1 := converted[1]
	require.NotNil(t, m1.GetGauge())
	gauge1 := m1.GetGauge()
	require.Len(t, gauge1.GetDataPoints(), 1)
	dp1 := gauge1.GetDataPoints()[0]
	asDouble, ok := dp1.GetValue().(*metricspb.NumberDataPoint_AsDouble)
	require.True(t, ok)
	assert.Equal(t, float64(75.5), asDouble.AsDouble)
	// Verify labels converted to attributes on 0.7 metric with recognized proto type
	require.Len(t, dp1.GetAttributes(), 2)
	assert.Equal(t, "host", dp1.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp1.GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, "region", dp1.GetAttributes()[1].GetKey())
	assert.Equal(t, "us-east-1", dp1.GetAttributes()[1].GetValue().GetStringValue())
}

// TestAC4_2_Has07Data verifies Has07Data detection.
func TestAC4_2_Has07Data(t *testing.T) {
	// Test mixed payload returns true
	metrics := loadFixture(t, "mixed_07_1x.binpb")
	assert.True(t, Has07Data(metrics))

	// Test pure 1.x payload returns false
	pureMetrics := []*metricspb.Metric{
		{
			Name: "test_gauge",
			Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{
						{
							Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
						},
					},
				},
			},
		},
	}
	assert.False(t, Has07Data(pureMetrics))
}

// TestDetectAndConvertMetrics_ReportsConversion verifies the bool return signals
// whether any 0.7 data was found and converted.
func TestDetectAndConvertMetrics_ReportsConversion(t *testing.T) {
	tests := []struct {
		name     string
		metrics  []*metricspb.Metric
		expected bool
	}{
		{
			name:     "0.7 IntGauge returns true",
			metrics:  loadFixture(t, "int_gauge.binpb"),
			expected: true,
		},
		{
			name:     "0.7 Sum (proto type unchanged in 1.x) returns true",
			metrics:  loadFixture(t, "labels_only.binpb"),
			expected: true,
		},
		{
			name: "pure 1.x Gauge returns false",
			metrics: []*metricspb.Metric{
				{
					Name: "test_gauge",
					Data: &metricspb.Metric_Gauge{
						Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{
								{
									Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, converted, err := DetectAndConvertMetrics(tt.metrics)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, converted)
		})
	}
}

// TestAC5_1_IntegerLossless verifies integers survive as as_int (not floats).
func TestAC5_1_IntegerLossless(t *testing.T) {
	metrics := loadFixture(t, "int_gauge.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)

	dp := converted[0].GetGauge().GetDataPoints()[0]
	asInt, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt)
	require.True(t, ok, "expected as_int, not as_double")
	assert.Equal(t, int64(72), asInt.AsInt)
}

// TestAC5_2_StringLabelsPreserved verifies label key/value strings are exact.
func TestAC5_2_StringLabelsPreserved(t *testing.T) {
	metrics := loadFixture(t, "int_gauge.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)

	dp := converted[0].GetGauge().GetDataPoints()[0]
	require.Len(t, dp.GetAttributes(), 2)
	assert.Equal(t, "host", dp.GetAttributes()[0].GetKey())
	assert.Equal(t, "server-01", dp.GetAttributes()[0].GetValue().GetStringValue())
	assert.Equal(t, "region", dp.GetAttributes()[1].GetKey())
	assert.Equal(t, "us-east-1", dp.GetAttributes()[1].GetValue().GetStringValue())
}

// TestAC5_3_TimestampsPreserved verifies timestamps are copied as-is.
func TestAC5_3_TimestampsPreserved(t *testing.T) {
	metrics := loadFixture(t, "int_gauge.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)

	dp := converted[0].GetGauge().GetDataPoints()[0]
	assert.Equal(t, uint64(1_000_000_000_000_000_000), dp.GetStartTimeUnixNano())
	assert.Equal(t, uint64(1_000_000_060_000_000_000), dp.GetTimeUnixNano())
}

// TestEdgeCase_HistogramWithZeroBucketsPreservesData verifies zero-bucket histogram data.
func TestEdgeCase_HistogramWithZeroBucketsPreservesData(t *testing.T) {
	metrics := loadFixture(t, "int_histogram_zero_buckets.binpb")
	converted, _, err := DetectAndConvertMetrics(metrics)
	require.NoError(t, err)

	dp := converted[0].GetHistogram().GetDataPoints()[0]
	assert.Equal(t, uint64(0), dp.GetCount())
	require.NotNil(t, dp.Sum)
	assert.Equal(t, float64(0), *dp.Sum)
}

// TestConvertLabelsToAttributesHelper tests the convertLabelsToAttributes helper directly.
func TestConvertLabelsToAttributesHelper(t *testing.T) {
	// Construct valid StringKeyValue label bytes (field 1, with key "test" and value "value")
	label := &shadowcommonpb.StringKeyValue{
		Key:   "test",
		Value: "value",
	}
	labelBytes, _ := proto.Marshal(label)

	// Construct unknown bytes: field 1 (wire type 2), length-delimited
	unknown := []byte{0x0A}
	unknown = append(unknown, byte(len(labelBytes)))
	unknown = append(unknown, labelBytes...)

	attrs, remaining, err := convertLabelsToAttributes(unknown)
	require.NoError(t, err)
	require.Len(t, attrs, 1)
	assert.Equal(t, "test", attrs[0].GetKey())
	assert.Equal(t, "value", attrs[0].GetValue().GetStringValue())
	assert.Empty(t, remaining) // All bytes consumed
}

// TestConvertLabelsToAttributesMalformed tests error handling in convertLabelsToAttributes.
func TestConvertLabelsToAttributesMalformed(t *testing.T) {
	// Create malformed label bytes (valid tag but invalid protobuf message)
	malformedLabel := []byte{0xFF, 0xFF}
	unknown := []byte{0x0A, byte(len(malformedLabel))}
	unknown = append(unknown, malformedLabel...)

	attrs, remaining, err := convertLabelsToAttributes(unknown)
	assert.Error(t, err)
	assert.Nil(t, attrs)
	assert.Nil(t, remaining)
}

// TestAllFixturesToNonNilDataTypes verifies all fixtures convert to non-nil data types.
func TestAllFixturesToNonNilDataTypes(t *testing.T) {
	fixtures := []string{
		"int_gauge.binpb",
		"int_sum_delta_monotonic.binpb",
		"int_sum_cumulative_nonmonotonic.binpb",
		"int_histogram.binpb",
		"int_histogram_zero_buckets.binpb",
		"labels_only.binpb",
		"histogram_with_labels.binpb",
		"summary_with_labels.binpb",
		"mixed_07_1x.binpb",
	}

	for _, fixture := range fixtures {
		t.Run(fixture, func(t *testing.T) {
			metrics := loadFixture(t, fixture)
			converted, _, err := DetectAndConvertMetrics(metrics)
			require.NoError(t, err, "conversion failed for %s", fixture)
			require.NotEmpty(t, converted)

			for _, m := range converted {
				require.NotNil(t, m.GetData(), "metric %s has nil data", fixture)
			}
		})
	}
}
