package otlp

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	collectorMetrics "github.com/honeycombio/husky/proto/otlp/collector/metrics/v1"
	common "github.com/honeycombio/husky/proto/otlp/common/v1"
	metrics "github.com/honeycombio/husky/proto/otlp/metrics/v1"
	resource "github.com/honeycombio/husky/proto/otlp/resource/v1"
	"github.com/honeycombio/husky/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestTranslateMetricsRequest(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}

	dataPoints := []*metrics.NumberDataPoint{
		createIntNumberDataPoint(2, uint64(593485), createUniqueAttributeSet(1)),
	}

	req := &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metrics.ResourceMetrics{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeMetrics: []*metrics.ScopeMetrics{{
				Metrics: []*metrics.Metric{createSumMetric("testMetricName", dataPoints)},
			}},
		}},
	}

	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "unknown_metrics", batch.Dataset)
	assert.Equal(t, proto.Size(req.ResourceMetrics[0]), batch.SizeBytes)
	events := batch.Events
	assert.Equal(t, 1, len(events))
	ev := events[0]
	assert.Equal(t, "metric", ev.Attributes["meta.signal_type"])
	assert.Equal(t, "metric_event", ev.Attributes["meta.annotation_type"])
	assert.Equal(t, "my-service", ev.Attributes["service.name"])
	assert.Equal(t, int64(2), ev.Attributes["testMetricName"])
}

func TestTranslateClassicMetricsRequest(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
		Dataset:     "legacy-dataset",
	}

	dataPoints := []*metrics.NumberDataPoint{
		createIntNumberDataPoint(2, uint64(593485), createUniqueAttributeSet(1)),
	}

	req := &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metrics.ResourceMetrics{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeMetrics: []*metrics.ScopeMetrics{{
				Metrics: []*metrics.Metric{createSumMetric("testMetricName", dataPoints)},
			}},
		}},
	}

	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "legacy-dataset", batch.Dataset)
	assert.Equal(t, proto.Size(req.ResourceMetrics[0]), batch.SizeBytes)
	events := batch.Events
	assert.Equal(t, 1, len(events))
	ev := events[0]
	assert.Equal(t, "metric", ev.Attributes["meta.signal_type"])
	assert.Equal(t, "metric_event", ev.Attributes["meta.annotation_type"])
	assert.Equal(t, "my-service", ev.Attributes["service.name"])
	assert.Equal(t, int64(2), ev.Attributes["testMetricName"])
}

// test each metric type
func TestDeprecatedIntGauge(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}

	dataPoints := []*metrics.IntDataPoint{{
		Value: 12,
	}, {
		Value: 13,
	}}

	req := &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metrics.ResourceMetrics{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeMetrics: []*metrics.ScopeMetrics{{
				Metrics: []*metrics.Metric{{
					Name: "testMetricName",
					Data: &metrics.Metric_IntGauge{
						IntGauge: &metrics.IntGauge{DataPoints: dataPoints},
					},
				}},
			}},
		}},
	}

	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	ev := events[0]
	assert.Equal(t, "unknown_metrics", batch.Dataset)
	assert.Equal(t, int64(13), ev.Attributes["testMetricName"])
}

func TestGauge(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}

	dataPoints := []*metrics.NumberDataPoint{
		createIntNumberDataPoint(2, uint64(2614537516000000000), createUniqueAttributeSet(1)),
		createIntNumberDataPoint(3, uint64(1614537516000000000), createUniqueAttributeSet(1)),
	}

	req := &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metrics.ResourceMetrics{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeMetrics: []*metrics.ScopeMetrics{{
				Metrics: []*metrics.Metric{{
					Name: "testMetricName",
					Data: &metrics.Metric_Gauge{
						Gauge: &metrics.Gauge{DataPoints: dataPoints},
					},
				}},
			}},
		}},
	}

	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	ev := events[0]
	assert.Equal(t, "unknown_metrics", batch.Dataset)
	assert.Equal(t, int64(2), ev.Attributes["testMetricName"])

}

func TestDeprecatedIntSum(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}

	dataPoints := []*metrics.IntDataPoint{{
		Value: 12,
	}, {
		Value: 13,
	}}

	req := &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metrics.ResourceMetrics{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeMetrics: []*metrics.ScopeMetrics{{
				Metrics: []*metrics.Metric{{
					Name: "testMetricName",
					Data: &metrics.Metric_IntSum{
						IntSum: &metrics.IntSum{DataPoints: dataPoints},
					},
				}},
			}},
		}},
	}

	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	ev := events[0]
	assert.Equal(t, "unknown_metrics", batch.Dataset)
	assert.Equal(t, int64(13), ev.Attributes["testMetricName"])
}

func TestHistograms(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}
	bucketCounts := []uint64{1, 1771, 231, 97, 0, 0}
	explicitBounds := []float64{10, 25, 50, 75, 100}

	resource := createResource([]map[string]string{{"service.name": "kafka", "broker": "2770"}})
	sharedAttributes := map[string]string{"topic": "foozle", "partition": "1"}
	sharedTimestamp := uint64(1614537516000000000)

	histogramDataPoint1 := createHistogramDataPoint(2100, 29391.0, bucketCounts, explicitBounds, sharedTimestamp, createStringAttributesList(sharedAttributes))
	histogramDataPoint2 := createHistogramDataPoint(2100, 29391.0, []uint64{1, 1, 1, 1, 1, 1}, explicitBounds, sharedTimestamp, createStringAttributesList(sharedAttributes))

	metricsList := []*metrics.Metric{
		createHistogramMetric("batch_send_size_bytes", []*metrics.HistogramDataPoint{histogramDataPoint1}),
		createHistogramMetric("busted_bucket_counts", []*metrics.HistogramDataPoint{histogramDataPoint2}),
	}

	req := createExportMetricsServiceRequest([]*metrics.ResourceMetrics{createResourceMetric(metricsList, resource)})
	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	event := events[0]
	assert.Equal(t, event.Attributes["service.name"], "kafka")
	assert.Equal(t, event.Attributes["broker"], "2770")
	assert.Equal(t, event.Attributes["topic"], "foozle")
	assert.Equal(t, event.Attributes["batch_send_size_bytes.count"], int64(2100))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.sum"], float64(29391.0))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.avg"], float64(13.995714285714286))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p001"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p01"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p10"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p25"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p50"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p75"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p90"], float64(25))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p95"], float64(25))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p99"], float64(50))
	assert.Equal(t, event.Attributes["batch_send_size_bytes.p999"], float64(50))
	assert.Equal(t, event.Attributes["busted_bucket_counts.count"], int64(2100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.sum"], float64(29391.0))
	assert.Equal(t, event.Attributes["busted_bucket_counts.avg"], float64(13.995714285714286))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p001"], float64(25))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p01"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p10"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p25"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p50"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p75"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p90"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p95"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p99"], float64(100))
	assert.Equal(t, event.Attributes["busted_bucket_counts.p999"], float64(100))
}

func TestDeprecatedHistograms(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}
	bucketCounts := []uint64{1, 1771, 231, 97, 0, 0}
	explicitBounds := []float64{10, 25, 50, 75, 100}

	resource := createResource([]map[string]string{{"service.name": "kafka", "broker": "2770"}})
	sharedAttributes := map[string]string{"topic": "foozle", "partition": "1"}
	sharedTimestamp := uint64(1614537516000000000)
	intHistogramDatapoint := createIntHistogramDataPoint(2100, 29391, bucketCounts, explicitBounds, sharedTimestamp, createLabelsList(sharedAttributes))
	metricsList := []*metrics.Metric{createIntHistogramMetric("batch_send_size", []*metrics.IntHistogramDataPoint{intHistogramDatapoint})}
	req := createExportMetricsServiceRequest([]*metrics.ResourceMetrics{createResourceMetric(metricsList, resource)})
	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	event := events[0]
	assert.Equal(t, event.Attributes["service.name"], "kafka")
	assert.Equal(t, event.Attributes["broker"], "2770")
	assert.Equal(t, event.Attributes["topic"], "foozle")
	assert.Equal(t, event.Attributes["batch_send_size.count"], int64(2100))
	assert.Equal(t, event.Attributes["batch_send_size.sum"], float64(29391))
	assert.Equal(t, event.Attributes["batch_send_size.avg"], float64(13.995714285714286))
	assert.Equal(t, event.Attributes["batch_send_size.p001"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size.p01"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size.p10"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size.p25"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size.p50"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size.p75"], float64(10))
	assert.Equal(t, event.Attributes["batch_send_size.p90"], float64(25))
	assert.Equal(t, event.Attributes["batch_send_size.p95"], float64(25))
	assert.Equal(t, event.Attributes["batch_send_size.p99"], float64(50))
	assert.Equal(t, event.Attributes["batch_send_size.p999"], float64(50))
}

func TestSummaries(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}
	resource := createResource([]map[string]string{{"service.name": "kafka", "broker": "2770"}})
	dataPoint := &metrics.SummaryDataPoint{
		Attributes:   createStringAttributesList(map[string]string{"topic": "foozle", "partition": "1"}),
		TimeUnixNano: uint64(time.Now().Nanosecond()),
		Count:        2100,
		Sum:          2616.1,
		QuantileValues: []*metrics.SummaryDataPoint_ValueAtQuantile{
			{Quantile: 0.0, Value: 111.22},
			{Quantile: 0.01, Value: 123.4},
			{Quantile: 0.25, Value: 456.7},
			{Quantile: 0.50, Value: 567.8},
			{Quantile: 0.75, Value: 678.8},
			{Quantile: 0.999, Value: 789.4},
			{Quantile: 1.0, Value: 959.31},
		},
	}

	summaryMetric := createSummaryMetric("summaryMetricName", []*metrics.SummaryDataPoint{dataPoint})
	req := createExportMetricsServiceRequest([]*metrics.ResourceMetrics{createResourceMetric([]*metrics.Metric{summaryMetric}, resource)})
	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	event := events[0]
	assert.Equal(t, event.Attributes["service.name"], "kafka")
	assert.Equal(t, event.Attributes["broker"], "2770")
	assert.Equal(t, event.Attributes["topic"], "foozle")
	assert.Equal(t, event.Attributes["summaryMetricName.count"], int64(2100))
	assert.Equal(t, event.Attributes["summaryMetricName.sum"], float64(2616.1))
	assert.Equal(t, event.Attributes["summaryMetricName.avg"], float64(1.2457619047619046))
	assert.Equal(t, event.Attributes["summaryMetricName.p01"], float64(123.4))
	assert.Equal(t, event.Attributes["summaryMetricName.p25"], float64(456.7))
	assert.Equal(t, event.Attributes["summaryMetricName.p50"], float64(567.8))
	assert.Equal(t, event.Attributes["summaryMetricName.p75"], float64(678.8))
	assert.Equal(t, event.Attributes["summaryMetricName.p999"], float64(789.4))
	assert.Equal(t, event.Attributes["summaryMetricName.min"], float64(111.22))
	assert.Equal(t, event.Attributes["summaryMetricName.max"], float64(959.31))
}

func TestAttributeValues(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}
	testTypes := []struct {
		name          string
		inputValue    *common.AnyValue
		expectedValue any
	}{
		{"String", &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "foozle"}}, "foozle"},
		{"Bool", &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}, true},
		{"Int", &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 72319}}, int64(72319)},
		{"Double", &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 2770}}, float64(2770)},

		// TODO: probably at some point we should support these data types, currently they get saved as nil
		{"Array", &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{
			{Value: &common.AnyValue_StringValue{StringValue: "one"}},
			{Value: &common.AnyValue_BoolValue{BoolValue: true}},
			{Value: &common.AnyValue_IntValue{IntValue: 3}},
			{Value: &common.AnyValue_DoubleValue{DoubleValue: 4}},
			{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{}}}},
			{Value: &common.AnyValue_BytesValue{BytesValue: []byte{0x1, 0xFF}}},
		}}}}, `["one",true,3,4,"[]",null]`},
		{"Kvlist", &common.AnyValue{Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{Values: []*common.KeyValue{
			{Key: "a", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "one"}}},
			{Key: "b", Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}},
			{Key: "c", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 3}}},
			{Key: "d", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 4}}},
			{Key: "e", Value: &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{}}}}},
			{Key: "f", Value: &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: []byte{0x1, 0xFF}}}},
		}}}}, `[{"a":"one"},{"b":true},{"c":3},{"d":4},{"e":"[]"},{"f":null}]`},
		{"Bytes", &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: []byte{0x1, 0xFF}}}, nil},
	}

	for _, testType := range testTypes {
		attributeList := []*common.KeyValue{{Key: "testKey", Value: testType.inputValue}}
		t.Run(fmt.Sprintf("Resource attribute with %s gets saved to event field", testType.name), func(t *testing.T) {

			resource := &resource.Resource{Attributes: attributeList}
			resourceMetrics := []*metrics.ResourceMetrics{createResourceMetric(createArbitraryMetricsList(), resource)}
			req := createExportMetricsServiceRequest(resourceMetrics)

			result, err := TranslateMetricsRequest(req, ri)
			assert.Nil(t, err)
			batch := result.Batches[0]
			events := batch.Events
			event := events[0]
			assert.Equal(t, testType.expectedValue, event.Attributes["testKey"])
		})
	}

}

func TestDeprecatedLabels(t *testing.T) {
	// data point labels were deprecated in OTLP 0.9, in favor of attributes, but we still support them.

	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}
	dataPoints := []*metrics.NumberDataPoint{{
		Labels:       []*common.StringKeyValue{createLabel("foo", "bar")},
		Value:        &metrics.NumberDataPoint_AsInt{AsInt: 10},
		TimeUnixNano: createArbitraryMetricTimestamp(),
	}}
	rs := createResourceMetric([]*metrics.Metric{createGaugeMetric("metricName", dataPoints)}, createArbitraryResource())
	req := createExportMetricsServiceRequest([]*metrics.ResourceMetrics{rs})
	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	event := events[0]

	assert.Equal(t, "bar", event.Attributes["foo"])
}

func TestMetricsAreCombinedIntoSingleEvent(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}
	// For test, build a request with 2 resourceMetrics,
	// each with a unique InstrumentationLibrary. The metrics in both resourceMetrics
	// share the resourceAttributes, timestamp after truncation, and datapoint attributes so that we can
	// test combining metrics into events
	// Mix of Gauge and Sum metrics

	// resources with the same attributes in a different order should still be combined
	resource1 := createResource([]map[string]string{{"service.name": "kafka"}, {"broker": "2770"}})
	resource2 := createResource([]map[string]string{{"broker": "2770"}, {"service.name": "kafka"}})

	// create data that will be shared across datapoints: timestamps that will truncate to the same second,
	// and two different versions of a label
	dp1Attr := []*common.KeyValue{createStringAttribute("topic", "foozle"), createStringAttribute("partition", "1")}
	dp2Attr := []*common.KeyValue{createStringAttribute("topic", "foozle"), createStringAttribute("partition", "2")}
	dpTimestamp := uint64(1614537516000000000)
	dpTimestamp2 := uint64(1614537516500000000)

	// create a whole lot of datapoints from kafka
	// 5 datapoints that share the same timestamp after truncation and label #1
	dp1 := createIntNumberDataPoint(102812, dpTimestamp, dp1Attr)
	dp3 := createIntNumberDataPoint(2109820931828, dpTimestamp2, dp1Attr)
	dp5 := createIntNumberDataPoint(322122382273, dpTimestamp2, dp1Attr)
	dp7 := createDoubleNumberDataPoint(63.1, dpTimestamp, dp1Attr)
	dp9 := createDoubleNumberDataPoint(453425421.1763, dpTimestamp, dp1Attr)

	// 5 datapoints that share the same timestamp after truncation and label #2
	dp2 := createIntNumberDataPoint(81277, dpTimestamp, dp2Attr)
	dp4 := createIntNumberDataPoint(1820918278377, dpTimestamp2, dp2Attr)
	dp6 := createIntNumberDataPoint(429496509698, dpTimestamp2, dp2Attr)
	dp8 := createDoubleNumberDataPoint(78.8, dpTimestamp, dp2Attr)
	dp10 := createDoubleNumberDataPoint(7894152147.1121, dpTimestamp, dp2Attr)

	kafkaMetrics := []*metrics.Metric{
		createGaugeMetric("event_throughput", []*metrics.NumberDataPoint{dp1, dp2}),
		createGaugeMetric("offset", []*metrics.NumberDataPoint{dp3, dp4}),
		createSumMetric("total_bytes", []*metrics.NumberDataPoint{dp5, dp6}),
		createGaugeMetric("cpu_util", []*metrics.NumberDataPoint{dp7, dp8}),
		createGaugeMetric("total_cpu_time", []*metrics.NumberDataPoint{dp9, dp10}),
	}

	// create a whole lot of datapoints from telegraf
	// 2 datapoints that share the same timestamp after truncation and label #1
	dp11 := createIntNumberDataPoint(100032, dpTimestamp, dp1Attr)
	dp13 := createDoubleNumberDataPoint(24.0, dpTimestamp2, dp1Attr)

	// 2 datapoints that share the same timestamp after truncation and label #2
	dp12 := createIntNumberDataPoint(810001, dpTimestamp, dp2Attr)
	dp14 := createDoubleNumberDataPoint(33.2, dpTimestamp2, dp2Attr)

	telegrafMetrics := []*metrics.Metric{
		createGaugeMetric("diskio.reads", []*metrics.NumberDataPoint{dp11, dp12}),
		createGaugeMetric("cpu_usage.idle", []*metrics.NumberDataPoint{dp13, dp14}),
	}

	metricsBundle := []*metrics.ResourceMetrics{}
	metricsBundle = append(metricsBundle,
		createResourceMetricsWithLibraryMetadata(kafkaMetrics, resource1, "kafkaMetrics", "0.7.0"),
		createResourceMetricsWithLibraryMetadata(telegrafMetrics, resource2, "telegraf", "0.8.1"),
	)
	req := createExportMetricsServiceRequest(metricsBundle)
	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	events := batch.Events
	event1 := events[0]
	assert.Equal(t, event1.Attributes["service.name"], "kafka")
	assert.Equal(t, event1.Attributes["broker"], "2770")
	assert.Equal(t, event1.Attributes["topic"], "foozle")
	assert.Equal(t, event1.Attributes["event_throughput"], int64(102812))
	assert.Equal(t, event1.Attributes["offset"], int64(2109820931828))
	assert.Equal(t, event1.Attributes["total_bytes"], int64(322122382273))
	assert.Equal(t, event1.Attributes["cpu_util"], float64(63.1))
	assert.Equal(t, event1.Attributes["total_cpu_time"], float64(453425421.1763))
	assert.Equal(t, event1.Attributes["cpu_usage.idle"], float64(24.0))
	assert.Equal(t, event1.Attributes["diskio.reads"], int64(100032))

	event2 := events[1]
	assert.Equal(t, event2.Attributes["service.name"], "kafka")
	assert.Equal(t, event2.Attributes["broker"], "2770")
	assert.Equal(t, event2.Attributes["topic"], "foozle")
	assert.Equal(t, event2.Attributes["event_throughput"], int64(81277))
	assert.Equal(t, event2.Attributes["offset"], int64(1820918278377))
	assert.Equal(t, event2.Attributes["total_bytes"], int64(429496509698))
	assert.Equal(t, event2.Attributes["cpu_util"], float64(78.8))
	assert.Equal(t, event2.Attributes["total_cpu_time"], float64(7894152147.1121))
	assert.Equal(t, event2.Attributes["cpu_usage.idle"], float64(33.2))
	assert.Equal(t, event2.Attributes["diskio.reads"], int64(810001))

}

func TestMetricsRequestWithInvalidContentTypeReturnsError(t *testing.T) {
	req := &collectorMetrics.ExportMetricsServiceRequest{}
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/json",
	}
	result, err := TranslateMetricsRequest(req, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrInvalidContentType, err)
}

func TestMetricsRequestWithInvalidBodyReturnsError(t *testing.T) {
	bodyBytes := test.RandomBytes(10)
	body := io.NopCloser(bytes.NewReader(bodyBytes))
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/protobuf",
	}
	result, err := TranslateMetricseRequestFromReader(body, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrFailedParseBody, err)

}

// helper functions
func createSumMetric(name string, datapoints []*metrics.NumberDataPoint) *metrics.Metric {
	return &metrics.Metric{
		Name: name,
		Data: &metrics.Metric_Sum{
			Sum: &metrics.Sum{DataPoints: datapoints},
		},
	}
}

func createGaugeMetric(name string, datapoints []*metrics.NumberDataPoint) *metrics.Metric {
	return &metrics.Metric{
		Name: name,
		Data: &metrics.Metric_Gauge{
			Gauge: &metrics.Gauge{DataPoints: datapoints},
		},
	}
}

// createIntNumberDataPoint creates an int datapoint for a metric
func createIntNumberDataPoint(
	value int64,
	currentTime uint64, // nanoseconds since epoch
	attributes []*common.KeyValue,
) *metrics.NumberDataPoint {
	return &metrics.NumberDataPoint{
		Attributes:   attributes,
		Value:        &metrics.NumberDataPoint_AsInt{AsInt: value},
		TimeUnixNano: currentTime,
	}
}

// createDoubleNumberDataPoint creates an double datapoint for a metric
// defaults start time to 0
func createDoubleNumberDataPoint(
	value float64,
	currentTime uint64, // nanoseconds since epoch
	attributes []*common.KeyValue, // use createStringAttribute(key, value) to create an attribute for the list
) *metrics.NumberDataPoint {
	return createDoubleNumberDataPointWithStartTime(value, uint64(0), currentTime, attributes)
}

// createDoubleNumberDataPointWithStartTime creates a double datapoint for a metric,
// accepts a start time
func createDoubleNumberDataPointWithStartTime(
	value float64,
	startTime uint64, // nanoseconds since epoch
	currentTime uint64, // nanoseconds since epoch
	attributes []*common.KeyValue, // use createStringAttribute(key,value) to create an individiual label for the list
) *metrics.NumberDataPoint {
	return &metrics.NumberDataPoint{
		Attributes:        attributes,
		Value:             &metrics.NumberDataPoint_AsDouble{AsDouble: value},
		StartTimeUnixNano: startTime,
		TimeUnixNano:      currentTime,
	}
}

func createStringAttribute(key string, value string) *common.KeyValue {
	return &common.KeyValue{
		Key: key,
		Value: &common.AnyValue{
			Value: &common.AnyValue_StringValue{StringValue: value},
		},
	}
}

// creates a new, unique attribute that can be applied to a datapoint
func createUniqueAttribute() *common.KeyValue {
	return createStringAttribute(test.RandomString(8), test.RandomString(8))
}

// create unique set of attributes
func createUniqueAttributeSet(numAttrs int) []*common.KeyValue {
	attributes := []*common.KeyValue{}
	for i := 0; i < numAttrs; i++ {
		attributes = append(attributes, createUniqueAttribute())
	}
	return attributes
}

func createResource(attributes []map[string]string) *resource.Resource {
	var labelsList []*common.KeyValue
	for _, attr := range attributes {
		for k, v := range attr {
			labelsList = append(labelsList, createStringAttribute(k, v))
		}
	}
	return &resource.Resource{
		Attributes: labelsList,
	}
}

// nolint: unused // false positive, used in skipped test
// https://github.com/dominikh/go-tools/issues/633
func createHistogramDataPoint(
	count uint64,
	sum float64,
	bucketCounts []uint64,
	explicitBounds []float64,
	currentTime uint64, // nanoseconds since epoch
	attributes []*common.KeyValue, // use createStringAttribute(key,value) to create an individiual attribute for the list
) *metrics.HistogramDataPoint {
	return &metrics.HistogramDataPoint{
		Attributes:     attributes,
		TimeUnixNano:   currentTime,
		Count:          count,
		Sum:            &sum,
		BucketCounts:   bucketCounts,
		ExplicitBounds: explicitBounds,
	}
}

func createIntHistogramDataPoint(
	count uint64,
	sum int64,
	bucketCounts []uint64,
	explicitBounds []float64,
	currentTime uint64, // nanoseconds since epoch
	labels []*common.StringKeyValue, // use createLabel(key,value) to create an individiual label for the list
) *metrics.IntHistogramDataPoint {
	return &metrics.IntHistogramDataPoint{
		Labels:         labels,
		TimeUnixNano:   currentTime,
		Count:          count,
		Sum:            sum,
		BucketCounts:   bucketCounts,
		ExplicitBounds: explicitBounds,
	}
}

// nolint: unused // false positive, used in skipped test
// https://github.com/dominikh/go-tools/issues/633
func createHistogramMetric(name string, datapoints []*metrics.HistogramDataPoint) *metrics.Metric {
	return &metrics.Metric{
		Name: name,
		Data: &metrics.Metric_Histogram{
			Histogram: &metrics.Histogram{DataPoints: datapoints},
		},
	}
}

func createStringAttributesList(attributes map[string]string) (attributesList []*common.KeyValue) {
	for k, v := range attributes {
		attributesList = append(attributesList, createStringAttribute(k, v))
	}
	return attributesList
}

// createResourceMetric takes the list of Metrics and a resource (use createResource if needed) and creates a ResourceMetrics struct.
// It does not include the instrumentation library and version - if you need these, use
// createResourceMetricsWithLibraryMetadata.
// This is the proper structure for a batch of metrics to be processed by Shepherd
func createResourceMetric(
	metricsList []*metrics.Metric,
	resource *resource.Resource) *metrics.ResourceMetrics {
	return createResourceMetricsWithLibraryMetadata(metricsList, resource, "", "")
}

// createResourceMetricsWithLibraryMetadata takes the list of Metrics and the resource attributes (resource attributes are
// descriptive information about the resource that is sending these metrics) and creates a ResourceMetrics struct.
// Includes the instrumentation library and version.
// This is the proper structure for a batch of metrics to be recognized by Shepherd for processing
func createResourceMetricsWithLibraryMetadata(
	metricsList []*metrics.Metric,
	resource *resource.Resource,
	libraryName string,
	libraryVersion string) *metrics.ResourceMetrics {
	return &metrics.ResourceMetrics{
		Resource: resource,
		ScopeMetrics: []*metrics.ScopeMetrics{{
			Scope:   &common.InstrumentationScope{Name: libraryName, Version: libraryVersion},
			Metrics: metricsList,
		}},
	}
}

// createExportMetricsServiceRequest
// This is the proper structure for a batch of metrics to be processed by Shepherd
func createExportMetricsServiceRequest(resourceMetrics []*metrics.ResourceMetrics) *collectorMetrics.ExportMetricsServiceRequest {
	return &collectorMetrics.ExportMetricsServiceRequest{ResourceMetrics: resourceMetrics}
}

func createIntHistogramMetric(name string, datapoints []*metrics.IntHistogramDataPoint) *metrics.Metric {
	return &metrics.Metric{
		Name: name,
		Data: &metrics.Metric_IntHistogram{
			IntHistogram: &metrics.IntHistogram{
				DataPoints: datapoints,
			},
		},
	}
}

// createLabel generates the proper structure for a label on
// a datapoint.
func createLabel(key string, value string) *common.StringKeyValue {
	return &common.StringKeyValue{
		Key:   key,
		Value: value,
	}
}

func createLabelsList(attributes map[string]string) (attributesList []*common.StringKeyValue) {
	for k, v := range attributes {
		attributesList = append(attributesList, createLabel(k, v))
	}
	return attributesList
}

func createSummaryMetric(name string, datapoints []*metrics.SummaryDataPoint) *metrics.Metric {
	return &metrics.Metric{
		Name: name,
		Data: &metrics.Metric_Summary{
			Summary: &metrics.Summary{DataPoints: datapoints},
		},
	}
}

// return a simple request with a resource name but a nil metric. Important to verify that we can
// handle nil metrics.
func createNilMetricServiceRequest() *collectorMetrics.ExportMetricsServiceRequest {
	resource := createResource([]map[string]string{{"service.name": "kafka", "broker": "2770"}})

	return createExportMetricsServiceRequest(
		[]*metrics.ResourceMetrics{createResourceMetric([]*metrics.Metric{nil}, resource)})
}

func createArbitraryMetricsList() []*metrics.Metric {
	return []*metrics.Metric{createArbitraryMetric()}
}

func createArbitraryMetric() *metrics.Metric {
	return createGaugeMetric("cpu_process_seconds", []*metrics.NumberDataPoint{
		createIntNumberDataPoint(rand.Int63n(1e9), createArbitraryMetricTimestamp(), createUniqueAttributeSet(3)),
	})
}

func createArbitraryMetricTimestamp() uint64 {
	return uint64(1614537516000000000)
}

func createArbitraryResource() *resource.Resource {
	return createResource([]map[string]string{{"service.name": "otel-collector"}})
}
