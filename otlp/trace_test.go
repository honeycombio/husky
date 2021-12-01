package otlp

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/husky/test"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestTranslateGrpcTraceRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()
	endTimestamp := startTimestamp.Add(time.Millisecond * 5)

	linkedTraceID := test.RandomBytes(16)
	linkedSpanID := test.RandomBytes(8)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "resource_attr",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "resource_attr_val"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId:           traceID,
					SpanId:            spanID,
					Name:              "test_span",
					Kind:              trace.Span_SPAN_KIND_CLIENT,
					Status:            &trace.Status{Code: trace.Status_STATUS_CODE_OK},
					StartTimeUnixNano: uint64(startTimestamp.Nanosecond()),
					EndTimeUnixNano:   uint64(endTimestamp.Nanosecond()),
					Attributes: []*common.KeyValue{
						{
							Key: "span_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_attr_val"},
							},
						},
						{
							Key: "sampleRate",
							Value: &common.AnyValue{
								Value: &common.AnyValue_IntValue{IntValue: 100},
							},
						},
					},
					Events: []*trace.Span_Event{{
						Name: "span_event",
						Attributes: []*common.KeyValue{{
							Key: "span_event_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_event_attr_val"},
							},
						}},
					}},
					Links: []*trace.Span_Link{{
						TraceId: linkedTraceID,
						SpanId:  linkedSpanID,
						Attributes: []*common.KeyValue{{
							Key: "span_link_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_link_attr_val"},
							},
						}},
					}},
				}},
			}},
		}},
	}

	result, err := TranslateTraceRequest(req)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 3, len(result.Events))

	// span
	ev := result.Events[0]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, int32(100), ev.SampleRate)
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
	assert.Equal(t, "client", ev.Attributes["type"])
	assert.Equal(t, "client", ev.Attributes["span.kind"])
	assert.Equal(t, "test_span", ev.Attributes["name"])
	assert.Equal(t, float64(endTimestamp.Nanosecond()-startTimestamp.Nanosecond())/float64(time.Millisecond), ev.Attributes["duration_ms"])
	assert.Equal(t, trace.Status_STATUS_CODE_OK, ev.Attributes["status_code"])
	assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

	// event
	ev = result.Events[1]
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, int32(0), ev.SampleRate)
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
	assert.Equal(t, "span_event", ev.Attributes["name"])
	assert.Equal(t, "test_span", ev.Attributes["parent_name"])
	assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
	assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

	// link
	ev = result.Events[2]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, int32(0), ev.SampleRate)
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
	assert.Equal(t, BytesToTraceID(linkedTraceID), ev.Attributes["trace.link.trace_id"])
	assert.Equal(t, hex.EncodeToString(linkedSpanID), ev.Attributes["trace.link.span_id"])
	assert.Equal(t, "test_span", ev.Attributes["parent_name"])
	assert.Equal(t, "link", ev.Attributes["meta.annotation_type"])
	assert.Equal(t, "span_link_attr_val", ev.Attributes["span_link_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
}

func TestTranslateHttpTraceRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()
	endTimestamp := startTimestamp.Add(time.Millisecond * 5)

	linkedTraceID := test.RandomBytes(16)
	linkedSpanID := test.RandomBytes(8)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "resource_attr",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "resource_attr_val"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId:           traceID,
					SpanId:            spanID,
					Name:              "test_span",
					Kind:              trace.Span_SPAN_KIND_CLIENT,
					Status:            &trace.Status{Code: trace.Status_STATUS_CODE_OK},
					StartTimeUnixNano: uint64(startTimestamp.Nanosecond()),
					EndTimeUnixNano:   uint64(endTimestamp.Nanosecond()),
					Attributes: []*common.KeyValue{{
						Key: "span_attr",
						Value: &common.AnyValue{
							Value: &common.AnyValue_StringValue{StringValue: "span_attr_val"},
						},
					}},
					Events: []*trace.Span_Event{{
						Name: "span_event",
						Attributes: []*common.KeyValue{{
							Key: "span_event_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_event_attr_val"},
							},
						}},
					}},
					Links: []*trace.Span_Link{{
						TraceId: linkedTraceID,
						SpanId:  linkedSpanID,
						Attributes: []*common.KeyValue{{
							Key: "span_link_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_link_attr_val"},
							},
						}},
					}},
				}},
			}},
		}},
	}

	bodyBytes, err := proto.Marshal(req)
	assert.Nil(t, err)

	for _, encoding := range []string{"", "gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			buf := new(bytes.Buffer)
			switch encoding {
			case "gzip":
				w := gzip.NewWriter(buf)
				w.Write(bodyBytes)
				w.Close()
			case "zstd":
				w, _ := zstd.NewWriter(buf)
				w.Write(bodyBytes)
				w.Close()
			default:
				buf.Write(bodyBytes)
			}

			body := io.NopCloser(strings.NewReader(buf.String()))
			ri := RequestInfo{
				ApiKey:          "apikey",
				Dataset:         "dataset",
				ContentType:     "application/protobuf",
				ContentEncoding: encoding,
			}

			result, err := TranslateTraceRequestFromReader(body, ri)
			assert.Nil(t, err)
			assert.Equal(t, proto.Size(req), result.RequestSize)
			assert.Equal(t, 3, len(result.Events))

			// span
			ev := result.Events[0]
			assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
			assert.Equal(t, "client", ev.Attributes["type"])
			assert.Equal(t, "client", ev.Attributes["span.kind"])
			assert.Equal(t, "test_span", ev.Attributes["name"])
			assert.Equal(t, float64(endTimestamp.Nanosecond()-startTimestamp.Nanosecond())/float64(time.Millisecond), ev.Attributes["duration_ms"])
			assert.Equal(t, trace.Status_STATUS_CODE_OK, ev.Attributes["status_code"])
			assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// event
			ev = result.Events[1]
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, "span_event", ev.Attributes["name"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// link
			ev = result.Events[2]
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, BytesToTraceID(linkedTraceID), ev.Attributes["trace.link.trace_id"])
			assert.Equal(t, hex.EncodeToString(linkedSpanID), ev.Attributes["trace.link.span_id"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "link", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, "span_link_attr_val", ev.Attributes["span_link_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
		})
	}
}

func TestInvalidContentTypeReturnsError(t *testing.T) {
	bodyBytes, _ := proto.Marshal(&collectortrace.ExportTraceServiceRequest{})
	body := io.NopCloser(bytes.NewReader(bodyBytes))
	ri := RequestInfo{
		ApiKey:      "apikey",
		Dataset:     "dataset",
		ContentType: "application/json",
	}

	result, err := TranslateTraceRequestFromReader(body, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrInvalidContentType, err)
}

func TestInvalidBodyReturnsError(t *testing.T) {
	bodyBytes := test.RandomBytes(10)
	body := io.NopCloser(bytes.NewReader(bodyBytes))
	ri := RequestInfo{
		ApiKey:      "apikey",
		Dataset:     "dataset",
		ContentType: "application/protobuf",
	}

	result, err := TranslateTraceRequestFromReader(body, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrFailedParseBody, err)
}

func TestNoSampleRateKeyReturnZero(t *testing.T) {
	attrs := map[string]interface{}{
		"not_a_sample_rate": 10,
	}
	sampleRate := getSampleRate(attrs)
	assert.Equal(t, int32(0), sampleRate)
}

func TestCanDetectSampleRateCapitalizations(t *testing.T) {
	t.Run("lowercase", func(t *testing.T) {
		attrs := map[string]interface{}{
			"sampleRate": 10,
		}
		key := getSampleRateKey(attrs)
		assert.Equal(t, "sampleRate", key)
	})
	t.Run("uppercase", func(t *testing.T) {
		attrs := map[string]interface{}{
			"SampleRate": 10,
		}
		key := getSampleRateKey(attrs)
		assert.Equal(t, "SampleRate", key)
	})
}

func TestGetSampleRateConversions(t *testing.T) {
	testCases := []struct {
		sampleRate interface{}
		expected   int32
	}{
		{sampleRate: nil, expected: 1},
		{sampleRate: "0", expected: 0},
		{sampleRate: "1", expected: 1},
		{sampleRate: "100", expected: 100},
		{sampleRate: "invalid", expected: 1},
		{sampleRate: strconv.Itoa(math.MaxInt32), expected: math.MaxInt32},
		{sampleRate: strconv.Itoa(math.MaxInt64), expected: math.MaxInt32},

		{sampleRate: 0, expected: 0},
		{sampleRate: 1, expected: 1},
		{sampleRate: 100, expected: 100},
		{sampleRate: math.MaxInt32, expected: math.MaxInt32},
		{sampleRate: math.MaxInt64, expected: math.MaxInt32},

		{sampleRate: int32(0), expected: 0},
		{sampleRate: int32(1), expected: 1},
		{sampleRate: int32(100), expected: 100},
		{sampleRate: int32(math.MaxInt32), expected: math.MaxInt32},

		{sampleRate: int64(0), expected: 0},
		{sampleRate: int64(1), expected: 1},
		{sampleRate: int64(100), expected: 100},
		{sampleRate: int64(math.MaxInt32), expected: math.MaxInt32},
		{sampleRate: int64(math.MaxInt64), expected: math.MaxInt32},
	}

	for _, tc := range testCases {
		attrs := map[string]interface{}{
			"sampleRate": tc.sampleRate,
		}
		assert.Equal(t, tc.expected, getSampleRate(attrs))
		assert.Equal(t, 0, len(attrs))
	}
}
