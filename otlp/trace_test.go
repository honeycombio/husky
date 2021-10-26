package otlp

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"
	"net/http"
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
	starTimestamp := time.Now()
	endTimestamp := starTimestamp.Add(time.Millisecond * 5)

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
					StartTimeUnixNano: uint64(starTimestamp.Nanosecond()),
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

	events, err := TranslateGrpcTraceRequest(req)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(events))

	// span
	span := events[0]
	assert.Equal(t, starTimestamp.Nanosecond(), span.timestamp.Nanosecond())
	assert.Equal(t, bytesToTraceID(traceID), span.fields["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), span.fields["trace.span_id"])
	assert.Equal(t, "client", span.fields["type"])
	assert.Equal(t, "client", span.fields["span.kind"])
	assert.Equal(t, "test_span", span.fields["name"])
	assert.Equal(t, float64(endTimestamp.Nanosecond()-starTimestamp.Nanosecond())/float64(time.Millisecond), span.fields["duration_ms"])
	assert.Equal(t, trace.Status_STATUS_CODE_OK, span.fields["status_code"])
	assert.Equal(t, "span_attr_val", span.fields["span_attr"])
	assert.Equal(t, "resource_attr_val", span.fields["resource_attr"])

	// event
	spanEvent := events[1]
	assert.Equal(t, bytesToTraceID(traceID), spanEvent.fields["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), spanEvent.fields["trace.parent_id"])
	assert.Equal(t, "span_event", spanEvent.fields["name"])
	assert.Equal(t, "test_span", spanEvent.fields["parent_name"])
	assert.Equal(t, "span_event", spanEvent.fields["meta.annotation_type"])
	assert.Equal(t, "span_event_attr_val", spanEvent.fields["span_event_attr"])
	assert.Equal(t, "resource_attr_val", spanEvent.fields["resource_attr"])

	// link
	spanLink := events[2]
	assert.Equal(t, bytesToTraceID(traceID), spanLink.fields["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), spanLink.fields["trace.parent_id"])
	assert.Equal(t, bytesToTraceID(linkedTraceID), spanLink.fields["trace.link.trace_id"])
	assert.Equal(t, hex.EncodeToString(linkedSpanID), spanLink.fields["trace.link.span_id"])
	assert.Equal(t, "test_span", spanLink.fields["parent_name"])
	assert.Equal(t, "link", spanLink.fields["meta.annotation_type"])
	assert.Equal(t, "span_link_attr_val", spanLink.fields["span_link_attr"])
	assert.Equal(t, "resource_attr_val", spanLink.fields["resource_attr"])
}

func TestTranslateHttpTraceRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	starTimestamp := time.Now()
	endTimestamp := starTimestamp.Add(time.Millisecond * 5)

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
					StartTimeUnixNano: uint64(starTimestamp.Nanosecond()),
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

	body, err := proto.Marshal(req)
	assert.Nil(t, err)

	for _, encoding := range []string{"", "gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			buf := new(bytes.Buffer)
			switch encoding {
			case "gzip":
				w := gzip.NewWriter(buf)
				w.Write(body)
				w.Close()
			case "zstd":
				w, _ := zstd.NewWriter(buf)
				w.Write(body)
				w.Close()
			default:
				buf.Write(body)
			}

			request, _ := http.NewRequest("POST", "", io.NopCloser(strings.NewReader(buf.String())))
			request.Header.Set("content-type", "application/protobuf")
			request.Header.Set("content-encoding", encoding)

			events, err := TranslateHttpTraceRequest(request)
			assert.Nil(t, err)
			assert.Equal(t, 3, len(events))

			// span
			span := events[0]
			assert.Equal(t, starTimestamp.Nanosecond(), span.timestamp.Nanosecond())
			assert.Equal(t, bytesToTraceID(traceID), span.fields["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), span.fields["trace.span_id"])
			assert.Equal(t, "client", span.fields["type"])
			assert.Equal(t, "client", span.fields["span.kind"])
			assert.Equal(t, "test_span", span.fields["name"])
			assert.Equal(t, float64(endTimestamp.Nanosecond()-starTimestamp.Nanosecond())/float64(time.Millisecond), span.fields["duration_ms"])
			assert.Equal(t, trace.Status_STATUS_CODE_OK, span.fields["status_code"])
			assert.Equal(t, "span_attr_val", span.fields["span_attr"])
			assert.Equal(t, "resource_attr_val", span.fields["resource_attr"])

			// event
			spanEvent := events[1]
			assert.Equal(t, bytesToTraceID(traceID), spanEvent.fields["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), spanEvent.fields["trace.parent_id"])
			assert.Equal(t, "span_event", spanEvent.fields["name"])
			assert.Equal(t, "test_span", spanEvent.fields["parent_name"])
			assert.Equal(t, "span_event", spanEvent.fields["meta.annotation_type"])
			assert.Equal(t, "span_event_attr_val", spanEvent.fields["span_event_attr"])
			assert.Equal(t, "resource_attr_val", spanEvent.fields["resource_attr"])

			// link
			spanLink := events[2]
			assert.Equal(t, bytesToTraceID(traceID), spanLink.fields["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), spanLink.fields["trace.parent_id"])
			assert.Equal(t, bytesToTraceID(linkedTraceID), spanLink.fields["trace.link.trace_id"])
			assert.Equal(t, hex.EncodeToString(linkedSpanID), spanLink.fields["trace.link.span_id"])
			assert.Equal(t, "test_span", spanLink.fields["parent_name"])
			assert.Equal(t, "link", spanLink.fields["meta.annotation_type"])
			assert.Equal(t, "span_link_attr_val", spanLink.fields["span_link_attr"])
			assert.Equal(t, "resource_attr_val", spanLink.fields["resource_attr"])
		})
	}
}
