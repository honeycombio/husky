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

func TestTranslateLegacyGrpcTraceRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()
	endTimestamp := startTimestamp.Add(time.Millisecond * 5)

	linkedTraceID := test.RandomBytes(16)
	linkedSpanID := test.RandomBytes(8)

	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "resource_attr",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "resource_attr_val"},
					},
				}, {
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
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

	result, err := TranslateTraceRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "legacy-dataset", batch.Dataset)
	assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
	events := batch.Events
	assert.Equal(t, 3, len(events))

	// span
	ev := events[0]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, int32(100), ev.SampleRate)
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
	assert.Equal(t, "client", ev.Attributes["type"])
	assert.Equal(t, "client", ev.Attributes["span.kind"])
	assert.Equal(t, "test_span", ev.Attributes["name"])
	assert.Equal(t, "my-service", ev.Attributes["service.name"])
	assert.Equal(t, float64(endTimestamp.Nanosecond()-startTimestamp.Nanosecond())/float64(time.Millisecond), ev.Attributes["duration_ms"])
	assert.Equal(t, int(trace.Status_STATUS_CODE_OK), ev.Attributes["status_code"])
	assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
	assert.Equal(t, 1, ev.Attributes["span.num_links"])
	assert.Equal(t, 1, ev.Attributes["span.num_events"])

	// event
	ev = events[1]
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, int32(0), ev.SampleRate)
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
	assert.Equal(t, "span_event", ev.Attributes["name"])
	assert.Equal(t, "test_span", ev.Attributes["parent_name"])
	assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
	assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

	// link
	ev = events[2]
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

func TestTranslateGrpcTraceRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()
	endTimestamp := startTimestamp.Add(time.Millisecond * 5)

	linkedTraceID := test.RandomBytes(16)
	linkedSpanID := test.RandomBytes(8)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "resource_attr",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "resource_attr_val"},
					},
				}, {
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
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

	result, err := TranslateTraceRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "my-service", batch.Dataset)
	assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
	events := batch.Events
	assert.Equal(t, 3, len(events))

	// span
	ev := events[0]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, int32(100), ev.SampleRate)
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
	assert.Equal(t, "client", ev.Attributes["type"])
	assert.Equal(t, "client", ev.Attributes["span.kind"])
	assert.Equal(t, "test_span", ev.Attributes["name"])
	assert.Equal(t, float64(endTimestamp.Nanosecond()-startTimestamp.Nanosecond())/float64(time.Millisecond), ev.Attributes["duration_ms"])
	assert.Equal(t, int(trace.Status_STATUS_CODE_OK), ev.Attributes["status_code"])
	assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
	assert.Equal(t, 1, ev.Attributes["span.num_links"])
	assert.Equal(t, 1, ev.Attributes["span.num_events"])

	// event
	ev = events[1]
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, int32(0), ev.SampleRate)
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
	assert.Equal(t, "span_event", ev.Attributes["name"])
	assert.Equal(t, "test_span", ev.Attributes["parent_name"])
	assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
	assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

	// link
	ev = events[2]
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

func TestTranslateGrpcTraceRequestFromMultipleServices(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service-a"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}},
		}, {
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service-b"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_b",
				}},
			}},
		}},
	}

	result, err := TranslateTraceRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 2, len(result.Batches))
	batchA := result.Batches[0]
	batchB := result.Batches[1]
	assert.Equal(t, "my-service-a", batchA.Dataset)
	assert.Equal(t, "my-service-b", batchB.Dataset)
	assert.Equal(t, proto.Size(req.ResourceSpans[0]), batchA.SizeBytes)
	assert.Equal(t, proto.Size(req.ResourceSpans[1]), batchB.SizeBytes)
	eventsA := batchA.Events
	eventsB := batchB.Events
	assert.Equal(t, 1, len(eventsA))
	assert.Equal(t, 1, len(eventsB))

	assert.Equal(t, "test_span_a", eventsA[0].Attributes["name"])
	assert.Equal(t, "test_span_b", eventsB[0].Attributes["name"])
}

func TestTranslateGrpcTraceRequestFromMultipleLibraries(t *testing.T) {
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service-a"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				InstrumentationLibrary: &common.InstrumentationLibrary{
					Name:    "First",
					Version: "1.1.1",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}, {
				InstrumentationLibrary: &common.InstrumentationLibrary{
					Name:    "Second",
					Version: "2.2.2",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_b",
				}},
			}, {
				InstrumentationLibrary: &common.InstrumentationLibrary{
					Name: "No Version Library",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_c",
				}},
			}},
		}},
	}

	result, err := TranslateTraceRequest(req, ri)
	assert.NoError(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "my-service-a", batch.Dataset)
	events := batch.Events
	assert.Equal(t, 3, len(events))

	first_event := events[0]
	assert.Equal(t, "test_span_a", first_event.Attributes["name"])
	assert.Equal(t, "First", first_event.Attributes["library.name"])
	assert.Equal(t, "1.1.1", first_event.Attributes["library.version"])
	second_event := events[1]
	assert.Equal(t, "test_span_b", second_event.Attributes["name"])
	assert.Equal(t, "Second", second_event.Attributes["library.name"])
	assert.Equal(t, "2.2.2", second_event.Attributes["library.version"])
	third_event := events[2]
	assert.Equal(t, "test_span_c", third_event.Attributes["name"])
	assert.Equal(t, "No Version Library", third_event.Attributes["library.name"])
	assert.Nil(t, third_event.Attributes["library.version"], "A library span with no library version shouldn't have a version.")
}

func TestTranslateLegacyHttpTraceRequest(t *testing.T) {
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
				}, {
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
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
				ApiKey:          "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				Dataset:         "legacy-dataset",
				ContentType:     "application/protobuf",
				ContentEncoding: encoding,
			}

			result, err := TranslateTraceRequestFromReader(body, ri)
			assert.Nil(t, err)
			assert.Equal(t, proto.Size(req), result.RequestSize)
			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, "legacy-dataset", batch.Dataset)
			assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
			events := batch.Events
			assert.Equal(t, 3, len(events))

			// span
			ev := events[0]
			assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
			assert.Equal(t, "client", ev.Attributes["type"])
			assert.Equal(t, "client", ev.Attributes["span.kind"])
			assert.Equal(t, "test_span", ev.Attributes["name"])
			assert.Equal(t, "my-service", ev.Attributes["service.name"])
			assert.Equal(t, float64(endTimestamp.Nanosecond()-startTimestamp.Nanosecond())/float64(time.Millisecond), ev.Attributes["duration_ms"])
			assert.Equal(t, int(trace.Status_STATUS_CODE_OK), ev.Attributes["status_code"])
			assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// event
			ev = events[1]
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, "span_event", ev.Attributes["name"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// link
			ev = events[2]
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
				}, {
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
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
				ApiKey:          "abc123DEF456ghi789jklm",
				Dataset:         "legacy-dataset",
				ContentType:     "application/protobuf",
				ContentEncoding: encoding,
			}

			result, err := TranslateTraceRequestFromReader(body, ri)
			assert.Nil(t, err)
			assert.Equal(t, proto.Size(req), result.RequestSize)
			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, "my-service", batch.Dataset)
			assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
			events := batch.Events
			assert.Equal(t, 3, len(events))

			// span
			ev := events[0]
			assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
			assert.Equal(t, "client", ev.Attributes["type"])
			assert.Equal(t, "client", ev.Attributes["span.kind"])
			assert.Equal(t, "test_span", ev.Attributes["name"])
			assert.Equal(t, "my-service", ev.Attributes["service.name"])
			assert.Equal(t, float64(endTimestamp.Nanosecond()-startTimestamp.Nanosecond())/float64(time.Millisecond), ev.Attributes["duration_ms"])
			assert.Equal(t, int(trace.Status_STATUS_CODE_OK), ev.Attributes["status_code"])
			assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// event
			ev = events[1]
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, "span_event", ev.Attributes["name"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// link
			ev = events[2]
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

func TestTranslateHttpTraceRequestFromMultipleServices(t *testing.T) {
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service-a"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}},
		}, {
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service-b"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_b",
				}},
			}},
		}},
	}

	bodyBytes, err := proto.Marshal(req)
	assert.Nil(t, err)

	buf := new(bytes.Buffer)
	buf.Write(bodyBytes)

	body := io.NopCloser(strings.NewReader(buf.String()))
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	result, err := TranslateTraceRequestFromReader(body, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 2, len(result.Batches))

	batchA := result.Batches[0]
	batchB := result.Batches[1]
	assert.Equal(t, "my-service-a", batchA.Dataset)
	assert.Equal(t, "my-service-b", batchB.Dataset)
	assert.Equal(t, proto.Size(req.ResourceSpans[0]), batchA.SizeBytes)
	assert.Equal(t, proto.Size(req.ResourceSpans[1]), batchB.SizeBytes)
	eventsA := batchA.Events
	eventsB := batchB.Events
	assert.Equal(t, 1, len(eventsA))
	assert.Equal(t, 1, len(eventsB))

	assert.Equal(t, "test_span_a", eventsA[0].Attributes["name"])
	assert.Equal(t, "test_span_b", eventsB[0].Attributes["name"])
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

func TestNoSampleRateKeyReturnOne(t *testing.T) {
	attrs := map[string]interface{}{
		"not_a_sample_rate": 10,
	}
	sampleRate := getSampleRate(attrs)
	assert.Equal(t, int32(1), sampleRate)
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
		{sampleRate: "0", expected: 1},
		{sampleRate: "1", expected: 1},
		{sampleRate: "100", expected: 100},
		{sampleRate: "invalid", expected: 1},
		{sampleRate: strconv.Itoa(math.MaxInt32), expected: math.MaxInt32},
		{sampleRate: strconv.Itoa(math.MaxInt64), expected: math.MaxInt32},

		{sampleRate: 0, expected: 1},
		{sampleRate: 1, expected: 1},
		{sampleRate: 100, expected: 100},
		{sampleRate: math.MaxInt32, expected: math.MaxInt32},
		{sampleRate: math.MaxInt64, expected: math.MaxInt32},

		{sampleRate: int32(0), expected: 1},
		{sampleRate: int32(1), expected: 1},
		{sampleRate: int32(100), expected: 100},
		{sampleRate: int32(math.MaxInt32), expected: math.MaxInt32},

		{sampleRate: int64(0), expected: 1},
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

func TestMissingServiceNameAttributeUsesDefault(t *testing.T) {
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "my-attribute",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "hello"},
					},
				}},
			},
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}},
		}},
	}

	bodyBytes, err := proto.Marshal(req)
	assert.Nil(t, err)

	buf := new(bytes.Buffer)
	buf.Write(bodyBytes)

	body := io.NopCloser(strings.NewReader(buf.String()))
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	result, err := TranslateTraceRequestFromReader(body, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	assert.Equal(t, "unknown_service", batch.Dataset)
}

func TestMissingServiceNameResourceUsesDefault(t *testing.T) {
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}},
		}},
	}

	bodyBytes, err := proto.Marshal(req)
	assert.Nil(t, err)

	buf := new(bytes.Buffer)
	buf.Write(bodyBytes)

	body := io.NopCloser(strings.NewReader(buf.String()))
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	result, err := TranslateTraceRequestFromReader(body, ri)
	assert.Nil(t, err)
	batch := result.Batches[0]
	assert.Equal(t, "unknown_service", batch.Dataset)
}

func TestEmptyOrInvalidServiceNameAttributeUsesDefault(t *testing.T) {
	testCases := []struct {
		name     string
		resource *resource.Resource
	}{
		{
			name:     "nil resource",
			resource: nil,
		},
		{
			name: "empty string",
			resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: ""},
					},
				}},
			},
		},
		{
			name: "string with whitespace",
			resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: " \t"},
					},
				}},
			},
		},
		{
			name: "integer",
			resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_IntValue{IntValue: 2},
					},
				}},
			},
		},
		{
			name: "boolean",
			resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_BoolValue{BoolValue: true},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &collectortrace.ExportTraceServiceRequest{
				ResourceSpans: []*trace.ResourceSpans{{
					Resource: tc.resource,
					InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
						Spans: []*trace.Span{{
							TraceId: test.RandomBytes(16),
							SpanId:  test.RandomBytes(8),
							Name:    "test_span_a",
						}},
					}},
				}},
			}

			bodyBytes, err := proto.Marshal(req)
			assert.Nil(t, err)

			buf := new(bytes.Buffer)
			buf.Write(bodyBytes)

			body := io.NopCloser(strings.NewReader(buf.String()))
			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			}

			result, err := TranslateTraceRequestFromReader(body, ri)
			assert.Nil(t, err)
			batch := result.Batches[0]
			assert.NotNil(t, batch.Dataset)
			assert.NotEqual(t, 2, batch.Dataset)
			assert.NotEqual(t, "2", batch.Dataset)
			assert.NotEqual(t, true, batch.Dataset)
			assert.NotEqual(t, "true", batch.Dataset)
			assert.Equal(t, "unknown_service", batch.Dataset)
		})
	}
}

func TestUnknownServiceNameIsTruncatedForDataset(t *testing.T) {
	testCases := []struct {
		resourceServiceName      string
		expectedDataset          string
		expectedEventServiceName string
	}{
		{
			resourceServiceName:      "unknown_service:go",
			expectedDataset:          "unknown_service",
			expectedEventServiceName: "unknown_service:go",
		}, {
			resourceServiceName:      "unknown_servicego",
			expectedDataset:          "unknown_service",
			expectedEventServiceName: "unknown_servicego",
		}, {
			resourceServiceName:      "so_unknown_service:go",
			expectedDataset:          "so_unknown_service:go",
			expectedEventServiceName: "so_unknown_service:go",
		}, {
			resourceServiceName:      "go:unknown_service",
			expectedDataset:          "go:unknown_service",
			expectedEventServiceName: "go:unknown_service",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.resourceServiceName, func(t *testing.T) {
			req := &collectortrace.ExportTraceServiceRequest{
				ResourceSpans: []*trace.ResourceSpans{{
					Resource: &resource.Resource{
						Attributes: []*common.KeyValue{{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: tc.resourceServiceName},
							},
						}},
					},
					InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
						Spans: []*trace.Span{{
							TraceId: test.RandomBytes(16),
							SpanId:  test.RandomBytes(8),
							Name:    "test_span_a",
						}},
					}},
				}},
			}

			bodyBytes, err := proto.Marshal(req)
			assert.Nil(t, err)

			buf := new(bytes.Buffer)
			buf.Write(bodyBytes)

			body := io.NopCloser(strings.NewReader(buf.String()))
			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			}

			result, err := TranslateTraceRequestFromReader(body, ri)
			assert.Nil(t, err)

			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, tc.expectedDataset, batch.Dataset)

			assert.Equal(t, 1, len(result.Batches[0].Events))
			event := result.Batches[0].Events[0]
			assert.Equal(t, tc.expectedEventServiceName, event.Attributes["service.name"])
		})
	}
}

func TestServiceNameIsTrimmedForDataset(t *testing.T) {
	testCases := []struct {
		resourceServiceName      string
		expectedDataset          string
		expectedEventServiceName string
	}{
		{
			resourceServiceName:      "no-whitespace-for-you",
			expectedDataset:          "no-whitespace-for-you",
			expectedEventServiceName: "no-whitespace-for-you",
		}, {
			resourceServiceName:      " leading-whitespace",
			expectedDataset:          "leading-whitespace",
			expectedEventServiceName: " leading-whitespace",
		}, {
			resourceServiceName:      "trailing-whitespace\t",
			expectedDataset:          "trailing-whitespace",
			expectedEventServiceName: "trailing-whitespace\t",
		}, {
			resourceServiceName:      "\tall the whitespace ",
			expectedDataset:          "all the whitespace",
			expectedEventServiceName: "\tall the whitespace ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.resourceServiceName, func(t *testing.T) {
			req := &collectortrace.ExportTraceServiceRequest{
				ResourceSpans: []*trace.ResourceSpans{{
					Resource: &resource.Resource{
						Attributes: []*common.KeyValue{{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: tc.resourceServiceName},
							},
						}},
					},
					InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
						Spans: []*trace.Span{{
							TraceId: test.RandomBytes(16),
							SpanId:  test.RandomBytes(8),
							Name:    "test_span_a",
						}},
					}},
				}},
			}

			bodyBytes, err := proto.Marshal(req)
			assert.Nil(t, err)

			buf := new(bytes.Buffer)
			buf.Write(bodyBytes)

			body := io.NopCloser(strings.NewReader(buf.String()))
			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			}

			result, err := TranslateTraceRequestFromReader(body, ri)
			assert.Nil(t, err)

			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, tc.expectedDataset, batch.Dataset)

			assert.Equal(t, 1, len(result.Batches[0].Events))
			event := result.Batches[0].Events[0]
			assert.Equal(t, tc.expectedEventServiceName, event.Attributes["service.name"])
		})
	}
}

func TestEvaluateSpanStatus(t *testing.T) {
	testCases := []struct {
		desc               string
		status             *trace.Status
		expectedStatusCode int
	}{
		{
			desc:               "returns unset when status is nil",
			status:             nil,
			expectedStatusCode: int(trace.Status_STATUS_CODE_UNSET),
		},
		// Cases for the rules for old receivers at:
		// https://github.com/open-telemetry/opentelemetry-proto/blob/59c488bfb8fb6d0458ad6425758b70259ff4a2bd/opentelemetry/proto/trace/v1/trace.proto#L251-L266
		//
		//   If code==STATUS_CODE_UNSET [and] deprecated_code==DEPRECATED_STATUS_CODE_OK
		//   then the receiver MUST interpret the overall status to be STATUS_CODE_UNSET.
		{
			desc: "returns unset when code is UNSET and deprecated_code is OK",
			status: &trace.Status{
				Code:           trace.Status_STATUS_CODE_UNSET,
				DeprecatedCode: trace.Status_DEPRECATED_STATUS_CODE_OK,
				Message:        "Old OK!",
			},
			expectedStatusCode: int(trace.Status_STATUS_CODE_UNSET),
		},
		//   If code==STATUS_CODE_UNSET [and] deprecated_code!=DEPRECATED_STATUS_CODE_OK
		//   then the receiver MUST interpret the overall status to be STATUS_CODE_ERROR.
		{
			desc: "returns error when code is UNSET and deprecated_code is not OK",
			status: &trace.Status{
				Code:           trace.Status_STATUS_CODE_UNSET,
				DeprecatedCode: trace.Status_DEPRECATED_STATUS_CODE_ABORTED,
				Message:        "Old not OK!",
			},
			expectedStatusCode: int(trace.Status_STATUS_CODE_ERROR),
		},
		//   If code!=STATUS_CODE_UNSET then the value of `deprecated_code` MUST be
		//   ignored, the `code` field is the sole carrier of the status.
		{
			desc: "returns code when code is not UNSET and deprecated_code is anything",
			status: &trace.Status{
				Code:           trace.Status_STATUS_CODE_ERROR,
				DeprecatedCode: trace.Status_DEPRECATED_STATUS_CODE_OK,
				Message:        "Old OK!",
			},
			expectedStatusCode: int(trace.Status_STATUS_CODE_ERROR),
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			status_code := evaluateSpanStatus(tC.status)
			assert.Equal(t, tC.expectedStatusCode, status_code)
		})
	}
}
