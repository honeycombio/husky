package otlp

import (
	"bytes"
	"encoding/hex"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/husky/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	testServiceName := "my-service"

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
						Value: &common.AnyValue_StringValue{StringValue: testServiceName},
					},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Scope: &common.InstrumentationScope{
					Name:    "library-name",
					Version: "library-version",
					Attributes: []*common.KeyValue{
						{
							Key: "scope_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "scope_attr_val"},
							},
						},
					},
				},
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

	testCases := []struct {
		Name            string
		ri              RequestInfo
		expectedDataset string
	}{
		{
			Name: "Classic",
			ri: RequestInfo{
				ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			},
			expectedDataset: "legacy-dataset",
		},
		{
			Name: "E&S",
			ri: RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			},
			expectedDataset: testServiceName,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Name, func(t *testing.T) {

			result, err := TranslateTraceRequest(req, tC.ri)
			assert.Nil(t, err)
			assert.Equal(t, proto.Size(req), result.RequestSize)
			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, tC.expectedDataset, batch.Dataset)
			assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
			events := batch.Events
			assert.Equal(t, 3, len(events))

			// event
			ev := events[0]
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, int32(100), ev.SampleRate)
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, "span_event", ev.Attributes["name"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// link
			ev = events[1]
			assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
			assert.Equal(t, int32(100), ev.SampleRate)
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, BytesToTraceID(linkedTraceID), ev.Attributes["trace.link.trace_id"])
			assert.Equal(t, hex.EncodeToString(linkedSpanID), ev.Attributes["trace.link.span_id"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "link", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, "span_link_attr_val", ev.Attributes["span_link_attr"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// span
			ev = events[2]
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
		})
	}
}

func TestTranslateException(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()
	endTimestamp := startTimestamp.Add(time.Millisecond * 5)

	testServiceName := "my-service"

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
						Value: &common.AnyValue_StringValue{StringValue: testServiceName},
					},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Scope: &common.InstrumentationScope{
					Name:    "library-name",
					Version: "library-version",
					Attributes: []*common.KeyValue{
						{
							Key: "scope_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "scope_attr_val"},
							},
						},
					},
				},
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
						Name: "exception",
						Attributes: []*common.KeyValue{
							{
								Key: "exception.type",
								Value: &common.AnyValue{
									Value: &common.AnyValue_StringValue{StringValue: "something_broke"},
								},
							},
							{
								Key: "exception.stacktrace",
								Value: &common.AnyValue{
									Value: &common.AnyValue_StringValue{StringValue: "this stacktrace should be long"},
								},
							},
							{
								Key: "exception.escaped",
								Value: &common.AnyValue{
									Value: &common.AnyValue_BoolValue{BoolValue: true},
								},
							},
							{
								Key: "exception.message",
								Value: &common.AnyValue{
									Value: &common.AnyValue_StringValue{StringValue: "aaaaaaa!!"},
								},
							},
						},
					}},
				}},
			}},
		}},
	}

	testCases := []struct {
		Name            string
		ri              RequestInfo
		expectedDataset string
	}{
		{
			Name: "Classic",
			ri: RequestInfo{
				ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			},
			expectedDataset: "legacy-dataset",
		},
		{
			Name: "E&S",
			ri: RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			},
			expectedDataset: testServiceName,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Name, func(t *testing.T) {

			result, err := TranslateTraceRequest(req, tC.ri)
			assert.Nil(t, err)
			assert.Equal(t, proto.Size(req), result.RequestSize)
			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, tC.expectedDataset, batch.Dataset)
			assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
			events := batch.Events
			assert.Equal(t, 2, len(events))

			// event
			ev := events[0]
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, int32(100), ev.SampleRate)
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, "exception", ev.Attributes["name"])
			assert.Equal(t, "test_span", ev.Attributes["parent_name"])
			assert.Equal(t, "something_broke", ev.Attributes["exception.type"])
			assert.Equal(t, "this stacktrace should be long", ev.Attributes["exception.stacktrace"])
			assert.Equal(t, true, ev.Attributes["exception.escaped"])
			assert.Equal(t, "aaaaaaa!!", ev.Attributes["exception.message"])
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

			// span
			ev = events[1]
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
			assert.Equal(t, true, ev.Attributes["exception.escaped"])
			assert.Equal(t, "aaaaaaa!!", ev.Attributes["exception.message"])
			assert.Equal(t, "something_broke", ev.Attributes["exception.type"])
			assert.Equal(t, "this stacktrace should be long", ev.Attributes["exception.stacktrace"])
			assert.Equal(t, 0, ev.Attributes["span.num_links"])
			assert.Equal(t, 1, ev.Attributes["span.num_events"])
		})
	}
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
			ScopeSpans: []*trace.ScopeSpans{{
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
			ScopeSpans: []*trace.ScopeSpans{{
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
			ScopeSpans: []*trace.ScopeSpans{{
				Scope: &common.InstrumentationScope{
					Name:    "First",
					Version: "1.1.1",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}, {
				Scope: &common.InstrumentationScope{
					Name:    "Second",
					Version: "2.2.2",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_b",
				}},
			}, {
				Scope: &common.InstrumentationScope{
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

func TestTranslateHttpTraceRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()
	endTimestamp := startTimestamp.Add(time.Millisecond * 5)

	linkedTraceID := test.RandomBytes(16)
	linkedSpanID := test.RandomBytes(8)

	testServiceName := "my-service"

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
						Value: &common.AnyValue_StringValue{StringValue: testServiceName},
					},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
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

	testCases := []struct {
		Name            string
		ri              RequestInfo
		expectedDataset string
	}{
		{
			Name: "Classic",
			ri: RequestInfo{
				ApiKey:  "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				Dataset: "legacy-dataset",
			},
			expectedDataset: "legacy-dataset",
		},
		{
			Name: "E&S",
			ri: RequestInfo{
				ApiKey:  "abc123DEF456ghi789jklm",
				Dataset: "legacy-dataset",
			},
			expectedDataset: testServiceName,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.Name, func(t *testing.T) {
			for _, testCaseContentType := range GetSupportedContentTypes() {
				t.Run(testCaseNameForContentType(testCaseContentType), func(t *testing.T) {
					for _, testCaseContentEncoding := range GetSupportedContentEncodings() {
						t.Run(testCaseNameForEncoding(testCaseContentEncoding), func(t *testing.T) {

							tC.ri.ContentType = testCaseContentType
							tC.ri.ContentEncoding = testCaseContentEncoding

							body, err := prepareOtlpRequestHttpBody(req, testCaseContentType, testCaseContentEncoding)
							require.NoError(t, err, "Womp womp. Ought to have been able to turn the OTLP trace request into an HTTP body.")

							result, err := TranslateTraceRequestFromReader(io.NopCloser(strings.NewReader(body)), tC.ri)
							assert.Nil(t, err)
							assert.Equal(t, proto.Size(req), result.RequestSize)
							assert.Equal(t, 1, len(result.Batches))
							batch := result.Batches[0]
							assert.Equal(t, tC.expectedDataset, batch.Dataset)
							assert.Equal(t, proto.Size(req.ResourceSpans[0]), batch.SizeBytes)
							events := batch.Events
							assert.Equal(t, 3, len(events))

							// event
							ev := events[0]
							assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
							assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
							assert.Equal(t, "span_event", ev.Attributes["name"])
							assert.Equal(t, "test_span", ev.Attributes["parent_name"])
							assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
							assert.Equal(t, "span_event_attr_val", ev.Attributes["span_event_attr"])
							assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

							// link
							ev = events[1]
							assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
							assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
							assert.Equal(t, BytesToTraceID(linkedTraceID), ev.Attributes["trace.link.trace_id"])
							assert.Equal(t, hex.EncodeToString(linkedSpanID), ev.Attributes["trace.link.span_id"])
							assert.Equal(t, "test_span", ev.Attributes["parent_name"])
							assert.Equal(t, "link", ev.Attributes["meta.annotation_type"])
							assert.Equal(t, "span_link_attr_val", ev.Attributes["span_link_attr"])
							assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])

							// span
							ev = events[2]
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
						})
					}
				})
			}
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
			ScopeSpans: []*trace.ScopeSpans{{
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
			ScopeSpans: []*trace.ScopeSpans{{
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
		ContentType: "application/binary",
	}

	result, err := TranslateTraceRequestFromReader(body, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrInvalidContentType, err)
}

func TestInvalidBodyReturnsError(t *testing.T) {
	bodyBytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
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

func TestDefaultServiceNameApplied(t *testing.T) {
	testCases := []struct {
		name     string
		resource *resource.Resource
	}{
		{
			name:     "nil resource",
			resource: nil,
		},
		{
			name: "missing in resource",
			resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "my-attribute",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "hello"},
					},
				}},
			},
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
					ScopeSpans: []*trace.ScopeSpans{{
						Spans: []*trace.Span{{
							TraceId: test.RandomBytes(16),
							SpanId:  test.RandomBytes(8),
							Name:    "test_span_a",
						}},
					}},
				}},
			}

			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			}

			result, err := TranslateTraceRequest(req, ri)
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
					ScopeSpans: []*trace.ScopeSpans{{
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
					ScopeSpans: []*trace.ScopeSpans{{
						Spans: []*trace.Span{{
							TraceId: test.RandomBytes(16),
							SpanId:  test.RandomBytes(8),
							Name:    "test_span_a",
						}},
					}},
				}},
			}

			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			}

			result, err := TranslateTraceRequest(req, ri)
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
		expectedIsError    bool
	}{
		{
			desc:               "returns unset when status is nil",
			status:             nil,
			expectedStatusCode: int(trace.Status_STATUS_CODE_UNSET),
			expectedIsError:    false,
		},
		{
			desc: "returns unset when code is UNSET",
			status: &trace.Status{
				Code:    trace.Status_STATUS_CODE_UNSET,
				Message: "UNSET!",
			},
			expectedStatusCode: int(trace.Status_STATUS_CODE_UNSET),
			expectedIsError:    false,
		},
		{
			desc: "returns error when code is ERROR",
			status: &trace.Status{
				Code:    trace.Status_STATUS_CODE_ERROR,
				Message: "ERROR!",
			},
			expectedStatusCode: int(trace.Status_STATUS_CODE_ERROR),
			expectedIsError:    true,
		},
		{
			desc: "returns ok when code is OK",
			status: &trace.Status{
				Code:    trace.Status_STATUS_CODE_OK,
				Message: "OK!",
			},
			expectedStatusCode: int(trace.Status_STATUS_CODE_OK),
			expectedIsError:    false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			statusCode, isError := getSpanStatusCode(tC.status)
			assert.Equal(t, tC.expectedStatusCode, statusCode)
			assert.Equal(t, tC.expectedIsError, isError)
		})
	}
}

func TestBadTraceRequest(t *testing.T) {
	for _, contentType := range []string{"application/protobuf", "application/json"} {
		for _, encoding := range []string{"", "gzip", "zstd"} {
			t.Run(encoding, func(t *testing.T) {
				body := io.NopCloser(strings.NewReader("lol"))
				ri := RequestInfo{
					ApiKey:          "abc123DEF456ghi789jklm",
					Dataset:         "legacy-dataset",
					ContentType:     contentType,
					ContentEncoding: encoding,
				}

				_, err := TranslateTraceRequestFromReader(body, ri)
				assert.NotNil(t, err)
				assert.Equal(t, ErrFailedParseBody, err)
			})
		}
	}
}

func TestInstrumentationLibrarySpansHaveAttributeAdded(t *testing.T) {
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
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Scope: &common.InstrumentationScope{
					Name:    "First",
					Version: "1.1.1",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_a",
				}},
			}, {
				Scope: &common.InstrumentationScope{
					Name:    "io.opentelemetry.instrumentation.http", // instrumentation library
					Version: "2.2.2",
				},
				Spans: []*trace.Span{{
					TraceId: test.RandomBytes(16),
					SpanId:  test.RandomBytes(8),
					Name:    "test_span_b",
				}},
			}},
		}},
	}

	result, err := TranslateTraceRequest(req, ri)
	assert.NoError(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "my-service", batch.Dataset)
	events := batch.Events
	assert.Equal(t, 2, len(events))

	first_event := events[0]
	assert.Equal(t, "test_span_a", first_event.Attributes["name"])
	assert.Equal(t, "First", first_event.Attributes["library.name"])
	assert.Equal(t, "1.1.1", first_event.Attributes["library.version"])
	assert.NotContains(t, first_event.Attributes, "telemtetry.instrumentation_library")
	second_event := events[1]
	assert.Equal(t, "test_span_b", second_event.Attributes["name"])
	assert.Equal(t, "io.opentelemetry.instrumentation.http", second_event.Attributes["library.name"])
	assert.Equal(t, "2.2.2", second_event.Attributes["library.version"])
	assert.Equal(t, true, second_event.Attributes["telemetry.instrumentation_library"])
}

func TestKnownInstrumentationPrefixesReturnTrue(t *testing.T) {
	tests := []struct {
		name                     string
		libraryName              string
		isInstrumentationLibrary bool
	}{
		{
			name:                     "empty",
			libraryName:              "",
			isInstrumentationLibrary: false,
		},
		{
			name:                     "unknown",
			libraryName:              "unknown",
			isInstrumentationLibrary: false,
		},
		{
			name:                     "java",
			libraryName:              "io.opentelemetry.tomcat-7.0",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "python",
			libraryName:              "opentelemetry.instrumentation.http",
			isInstrumentationLibrary: true,
		},
		{
			name:                     ".net",
			libraryName:              "OpenTelemetry.Instrumentation.AspNetCore",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "ruby",
			libraryName:              "OpenTelemetry::Instrumentation::HTTP",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "go",
			libraryName:              "go.opentelemetry.io/contrib/instrumentation/http",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "js",
			libraryName:              "@opentelemetry/instrumentation/http",
			isInstrumentationLibrary: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.isInstrumentationLibrary, isInstrumentationLibrary(test.libraryName))
		})
	}
}
