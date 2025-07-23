package otlp

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// convertBatchMsgpToBatch converts a BatchMsgp to a Batch by deserializing the msgpack attributes
func convertBatchMsgpToBatch(t testing.TB, msgpBatch BatchMsgp) Batch {
	batch := Batch{
		Dataset: msgpBatch.Dataset,
		Events:  make([]Event, len(msgpBatch.Events)),
	}

	for i, msgpEvent := range msgpBatch.Events {
		attrs := decodeMessagePackAttributes(t, msgpEvent.Attributes)

		// Normalize integer types to match what the regular unmarshaling produces
		normalizeIntegerTypes(attrs)

		batch.Events[i] = Event{
			Attributes: attrs,
			Timestamp:  msgpEvent.Timestamp,
			SampleRate: msgpEvent.SampleRate,
		}
	}

	return batch
}

// normalizeIntegerTypes converts integer types to match the regular unmarshaling behavior
func normalizeIntegerTypes(attrs map[string]any) {
	for k, v := range attrs {
		switch val := v.(type) {
		case int64:
			// For the special fields, convert to int
			if k == "status_code" || k == "span.num_events" || k == "span.num_links" {
				attrs[k] = int(val)
			}
		}
	}
}

func hexToBin(s string) []byte {
	bin, _ := hex.DecodeString(s)
	return bin
}

func TestUnmarshalTraceRequestDirect_Complete(t *testing.T) {
	// Create a comprehensive test request with all supported features
	// Using human-readable hex strings for clarity
	traceID1 := "0102030405060708090a0b0c0d0e0f10"
	spanID1 := "0102030405060708"
	parentSpanID1 := "1112131415161718"

	// Create a trace ID with leading zeros to test trimming
	traceID2 := "00000000000000001a2b3c4d5e6f7089"
	spanID2 := "2122232425262728"

	linkedTraceID := "3132333435363738393a3b3c3d3e3f40"
	linkedSpanID := "4142434445464748"

	errorSpanID := "5152535455565758"

	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)
	event1Time := uint64(1234567890123456789 + 100_000_000) // 100ms after start
	event2Time := uint64(1234567890123456789 + 200_000_000) // 200ms after start

	// For the error span with negative duration
	errorStartTime := uint64(1234567890987654321)
	errorEndTime := uint64(1234567890123456789) // end before start

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			// First ResourceSpan - service1 with comprehensive attributes
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "service1"},
							},
						},
						{
							Key: "deployment.environment",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "production"},
							},
						},
						{
							Key: "bytes_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_BytesValue{BytesValue: []byte{0x01, 0x02, 0x03, 0x04}},
							},
						},
						{
							Key: "array_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_ArrayValue{
									ArrayValue: &common.ArrayValue{
										Values: []*common.AnyValue{
											{Value: &common.AnyValue_StringValue{StringValue: "item1"}},
											{Value: &common.AnyValue_IntValue{IntValue: 42}},
											{Value: &common.AnyValue_BoolValue{BoolValue: true}},
											{Value: &common.AnyValue_DoubleValue{DoubleValue: 3.14}},
										},
									},
								},
							},
						},
						{
							Key: "kvlist_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_KvlistValue{
									KvlistValue: &common.KeyValueList{
										Values: []*common.KeyValue{
											{
												Key: "nested_string",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "nested_value"},
												},
											},
											{
												Key: "nested_int",
												Value: &common.AnyValue{
													Value: &common.AnyValue_IntValue{IntValue: 123},
												},
											},
											{
												Key: "nested_bool",
												Value: &common.AnyValue{
													Value: &common.AnyValue_BoolValue{BoolValue: false},
												},
											},
											{
												Key: "nested_double",
												Value: &common.AnyValue{
													Value: &common.AnyValue_DoubleValue{DoubleValue: 456.789},
												},
											},
										},
									},
								},
							},
						},
						{
							// Present at multiple levels, to test field precedence.
							Key: "conflicting",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "resource"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					// First scope - recognized instrumentation library
					{
						Scope: &common.InstrumentationScope{
							Name:    "go.opentelemetry.io/contrib/instrumentation/net/http",
							Version: "1.0.0",
							Attributes: []*common.KeyValue{
								{
									Key: "scope.attr",
									Value: &common.AnyValue{
										Value: &common.AnyValue_StringValue{StringValue: "scope_value"},
									},
								},
								{
									Key: "conflicting",
									Value: &common.AnyValue{
										Value: &common.AnyValue_StringValue{StringValue: "scope"},
									},
								},
							},
						},
						Spans: []*trace.Span{
							// Span 1: Server span with all features
							{
								TraceId:           hexToBin(traceID1),
								SpanId:            hexToBin(spanID1),
								ParentSpanId:      hexToBin(parentSpanID1),
								TraceState:        "w3c=true;th=8",
								Name:              "HTTP GET /api/users",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
								Attributes: []*common.KeyValue{
									{
										Key: "http.method",
										Value: &common.AnyValue{
											Value: &common.AnyValue_StringValue{StringValue: "GET"},
										},
									},
									{
										Key: "http.status_code",
										Value: &common.AnyValue{
											Value: &common.AnyValue_IntValue{IntValue: 200},
										},
									},
									{
										Key: "http.url",
										Value: &common.AnyValue{
											Value: &common.AnyValue_StringValue{StringValue: "https://example.com/api/users"},
										},
									},
									{
										Key: "response.size",
										Value: &common.AnyValue{
											Value: &common.AnyValue_DoubleValue{DoubleValue: 1234.56},
										},
									},
									{
										Key: "success",
										Value: &common.AnyValue{
											Value: &common.AnyValue_BoolValue{BoolValue: true},
										},
									},
									{
										Key: "sampleRate",
										Value: &common.AnyValue{
											Value: &common.AnyValue_IntValue{IntValue: 10},
										},
									},
									// Empty and nil values are dropped from the output.
									{
										Key: "empty.attr",
									},
									{
										Key:   "nil.attr",
										Value: &common.AnyValue{},
									},
									{
										Key: "conflicting",
										Value: &common.AnyValue{
											Value: &common.AnyValue_StringValue{StringValue: "span"},
										},
									},
								},
								Status: &trace.Status{
									Code:    trace.Status_STATUS_CODE_OK,
									Message: "Request completed successfully",
								},
								Events: []*trace.Span_Event{
									{
										TimeUnixNano: event1Time,
										Name:         "cache_miss",
										Attributes: []*common.KeyValue{
											{
												Key: "cache.key",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "user:123"},
												},
											},
											{
												Key: "cache.ttl",
												Value: &common.AnyValue{
													Value: &common.AnyValue_IntValue{IntValue: 3600},
												},
											},
											{
												Key: "conflicting",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "event"},
												},
											},
										},
									},
									{
										TimeUnixNano: event2Time,
										Name:         "exception",
										Attributes: []*common.KeyValue{
											{
												Key: "exception.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "ValueError"},
												},
											},
											{
												Key: "exception.message",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "Invalid user ID"},
												},
											},
											{
												Key: "exception.stacktrace",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "stack trace here..."},
												},
											},
											{
												Key: "exception.escaped",
												Value: &common.AnyValue{
													Value: &common.AnyValue_BoolValue{BoolValue: true},
												},
											},
										},
									},
								},
								Links: []*trace.Span_Link{
									{
										TraceId:    hexToBin(linkedTraceID),
										SpanId:     hexToBin(linkedSpanID),
										TraceState: "vendor=value",
										Attributes: []*common.KeyValue{
											{
												Key: "link.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "parent"},
												},
											},
											{
												Key: "conflicting",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "link"},
												},
											},
										},
									},
								},
							},
							// Span 2: Client span
							{
								TraceId:           hexToBin(traceID1),
								SpanId:            hexToBin(spanID2),
								ParentSpanId:      hexToBin(spanID1),
								Name:              "DB Query",
								Kind:              trace.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: startTime + 50_000_000,
								EndTimeUnixNano:   endTime - 50_000_000,
								Attributes: []*common.KeyValue{
									{
										Key: "db.statement",
										Value: &common.AnyValue{
											Value: &common.AnyValue_StringValue{StringValue: "SELECT * FROM users WHERE id = ?"},
										},
									},
								},
								Status: &trace.Status{
									Code: trace.Status_STATUS_CODE_UNSET,
								},
							},
						},
					},
					// Second scope - unrecognized library
					{
						Scope: &common.InstrumentationScope{
							Name:    "custom-library",
							Version: "2.0.0",
						},
						Spans: []*trace.Span{
							// Span 3: Error span with negative duration
							{
								TraceId:           hexToBin(traceID1),
								SpanId:            hexToBin(errorSpanID),
								Name:              "Error Operation",
								Kind:              trace.Span_SPAN_KIND_INTERNAL,
								StartTimeUnixNano: errorStartTime,
								EndTimeUnixNano:   errorEndTime,
								Status: &trace.Status{
									Code:    trace.Status_STATUS_CODE_ERROR,
									Message: "Operation failed",
								},
								Events: []*trace.Span_Event{
									// Multiple exception events - only first should be copied to span
									{
										TimeUnixNano: errorStartTime - 100_000_000, // Before span start
										Name:         "exception",
										Attributes: []*common.KeyValue{
											{
												Key: "exception.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "RuntimeError"},
												},
											},
											{
												Key: "exception.message",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "First exception"},
												},
											},
										},
									},
									{
										TimeUnixNano: errorStartTime + 100_000_000,
										Name:         "exception",
										Attributes: []*common.KeyValue{
											{
												Key: "exception.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "IOError"},
												},
											},
											{
												Key: "exception.message",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "Second exception"},
												},
											},
										},
									},
								},
								Links: []*trace.Span_Link{
									{
										TraceId:    hexToBin(linkedTraceID),
										SpanId:     hexToBin(linkedSpanID),
										TraceState: "ignored=state",
										Attributes: []*common.KeyValue{
											{
												Key: "link.from",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "error_span"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			// Second ResourceSpan - service2
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "service2"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							// Span 4: Producer span
							{
								TraceId:           hexToBin(traceID2),
								SpanId:            hexToBin(spanID2),
								Name:              "Publish Message",
								Kind:              trace.Span_SPAN_KIND_PRODUCER,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
							},
						},
					},
				},
			},
			// Third ResourceSpan - service1 again, should create a new batch
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "service1"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							// Span 5: empty span
							{},
						},
					},
				},
			},
		},
	}

	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	// Unmarshal twice, which is fine because this should be idempotent.
	// Also assert we are not changing anything about the input buffer.
	// In practice it would probably be ok if we did, but it implies a bug if
	// it happens. (Reader, it happened.)
	before := slices.Clone(data)
	_, err := unmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
	require.NoError(t, err)
	require.Equal(t, before, data)

	result, err := unmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, len(data), result.RequestSize)
	assert.Len(t, result.Batches, 3)

	// Batch 1: service1
	// addService1CommonAttributes adds the common resource attributes for batch 1,
	// to reduce the verbosity here.
	addService1CommonAttributes := func(attrs map[string]any) map[string]any {
		// Universal
		attrs["meta.signal_type"] = "trace"

		// Resource attributes from service1
		attrs["service.name"] = "service1"
		attrs["deployment.environment"] = "production"
		attrs["bytes_attr"] = "\"AQIDBA==\"\n"
		attrs["array_attr"] = "[\"item1\",42,true,3.14]\n"
		attrs["kvlist_attr.nested_string"] = "nested_value"
		attrs["kvlist_attr.nested_int"] = int64(123)
		attrs["kvlist_attr.nested_bool"] = false
		attrs["kvlist_attr.nested_double"] = float64(456.789)

		return attrs
	}

	batch1 := result.Batches[0]
	assert.Equal(t, "service1", batch1.Dataset)
	// 3 spans + 2 events from span1 + 2 events from error span + 1 link from span1 + 1 link from error span = 9 events
	assert.Len(t, batch1.Events, 9)

	// Events are in deterministic order:
	// 0: cache_miss event (from span1)
	// 1: exception event (from span1)
	// 2: link (from span1)
	// 3: HTTP GET /api/users span (span1 itself)
	// 4: DB Query span (span2)
	// 5: First exception event (from error span)
	// 6: Second exception event (from error span)
	// 7: link (from error span)
	// 8: Error Operation span (error span itself)

	// Verify the main span (HTTP GET /api/users) at index 3
	mainSpan := &batch1.Events[3]
	mainAttrs := decodeMessagePackAttributes(t, mainSpan.Attributes)

	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"trace.trace_id":       traceID1,
		"trace.span_id":        spanID1,
		"trace.parent_id":      parentSpanID1,
		"trace.trace_state":    "w3c=true;th=8",
		"name":                 "HTTP GET /api/users",
		"span.kind":            "server",
		"type":                 "server",
		"http.method":          "GET",
		"http.status_code":     int64(200),
		"http.url":             "https://example.com/api/users",
		"response.size":        float64(1234.56),
		"success":              true,
		"status_code":          int64(1),
		"status_message":       "Request completed successfully",
		"duration_ms":          float64(864.197532),
		"span.num_events":      int64(2),
		"span.num_links":       int64(1),
		"exception.type":       "ValueError",
		"exception.message":    "Invalid user ID",
		"exception.stacktrace": "stack trace here...",
		"exception.escaped":    true,
		"conflicting":          "span",

		// Scope attributes from HTTP instrumentation library
		"library.name":                      "go.opentelemetry.io/contrib/instrumentation/net/http",
		"library.version":                   "1.0.0",
		"telemetry.instrumentation_library": true,
		"scope.attr":                        "scope_value",
	}), mainAttrs)

	// Sample rate and timestamp
	assert.Equal(t, int32(10), mainSpan.SampleRate)
	assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), mainSpan.Timestamp)

	// Verify span events
	cacheEvent := &batch1.Events[0]
	cacheAttrs := decodeMessagePackAttributes(t, cacheEvent.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"name":                          "cache_miss",
		"trace.trace_id":                traceID1,
		"trace.parent_id":               spanID1,
		"parent_name":                   "HTTP GET /api/users",
		"cache.key":                     "user:123",
		"cache.ttl":                     int64(3600),
		"meta.time_since_span_start_ms": float64(100),
		"meta.annotation_type":          "span_event",
		"conflicting":                   "event",

		// Scope attributes from HTTP instrumentation library
		"library.name":                      "go.opentelemetry.io/contrib/instrumentation/net/http",
		"library.version":                   "1.0.0",
		"telemetry.instrumentation_library": true,
		"scope.attr":                        "scope_value",
	}), cacheAttrs)
	assert.Equal(t, int32(10), cacheEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(event1Time)).UTC(), cacheEvent.Timestamp)

	// Exception event at index 1
	exceptionEvent := &batch1.Events[1]
	exceptionAttrs := decodeMessagePackAttributes(t, exceptionEvent.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"name":                          "exception",
		"trace.trace_id":                traceID1,
		"trace.parent_id":               spanID1,
		"parent_name":                   "HTTP GET /api/users",
		"meta.annotation_type":          "span_event",
		"meta.time_since_span_start_ms": float64(200),
		"exception.type":                "ValueError",
		"exception.message":             "Invalid user ID",
		"exception.stacktrace":          "stack trace here...",
		"exception.escaped":             true,
		"conflicting":                   "scope",

		// Scope attributes from HTTP instrumentation library
		"library.name":                      "go.opentelemetry.io/contrib/instrumentation/net/http",
		"library.version":                   "1.0.0",
		"telemetry.instrumentation_library": true,
		"scope.attr":                        "scope_value",
	}), exceptionAttrs)
	assert.Equal(t, int32(10), exceptionEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(event2Time)).UTC(), exceptionEvent.Timestamp)

	// Link at index 2
	linkEvent := &batch1.Events[2]
	linkAttrs := decodeMessagePackAttributes(t, linkEvent.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"trace.trace_id":       traceID1,
		"trace.parent_id":      spanID1,
		"trace.link.trace_id":  linkedTraceID,
		"trace.link.span_id":   linkedSpanID,
		"link.type":            "parent",
		"meta.annotation_type": "link",
		"parent_name":          "HTTP GET /api/users",
		"conflicting":          "link",

		// Scope attributes from HTTP instrumentation library
		"library.name":                      "go.opentelemetry.io/contrib/instrumentation/net/http",
		"library.version":                   "1.0.0",
		"telemetry.instrumentation_library": true,
		"scope.attr":                        "scope_value",
	}), linkAttrs)
	assert.Equal(t, int32(10), linkEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), linkEvent.Timestamp) // Links use parent span timestamp

	// DB Query span at index 4
	dbSpan := &batch1.Events[4]
	dbAttrs := decodeMessagePackAttributes(t, dbSpan.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"name":            "DB Query",
		"trace.trace_id":  traceID1,
		"trace.span_id":   spanID2,
		"trace.parent_id": spanID1,
		"span.kind":       "client",
		"type":            "client",
		"db.statement":    "SELECT * FROM users WHERE id = ?",
		"status_code":     int64(0),
		"duration_ms":     float64(764.197532),
		"span.num_events": int64(0),
		"span.num_links":  int64(0),
		"conflicting":     "scope",

		// Scope attributes from HTTP instrumentation library
		"library.name":                      "go.opentelemetry.io/contrib/instrumentation/net/http",
		"library.version":                   "1.0.0",
		"telemetry.instrumentation_library": true,
		"scope.attr":                        "scope_value",
	}), dbAttrs)
	assert.Equal(t, int32(1), dbSpan.SampleRate) // DB Query span has no explicit sampleRate, gets default
	assert.Equal(t, time.Unix(0, int64(startTime+50_000_000)).UTC(), dbSpan.Timestamp)

	// Error span's link at index 7
	errorLinkEvent := &batch1.Events[7]
	errorLinkAttrs := decodeMessagePackAttributes(t, errorLinkEvent.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"trace.trace_id":       traceID1,
		"trace.parent_id":      errorSpanID,
		"trace.link.trace_id":  linkedTraceID,
		"trace.link.span_id":   linkedSpanID,
		"link.from":            "error_span",
		"meta.annotation_type": "link",
		"parent_name":          "Error Operation",
		"error":                true,
		"conflicting":          "resource",

		// Scope attributes from custom-library
		"library.name":    "custom-library",
		"library.version": "2.0.0",
	}), errorLinkAttrs)
	assert.Equal(t, int32(1), errorLinkEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(errorStartTime)).UTC(), errorLinkEvent.Timestamp) // Links use parent span timestamp

	// Error span at index 8
	errorSpan := &batch1.Events[8]
	errorAttrs := decodeMessagePackAttributes(t, errorSpan.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"name":                  "Error Operation",
		"trace.trace_id":        traceID1,
		"trace.span_id":         errorSpanID,
		"span.kind":             "internal",
		"type":                  "internal",
		"error":                 true,
		"status_code":           int64(2),
		"status_message":        "Operation failed",
		"duration_ms":           float64(0),
		"meta.invalid_duration": true,
		"span.num_events":       int64(2),
		"span.num_links":        int64(1),
		"exception.type":        "RuntimeError",
		"exception.message":     "First exception",
		"conflicting":           "resource",

		// Scope attributes from custom-library
		"library.name":    "custom-library",
		"library.version": "2.0.0",
	}), errorAttrs)
	assert.Equal(t, int32(1), errorSpan.SampleRate) // Default sample rate
	assert.Equal(t, time.Unix(0, int64(errorStartTime)).UTC(), errorSpan.Timestamp)

	// Error span's exception events at indices 5 and 6
	firstExceptionEvent := &batch1.Events[5]
	firstExceptionAttrs := decodeMessagePackAttributes(t, firstExceptionEvent.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"name":                               "exception",
		"trace.trace_id":                     traceID1,
		"trace.parent_id":                    errorSpanID,
		"parent_name":                        "Error Operation",
		"exception.type":                     "RuntimeError",
		"exception.message":                  "First exception",
		"error":                              true,
		"meta.annotation_type":               "span_event",
		"meta.time_since_span_start_ms":      float64(0),
		"meta.invalid_time_since_span_start": true,
		"conflicting":                        "resource",

		// Scope attributes from custom-library
		"library.name":    "custom-library",
		"library.version": "2.0.0",
	}), firstExceptionAttrs)
	assert.Equal(t, int32(1), firstExceptionEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(errorStartTime-100_000_000)).UTC(), firstExceptionEvent.Timestamp)

	// Second exception event at index 6 (only first exception was copied to error span)
	secondExceptionEvent := &batch1.Events[6]
	secondExceptionAttrs := decodeMessagePackAttributes(t, secondExceptionEvent.Attributes)
	assert.Equal(t, addService1CommonAttributes(map[string]any{
		"name":                          "exception",
		"trace.trace_id":                traceID1,
		"trace.parent_id":               errorSpanID,
		"parent_name":                   "Error Operation",
		"exception.type":                "IOError",
		"exception.message":             "Second exception",
		"error":                         true,
		"meta.annotation_type":          "span_event",
		"meta.signal_type":              "trace",
		"meta.time_since_span_start_ms": float64(100),
		"conflicting":                   "resource",

		// Scope attributes from custom-library
		"library.name":    "custom-library",
		"library.version": "2.0.0",
	}), secondExceptionAttrs)
	assert.Equal(t, int32(1), secondExceptionEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(errorStartTime+100_000_000)).UTC(), secondExceptionEvent.Timestamp)

	// Batch 2: service2
	batch2 := result.Batches[1]
	assert.Equal(t, "service2", batch2.Dataset)
	assert.Len(t, batch2.Events, 1) // Just the producer span

	producerEvent := batch2.Events[0]
	producerAttrs := decodeMessagePackAttributes(t, producerEvent.Attributes)
	assert.Equal(t, map[string]any{
		"trace.trace_id":   "1a2b3c4d5e6f7089",
		"trace.span_id":    spanID2,
		"name":             "Publish Message",
		"span.kind":        "producer",
		"type":             "producer",
		"status_code":      int64(0),
		"duration_ms":      float64(864.197532),
		"span.num_events":  int64(0),
		"span.num_links":   int64(0),
		"meta.signal_type": "trace",
		"service.name":     "service2",
	}, producerAttrs)
	assert.Equal(t, int32(1), producerEvent.SampleRate) // Default sample rate
	assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), producerEvent.Timestamp)

	// Batch 3: service1 is batched seperately due to arriving in a different
	// Resource message.
	batch3 := result.Batches[2]
	assert.Equal(t, "service1", batch3.Dataset)
	assert.Len(t, batch3.Events, 1)

	// Since this is an effectively empty message, confirm all of our default field values here.
	batch3Event := batch3.Events[0]
	batch3Attrs := decodeMessagePackAttributes(t, batch3Event.Attributes)
	assert.Equal(t, map[string]any{
		"trace.trace_id":   "",
		"trace.span_id":    "",
		"span.kind":        "unspecified",
		"type":             "unspecified",
		"name":             "",
		"status_code":      int64(0),
		"duration_ms":      float64(0),
		"span.num_events":  int64(0),
		"span.num_links":   int64(0),
		"meta.signal_type": "trace",
		"service.name":     "service1",
	}, batch3Attrs)
	assert.Equal(t, int32(1), batch3Event.SampleRate) // Default sample rate
	assert.Equal(t, time.Unix(0, 0).UTC(), batch3Event.Timestamp)

	t.Run("ErrorHandling", func(t *testing.T) {
		// Shave 5 bytes off the end of our serialized message, which should net
		// us an EOF error from deep within the stack.
		data = data[:len(data)-5]
		_, err := unmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	// Now compare with the regular (non-direct) unmarshaling to ensure consistency
	t.Run("CompareWithRegularUnmarshaling", func(t *testing.T) {
		// Use the same serialized data with the regular unmarshaling path
		regularResult, err := TranslateTraceRequest(context.Background(), req, ri)
		require.NoError(t, err)
		assert.NotNil(t, regularResult)

		// TODO: Fix these bugs in the regular unmarshaling (traces.go):
		// 1. Negative duration calculation causes unsigned integer overflow
		//    - When endTime < startTime, should set duration_ms=0 and meta.invalid_duration=true
		//    - Currently produces huge positive values like 1.8446744072845355e+13
		// 2. Negative time_since_span_start calculation causes unsigned integer overflow
		//    - When event time < span start time, should set time=0 and meta.invalid_time_since_span_start=true
		//    - Currently produces huge positive values like 1.844674407360955e+13

		// Compare high-level structure
		assert.Equal(t, result.RequestSize, regularResult.RequestSize)
		assert.Equal(t, len(result.Batches), len(regularResult.Batches))

		// Convert msgpack batches to regular batches for comparison
		directBatches := make([]Batch, len(result.Batches))
		for i, msgpBatch := range result.Batches {
			directBatches[i] = convertBatchMsgpToBatch(t, msgpBatch)
		}

		// Compare each batch
		for i := range directBatches {
			directBatch := directBatches[i]
			regularBatch := regularResult.Batches[i]

			assert.Equal(t, directBatch.Dataset, regularBatch.Dataset, "Batch %d dataset mismatch", i)
			assert.Equal(t, len(directBatch.Events), len(regularBatch.Events), "Batch %d event count mismatch", i)

			// Compare each event
			for j := range directBatch.Events {
				directEvent := directBatch.Events[j]
				regularEvent := regularBatch.Events[j]

				// Compare timestamps and sample rates
				assert.Equal(t, directEvent.Timestamp, regularEvent.Timestamp, "Batch %d Event %d timestamp mismatch", i, j)
				assert.Equal(t, directEvent.SampleRate, regularEvent.SampleRate, "Batch %d Event %d sample rate mismatch", i, j)

				// Compare attributes
				// Check for known discrepancies that are actually improvements in the direct implementation
				if i == 0 && j == 5 { // First exception event
					// The direct implementation correctly handles negative time_since_span_start
					// Regular: meta.time_since_span_start_ms = 1.844674407360955e+13 (overflow)
					// Direct: meta.time_since_span_start_ms = 0 + meta.invalid_time_since_span_start = true
					assert.Equal(t, float64(0), directEvent.Attributes["meta.time_since_span_start_ms"])
					assert.Equal(t, true, directEvent.Attributes["meta.invalid_time_since_span_start"])
					// Remove the fields that differ to check the rest
					delete(regularEvent.Attributes, "meta.time_since_span_start_ms")
					delete(directEvent.Attributes, "meta.time_since_span_start_ms")
					delete(directEvent.Attributes, "meta.invalid_time_since_span_start")
				}
				if i == 0 && j == 8 { // Error span with negative duration
					// The direct implementation correctly handles negative duration
					// Regular: duration_ms = 1.8446744072845355e+13 (overflow)
					// Direct: duration_ms = 0 + meta.invalid_duration = true
					assert.Equal(t, float64(0), directEvent.Attributes["duration_ms"])
					assert.Equal(t, true, directEvent.Attributes["meta.invalid_duration"])
					// Remove the fields that differ to check the rest
					delete(regularEvent.Attributes, "duration_ms")
					delete(directEvent.Attributes, "duration_ms")
					delete(directEvent.Attributes, "meta.invalid_duration")
				}

				// Now compare the remaining attributes
				assert.Equal(t, regularEvent.Attributes, directEvent.Attributes, "Batch %d Event %d attributes mismatch (after removing known improvements)", i, j)
			}
		}
	})
}

func TestUnmarshalTraceRequestDirect_WithUnknownFields(t *testing.T) {
	// Create a serialized message with unknown fields
	// We'll manually create a protobuf message that includes fields not in the current schema
	// Field 100 in ExportTraceServiceRequest (unknown field)
	// Field 50 in ResourceSpans (unknown field)
	// Field 25 in Span (unknown field)

	// Start with a basic valid request
	traceID := "0102030405060708090a0b0c0d0e0f10"
	spanID := "0102030405060708"
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "test-service"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							{
								TraceId:           hexToBin(traceID),
								SpanId:            hexToBin(spanID),
								Name:              "test-span",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
							},
						},
					},
				},
			},
		},
	}

	// Serialize to get a base message
	baseData := serializeTraceRequest(t, req)

	// Inject unknown fields into the protobuf message
	// We'll append some extra fields manually
	// Field 100 with string value "unknown_field" at the root level
	unknownField100 := []byte{
		0xa2, 0x06, // field 100, wire type 2 (length-delimited)
		0x0d, // length 13
		'u', 'n', 'k', 'n', 'o', 'w', 'n', '_', 'f', 'i', 'e', 'l', 'd',
	}

	// Combine the data - insert the unknown field before the existing data
	data := append(unknownField100, baseData...)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	// The direct unmarshaling should skip unknown fields gracefully
	result, err := unmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)

	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)

	event := batch.Events[0]
	attrs := decodeMessagePackAttributes(t, event.Attributes)
	assert.Equal(t, traceID, attrs["trace.trace_id"])
	assert.Equal(t, spanID, attrs["trace.span_id"])
	assert.Equal(t, "test-span", attrs["name"])
}

func TestUnmarshalTraceRequestDirect_NestedMapAttributes(t *testing.T) {
	// Test nested map attribute flattening at various depths
	// Expected behavior:
	// - Maps are flattened with dot notation up to maxDepth (5)
	// - When a kvlist is encountered at depth >= maxDepth, it's JSON encoded
	// - This means we can have up to 6 segments in a flattened path (0-5 depth levels)
	// - Example: map6.level6.level5.level4.level3.level2 is 6 segments (depths 0-5)
	//   The value at level2 (a kvlist containing level1) is JSON encoded

	traceID := "0102030405060708090a0b0c0d0e0f10"
	spanID := "0102030405060708"
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)

	// Helper to create nested kvlist attributes
	var createNestedMap func(depth int, leafValue string) *common.AnyValue
	createNestedMap = func(depth int, leafValue string) *common.AnyValue {
		if depth == 0 {
			return &common.AnyValue{
				Value: &common.AnyValue_StringValue{StringValue: leafValue},
			}
		}

		return &common.AnyValue{
			Value: &common.AnyValue_KvlistValue{
				KvlistValue: &common.KeyValueList{
					Values: []*common.KeyValue{
						{
							Key:   fmt.Sprintf("level%d", depth),
							Value: createNestedMap(depth-1, leafValue),
						},
					},
				},
			},
		}
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "nested-test"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							{
								TraceId:           hexToBin(traceID),
								SpanId:            hexToBin(spanID),
								Name:              "nested-test-span",
								Kind:              trace.Span_SPAN_KIND_INTERNAL,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
								Attributes: []*common.KeyValue{
									// Depth 1: should be flattened
									{
										Key:   "map1",
										Value: createNestedMap(1, "value1"),
									},
									// Depth 2: should be flattened
									{
										Key:   "map2",
										Value: createNestedMap(2, "value2"),
									},
									// Depth 3: should be flattened
									{
										Key:   "map3",
										Value: createNestedMap(3, "value3"),
									},
									// Depth 4: should be flattened
									{
										Key:   "map4",
										Value: createNestedMap(4, "value4"),
									},
									// Depth 5: should be flattened
									{
										Key:   "map5",
										Value: createNestedMap(5, "value5"),
									},
									// Depth 6: should be JSON encoded
									{
										Key:   "map6",
										Value: createNestedMap(6, "value6"),
									},
									// Depth 7: should be JSON encoded
									{
										Key:   "map7",
										Value: createNestedMap(7, "value7"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	// First, get results from direct unmarshaling
	data := serializeTraceRequest(t, req)
	directResult, err := unmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
	require.NoError(t, err)
	require.Len(t, directResult.Batches, 1)
	require.Len(t, directResult.Batches[0].Events, 1)

	directEvent := directResult.Batches[0].Events[0]
	directAttrs := decodeMessagePackAttributes(t, directEvent.Attributes)

	// Then get results from regular unmarshaling
	regularResult, err := TranslateTraceRequest(context.Background(), req, ri)
	require.NoError(t, err)
	require.Len(t, regularResult.Batches, 1)
	require.Len(t, regularResult.Batches[0].Events, 1)

	regularEvent := regularResult.Batches[0].Events[0]
	regularAttrs := regularEvent.Attributes

	// Verify depth 1-5 flattening behavior
	t.Run("Depth1-5_Flattening", func(t *testing.T) {
		assert.Equal(t, "value1", directAttrs["map1.level1"])
		assert.Equal(t, "value1", regularAttrs["map1.level1"])

		assert.Equal(t, "value2", directAttrs["map2.level2.level1"])
		assert.Equal(t, "value2", regularAttrs["map2.level2.level1"])

		assert.Equal(t, "value3", directAttrs["map3.level3.level2.level1"])
		assert.Equal(t, "value3", regularAttrs["map3.level3.level2.level1"])

		assert.Equal(t, "value4", directAttrs["map4.level4.level3.level2.level1"])
		assert.Equal(t, "value4", regularAttrs["map4.level4.level3.level2.level1"])

		assert.Equal(t, "value5", directAttrs["map5.level5.level4.level3.level2.level1"])
		assert.Equal(t, "value5", regularAttrs["map5.level5.level4.level3.level2.level1"])
	})

	// Verify depth 6-7 JSON encoding behavior
	t.Run("Depth6-7_JSONEncoding", func(t *testing.T) {
		expectedMap6 := `{"level1":"value6"}` + "\n"
		assert.Equal(t, expectedMap6, regularAttrs["map6.level6.level5.level4.level3.level2"])
		assert.Equal(t, expectedMap6, directAttrs["map6.level6.level5.level4.level3.level2"])

		expectedMap7 := `{"level2":{"level1":"value7"}}` + "\n"
		assert.Equal(t, expectedMap7, regularAttrs["map7.level7.level6.level5.level4.level3"])
		assert.Equal(t, expectedMap7, directAttrs["map7.level7.level6.level5.level4.level3"])
	})
}

// BenchmarkUnmarshalTraceRequestDirectMsgp benchmarks the direct unmarshaling with 100 scalar attributes
func BenchmarkUnmarshalTraceRequestDirectMsgp(b *testing.B) {
	// Create a trace request with a single span containing 100 scalar attributes
	traceID := "0102030405060708090a0b0c0d0e0f10"
	spanID := "0102030405060708"
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)

	// Generate 100 attributes with mixed scalar types
	attributes := make([]*common.KeyValue, 100)
	for i := 0; i < 100; i++ {
		var value *common.AnyValue

		switch i % 4 {
		case 0: // String
			value = &common.AnyValue{
				Value: &common.AnyValue_StringValue{
					StringValue: fmt.Sprintf("string_value_%d_lorem_ipsum_dolor_sit_amet", i),
				},
			}
		case 1: // Integer
			value = &common.AnyValue{
				Value: &common.AnyValue_IntValue{
					IntValue: int64(i * 12345),
				},
			}
		case 2: // Float
			value = &common.AnyValue{
				Value: &common.AnyValue_DoubleValue{
					DoubleValue: float64(i) * 3.14159,
				},
			}
		case 3: // Boolean
			value = &common.AnyValue{
				Value: &common.AnyValue_BoolValue{
					BoolValue: i%2 == 0,
				},
			}
		}

		attributes[i] = &common.KeyValue{
			Key:   fmt.Sprintf("attribute_%03d", i),
			Value: value,
		}
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				Resource: &resource.Resource{
					Attributes: []*common.KeyValue{
						{
							Key: "service.name",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "benchmark-service"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							{
								TraceId:           hexToBin(traceID),
								SpanId:            hexToBin(spanID),
								Name:              "benchmark-span",
								Kind:              trace.Span_SPAN_KIND_INTERNAL,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
								Attributes:        attributes,
							},
						},
					},
				},
			},
		},
	}

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "benchmark-dataset",
		ContentType: "application/protobuf",
	}

	// Serialize the request once before benchmarking
	data, err := proto.Marshal(req)
	if err != nil {
		b.Fatal("Failed to serialize trace request:", err)
	}
	ctx := context.Background()

	// Reset timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		result, err := unmarshalTraceRequestDirectMsgp(ctx, data, ri)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Batches) != 1 || len(result.Batches[0].Events) != 1 {
			b.Fatal("unexpected result structure")
		}
	}
}

func TestSampleRateFromFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected int32
	}{
		// Normal positive values
		{
			name:     "positive float rounds down",
			input:    10.4,
			expected: 10,
		},
		{
			name:     "positive float rounds up",
			input:    10.5,
			expected: 11,
		},

		// Edge cases around zero and negative
		{
			name:     "zero",
			input:    0.0,
			expected: 1, // defaultSampleRate
		},
		{
			name:     "negative zero",
			input:    -0.0,
			expected: 1, // defaultSampleRate
		},
		{
			name:     "negative value",
			input:    -10.0,
			expected: 1, // defaultSampleRate
		},

		// Large values near MaxInt32
		{
			name:     "MaxInt32 exactly",
			input:    math.MaxInt32,
			expected: math.MaxInt32,
		},
		{
			name:     "slightly above MaxInt32",
			input:    math.MaxInt32 + 1,
			expected: math.MaxInt32,
		},

		// Special float values
		{
			name:     "positive infinity",
			input:    math.Inf(1),
			expected: math.MaxInt32,
		},
		{
			name:     "negative infinity",
			input:    math.Inf(-1),
			expected: 1, // defaultSampleRate
		},
		{
			name:     "NaN",
			input:    math.NaN(),
			expected: defaultSampleRate, // NaN > MaxInt32 is false, NaN <= 0 is false, so int32(NaN + 0.5) = 0
		},
		{
			name:     "epsilon",
			input:    math.SmallestNonzeroFloat64,
			expected: defaultSampleRate, // Rounds to 0, but f > 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sampleRateFromFloat(tt.input)
			assert.Equal(t, tt.expected, result, "sampleRateFromFloat(%v) = %v, want %v", tt.input, result, tt.expected)
		})
	}
}
