package otlp

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// decodeMessagePackAttributes unmarshals MessagePack data into a map for testing
func decodeMessagePackAttributes(t testing.TB, data []byte) map[string]any {
	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	decoder.UseDecodeInterfaceLoose(true)

	var attrs map[string]any
	err := decoder.Decode(&attrs)
	require.NoError(t, err, "Failed to unmarshal MessagePack attributes")
	return attrs
}

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

func TestUnmarshalTraceRequestDirect_Complete(t *testing.T) {
	// Create a comprehensive test request with all supported features
	traceID1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	parentSpanID1 := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}

	traceID2 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x20}
	spanID2 := []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}

	linkedTraceID := []byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40}
	linkedSpanID := []byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48}

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
							},
						},
						Spans: []*trace.Span{
							// Span 1: Server span with all features
							{
								TraceId:           traceID1,
								SpanId:            spanID1,
								ParentSpanId:      parentSpanID1,
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
										TraceId:    linkedTraceID,
										SpanId:     linkedSpanID,
										TraceState: "vendor=value",
										Attributes: []*common.KeyValue{
											{
												Key: "link.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "parent"},
												},
											},
										},
									},
								},
							},
							// Span 2: Client span
							{
								TraceId:           traceID1,
								SpanId:            spanID2,
								ParentSpanId:      spanID1,
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
								TraceId:           traceID1,
								SpanId:            []byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38},
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
								TraceId:           traceID2,
								SpanId:            spanID2,
								Name:              "Publish Message",
								Kind:              trace.Span_SPAN_KIND_PRODUCER,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
							},
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

	result, err := UnmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, len(data), result.RequestSize)
	assert.Len(t, result.Batches, 2) // Two different services

	// Batch 1: service1
	batch1 := result.Batches[0]
	assert.Equal(t, "service1", batch1.Dataset)
	// 3 spans + 2 events from span1 + 2 events from error span + 1 link = 8 events
	assert.Len(t, batch1.Events, 8)

	// Events are in deterministic order:
	// 0: cache_miss event (from span1)
	// 1: exception event (from span1)
	// 2: link (from span1)
	// 3: HTTP GET /api/users span (span1 itself)
	// 4: DB Query span (span2)
	// 5: First exception event (from error span)
	// 6: Second exception event (from error span)
	// 7: Error Operation span (error span itself)

	// Verify the main span (HTTP GET /api/users) at index 3
	mainSpan := &batch1.Events[3]

	mainAttrs := decodeMessagePackAttributes(t, mainSpan.Attributes)

	// Core attributes
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", mainAttrs["trace.trace_id"])
	assert.Equal(t, "0102030405060708", mainAttrs["trace.span_id"])
	assert.Equal(t, "1112131415161718", mainAttrs["trace.parent_id"])
	assert.Equal(t, "w3c=true;th=8", mainAttrs["trace.trace_state"])
	assert.Equal(t, "HTTP GET /api/users", mainAttrs["name"])
	assert.Equal(t, "server", mainAttrs["span.kind"])
	assert.Equal(t, "server", mainAttrs["type"])

	// Resource attributes
	assert.Equal(t, "service1", mainAttrs["service.name"])
	assert.Equal(t, "production", mainAttrs["deployment.environment"])
	assert.Equal(t, "\"AQIDBA==\"\n", mainAttrs["bytes_attr"])
	assert.Equal(t, "[\"item1\",42,true,3.14]\n", mainAttrs["array_attr"])

	// Flattened kvlist attributes
	assert.Equal(t, "nested_value", mainAttrs["kvlist_attr.nested_string"])
	assert.Equal(t, int64(123), mainAttrs["kvlist_attr.nested_int"])
	assert.Equal(t, false, mainAttrs["kvlist_attr.nested_bool"])
	assert.Equal(t, float64(456.789), mainAttrs["kvlist_attr.nested_double"])

	// Scope attributes
	assert.Equal(t, "go.opentelemetry.io/contrib/instrumentation/net/http", mainAttrs["library.name"])
	assert.Equal(t, "1.0.0", mainAttrs["library.version"])
	assert.Equal(t, true, mainAttrs["telemetry.instrumentation_library"])
	assert.Equal(t, "scope_value", mainAttrs["scope.attr"])

	// Span attributes
	assert.Equal(t, "GET", mainAttrs["http.method"])
	assert.Equal(t, int64(200), mainAttrs["http.status_code"])
	assert.Equal(t, "https://example.com/api/users", mainAttrs["http.url"])
	assert.Equal(t, float64(1234.56), mainAttrs["response.size"])
	assert.Equal(t, true, mainAttrs["success"])

	// Status
	assert.Equal(t, int64(1), mainAttrs["status_code"]) // STATUS_CODE_OK = 1
	// Note: traces_direct.go doesn't add status.code or status.message fields
	assert.Equal(t, "Request completed successfully", mainAttrs["status_message"])

	// Duration
	assert.InDelta(t, float64(864.197532), mainAttrs["duration_ms"], 0.01)

	// Span event/link counts
	assert.Equal(t, int64(2), mainAttrs["span.num_events"])
	assert.Equal(t, int64(1), mainAttrs["span.num_links"])

	// Meta attributes
	assert.Equal(t, "trace", mainAttrs["meta.signal_type"])

	// Exception attributes (should be copied from first exception event)
	assert.Equal(t, "ValueError", mainAttrs["exception.type"])
	assert.Equal(t, "Invalid user ID", mainAttrs["exception.message"])
	assert.Equal(t, "stack trace here...", mainAttrs["exception.stacktrace"])
	assert.Equal(t, true, mainAttrs["exception.escaped"])

	// Sample rate and timestamp
	assert.Equal(t, int32(10), mainSpan.SampleRate)
	assert.Nil(t, mainAttrs["sampleRate"]) // Should be removed
	assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), mainSpan.Timestamp)

	// Verify span events
	cacheEvent := &batch1.Events[0]
	cacheAttrs := decodeMessagePackAttributes(t, cacheEvent.Attributes)
	assert.Equal(t, "cache_miss", cacheAttrs["name"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", cacheAttrs["trace.trace_id"])
	assert.Equal(t, "0102030405060708", cacheAttrs["trace.parent_id"])
	assert.Equal(t, "HTTP GET /api/users", cacheAttrs["parent_name"])
	assert.Equal(t, "user:123", cacheAttrs["cache.key"])
	assert.Equal(t, int64(3600), cacheAttrs["cache.ttl"])
	assert.Equal(t, float64(100), cacheAttrs["meta.time_since_span_start_ms"])
	assert.Equal(t, "span_event", cacheAttrs["meta.annotation_type"])
	assert.Nil(t, cacheAttrs["error"]) // Parent span is not error
	assert.Equal(t, int32(10), cacheEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(event1Time)).UTC(), cacheEvent.Timestamp)

	// Exception event at index 1
	exceptionEvent := &batch1.Events[1]
	exceptionAttrs := decodeMessagePackAttributes(t, exceptionEvent.Attributes)
	assert.Equal(t, "exception", exceptionAttrs["name"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", exceptionAttrs["trace.trace_id"])
	assert.Equal(t, "0102030405060708", exceptionAttrs["trace.parent_id"])
	assert.Equal(t, "span_event", exceptionAttrs["meta.annotation_type"])
	assert.Equal(t, int32(10), exceptionEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(event2Time)).UTC(), exceptionEvent.Timestamp)

	// Link at index 2
	linkEvent := &batch1.Events[2]
	linkAttrs := decodeMessagePackAttributes(t, linkEvent.Attributes)
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", linkAttrs["trace.trace_id"])
	assert.Equal(t, "0102030405060708", linkAttrs["trace.parent_id"])
	assert.Equal(t, "3132333435363738393a3b3c3d3e3f40", linkAttrs["trace.link.trace_id"])
	assert.Equal(t, "4142434445464748", linkAttrs["trace.link.span_id"])
	assert.Equal(t, "parent", linkAttrs["link.type"])
	assert.Equal(t, "link", linkAttrs["meta.annotation_type"])
	assert.Equal(t, int32(10), linkEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), linkEvent.Timestamp) // Links use parent span timestamp

	// DB Query span at index 4
	dbSpan := &batch1.Events[4]
	dbAttrs := decodeMessagePackAttributes(t, dbSpan.Attributes)
	assert.Equal(t, "DB Query", dbAttrs["name"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", dbAttrs["trace.trace_id"])
	assert.Equal(t, "2122232425262728", dbAttrs["trace.span_id"])
	assert.Equal(t, "0102030405060708", dbAttrs["trace.parent_id"])
	assert.Equal(t, "client", dbAttrs["span.kind"])
	assert.Equal(t, "SELECT * FROM users WHERE id = ?", dbAttrs["db.statement"])
	assert.Equal(t, int32(1), dbSpan.SampleRate) // DB Query span has no explicit sampleRate, gets default
	assert.Equal(t, time.Unix(0, int64(startTime+50_000_000)).UTC(), dbSpan.Timestamp)

	// Error span at index 7
	errorSpan := &batch1.Events[7]
	errorAttrs := decodeMessagePackAttributes(t, errorSpan.Attributes)
	assert.Equal(t, "Error Operation", errorAttrs["name"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", errorAttrs["trace.trace_id"])
	assert.Equal(t, "3132333435363738", errorAttrs["trace.span_id"])
	assert.Equal(t, true, errorAttrs["error"])
	assert.Equal(t, int64(2), errorAttrs["status_code"]) // STATUS_CODE_ERROR = 2
	assert.Equal(t, "Operation failed", errorAttrs["status_message"])
	// Note: traces_direct.go doesn't add status.code field
	// Verify negative duration is handled correctly
	assert.Equal(t, float64(0), errorAttrs["duration_ms"]) // Negative duration should be clamped to 0
	assert.Equal(t, true, errorAttrs["meta.invalid_duration"])
	// Only first exception should be copied
	assert.Equal(t, "RuntimeError", errorAttrs["exception.type"])
	assert.Equal(t, "First exception", errorAttrs["exception.message"])
	assert.Equal(t, int32(1), errorSpan.SampleRate) // Default sample rate
	assert.Equal(t, time.Unix(0, int64(errorStartTime)).UTC(), errorSpan.Timestamp)

	// Error span's exception events at indices 5 and 6
	firstExceptionEvent := &batch1.Events[5]
	firstExceptionAttrs := decodeMessagePackAttributes(t, firstExceptionEvent.Attributes)
	assert.Equal(t, "exception", firstExceptionAttrs["name"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", firstExceptionAttrs["trace.trace_id"])
	assert.Equal(t, "3132333435363738", firstExceptionAttrs["trace.parent_id"])
	assert.Equal(t, "Error Operation", firstExceptionAttrs["parent_name"])
	assert.Equal(t, "RuntimeError", firstExceptionAttrs["exception.type"])
	assert.Equal(t, "First exception", firstExceptionAttrs["exception.message"])
	assert.Equal(t, true, firstExceptionAttrs["error"]) // Parent span is error
	assert.Equal(t, "span_event", firstExceptionAttrs["meta.annotation_type"])
	// Verify time since span start calculation handles negative values correctly
	assert.Equal(t, float64(0), firstExceptionAttrs["meta.time_since_span_start_ms"])
	assert.Equal(t, true, firstExceptionAttrs["meta.invalid_time_since_span_start"])
	assert.Equal(t, int32(1), firstExceptionEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(errorStartTime-100_000_000)).UTC(), firstExceptionEvent.Timestamp)

	// Second exception event at index 6 (only first exception was copied to error span)
	secondExceptionEvent := &batch1.Events[6]
	secondExceptionAttrs := decodeMessagePackAttributes(t, secondExceptionEvent.Attributes)
	assert.Equal(t, "exception", secondExceptionAttrs["name"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", secondExceptionAttrs["trace.trace_id"])
	assert.Equal(t, "3132333435363738", secondExceptionAttrs["trace.parent_id"])
	assert.Equal(t, "Error Operation", secondExceptionAttrs["parent_name"])
	assert.Equal(t, "IOError", secondExceptionAttrs["exception.type"])
	assert.Equal(t, "Second exception", secondExceptionAttrs["exception.message"])
	assert.Equal(t, true, secondExceptionAttrs["error"]) // Parent span is error
	assert.Equal(t, "span_event", secondExceptionAttrs["meta.annotation_type"])
	assert.Equal(t, float64(100), secondExceptionAttrs["meta.time_since_span_start_ms"])
	assert.Equal(t, int32(1), secondExceptionEvent.SampleRate)
	assert.Equal(t, time.Unix(0, int64(errorStartTime+100_000_000)).UTC(), secondExceptionEvent.Timestamp)

	// Batch 2: service2
	batch2 := result.Batches[1]
	assert.Equal(t, "service2", batch2.Dataset)
	assert.Len(t, batch2.Events, 1) // Just the producer span

	producerEvent := batch2.Events[0]
	producerAttrs := decodeMessagePackAttributes(t, producerEvent.Attributes)
	assert.Equal(t, "292a2b2c2d2e2f20", producerAttrs["trace.trace_id"]) // Leading zeros trimmed
	assert.Equal(t, "2122232425262728", producerAttrs["trace.span_id"])
	assert.Equal(t, "Publish Message", producerAttrs["name"])
	assert.Equal(t, "producer", producerAttrs["span.kind"])
	assert.Equal(t, "producer", producerAttrs["type"])
	assert.Nil(t, producerAttrs["telemetry.instrumentation_library"]) // No recognized library
	assert.Equal(t, int32(1), producerEvent.SampleRate)               // Default sample rate
	assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), producerEvent.Timestamp)

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
				if i == 0 && j == 7 { // Error span with negative duration
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
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
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
								TraceId:           traceID,
								SpanId:            spanID,
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
	result, err := UnmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)

	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)

	event := batch.Events[0]
	attrs := decodeMessagePackAttributes(t, event.Attributes)
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", attrs["trace.trace_id"])
	assert.Equal(t, "0102030405060708", attrs["trace.span_id"])
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

	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
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
								TraceId:           traceID,
								SpanId:            spanID,
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
	directResult, err := UnmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
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
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
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
								TraceId:           traceID,
								SpanId:            spanID,
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
		result, err := UnmarshalTraceRequestDirectMsgp(ctx, data, ri)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Batches) != 1 || len(result.Batches[0].Events) != 1 {
			b.Fatal("unexpected result structure")
		}
	}
}
