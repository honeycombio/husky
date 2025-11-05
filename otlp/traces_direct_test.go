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

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
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

	// Threshold used in some test spans' tracestate. Only expected if sampleRate attr not set.
	const expectedSampleRateFromThreshold = 7
	samplingThreshold, err := sampling.ProbabilityToThreshold(1.0 / float64(expectedSampleRateFromThreshold))
	require.NoError(t, err)

	// Service1's sampleRate resource attribute should win on every service1 event.
	const expectedService1SampleRate = 10
	service1Resource := &resource.Resource{
		Attributes: []*common.KeyValue{
			{
				Key: "service.name",
				Value: &common.AnyValue{
					Value: &common.AnyValue_StringValue{StringValue: "service1"},
				},
			},
			{
				Key: "sampleRate",
				Value: &common.AnyValue{
					Value: &common.AnyValue_IntValue{IntValue: int64(expectedService1SampleRate)},
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
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			// First ResourceSpan - service1 with comprehensive attributes
			{
				Resource: service1Resource,
				ScopeSpans: []*trace.ScopeSpans{
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
										Key: `unfriendly\to "json"`,
										Value: &common.AnyValue{
											Value: &common.AnyValue_StringValue{StringValue: `I need\ to "escape"`},
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
								Status: &trace.Status{
									Code:    trace.Status_STATUS_CODE_OK,
									Message: "Request completed successfully",
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
								TraceState:        "w3c=false;th=" + samplingThreshold.TValue(),
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
								StartTimeUnixNano: errorStartTime, // start is after end
								EndTimeUnixNano:   errorEndTime,
								Status: &trace.Status{
									Code:    trace.Status_STATUS_CODE_ERROR,
									Message: "Operation failed",
								},
								Events: []*trace.Span_Event{
									{
										TimeUnixNano: errorStartTime - 100_000_000, // Event before span start
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
								TraceState:        "w3c=false;th=" + samplingThreshold.TValue(),
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
				Resource: service1Resource,
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

	// Test both protobuf and JSON serialization formats
	testCases := []struct {
		name        string
		contentType string
		serialize   func(*collectortrace.ExportTraceServiceRequest) ([]byte, error)
		unmarshal   func(context.Context, []byte, RequestInfo) (*TranslateOTLPRequestResultMsgp, error)
	}{
		{
			name:        "protobuf",
			contentType: "application/protobuf",
			serialize: func(req *collectortrace.ExportTraceServiceRequest) ([]byte, error) {
				return proto.Marshal(req)
			},
			unmarshal: UnmarshalTraceRequestDirectMsgp,
		},
		{
			name:        "json",
			contentType: "application/json",
			serialize: func(req *collectortrace.ExportTraceServiceRequest) ([]byte, error) {
				return protojson.Marshal(req)
			},
			unmarshal: unmarshalTraceRequestDirectMsgpJSON,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.serialize(req)
			require.NoError(t, err)

			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "test-dataset",
				ContentType: tc.contentType,
			}

			// Unmarshal twice, which is fine because this should be idempotent.
			// Also assert we are not changing anything about the input buffer.
			// In practice it would probably be ok if we did, but it implies a bug if
			// it happens. (Reader, it happened.)
			before := slices.Clone(data)
			_, err = tc.unmarshal(context.Background(), data, ri)
			require.NoError(t, err)
			require.Equal(t, before, data)

			result, err := tc.unmarshal(context.Background(), data, ri)
			require.NoError(t, err)
			require.Equal(t, before, data)

			batch1 := result.Batches[0]
			assert.Equal(t, "service1", batch1.Dataset)
			// 3 spans + 2 events from span1 + 2 events from error span + 1 link from span1 + 1 link from error span = 9 events
			assert.Len(t, batch1.Events, 9)

			// Events are in deterministic order:
			// 0: cache_miss event (from span1)
			// 1: exception event (from span1)
			// 2: link (from span1)
			// 3: Server span, HTTP GET /api/users (span1 itself)
			// 4: Client span, DB Query (span2)
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
				`unfriendly\to "json"`: `I need\ to "escape"`,
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
			assert.Equal(t, int32(expectedService1SampleRate), mainSpan.SampleRate)
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
			assert.Equal(t, int32(expectedService1SampleRate), cacheEvent.SampleRate)
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
			assert.Equal(t, int32(expectedService1SampleRate), exceptionEvent.SampleRate)
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
			assert.Equal(t, int32(expectedService1SampleRate), linkEvent.SampleRate)
			assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), linkEvent.Timestamp, "Links use parent span timestamp")

			// DB Query span at index 4
			dbSpan := &batch1.Events[4]
			dbAttrs := decodeMessagePackAttributes(t, dbSpan.Attributes)
			assert.Equal(t, addService1CommonAttributes(map[string]any{
				"name":              "DB Query",
				"trace.trace_id":    traceID1,
				"trace.span_id":     spanID2,
				"trace.parent_id":   spanID1,
				"span.kind":         "client",
				"type":              "client",
				"db.statement":      "SELECT * FROM users WHERE id = ?",
				"status_code":       int64(0),
				"duration_ms":       float64(764.197532),
				"span.num_events":   int64(0),
				"span.num_links":    int64(0),
				"conflicting":       "scope",
				"trace.trace_state": "w3c=false;th=db6db6db6db6dc",

				// Scope attributes from HTTP instrumentation library
				"library.name":                      "go.opentelemetry.io/contrib/instrumentation/net/http",
				"library.version":                   "1.0.0",
				"telemetry.instrumentation_library": true,
				"scope.attr":                        "scope_value",
			}), dbAttrs)
			assert.Equal(t, int32(expectedService1SampleRate), dbSpan.SampleRate,
				"Service1's sampleRate resource attr overrides the tracestate sampling threshold.")
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
			assert.Equal(t, int32(expectedService1SampleRate), errorLinkEvent.SampleRate)
			assert.Equal(t, time.Unix(0, int64(errorStartTime)).UTC(), errorLinkEvent.Timestamp, "Links use parent span timestamp")

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
			assert.Equal(t, int32(expectedService1SampleRate), errorSpan.SampleRate)
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
			assert.Equal(t, int32(expectedService1SampleRate), firstExceptionEvent.SampleRate)
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
			assert.Equal(t, int32(expectedService1SampleRate), secondExceptionEvent.SampleRate)
			assert.Equal(t, time.Unix(0, int64(errorStartTime+100_000_000)).UTC(), secondExceptionEvent.Timestamp)

			// Batch 2: service2
			batch2 := result.Batches[1]
			assert.Equal(t, "service2", batch2.Dataset)
			assert.Len(t, batch2.Events, 1, "Should only contain the producer span")

			producerEvent := batch2.Events[0]
			producerAttrs := decodeMessagePackAttributes(t, producerEvent.Attributes)
			assert.Equal(t, map[string]any{
				"trace.trace_id":    "1a2b3c4d5e6f7089",
				"trace.span_id":     spanID2,
				"trace.trace_state": "w3c=false;th=db6db6db6db6dc",
				"name":              "Publish Message",
				"span.kind":         "producer",
				"type":              "producer",
				"status_code":       int64(0),
				"duration_ms":       float64(864.197532),
				"span.num_events":   int64(0),
				"span.num_links":    int64(0),
				"meta.signal_type":  "trace",
				"service.name":      "service2",
			}, producerAttrs)
			assert.Equal(t, int32(expectedSampleRateFromThreshold), producerEvent.SampleRate,
				"Producer span with no sampleRate attr set, final sampleRate should be based on OTel sampling threshold.")
			assert.Equal(t, time.Unix(0, int64(startTime)).UTC(), producerEvent.Timestamp)

			// Batch 3: service1 is batched seperately due to arriving in a different
			// Resource message.
			batch3 := result.Batches[2]
			assert.Equal(t, "service1", batch3.Dataset)
			assert.Len(t, batch3.Events, 1)

			// Since this is an effectively empty message, confirm all of our default field values here.
			batch3Event := batch3.Events[0]
			batch3Attrs := decodeMessagePackAttributes(t, batch3Event.Attributes)
			assert.Equal(t, addService1CommonAttributes(map[string]any{
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
				"conflicting":      "resource",
			}), batch3Attrs)
			assert.Equal(t, int32(expectedService1SampleRate), batch3Event.SampleRate)
			assert.Equal(t, time.Unix(0, 0).UTC(), batch3Event.Timestamp)

			t.Run("ErrorHandling", func(t *testing.T) {
				// Shave 5 bytes off the end of our serialized message, which should net
				// us an EOF error from deep within the stack.
				data = data[:len(data)-5]
				_, err := tc.unmarshal(context.Background(), data, ri)
				if tc.contentType == "application/json" {
					// JSON parsing errors are different from protobuf.
					assert.Error(t, err)
				} else {
					assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
				}
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

				// RequestSize will be different for JSON vs protobuf
				if tc.contentType == "application/json" {
					assert.Less(t, regularResult.RequestSize, result.RequestSize)
				} else {
					assert.Equal(t, regularResult.RequestSize, result.RequestSize)
				}
				assert.Equal(t, len(regularResult.Batches), len(result.Batches))

				// Convert msgpack batches to regular batches for comparison
				directBatches := make([]Batch, len(result.Batches))
				for i, msgpBatch := range result.Batches {
					directBatches[i] = convertBatchMsgpToBatch(t, msgpBatch)
				}

				// Compare each batch
				for i := range directBatches {
					t.Run("Batch "+fmt.Sprint(i), func(t *testing.T) {
						directBatch := directBatches[i]
						regularBatch := regularResult.Batches[i]

						assert.Equal(t, regularBatch.Dataset, directBatch.Dataset, "Batch %d dataset mismatch", i)
						assert.Equal(t, len(regularBatch.Events), len(directBatch.Events), "Batch %d event count mismatch", i)

						// Compare each event
						for j := range directBatch.Events {
							t.Run("Event "+fmt.Sprint(j), func(t *testing.T) {
								directEvent := directBatch.Events[j]
								regularEvent := regularBatch.Events[j]

								// Compare timestamps and sample rates
								assert.Equal(t, regularEvent.Timestamp, directEvent.Timestamp, "Batch %d Event %d timestamp mismatch", i, j)
								assert.Equal(t, regularEvent.SampleRate, directEvent.SampleRate, "Batch %d Event %d sample rate mismatch", i, j)
								// Having confirmed SampleRate field matches on both events,
								// omit the sampleRate Attribute from regularEvent because the
								// direct implementation intentionally does not record sampleRate
								// in Attributes because it is ignored during ingest in favor of the
								// Event struct's SampleRate field.
								delete(regularEvent.Attributes, "sampleRate")

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
								assert.Equal(t, regularEvent.Attributes, directEvent.Attributes,
									"Batch %d Event %d attributes mismatch (after removing known improvements)", i, j)
							})
						}
					})
				}
			})
		})
	}
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
	result, err := UnmarshalTraceRequestDirectMsgp(context.Background(), data, ri)
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
	assert.Equal(t, defaultSampleRate, event.SampleRate, "We still set a default sample rate.")
}

// Helper to create nested kvlist attributes with any value type at the leaf
func createNestedMapWithAnyValue(depth int, leafValue *common.AnyValue) *common.AnyValue {
	if depth == 0 {
		return leafValue
	}

	return &common.AnyValue{
		Value: &common.AnyValue_KvlistValue{
			KvlistValue: &common.KeyValueList{
				Values: []*common.KeyValue{
					{
						Key:   fmt.Sprintf("level%d", depth),
						Value: createNestedMapWithAnyValue(depth-1, leafValue),
					},
				},
			},
		},
	}
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

	// Helper to create nested kvlist attributes with string values
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
									// Depth 7: should be JSON encoded - test all supported field types
									{
										Key:   "map7_string",
										Value: createNestedMap(7, "value7"),
									},
									{
										Key: "map7_bool",
										Value: createNestedMapWithAnyValue(6, &common.AnyValue{
											Value: &common.AnyValue_KvlistValue{
												KvlistValue: &common.KeyValueList{
													Values: []*common.KeyValue{
														{
															Key: "level1",
															Value: &common.AnyValue{
																Value: &common.AnyValue_BoolValue{BoolValue: true},
															},
														},
													},
												},
											},
										}),
									},
									{
										Key: "map7_int",
										Value: createNestedMapWithAnyValue(6, &common.AnyValue{
											Value: &common.AnyValue_KvlistValue{
												KvlistValue: &common.KeyValueList{
													Values: []*common.KeyValue{
														{
															Key: "level1",
															Value: &common.AnyValue{
																Value: &common.AnyValue_IntValue{IntValue: 42},
															},
														},
													},
												},
											},
										}),
									},
									{
										Key: "map7_double",
										Value: createNestedMapWithAnyValue(6, &common.AnyValue{
											Value: &common.AnyValue_KvlistValue{
												KvlistValue: &common.KeyValueList{
													Values: []*common.KeyValue{
														{
															Key: "level1",
															Value: &common.AnyValue{
																Value: &common.AnyValue_DoubleValue{DoubleValue: 3.14159},
															},
														},
													},
												},
											},
										}),
									},
									{
										Key: "map7_bytes",
										Value: createNestedMapWithAnyValue(6, &common.AnyValue{
											Value: &common.AnyValue_KvlistValue{
												KvlistValue: &common.KeyValueList{
													Values: []*common.KeyValue{
														{
															Key: "level1",
															Value: &common.AnyValue{
																Value: &common.AnyValue_BytesValue{BytesValue: []byte("hello world")},
															},
														},
													},
												},
											},
										}),
									},
									{
										Key: "map7_array",
										Value: createNestedMapWithAnyValue(6, &common.AnyValue{
											Value: &common.AnyValue_KvlistValue{
												KvlistValue: &common.KeyValueList{
													Values: []*common.KeyValue{
														{
															Key: "level1",
															Value: &common.AnyValue{
																Value: &common.AnyValue_ArrayValue{
																	ArrayValue: &common.ArrayValue{
																		Values: []*common.AnyValue{
																			{Value: &common.AnyValue_StringValue{StringValue: "item1"}},
																			{Value: &common.AnyValue_IntValue{IntValue: 123}},
																			{Value: &common.AnyValue_BoolValue{BoolValue: false}},
																		},
																	},
																},
															},
														},
													},
												},
											},
										}),
									},
									{
										Key: "map7_nested_kvlist",
										Value: createNestedMapWithAnyValue(6, &common.AnyValue{
											Value: &common.AnyValue_KvlistValue{
												KvlistValue: &common.KeyValueList{
													Values: []*common.KeyValue{
														{
															Key: "level1",
															Value: &common.AnyValue{
																Value: &common.AnyValue_KvlistValue{
																	KvlistValue: &common.KeyValueList{
																		Values: []*common.KeyValue{
																			{
																				Key: "inner_key",
																				Value: &common.AnyValue{
																					Value: &common.AnyValue_StringValue{StringValue: "inner_value"},
																				},
																			},
																			{
																				Key: "inner_num",
																				Value: &common.AnyValue{
																					Value: &common.AnyValue_IntValue{IntValue: 999},
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
										}),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Test both protobuf and JSON serialization formats
	testCases := []struct {
		name        string
		contentType string
		serialize   func(*collectortrace.ExportTraceServiceRequest) ([]byte, error)
		unmarshal   func(context.Context, []byte, RequestInfo) (*TranslateOTLPRequestResultMsgp, error)
	}{
		{
			name:        "protobuf",
			contentType: "application/protobuf",
			serialize: func(req *collectortrace.ExportTraceServiceRequest) ([]byte, error) {
				return proto.Marshal(req)
			},
			unmarshal: UnmarshalTraceRequestDirectMsgp,
		},
		{
			name:        "json",
			contentType: "application/json",
			serialize: func(req *collectortrace.ExportTraceServiceRequest) ([]byte, error) {
				return protojson.Marshal(req)
			},
			unmarshal: unmarshalTraceRequestDirectMsgpJSON,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.serialize(req)
			require.NoError(t, err)

			ri := RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "test-dataset",
				ContentType: tc.contentType,
			}

			// First, get results from direct unmarshaling
			directResult, err := tc.unmarshal(context.Background(), data, ri)
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

				// String value (original test)
				expectedMap7String := `{"level2":{"level1":"value7"}}` + "\n"
				assert.Equal(t, expectedMap7String, regularAttrs["map7_string.level7.level6.level5.level4.level3"])
				assert.Equal(t, expectedMap7String, directAttrs["map7_string.level7.level6.level5.level4.level3"])

				// Bool value
				expectedMap7Bool := `{"level1":{"level1":true}}` + "\n"
				assert.Equal(t, expectedMap7Bool, regularAttrs["map7_bool.level6.level5.level4.level3.level2"])
				assert.Equal(t, expectedMap7Bool, directAttrs["map7_bool.level6.level5.level4.level3.level2"])

				// Int value
				expectedMap7Int := `{"level1":{"level1":42}}` + "\n"
				assert.Equal(t, expectedMap7Int, regularAttrs["map7_int.level6.level5.level4.level3.level2"])
				assert.Equal(t, expectedMap7Int, directAttrs["map7_int.level6.level5.level4.level3.level2"])

				// Double value
				expectedMap7Double := `{"level1":{"level1":3.14159}}` + "\n"
				assert.Equal(t, expectedMap7Double, regularAttrs["map7_double.level6.level5.level4.level3.level2"])
				assert.Equal(t, expectedMap7Double, directAttrs["map7_double.level6.level5.level4.level3.level2"])

				// Bytes value (base64 encoded in JSON)
				expectedMap7Bytes := `{"level1":{"level1":"aGVsbG8gd29ybGQ="}}` + "\n"
				assert.Equal(t, expectedMap7Bytes, regularAttrs["map7_bytes.level6.level5.level4.level3.level2"])
				assert.Equal(t, expectedMap7Bytes, directAttrs["map7_bytes.level6.level5.level4.level3.level2"])

				// Array value
				expectedMap7Array := `{"level1":{"level1":["item1",123,false]}}` + "\n"
				assert.Equal(t, expectedMap7Array, regularAttrs["map7_array.level6.level5.level4.level3.level2"])
				assert.Equal(t, expectedMap7Array, directAttrs["map7_array.level6.level5.level4.level3.level2"])

				// Nested kvlist value
				expectedMap7Nested := `{"level1":{"level1":{"inner_key":"inner_value","inner_num":999}}}` + "\n"
				assert.Equal(t, expectedMap7Nested, regularAttrs["map7_nested_kvlist.level6.level5.level4.level3.level2"])
				assert.Equal(t, expectedMap7Nested, directAttrs["map7_nested_kvlist.level6.level5.level4.level3.level2"])
			})
		})
	}
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

	// Run sub-benchmarks for protobuf and JSON
	b.Run("Protobuf", func(b *testing.B) {
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
	})

	b.Run("JSON", func(b *testing.B) {
		ri := RequestInfo{
			ApiKey:      "abc123DEF456ghi789jklm",
			Dataset:     "benchmark-dataset",
			ContentType: "application/json",
		}

		// Serialize the request to JSON once before benchmarking
		jsonData, err := protojson.Marshal(req)
		if err != nil {
			b.Fatal("Failed to serialize trace request to JSON:", err)
		}
		ctx := context.Background()

		// Reset timer to exclude setup time
		b.ResetTimer()

		// Run the benchmark
		for i := 0; i < b.N; i++ {
			result, err := unmarshalTraceRequestDirectMsgpJSON(ctx, jsonData, ri)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Batches) != 1 || len(result.Batches[0].Events) != 1 {
				b.Fatal("unexpected result structure")
			}
		}
	})
}

// generateComprehensiveOTLPTraceJSON generates a comprehensive OTLP trace request JSON
// with all possible fields populated. The errorLocation parameter specifies where
// to inject an invalid field that should cause parsing to fail.
func generateComprehensiveOTLPTraceJSON(errorLocation string, invalidValue string) string {
	// Helper functions to inject errors based on location and data type
	getIntValue := func(location string, validValue int) interface{} {
		if errorLocation == location {
			return invalidValue
		}
		return validValue
	}

	// Build the JSON structure programmatically to handle different data types correctly
	data := map[string]interface{}{
		"resourceSpans": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{
							"key":   "int.attr",
							"value": map[string]interface{}{"intValue": getIntValue("resource.int.attr", 12345)},
						},
						{
							"key":   "bool.attr",
							"value": true,
						},
						{
							"key":   "double.attr",
							"value": 123.456,
						},
						{
							"key":   "bytes.attr",
							"value": "aGVsbG8gd29ybGQ=",
						},
					},
				},
				"scopeSpans": []map[string]interface{}{
					{
						"scope": map[string]interface{}{
							"name":    "test-scope",
							"version": "1.2.3",
							"attributes": []map[string]interface{}{
								{
									"key":   "attribute.int",
									"value": map[string]interface{}{"intValue": getIntValue("scope.attribute.int", 456)},
								},
								{
									"key":   "attribute.bool",
									"value": map[string]interface{}{"boolValue": false},
								},
								{
									"key":   "attribute.double",
									"value": map[string]interface{}{"doubleValue": 789.123},
								},
							},
						},
						"spans": []map[string]interface{}{
							{
								"traceId":           "AQIDBAUGAAAAAAAAAAAAAAAAAA==",
								"spanId":            "AQIDBAUGAA==",
								"parentSpanId":      "ERERFREUAAAAAA==",
								"name":              "test-span",
								"kind":              getIntValue("span.kind", 2),
								"startTimeUnixNano": "1234567890123456789",
								"endTimeUnixNano":   "1234567890987654321",
								"attributes": []map[string]interface{}{
									{
										"key":   "attributes.int",
										"value": map[string]interface{}{"intValue": getIntValue("span.attributes.int", 200)},
									},
									{
										"key":   "attributes.bool",
										"value": map[string]interface{}{"boolValue": true},
									},
									{
										"key":   "attributes.double",
										"value": map[string]interface{}{"doubleValue": 12.34},
									},
								},
								"events": []map[string]interface{}{
									{
										"timeUnixNano": "1234567890223456789",
										"name":         "test-event-1",
										"attributes": []map[string]interface{}{
											{
												"key":   "event.int",
												"value": map[string]interface{}{"intValue": getIntValue("event1.int", 3600)},
											},
											{
												"key":   "event.double",
												"value": map[string]interface{}{"doubleValue": 0.85},
											},
											{
												"key":   "event.bool",
												"value": map[string]interface{}{"boolValue": true},
											},
										},
									},
								},
								"links": []map[string]interface{}{
									{
										"traceId": "MTIzNDU2Nzg5OnV7PD0+P0E=",
										"spanId":  "QUJDREVGRkg=",
										"attributes": []map[string]interface{}{
											{
												"key":   "links.int",
												"value": map[string]interface{}{"intValue": getIntValue("link1.int", 5)},
											},
											{
												"key":   "links.double",
												"value": map[string]interface{}{"doubleValue": 0.75},
											},
											{
												"key":   "links.bool",
												"value": map[string]interface{}{"boolValue": true},
											},
										},
									},
								},
								"status": map[string]interface{}{
									"code": getIntValue("status.code", 1),
								},
							},
						},
					},
				},
			},
		},
	}

	jsonBytes, _ := json.Marshal(data)
	return string(jsonBytes)
}
func TestUnmarshalTraceRequestDirectJSON_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/json",
	}

	testCases := []struct {
		name     string
		jsonData string
		errorMsg string
	}{
		{
			name: "invalid_int_value",
			jsonData: `{
				"resourceSpans": [{
					"resource": {
						"attributes": [{
							"key": "test_int",
							"value": {
								"intValue": "not-a-number"
							}
						}]
					},
					"scopeSpans": [{
						"spans": [{
							"traceId": "AQIDBAUGAAAAAAAAAAAAAAAAAA==",
							"spanId": "AQIDBAUGAA==",
							"name": "test-span"
						}]
					}]
				}]
			}`,
			errorMsg: "invalid syntax",
		},
		{
			name: "invalid_span_attributes",
			jsonData: `{
				"resourceSpans": [{
					"scopeSpans": [{
						"spans": [{
							"traceId": "AQIDBAUGAAAAAAAAAAAAAAAAAA==",
							"spanId": "AQIDBAUGAA==",
							"name": "test-span",
							"attributes": [{
								"key": "bad_int",
								"value": {
									"intValue": "invalid"
								}
							}]
						}]
					}]
				}]
			}`,
			errorMsg: "invalid syntax",
		},
		{
			name: "invalid_json_syntax",
			jsonData: `{
				"resourceSpans": [{
					"resource": {
						"attributes": [{invalid json here}]
					}
				}]
			}`,
			errorMsg: "", // fastjson will return a parse error
		},
		{
			name:     "resource_int_error",
			jsonData: generateComprehensiveOTLPTraceJSON("resource.int.attr", "not-a-number"),
			errorMsg: "invalid syntax",
		},
		{
			name:     "scope_int_error",
			jsonData: generateComprehensiveOTLPTraceJSON("scope.attribute.int", "invalid-int"),
			errorMsg: "invalid syntax",
		},
		{
			name:     "span_attribute_int_error",
			jsonData: generateComprehensiveOTLPTraceJSON("span.attributes.int", "not-a-number"),
			errorMsg: "invalid syntax",
		},
		{
			name:     "event_int_error",
			jsonData: generateComprehensiveOTLPTraceJSON("event1.int", "invalid-int"),
			errorMsg: "invalid syntax",
		},
		{
			name:     "link_int_error",
			jsonData: generateComprehensiveOTLPTraceJSON("link1.int", "invalid-int"),
			errorMsg: "invalid syntax",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := unmarshalTraceRequestDirectMsgpJSON(ctx, []byte(tc.jsonData), ri)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errorMsg)
		})
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
