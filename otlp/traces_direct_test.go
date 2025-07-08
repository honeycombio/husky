package otlp

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestUnmarshalTraceRequestDirect_Empty(t *testing.T) {
	// Create an empty request
	req := &collectortrace.ExportTraceServiceRequest{}
	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, len(data), result.RequestSize)
	assert.Empty(t, result.Batches)
}

func TestUnmarshalTraceRequestDirect_WithResourceSpans(t *testing.T) {
	// Create a request with ResourceSpans but no actual spans
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{},
			{},
		},
	}
	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, len(data), result.RequestSize)
	// No batches yet since we don't parse spans
	assert.Empty(t, result.Batches)
}

func TestUnmarshalTraceRequestDirect_WithResource(t *testing.T) {
	// Create a request with ResourceSpans including resource attributes
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
						{
							Key: "resource.attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "resource-value"},
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

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	// No batches yet since we don't parse spans
	assert.Empty(t, result.Batches)
}

func TestUnmarshalTraceRequestDirect_WithScopeSpans(t *testing.T) {
	// Create a request with ScopeSpans and InstrumentationScope
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
						Scope: &common.InstrumentationScope{
							Name:    "test-library",
							Version: "1.0.0",
							Attributes: []*common.KeyValue{
								{
									Key: "scope.attr",
									Value: &common.AnyValue{
										Value: &common.AnyValue_StringValue{StringValue: "scope-value"},
									},
								},
							},
						},
						// We'll add spans in the next test
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

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	// No batches yet since we don't parse spans
	assert.Empty(t, result.Batches)
}

func TestUnmarshalTraceRequestDirect_WithBasicSpan(t *testing.T) {
	// Create a simple span
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)
	// Expected duration: (1234567890987654321 - 1234567890123456789) = 864197532 ns = 864.197532 ms ≈ 864 ms

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
						Scope: &common.InstrumentationScope{
							Name:    "test-library",
							Version: "1.0.0",
						},
						Spans: []*trace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "test-span",
								Kind:              trace.Span_SPAN_KIND_CLIENT,
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

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)
	
	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)
	
	event := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event.Attributes["trace.span_id"])
	assert.Equal(t, "test-span", event.Attributes["name"])
	assert.Equal(t, "client", event.Attributes["span.kind"])
	assert.InDelta(t, float64(864.197532), event.Attributes["duration_ms"], 0.01)
	assert.Equal(t, "test-library", event.Attributes["library.name"])
	assert.Equal(t, "1.0.0", event.Attributes["library.version"])
	assert.Equal(t, "test-service", event.Attributes["service.name"])
}

func TestUnmarshalTraceRequestDirect_WithSpanAttributesAndStatus(t *testing.T) {
	// Create a span with attributes and status
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	parentSpanID := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
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
						Scope: &common.InstrumentationScope{
							Name:    "test-library",
							Version: "1.0.0",
						},
						Spans: []*trace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								ParentSpanId:      parentSpanID,
								TraceState:        "w3c=true",
								Name:              "test-span",
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
											Value: &common.AnyValue_StringValue{StringValue: "https://example.com/api/v1/users"},
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

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)
	
	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)
	
	event := batch.Events[0]
	// Basic fields
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event.Attributes["trace.span_id"])
	assert.Equal(t, "1112131415161718", event.Attributes["trace.parent_id"])
	assert.Equal(t, "w3c=true", event.Attributes["trace.trace_state"])
	assert.Equal(t, "test-span", event.Attributes["name"])
	assert.Equal(t, "server", event.Attributes["span.kind"])
	
	// Span attributes
	assert.Equal(t, "GET", event.Attributes["http.method"])
	assert.Equal(t, int64(200), event.Attributes["http.status_code"])
	assert.Equal(t, "https://example.com/api/v1/users", event.Attributes["http.url"])
	assert.Equal(t, float64(1234.56), event.Attributes["response.size"])
	assert.Equal(t, true, event.Attributes["success"])
	
	// Status
	assert.Equal(t, 1, event.Attributes["status_code"]) // STATUS_CODE_OK = 1
	assert.Equal(t, "Request completed successfully", event.Attributes["status_message"])
	
	// Sample rate should be extracted from attributes
	assert.Equal(t, int32(10), event.SampleRate)
	// sampleRate attribute should be removed
	assert.Nil(t, event.Attributes["sampleRate"])
}

func TestUnmarshalTraceRequestDirect_WithSpanEvents(t *testing.T) {
	// Create a span with events
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)
	event1Time := uint64(1234567890123456789 + 100_000_000) // 100ms after start
	event2Time := uint64(1234567890123456789 + 200_000_000) // 200ms after start

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
						Scope: &common.InstrumentationScope{
							Name: "test-library",
						},
						Spans: []*trace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "test-span",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
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
													Value: &common.AnyValue_StringValue{StringValue: "Invalid input"},
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
		},
	}
	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)
	
	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	// Should have 3 events: 1 span + 2 span events
	assert.Len(t, batch.Events, 3)
	
	// First event is the cache_miss event
	event1 := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event1.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event1.Attributes["trace.parent_id"]) // parent is the span
	assert.Nil(t, event1.Attributes["trace.span_id"]) // span events don't have their own span ID
	assert.Equal(t, "cache_miss", event1.Attributes["name"])
	assert.Equal(t, "span_event", event1.Attributes["meta.annotation_type"])
	assert.Equal(t, "user:123", event1.Attributes["cache.key"])
	assert.Equal(t, int64(3600), event1.Attributes["cache.ttl"])
	assert.Equal(t, float64(100), event1.Attributes["meta.time_since_span_start_ms"]) // relative to span start
	
	// Second event is the exception event  
	event2 := batch.Events[1]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event2.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event2.Attributes["trace.parent_id"])
	assert.Nil(t, event2.Attributes["trace.span_id"]) // span events don't have their own span ID
	assert.Equal(t, "exception", event2.Attributes["name"])
	assert.Equal(t, "span_event", event2.Attributes["meta.annotation_type"])
	assert.Equal(t, "ValueError", event2.Attributes["exception.type"])
	assert.Equal(t, "Invalid input", event2.Attributes["exception.message"])
	assert.Equal(t, float64(200), event2.Attributes["meta.time_since_span_start_ms"])
	
	// Third event is the span itself
	spanEvent := batch.Events[2]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", spanEvent.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", spanEvent.Attributes["trace.span_id"])
	assert.Equal(t, "test-span", spanEvent.Attributes["name"])
}

func TestUnmarshalTraceRequestDirect_WithSpanLinks(t *testing.T) {
	// Create a span with links
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	linkedTraceID1 := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}
	linkedSpanID1 := []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}
	linkedTraceID2 := []byte{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40}
	linkedSpanID2 := []byte{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48}
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
						Scope: &common.InstrumentationScope{
							Name: "test-library",
						},
						Spans: []*trace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "test-span",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: startTime,
								EndTimeUnixNano:   endTime,
								Links: []*trace.Span_Link{
									{
										TraceId:    linkedTraceID1,
										SpanId:     linkedSpanID1,
										TraceState: "vendor1=value1",
										Attributes: []*common.KeyValue{
											{
												Key: "link.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "parent"},
												},
											},
										},
									},
									{
										TraceId:    linkedTraceID2,
										SpanId:     linkedSpanID2,
										TraceState: "vendor2=value2",
										Attributes: []*common.KeyValue{
											{
												Key: "link.type",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "child"},
												},
											},
											{
												Key: "link.service",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "downstream-service"},
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
		},
	}
	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)
	
	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	// Should have 3 events: 1 span + 2 span links
	assert.Len(t, batch.Events, 3)
	
	// First event is the first link
	link1 := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", link1.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", link1.Attributes["trace.parent_id"]) // parent is the span
	assert.Nil(t, link1.Attributes["trace.span_id"]) // span links don't have their own span ID
	assert.Equal(t, "1112131415161718191a1b1c1d1e1f20", link1.Attributes["trace.link.trace_id"])
	assert.Equal(t, "2122232425262728", link1.Attributes["trace.link.span_id"])
	// trace.link.trace_state is not added by the implementation
	assert.Equal(t, "link", link1.Attributes["meta.annotation_type"])
	assert.Equal(t, "parent", link1.Attributes["link.type"])
	
	// Second event is the second link  
	link2 := batch.Events[1]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", link2.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", link2.Attributes["trace.parent_id"])
	assert.Nil(t, link2.Attributes["trace.span_id"]) // span links don't have their own span ID
	assert.Equal(t, "3132333435363738393a3b3c3d3e3f40", link2.Attributes["trace.link.trace_id"])
	assert.Equal(t, "4142434445464748", link2.Attributes["trace.link.span_id"])
	// trace.link.trace_state is not added by the implementation
	assert.Equal(t, "link", link2.Attributes["meta.annotation_type"])
	assert.Equal(t, "child", link2.Attributes["link.type"])
	assert.Equal(t, "downstream-service", link2.Attributes["link.service"])
	
	// Third event is the span itself
	spanEvent := batch.Events[2]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", spanEvent.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", spanEvent.Attributes["trace.span_id"])
	assert.Equal(t, "test-span", spanEvent.Attributes["name"])
}

func TestTranslateTraceRequestFromReaderDirect(t *testing.T) {
	// Create a test request with all features we support
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	parentSpanID := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)
	eventTime := uint64(1234567890123456789 + 100_000_000) // 100ms after start

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
						{
							Key: "deployment.environment", 
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "production"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Scope: &common.InstrumentationScope{
							Name:    "test-library",
							Version: "1.0.0",
						},
						Spans: []*trace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								ParentSpanId:      parentSpanID,
								TraceState:        "w3c=true",
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
										Key: "sampleRate",
										Value: &common.AnyValue{
											Value: &common.AnyValue_IntValue{IntValue: 10},
										},
									},
								},
								Status: &trace.Status{
									Code:    trace.Status_STATUS_CODE_OK,
									Message: "Success",
								},
								Events: []*trace.Span_Event{
									{
										TimeUnixNano: eventTime,
										Name:         "db_query",
										Attributes: []*common.KeyValue{
											{
												Key: "db.statement",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "SELECT * FROM users"},
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
		},
	}

	// Serialize the request
	data := serializeTraceRequest(t, req)
	
	// Create a reader from the data
	body := io.NopCloser(bytes.NewReader(data))
	
	// Create request info
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}
	
	// Call the new direct function
	result, err := TranslateTraceRequestFromReaderDirect(context.Background(), body, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Greater(t, result.RequestSize, 0)
	assert.Len(t, result.Batches, 1)
	
	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	// Should have 2 events: 1 span + 1 span event
	assert.Len(t, batch.Events, 2)
	
	// First event should be the span event (db_query)
	dbEvent := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", dbEvent.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", dbEvent.Attributes["trace.parent_id"])
	assert.Nil(t, dbEvent.Attributes["trace.span_id"]) // span events don't have their own span ID
	assert.Equal(t, "db_query", dbEvent.Attributes["name"])
	assert.Equal(t, "span_event", dbEvent.Attributes["meta.annotation_type"])
	assert.Equal(t, "SELECT * FROM users", dbEvent.Attributes["db.statement"])
	assert.Equal(t, float64(100), dbEvent.Attributes["meta.time_since_span_start_ms"])
	assert.Equal(t, int32(10), dbEvent.SampleRate)
	
	// Second event should be the main span
	spanEvent := batch.Events[1]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", spanEvent.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", spanEvent.Attributes["trace.span_id"])
	assert.Equal(t, "1112131415161718", spanEvent.Attributes["trace.parent_id"])
	assert.Equal(t, "HTTP GET /api/users", spanEvent.Attributes["name"])
	assert.Equal(t, "server", spanEvent.Attributes["span.kind"])
	assert.Equal(t, "GET", spanEvent.Attributes["http.method"])
	assert.Equal(t, int64(200), spanEvent.Attributes["http.status_code"])
	assert.Equal(t, 1, spanEvent.Attributes["status_code"]) // STATUS_CODE_OK = 1
	assert.Equal(t, "Success", spanEvent.Attributes["status_message"])
	assert.Equal(t, int32(10), spanEvent.SampleRate)
	assert.Equal(t, "production", spanEvent.Attributes["deployment.environment"])
}

func TestCompareDirectVsRegularUnmarshaling(t *testing.T) {
	// Create a complex test request
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	parentSpanID := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)
	eventTime := uint64(1234567890123456789 + 100_000_000)

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
						{
							Key: "resource.attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "resource-value"},
							},
						},
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Scope: &common.InstrumentationScope{
							Name:    "test-library",
							Version: "1.0.0",
							Attributes: []*common.KeyValue{
								{
									Key: "scope.attr",
									Value: &common.AnyValue{
										Value: &common.AnyValue_StringValue{StringValue: "scope-value"},
									},
								},
							},
						},
						Spans: []*trace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								ParentSpanId:      parentSpanID,
								TraceState:        "w3c=true",
								Name:              "test-span",
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
										Key: "sampleRate",
										Value: &common.AnyValue{
											Value: &common.AnyValue_IntValue{IntValue: 10},
										},
									},
								},
								Status: &trace.Status{
									Code:    trace.Status_STATUS_CODE_OK,
									Message: "Success",
								},
								Events: []*trace.Span_Event{
									{
										TimeUnixNano: eventTime,
										Name:         "test_event",
										Attributes: []*common.KeyValue{
											{
												Key: "event.attr",
												Value: &common.AnyValue{
													Value: &common.AnyValue_StringValue{StringValue: "event-value"},
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
		},
	}

	// Serialize the request
	data := serializeTraceRequest(t, req)
	
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}
	
	// Get results from regular unmarshaling
	body1 := io.NopCloser(bytes.NewReader(data))
	regularResult, err := TranslateTraceRequestFromReader(context.Background(), body1, ri)
	require.NoError(t, err)
	
	// Get results from direct unmarshaling
	body2 := io.NopCloser(bytes.NewReader(data))
	directResult, err := TranslateTraceRequestFromReaderDirect(context.Background(), body2, ri)
	require.NoError(t, err)
	
	// Compare results
	assert.Equal(t, len(regularResult.Batches), len(directResult.Batches), "Batch count mismatch")
	
	for i, regularBatch := range regularResult.Batches {
		directBatch := directResult.Batches[i]
		assert.Equal(t, regularBatch.Dataset, directBatch.Dataset, "Dataset mismatch")
		assert.Equal(t, len(regularBatch.Events), len(directBatch.Events), "Event count mismatch")
		
		// For each event, compare key attributes
		for j, regularEvent := range regularBatch.Events {
			directEvent := directBatch.Events[j]
			
			// Check core trace attributes
			assert.Equal(t, regularEvent.Attributes["trace.trace_id"], directEvent.Attributes["trace.trace_id"], "trace_id mismatch")
			assert.Equal(t, regularEvent.Attributes["trace.span_id"], directEvent.Attributes["trace.span_id"], "span_id mismatch")
			assert.Equal(t, regularEvent.Attributes["trace.parent_id"], directEvent.Attributes["trace.parent_id"], "parent_id mismatch")
			assert.Equal(t, regularEvent.Attributes["name"], directEvent.Attributes["name"], "name mismatch")
			assert.Equal(t, regularEvent.Attributes["span.kind"], directEvent.Attributes["span.kind"], "span.kind mismatch")
			assert.Equal(t, regularEvent.SampleRate, directEvent.SampleRate, "sample rate mismatch")
			
			// Check timestamps are approximately equal (within 1ms)
			timeDiff := regularEvent.Timestamp.Sub(directEvent.Timestamp).Abs()
			assert.Less(t, timeDiff, time.Millisecond, "timestamp difference too large")
		}
	}
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
	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)

	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)

	event := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event.Attributes["trace.span_id"])
	assert.Equal(t, "test-span", event.Attributes["name"])
}


func TestUnmarshalTraceRequestDirect_WithBytesValue(t *testing.T) {
	// Test with bytes value type
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{
					{
						Key: "service.name",
						Value: &common.AnyValue{
							Value: &common.AnyValue_StringValue{StringValue: "test-service"},
						},
					},
					{
						Key: "bytes_attr",
						Value: &common.AnyValue{
							Value: &common.AnyValue_BytesValue{BytesValue: []byte{0x01, 0x02, 0x03, 0x04}},
						},
					},
				},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					TraceId:           traceID,
					SpanId:            spanID,
					Name:              "test-span",
					Kind:              trace.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: startTime,
					EndTimeUnixNano:   endTime,
				}},
			}},
		}},
	}

	// Serialize the request
	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	// Test with direct unmarshaling
	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)

	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)

	event := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event.Attributes["trace.span_id"])
	assert.Equal(t, "test-span", event.Attributes["name"])

	// Check bytes attribute - should be JSON encoded
	bytesAttr, ok := event.Attributes["bytes_attr"].(string)
	assert.True(t, ok, "bytes_attr should be a string")
	assert.Equal(t, "\"AQIDBA==\"\n", bytesAttr) // base64 encoded with JSON encoding and newline
}

func TestUnmarshalTraceRequestDirect_WithUnsupportedAnyValueTypes(t *testing.T) {
	// Test with unsupported AnyValue types to ensure our direct unmarshaling now supports them
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	startTime := uint64(1234567890123456789)
	endTime := uint64(1234567890987654321)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{
					{
						Key: "service.name",
						Value: &common.AnyValue{
							Value: &common.AnyValue_StringValue{StringValue: "test-service"},
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
									},
								},
							},
						},
					},
				},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					TraceId:           traceID,
					SpanId:            spanID,
					Name:              "test-span",
					Kind:              trace.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: startTime,
					EndTimeUnixNano:   endTime,
					Attributes: []*common.KeyValue{
						{
							Key: "span_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_attr_val"},
							},
						},
					},
				}},
			}},
		}},
	}

	// Serialize the request
	data := serializeTraceRequest(t, req)

	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "test-dataset",
		ContentType: "application/protobuf",
	}

	// Test with direct unmarshaling - currently it doesn't support bytes/array/kvlist
	result, err := UnmarshalTraceRequestDirect(context.Background(), data, ri)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Batches, 1)

	batch := result.Batches[0]
	assert.Equal(t, "test-service", batch.Dataset)
	assert.Len(t, batch.Events, 1)

	event := batch.Events[0]
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", event.Attributes["trace.trace_id"])
	assert.Equal(t, "0102030405060708", event.Attributes["trace.span_id"])
	assert.Equal(t, "test-span", event.Attributes["name"])
	assert.Equal(t, "span_attr_val", event.Attributes["span_attr"])

	// With the new implementation, these should now be supported
	// Check bytes attribute - should be JSON encoded
	bytesAttr, ok := event.Attributes["bytes_attr"].(string)
	assert.True(t, ok, "bytes_attr should be a string")
	assert.Equal(t, "\"AQIDBA==\"\n", bytesAttr) // base64 encoded with JSON encoding and newline

	// Check array attribute - should be JSON encoded
	arrayAttr, ok := event.Attributes["array_attr"].(string)
	assert.True(t, ok, "array_attr should be a string")
	assert.Equal(t, "[\"item1\",42,true]\n", arrayAttr)

	// Check kvlist attribute - should be flattened
	assert.Equal(t, "nested_value", event.Attributes["kvlist_attr.nested_string"])
	assert.Equal(t, int64(123), event.Attributes["kvlist_attr.nested_int"])
}
