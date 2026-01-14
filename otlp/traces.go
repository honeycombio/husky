package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	traceIDShortLength = 8
	traceIDLongLength  = 16
	traceIDb64Length   = 24
	spanIDb64Length    = 12
)

// TranslateTraceRequestFromReader translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
func TranslateTraceRequestFromReader(ctx context.Context, body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	return TranslateTraceRequestFromReaderSized(ctx, body, ri, defaultMaxRequestBodySize)
}

// TranslateTraceRequestFromReaderSized translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
// maxSize is the maximum size of the request body in bytes
func TranslateTraceRequestFromReaderSized(ctx context.Context, body io.ReadCloser, ri RequestInfo, maxSize int64) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}
	request := &collectorTrace.ExportTraceServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentType, ri.ContentEncoding, request, maxSize); err != nil {
		return nil, ErrFailedParseBody
	}
	return TranslateTraceRequest(ctx, request, ri)
}

var httpBodyBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var zstdDecoderPool = sync.Pool{
	New: func() any {
		reader, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		return reader
	},
}

// TranslateTraceRequestFromReaderSizedWithMsgp translates an OTLP/HTTP request
// directly into our Honeycomb-friendly structure, avoiding intermediate proto
// structs.
// Note the returned data is shiny new heap memory and is safe for callers to keep.
func TranslateTraceRequestFromReaderSizedWithMsgp(
	ctx context.Context,
	body io.ReadCloser,
	ri RequestInfo,
	maxSize int64,
) (*TranslateOTLPRequestResultMsgp, error) {
	defer body.Close()

	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}

	// Check content type
	switch ri.ContentType {
	case "application/protobuf", "application/x-protobuf", "application/json":
		// supported
	default:
		return nil, ErrInvalidContentType
	}

	reader := io.LimitReader(body, maxSize)

	// Handle decompression
	switch ri.ContentEncoding {
	case "gzip":
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, ErrFailedParseBody
		}
		defer gzipReader.Close()
		reader = gzipReader
	case "zstd":
		// zstd decoders are extremely expensive to set up (big internal buffers),
		// so use a pool for them.
		zstdReader := zstdDecoderPool.Get().(*zstd.Decoder)
		defer func() {
			zstdReader.Reset(nil)
			zstdDecoderPool.Put(zstdReader)
		}()

		err := zstdReader.Reset(reader)
		if err != nil {
			return nil, ErrFailedParseBody
		}

		reader = zstdReader
	case "":
		// cool
	default:
		return nil, ErrFailedParseBody
	}

	bodyBuffer := httpBodyBufferPool.Get().(*bytes.Buffer)
	defer func() {
		bodyBuffer.Reset()
		httpBodyBufferPool.Put(bodyBuffer)
	}()

	_, err := bodyBuffer.ReadFrom(reader)
	if err != nil {
		return nil, ErrFailedParseBody
	}

	// Unmarshal based on content type
	switch ri.ContentType {
	case "application/json":
		return unmarshalTraceRequestDirectMsgpJSON(ctx, bodyBuffer.Bytes(), ri)
	default:
		// protobuf
		return UnmarshalTraceRequestDirectMsgp(ctx, bodyBuffer.Bytes(), ri)
	}
}

// TranslateTraceRequest translates an OTLP/gRPC request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateTraceRequest(ctx context.Context, request *collectorTrace.ExportTraceServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}
	var batches []Batch
	for _, resourceSpan := range request.ResourceSpans {
		var events []Event
		resourceAttrs := getResourceAttributes(ctx, resourceSpan.Resource)
		dataset := getDataset(ri, resourceAttrs)

		for _, scopeSpan := range resourceSpan.ScopeSpans {
			scopeAttrs := getScopeAttributes(ctx, scopeSpan.Scope)

			for _, span := range scopeSpan.GetSpans() {
				traceID := BytesToTraceID(span.TraceId)
				spanID := BytesToSpanID(span.SpanId)

				spanKind := getSpanKind(span.Kind)
				statusCode, isError := getSpanStatusCode(span.Status)

				eventAttrs := map[string]interface{}{
					"trace.trace_id":   traceID,
					"trace.span_id":    spanID,
					"type":             spanKind,
					"span.kind":        spanKind,
					"name":             span.Name,
					"status_code":      statusCode,
					"span.num_links":   len(span.Links),
					"span.num_events":  len(span.Events),
					"meta.signal_type": "trace",
				}
				duration := float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond)
				if duration < 0 {
					duration = 0
					eventAttrs["meta.invalid_duration"] = true
				}
				eventAttrs["duration_ms"] = duration

				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = BytesToSpanID(span.ParentSpanId)
				}
				if isError {
					eventAttrs["error"] = true
				}
				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if span.TraceState != "" {
					eventAttrs["trace.trace_state"] = span.TraceState
				}

				// copy resource & scope attributes then span attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}
				for k, v := range scopeAttrs {
					eventAttrs[k] = v
				}
				if span.Attributes != nil {
					AddAttributesToMap(ctx, eventAttrs, span.Attributes)
				}

				// get sample rate after resource and scope attributes have been added
				sampleRate := getSampleRate(eventAttrs, span.TraceState)

				// Now we need to wrap the eventAttrs in an event so we can specify the timestamp
				// which is the StartTime as a time.Time object
				timestamp := time.Unix(0, int64(span.StartTimeUnixNano)).UTC()

				for _, sevent := range span.Events {
					timestamp := time.Unix(0, int64(sevent.TimeUnixNano)).UTC()
					attrs := map[string]interface{}{
						"trace.trace_id":       traceID,
						"trace.parent_id":      spanID,
						"name":                 sevent.Name,
						"parent_name":          span.Name,
						"meta.annotation_type": "span_event",
						"meta.signal_type":     "trace",
					}
					// calculate time since span start for querying capabilities
					time_since_span_start := float64(sevent.TimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond)
					if time_since_span_start < 0 {
						time_since_span_start = 0
						attrs["meta.invalid_time_since_span_start"] = true
					}
					attrs["meta.time_since_span_start_ms"] = time_since_span_start
					// copy resource & scope attributes then span event attributes
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					for k, v := range scopeAttrs {
						attrs[k] = v
					}

					if sevent.Attributes != nil {
						AddAttributesToMap(ctx, attrs, sevent.Attributes)
					}
					if isError {
						attrs["error"] = true
					}

					// For span events that are following the "exception" semantic convention,
					// we're going to copy their attributes to the parent span because:
					// 1. They are common and high-value for error investigations
					// 2. It sucks to have to look at span events in our trace UI today to hunt these down on an error span
					// 3. This makes bubble up better because you can see these error details without having to query the span events
					// If there is more than one exception event, only the first one we encounter will be copied.
					if sevent.Name == "exception" {
						for _, seventAttr := range sevent.Attributes {
							switch seventAttr.Key {
							case "exception.message", "exception.type", "exception.stacktrace":
								// don't overwrite if the value is already on the span
								if _, present := eventAttrs[seventAttr.Key]; !present {
									eventAttrs[seventAttr.Key] = seventAttr.Value.GetStringValue()
								}
							case "exception.escaped":
								// don't overwrite if the value is already on the span
								if _, present := eventAttrs[seventAttr.Key]; !present {
									eventAttrs[seventAttr.Key] = seventAttr.Value.GetBoolValue()
								}
							}
						}
					}

					events = append(events, Event{
						Attributes: attrs,
						Timestamp:  timestamp,
						SampleRate: sampleRate,
					})
				}

				for _, slink := range span.Links {
					attrs := map[string]interface{}{
						"trace.trace_id":       traceID,
						"trace.parent_id":      spanID,
						"trace.link.trace_id":  BytesToTraceID(slink.TraceId),
						"trace.link.span_id":   BytesToSpanID(slink.SpanId),
						"parent_name":          span.Name,
						"meta.annotation_type": "link",
						"meta.signal_type":     "trace",
					}

					// copy resource & scope attributes then span link attributes
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					for k, v := range scopeAttrs {
						attrs[k] = v
					}

					if slink.Attributes != nil {
						AddAttributesToMap(ctx, attrs, slink.Attributes)
					}
					if isError {
						attrs["error"] = true
					}

					events = append(events, Event{
						Attributes: attrs,
						Timestamp:  timestamp, // use timestamp from parent span
						SampleRate: sampleRate,
					})
				}

				events = append(events, Event{
					Attributes: eventAttrs,
					Timestamp:  timestamp,
					SampleRate: sampleRate,
				})
			}
		}
		batches = append(batches, Batch{
			Dataset: dataset,
			Events:  events,
		})
	}
	return &TranslateOTLPRequestResult{
		RequestSize: proto.Size(request),
		Batches:     batches,
	}, nil
}

func getSpanKind(kind trace.Span_SpanKind) string {
	switch kind {
	case trace.Span_SPAN_KIND_CLIENT:
		return "client"
	case trace.Span_SPAN_KIND_SERVER:
		return "server"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "producer"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "consumer"
	case trace.Span_SPAN_KIND_INTERNAL:
		return "internal"
	case trace.Span_SPAN_KIND_UNSPECIFIED:
		fallthrough
	default:
		return "unspecified"
	}
}

// getSpanStatusCode returns the integer value of the span's status code and
// a bool for whether to consider the status an error.
//
// The type conversion from proto enum value to an integer is done here because
// the events we produce from OTLP spans have no knowledge of or interest in
// the OTLP types generated from enums in the proto definitions.
func getSpanStatusCode(status *trace.Status) (int, bool) {
	if status == nil {
		return int(trace.Status_STATUS_CODE_UNSET), false
	}
	return int(status.Code), status.Code == trace.Status_STATUS_CODE_ERROR
}
