package otlp

import (
	"encoding/hex"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	collectorTrace "github.com/honeycombio/husky/proto/otlp/collector/trace/v1"
	trace "github.com/honeycombio/husky/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	traceIDShortLength = 8
	traceIDLongLength  = 16
	defaultSampleRate  = int32(1)
	defaultServiceName = "unknown_service"
)

// TranslateTraceRequestFromReader translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
func TranslateTraceRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}
	request := &collectorTrace.ExportTraceServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentEncoding, request); err != nil {
		return nil, ErrFailedParseBody
	}
	return TranslateTraceRequest(request, ri)
}

// TranslateTraceRequest translates an OTLP/gRPC request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateTraceRequest(request *collectorTrace.ExportTraceServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}
	var batches []Batch
	isLegacy := isLegacy(ri.ApiKey)
	for _, resourceSpan := range request.ResourceSpans {
		var events []Event
		resourceAttrs := make(map[string]interface{})

		if resourceSpan.Resource != nil {
			addAttributesToMap(resourceAttrs, resourceSpan.Resource.Attributes)
		}

		var dataset string
		if isLegacy {
			dataset = ri.Dataset
		} else {
			serviceName, ok := resourceAttrs["service.name"].(string)
			if !ok ||
				strings.TrimSpace(serviceName) == "" ||
				strings.HasPrefix(serviceName, "unknown_service") {
				dataset = defaultServiceName
			} else {
				dataset = strings.TrimSpace(serviceName)
			}
		}

		for _, scopeSpan := range resourceSpan.ScopeSpans {
			scope := scopeSpan.Scope
			addScopeToMap(resourceAttrs, scope)

			for _, span := range scopeSpan.GetSpans() {
				traceID := BytesToTraceID(span.TraceId)
				spanID := hex.EncodeToString(span.SpanId)

				spanKind := getSpanKind(span.Kind)
				statusCode, isError := getSpanStatusCode(span.Status)

				eventAttrs := map[string]interface{}{
					"trace.trace_id":   traceID,
					"trace.span_id":    spanID,
					"type":             spanKind,
					"span.kind":        spanKind,
					"name":             span.Name,
					"duration_ms":      float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"status_code":      statusCode,
					"span.num_links":   len(span.Links),
					"span.num_events":  len(span.Events),
					"meta.signal_type": "trace",
				}
				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(span.ParentSpanId)
				}
				if isError {
					eventAttrs["error"] = true
				}
				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if scope != nil {
					if len(scope.Name) > 0 {
						eventAttrs["library.name"] = scope.Name
					}
					if len(scope.Version) > 0 {
						eventAttrs["library.version"] = scope.Version
					}
				}

				// copy resource attributes to event attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}

				// copy span attribures after resource attributes so span attributes write last and are preserved
				if span.Attributes != nil {
					addAttributesToMap(eventAttrs, span.Attributes)
				}

				// Now we need to wrap the eventAttrs in an event so we can specify the timestamp
				// which is the StartTime as a time.Time object
				timestamp := time.Unix(0, int64(span.StartTimeUnixNano)).UTC()
				events = append(events, Event{
					Attributes: eventAttrs,
					Timestamp:  timestamp,
					SampleRate: getSampleRate(eventAttrs),
				})

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

					if sevent.Attributes != nil {
						addAttributesToMap(attrs, sevent.Attributes)
					}
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					events = append(events, Event{
						Attributes: attrs,
						Timestamp:  timestamp,
					})
				}

				for _, slink := range span.Links {
					attrs := map[string]interface{}{
						"trace.trace_id":       traceID,
						"trace.parent_id":      spanID,
						"trace.link.trace_id":  BytesToTraceID(slink.TraceId),
						"trace.link.span_id":   hex.EncodeToString(slink.SpanId),
						"parent_name":          span.Name,
						"meta.annotation_type": "link",
						"meta.signal_type":     "trace",
					}

					if slink.Attributes != nil {
						addAttributesToMap(attrs, slink.Attributes)
					}
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					events = append(events, Event{
						Attributes: attrs,
						Timestamp:  timestamp, // use timestamp from parent span
					})
				}
			}
		}
		batches = append(batches, Batch{
			Dataset:   dataset,
			SizeBytes: proto.Size(resourceSpan),
			Events:    events,
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

// BytesToTraceID returns an ID suitable for use for spans and traces. Before
// encoding the bytes as a hex string, we want to handle cases where we are
// given 128-bit IDs with zero padding, e.g. 0000000000000000f798a1e7f33c8af6.
// There are many ways to achieve this, but careful benchmarking and testing
// showed the below as the most performant, avoiding memory allocations
// and the use of flexible but expensive library functions. As this is hot code,
// it seemed worthwhile to do it this way.
func BytesToTraceID(traceID []byte) string {
	var encoded []byte
	switch len(traceID) {
	case traceIDLongLength: // 16 bytes, trim leading 8 bytes if all 0's
		if shouldTrimTraceId(traceID) {
			encoded = make([]byte, 16)
			traceID = traceID[traceIDShortLength:]
		} else {
			encoded = make([]byte, 32)
		}
	case traceIDShortLength: // 8 bytes
		encoded = make([]byte, 16)
	default:
		encoded = make([]byte, len(traceID)*2)
	}
	hex.Encode(encoded, traceID)
	return string(encoded)
}

func shouldTrimTraceId(traceID []byte) bool {
	for i := 0; i < 8; i++ {
		if traceID[i] != 0 {
			return false
		}
	}
	return true
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

func getSampleRate(attrs map[string]interface{}) int32 {
	sampleRateKey := getSampleRateKey(attrs)
	if sampleRateKey == "" {
		return defaultSampleRate
	}

	sampleRate := defaultSampleRate
	sampleRateVal := attrs[sampleRateKey]
	switch v := sampleRateVal.(type) {
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			if i < math.MaxInt32 {
				sampleRate = int32(i)
			} else {
				sampleRate = math.MaxInt32
			}
		}
	case int32:
		sampleRate = v
	case int:
		if v < math.MaxInt32 {
			sampleRate = int32(v)
		} else {
			sampleRate = math.MaxInt32
		}
	case int64:
		if v < math.MaxInt32 {
			sampleRate = int32(v)
		} else {
			sampleRate = math.MaxInt32
		}
	}
	// To make sampleRate consistent between Otel and Honeycomb, we coerce all 0 values to 1 here
	// A value of 1 means the span was not sampled
	// For full explanation, see https://app.asana.com/0/365940753298424/1201973146987622/f
	if sampleRate == 0 {
		sampleRate = defaultSampleRate
	}
	delete(attrs, sampleRateKey) // remove attr
	return sampleRate
}

func getSampleRateKey(attrs map[string]interface{}) string {
	if _, ok := attrs["sampleRate"]; ok {
		return "sampleRate"
	}
	if _, ok := attrs["SampleRate"]; ok {
		return "SampleRate"
	}
	return ""
}
