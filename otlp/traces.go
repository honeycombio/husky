package otlp

import (
	"encoding/hex"
	"io"
	"math"
	"strconv"
	"time"

	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	traceIDShortLength = 8
	traceIDLongLength  = 16
	traceIDb64Length   = 24
	spanIDb64Length    = 12
	defaultSampleRate  = int32(1)
)

// TranslateTraceRequestFromReader translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
func TranslateTraceRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}
	request := &collectorTrace.ExportTraceServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentType, ri.ContentEncoding, request); err != nil {
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
	for _, resourceSpan := range request.ResourceSpans {
		var events []Event
		resourceAttrs := getResourceAttributes(resourceSpan.Resource)
		dataset := getDataset(ri, resourceAttrs)

		for _, scopeSpan := range resourceSpan.ScopeSpans {
			scopeAttrs := getScopeAttributes(scopeSpan.Scope)

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

				// copy resource & scope attributes then span attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}
				for k, v := range scopeAttrs {
					eventAttrs[k] = v
				}
				if span.Attributes != nil {
					AddAttributesToMap(eventAttrs, span.Attributes)
				}

				// get sample rate after resource and scope attributes have been added
				sampleRate := getSampleRate(eventAttrs)

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

					// copy resource & scope attributes then span event attributes
					for k, v := range resourceAttrs {
						attrs[k] = v
					}
					for k, v := range scopeAttrs {
						attrs[k] = v
					}

					if sevent.Attributes != nil {
						AddAttributesToMap(attrs, sevent.Attributes)
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
						"trace.link.span_id":   hex.EncodeToString(slink.SpanId),
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
						AddAttributesToMap(attrs, slink.Attributes)
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
