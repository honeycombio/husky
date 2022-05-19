package otlp

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	traceIDShortLength = 8
	traceIDLongLength  = 16
	defaultSampleRate  = int32(1)
	defaultServiceName = "unknown_service"
)

// TranslateTraceRequestResult represents an OTLP trace request translated into Honeycomb-friendly structure
// RequestSize is total byte size of the entire OTLP request
// Batches represent events grouped by their target dataset
type TranslateTraceRequestResult struct {
	RequestSize int
	Batches     []Batch
}

// Batch represents Honeycomb events grouped by their target dataset
// SizeBytes is the total byte size of the OTLP structure that represents this batch
type Batch struct {
	Dataset   string
	SizeBytes int
	Events    []Event
}

// Event represents a single Honeycomb event
type Event struct {
	Attributes map[string]interface{}
	Timestamp  time.Time
	SampleRate int32
}

// TranslateTraceRequestFromReader translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
func TranslateTraceRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateTraceRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}
	request, err := parseOTLPBody(body, ri.ContentEncoding)
	if err != nil {
		return nil, ErrFailedParseBody
	}
	return TranslateTraceRequest(request, ri)
}

// TranslateTraceRequest translates an OTLP/gRPC request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateTraceRequest(request *collectorTrace.ExportTraceServiceRequest, ri RequestInfo) (*TranslateTraceRequestResult, error) {
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

		for _, librarySpan := range resourceSpan.InstrumentationLibrarySpans {
			library := librarySpan.InstrumentationLibrary

			for _, span := range librarySpan.GetSpans() {
				traceID := BytesToTraceID(span.TraceId)
				spanID := hex.EncodeToString(span.SpanId)

				spanKind := getSpanKind(span.Kind)
				eventAttrs := map[string]interface{}{
					"trace.trace_id":  traceID,
					"trace.span_id":   spanID,
					"type":            spanKind,
					"span.kind":       spanKind,
					"name":            span.Name,
					"duration_ms":     float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"status_code":     evaluateSpanStatus(span.Status),
					"span.num_links":  len(span.Links),
					"span.num_events": len(span.Events),
				}
				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(span.ParentSpanId)
				}
				if evaluateSpanStatus(span.Status) == trace.Status_STATUS_CODE_ERROR {
					eventAttrs["error"] = true
				}
				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if library != nil {
					if len(library.Name) > 0 {
						eventAttrs["library.name"] = library.Name
					}
					if len(library.Version) > 0 {
						eventAttrs["library.version"] = library.Version
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
	return &TranslateTraceRequestResult{
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

// evaluateSpanStatus checks the value of both the deprecated code and code fields
// on the span status and using the rules specified in the backward compatibility
// notes in the protobuf definitions. See:
//
// https://github.com/open-telemetry/opentelemetry-proto/blob/59c488bfb8fb6d0458ad6425758b70259ff4a2bd/opentelemetry/proto/trace/v1/trace.proto#L230
func evaluateSpanStatus(status *trace.Status) trace.Status_StatusCode {
	if status == nil {
		return trace.Status_STATUS_CODE_UNSET
	}
	if status.Code == trace.Status_STATUS_CODE_UNSET {
		//lint:ignore SA1019 keep DepCode until we move to proto v0.12.0+
		if status.DeprecatedCode == trace.Status_DEPRECATED_STATUS_CODE_OK {
			return trace.Status_STATUS_CODE_UNSET
		}
		return trace.Status_STATUS_CODE_ERROR
	}
	return status.Code
}

func parseOTLPBody(body io.ReadCloser, contentEncoding string) (request *collectorTrace.ExportTraceServiceRequest, err error) {
	defer body.Close()
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}
	bodyReader := bytes.NewReader(bodyBytes)

	var reader io.Reader
	switch contentEncoding {
	case "gzip":
		gzipReader, err := gzip.NewReader(bodyReader)
		defer gzipReader.Close()
		if err != nil {
			return nil, err
		}
		reader = gzipReader
	case "zstd":
		zstdReader, err := zstd.NewReader(bodyReader)
		defer zstdReader.Close()
		if err != nil {
			return nil, err
		}
		reader = zstdReader
	default:
		reader = bodyReader
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	request = &collectorTrace.ExportTraceServiceRequest{}
	err = proto.Unmarshal(bytes, request)
	if err != nil {
		return nil, err
	}

	return request, nil
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
