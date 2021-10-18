package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/zstd"
	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	traceIDShortLength = 8
	traceIDLongLength  = 16
)

type OTLPTranslator struct{}

func (t *OTLPTranslator) TranslateHttpTraceRequest(req *http.Request, zstdDecoders chan *zstd.Decoder) ([]map[string]interface{}, error) {
	contentType := req.Header.Get("content-type")
	if contentType != "application/protobuf" && contentType != "application/x-protobuf" {
		return nil, errors.New("invalid content-type")
	}

	request, cleanup, err := parseOTLPBody(req, zstdDecoders)
	defer cleanup()
	if err != nil {
		return nil, errors.New("parse error")
	}

	return t.TranslateGrpcTraceRequest(req.Context(), request)
}

func (t *OTLPTranslator) TranslateGrpcTraceRequest(ctx context.Context, request *collectorTrace.ExportTraceServiceRequest) ([]map[string]interface{}, error) {
	batch := []map[string]interface{}{}
	for _, resourceSpan := range request.ResourceSpans {
		resourceAttrs := make(map[string]interface{})

		if resourceSpan.Resource != nil {
			addAttributesToMap(resourceAttrs, resourceSpan.Resource.Attributes)
		}

		for _, librarySpan := range resourceSpan.InstrumentationLibrarySpans {
			library := librarySpan.InstrumentationLibrary
			if library != nil {
				if len(library.Name) > 0 {
					resourceAttrs["library.name"] = library.Name
				}
				if len(library.Version) > 0 {
					resourceAttrs["library.version"] = library.Version
				}
			}

			for _, span := range librarySpan.GetSpans() {
				traceID := bytesToTraceID(span.TraceId)
				spanID := hex.EncodeToString(span.SpanId)

				spanKind := getSpanKind(span.Kind)
				eventAttrs := map[string]interface{}{
					"trace.trace_id": traceID,
					"trace.span_id":  spanID,
					"type":           spanKind,
					"span.kind":      spanKind,
					"name":           span.Name,
					"duration_ms":    float64(span.EndTimeUnixNano-span.StartTimeUnixNano) / float64(time.Millisecond),
					"status_code":    getSpanStatusCode(span.Status),
				}
				if span.ParentSpanId != nil {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(span.ParentSpanId)
				}
				if getSpanStatusCode(span.Status) == trace.Status_STATUS_CODE_ERROR {
					eventAttrs["error"] = true
				}
				if span.Status != nil && len(span.Status.Message) > 0 {
					eventAttrs["status_message"] = span.Status.Message
				}
				if span.Attributes != nil {
					addAttributesToMap(eventAttrs, span.Attributes)
				}

				// copy resource attributes to event attributes
				for k, v := range resourceAttrs {
					eventAttrs[k] = v
				}

				// Now we need to wrap the eventAttrs in an event so we can specify the timestamp
				// which is the StartTime as a time.Time object
				timestamp := time.Unix(0, int64(span.StartTimeUnixNano)).UTC()
				batchEvent := map[string]interface{}{
					"time": timestamp,
					"data": eventAttrs,
				}
				batch = append(batch, batchEvent)

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
					batch = append(batch, map[string]interface{}{
						"time": timestamp,
						"data": attrs,
					})
				}

				for _, slink := range span.Links {
					var timestamp time.Time // links don't have a start time, use empty time
					attrs := map[string]interface{}{
						"trace.trace_id":       traceID,
						"trace.parent_id":      spanID,
						"trace.link.trace_id":  bytesToTraceID(slink.TraceId),
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
					batch = append(batch, map[string]interface{}{
						"time": timestamp,
						"data": attrs,
					})
				}
			}
		}
	}
	return batch, nil
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

// bytesToTraceID returns an ID suitable for use for spans and traces. Before
// encoding the bytes as a hex string, we want to handle cases where we are
// given 128-bit IDs with zero padding, e.g. 0000000000000000f798a1e7f33c8af6.
// There are many ways to achieve this, but careful benchmarking and testing
// showed the below as the most performant, avoiding memory allocations
// and the use of flexible but expensive library functions. As this is hot code,
// it seemed worthwhile to do it this way.
func bytesToTraceID(traceID []byte) string {
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

// getSpanStatusCode checks the value of both the deprecated code and code fields
// on the span status and using the rules specified in the backward compatibility
// notes in the protobuf definitions. See:
//
// https://github.com/open-telemetry/opentelemetry-proto/blob/59c488bfb8fb6d0458ad6425758b70259ff4a2bd/opentelemetry/proto/trace/v1/trace.proto#L230
func getSpanStatusCode(status *trace.Status) trace.Status_StatusCode {
	if status == nil {
		return trace.Status_STATUS_CODE_UNSET
	}
	if status.Code == trace.Status_STATUS_CODE_UNSET {
		if status.DeprecatedCode == trace.Status_DEPRECATED_STATUS_CODE_OK {
			return trace.Status_STATUS_CODE_UNSET
		}
		return trace.Status_STATUS_CODE_ERROR
	}
	return status.Code
}

func parseOTLPBody(r *http.Request, zstdDecoders chan *zstd.Decoder) (request *collectorTrace.ExportTraceServiceRequest, cleanup func(), err error) {
	cleanup = func() { /* empty cleanup */ }

	defer r.Body.Close()
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, cleanup, err
	}
	bodyReader := bytes.NewReader(bodyBytes)

	var reader io.Reader
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		var err error
		reader, err = gzip.NewReader(bodyReader)
		if err != nil {
			return nil, cleanup, err
		}
	case "zstd":
		zReader := <-zstdDecoders
		cleanup = func() {
			zReader.Reset(nil)
			zstdDecoders <- zReader
		}

		err = zReader.Reset(bodyReader)
		if err != nil {
			return nil, cleanup, err
		}

		reader = zReader
	default:
		reader = bodyReader
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, cleanup, err
	}

	request = &collectorTrace.ExportTraceServiceRequest{}
	err = proto.Unmarshal(bytes, request)
	if err != nil {
		return nil, cleanup, err
	}

	return request, cleanup, nil
}
