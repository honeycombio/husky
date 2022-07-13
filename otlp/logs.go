package otlp

import (
	"encoding/hex"
	"io"
	"strings"
	"time"

	collectorLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// TranslateLogsRequestFromReader translates an OTLP log request into Honeycomb-friendly structure from a reader (eg HTTP body)
// RequestInfo is the parsed information from the gRPC metadata
func TranslateLogsRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateTraceRequestResult, error) {
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, err
	}
	request := &collectorLogs.ExportLogsServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentEncoding, request); err != nil {
		return nil, ErrFailedParseBody
	}
	return TranslateLogsRequest(request, ri)
}

// TranslateLogsRequest translates an OTLP proto log request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateLogsRequest(request *collectorLogs.ExportLogsServiceRequest, ri RequestInfo) (*TranslateTraceRequestResult, error) {
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, err
	}
	batches := []Batch{}
	isLegacy := isLegacy(ri.ApiKey)
	for _, resourceLog := range request.ResourceLogs {
		var events []Event
		resourceAttrs := make(map[string]interface{})

		if resourceLog.Resource != nil {
			addAttributesToMap(resourceAttrs, resourceLog.Resource.Attributes)
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

		for _, librarySpan := range resourceLog.InstrumentationLibraryLogs {
			library := librarySpan.InstrumentationLibrary

			for _, log := range librarySpan.GetLogs() {
				eventAttrs := map[string]interface{}{
					"severity":            getLogSeverity(log.SeverityNumber),
					"severity_code":       int(log.SeverityNumber),
					"meta.telemetry_type": "log",
					"flags":               log.Flags,
				}
				if len(log.TraceId) > 0 {
					eventAttrs["trace.trace_id"] = BytesToTraceID(log.TraceId)
				}
				if len(log.SpanId) > 0 {
					eventAttrs["trace.parent_id"] = hex.EncodeToString(log.SpanId)
				}
				if log.Name != "" {
					eventAttrs["name"] = log.Name
				}
				if log.SeverityText != "" {
					eventAttrs["severity_text"] = log.SeverityText
				}
				if log.Body != nil {
					if val := getValue(log.Body); val != nil {
						eventAttrs["body"] = val
					}
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
				if log.Attributes != nil {
					addAttributesToMap(eventAttrs, log.Attributes)
				}

				// Now we need to wrap the eventAttrs in an event so we can specify the timestamp
				// which is the StartTime as a time.Time object
				timestamp := time.Unix(0, int64(log.TimeUnixNano)).UTC()
				events = append(events, Event{
					Attributes: eventAttrs,
					Timestamp:  timestamp,
				})
			}
		}
		batches = append(batches, Batch{
			Dataset:   dataset,
			SizeBytes: proto.Size(resourceLog),
			Events:    events,
		})
	}
	return &TranslateTraceRequestResult{
		RequestSize: proto.Size(request),
		Batches:     batches,
	}, nil
}

func getLogSeverity(severity logs.SeverityNumber) string {
	switch severity {
	case logs.SeverityNumber_SEVERITY_NUMBER_TRACE, logs.SeverityNumber_SEVERITY_NUMBER_TRACE2, logs.SeverityNumber_SEVERITY_NUMBER_TRACE3, logs.SeverityNumber_SEVERITY_NUMBER_TRACE4:
		return "trace"
	case logs.SeverityNumber_SEVERITY_NUMBER_DEBUG, logs.SeverityNumber_SEVERITY_NUMBER_DEBUG2, logs.SeverityNumber_SEVERITY_NUMBER_DEBUG3, logs.SeverityNumber_SEVERITY_NUMBER_DEBUG4:
		return "debug"
	case logs.SeverityNumber_SEVERITY_NUMBER_INFO, logs.SeverityNumber_SEVERITY_NUMBER_INFO2, logs.SeverityNumber_SEVERITY_NUMBER_INFO3, logs.SeverityNumber_SEVERITY_NUMBER_INFO4:
		return "info"
	case logs.SeverityNumber_SEVERITY_NUMBER_WARN, logs.SeverityNumber_SEVERITY_NUMBER_WARN2, logs.SeverityNumber_SEVERITY_NUMBER_WARN3, logs.SeverityNumber_SEVERITY_NUMBER_WARN4:
		return "warn"
	case logs.SeverityNumber_SEVERITY_NUMBER_ERROR, logs.SeverityNumber_SEVERITY_NUMBER_ERROR2, logs.SeverityNumber_SEVERITY_NUMBER_ERROR3, logs.SeverityNumber_SEVERITY_NUMBER_ERROR4:
		return "error"
	case logs.SeverityNumber_SEVERITY_NUMBER_FATAL, logs.SeverityNumber_SEVERITY_NUMBER_FATAL2, logs.SeverityNumber_SEVERITY_NUMBER_FATAL3, logs.SeverityNumber_SEVERITY_NUMBER_FATAL4:
		return "fatal"
	case logs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED:
		fallthrough
	default:
		return "unspecified"
	}
}
