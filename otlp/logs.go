package otlp

import (
	"encoding/hex"
	"io"
	"time"

	collectorLogs "github.com/maxedmands/opentelemetry-proto-go-compat/otlp/collector/logs/v1"
	logs "github.com/maxedmands/opentelemetry-proto-go-compat/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// TranslateLogsRequestFromReader translates an OTLP log request into Honeycomb-friendly structure from a reader (eg HTTP body)
// RequestInfo is the parsed information from the gRPC metadata
func TranslateLogsRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, err
	}
	request := &collectorLogs.ExportLogsServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentType, ri.ContentEncoding, request); err != nil {
		return nil, ErrFailedParseBody
	}
	return TranslateLogsRequest(request, ri)
}

// TranslateLogsRequest translates an OTLP proto log request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateLogsRequest(request *collectorLogs.ExportLogsServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateLogsHeaders(); err != nil {
		return nil, err
	}
	batches := []Batch{}
	for _, resourceLog := range request.ResourceLogs {
		var events []Event
		resourceAttrs := getResourceAttributes(resourceLog.Resource)
		dataset := getLogsDataset(ri, resourceAttrs)

		for _, scopeLog := range resourceLog.ScopeLogs {
			scopeAttrs := getScopeAttributes(scopeLog.Scope)

			for _, log := range scopeLog.GetLogRecords() {
				attrs := map[string]interface{}{
					"severity":         getLogSeverity(log.SeverityNumber),
					"severity_code":    int(log.SeverityNumber),
					"meta.signal_type": "log",
					"flags":            log.Flags,
				}
				if len(log.TraceId) > 0 {
					attrs["trace.trace_id"] = BytesToTraceID(log.TraceId)
					// only add meta.annotation_type if the log is associated to a trace
					attrs["meta.annotation_type"] = "span_event"
				}
				if len(log.SpanId) > 0 {
					attrs["trace.parent_id"] = hex.EncodeToString(log.SpanId)
				}
				if log.SeverityText != "" {
					attrs["severity_text"] = log.SeverityText
				}
				if log.Body != nil {
					if val, truncatedBytes := getValue(log.Body); val != nil {
						attrs["body"] = val
						if truncatedBytes != 0 {
							// if we trim the body, add telemetry about it
							attrs["meta.truncated_bytes"] = truncatedBytes
							attrs["meta.truncated_field"] = "body"
						}
					}
				}

				// copy resource & scope attributes then log attributes
				for k, v := range resourceAttrs {
					attrs[k] = v
				}
				for k, v := range scopeAttrs {
					attrs[k] = v
				}
				if log.Attributes != nil {
					addAttributesToMap(attrs, log.Attributes)
				}

				// Now we need to wrap the eventAttrs in an event so we can specify the timestamp
				// which is the StartTime as a time.Time object
				timestamp := time.Unix(0, int64(log.TimeUnixNano)).UTC()
				events = append(events, Event{
					Attributes: attrs,
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
	return &TranslateOTLPRequestResult{
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
