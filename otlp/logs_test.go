package otlp

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/honeycombio/husky/test"
	"github.com/stretchr/testify/assert"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func TestTranslateLogsRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()

	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}

	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "resource_attr",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "resource_attr_val"},
					},
				}, {
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			InstrumentationLibraryLogs: []*logs.InstrumentationLibraryLogs{{
				Logs: []*logs.LogRecord{{
					TraceId:        traceID,
					SpanId:         spanID,
					Name:           "test_log",
					TimeUnixNano:   uint64(startTimestamp.Nanosecond()),
					SeverityText:   "test_severity_text",
					SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_DEBUG,
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

	result, err := TranslateLogsRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "my-service", batch.Dataset)
	assert.Equal(t, proto.Size(req.ResourceLogs[0]), batch.SizeBytes)
	events := batch.Events
	assert.Equal(t, 1, len(events))

	ev := events[0]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
	assert.Equal(t, "test_log", ev.Attributes["name"])
	assert.Equal(t, "test_severity_text", ev.Attributes["severity_text"])
	assert.Equal(t, "debug", ev.Attributes["severity"])
	assert.Equal(t, "my-service", ev.Attributes["service.name"])
	assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
}

func TestTranslateClassicLogsRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()

	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "resource_attr",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "resource_attr_val"},
					},
				}, {
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			InstrumentationLibraryLogs: []*logs.InstrumentationLibraryLogs{{
				Logs: []*logs.LogRecord{{
					TraceId:        traceID,
					SpanId:         spanID,
					Name:           "test_log",
					TimeUnixNano:   uint64(startTimestamp.Nanosecond()),
					SeverityText:   "test_severity_text",
					SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_DEBUG,
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

	result, err := TranslateLogsRequest(req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "legacy-dataset", batch.Dataset)
	assert.Equal(t, proto.Size(req.ResourceLogs[0]), batch.SizeBytes)
	events := batch.Events
	assert.Equal(t, 1, len(events))

	ev := events[0]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
	assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.span_id"])
	assert.Equal(t, "test_log", ev.Attributes["name"])
	assert.Equal(t, "test_severity_text", ev.Attributes["severity_text"])
	assert.Equal(t, "debug", ev.Attributes["severity"])
	assert.Equal(t, "my-service", ev.Attributes["service.name"])
	assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
	assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
}

func TestCanDetectLogSeverity(t *testing.T) {
	testCases := []struct {
		name       string
		severities []logs.SeverityNumber
	}{
		{
			name:       "trace",
			severities: []logs.SeverityNumber{logs.SeverityNumber_SEVERITY_NUMBER_TRACE, logs.SeverityNumber_SEVERITY_NUMBER_TRACE2, logs.SeverityNumber_SEVERITY_NUMBER_TRACE3, logs.SeverityNumber_SEVERITY_NUMBER_TRACE4},
		},
		{
			name:       "debug",
			severities: []logs.SeverityNumber{logs.SeverityNumber_SEVERITY_NUMBER_DEBUG, logs.SeverityNumber_SEVERITY_NUMBER_DEBUG2, logs.SeverityNumber_SEVERITY_NUMBER_DEBUG3, logs.SeverityNumber_SEVERITY_NUMBER_DEBUG4},
		},
		{
			name:       "info",
			severities: []logs.SeverityNumber{logs.SeverityNumber_SEVERITY_NUMBER_INFO, logs.SeverityNumber_SEVERITY_NUMBER_INFO2, logs.SeverityNumber_SEVERITY_NUMBER_INFO3, logs.SeverityNumber_SEVERITY_NUMBER_INFO4},
		},
		{
			name:       "warn",
			severities: []logs.SeverityNumber{logs.SeverityNumber_SEVERITY_NUMBER_WARN, logs.SeverityNumber_SEVERITY_NUMBER_WARN2, logs.SeverityNumber_SEVERITY_NUMBER_WARN3, logs.SeverityNumber_SEVERITY_NUMBER_WARN4},
		},
		{
			name:       "fatal",
			severities: []logs.SeverityNumber{logs.SeverityNumber_SEVERITY_NUMBER_FATAL, logs.SeverityNumber_SEVERITY_NUMBER_FATAL2, logs.SeverityNumber_SEVERITY_NUMBER_FATAL3, logs.SeverityNumber_SEVERITY_NUMBER_FATAL4},
		},
		{
			name:       "unspecified",
			severities: []logs.SeverityNumber{logs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED, -100, 100}, // includes a couple of unknown values
		},
	}
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/protobuf",
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, severity := range tc.severities {
				req := &collectorlogs.ExportLogsServiceRequest{
					ResourceLogs: []*logs.ResourceLogs{{
						InstrumentationLibraryLogs: []*logs.InstrumentationLibraryLogs{{
							Logs: []*logs.LogRecord{{
								Name:           "test_log",
								SeverityNumber: logs.SeverityNumber(severity),
							}},
						}},
					}},
				}

				result, err := TranslateLogsRequest(req, ri)
				assert.NotNil(t, result)
				assert.Nil(t, err)
				assert.Equal(t, tc.name, result.Batches[0].Events[0].Attributes["severity"])
			}
		})
	}
}

func TestLogsRequestWithInvalidContentTypeReturnsError(t *testing.T) {
	req := &collectorlogs.ExportLogsServiceRequest{}
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/json",
	}

	result, err := TranslateLogsRequest(req, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrInvalidContentType, err)
}

func TestLogsRequestWithInvalidBodyReturnsError(t *testing.T) {
	bodyBytes := test.RandomBytes(10)
	body := io.NopCloser(bytes.NewReader(bodyBytes))
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/protobuf",
	}

	result, err := TranslateLogsRequestFromReader(body, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrFailedParseBody, err)
}
