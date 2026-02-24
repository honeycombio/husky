package otlp

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/husky/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	testServiceName := "my-service"

	req := buildExportLogsServiceRequest(traceID, spanID, startTimestamp, testServiceName)

	testCases := []struct {
		Name            string
		ri              RequestInfo
		expectedDataset string
	}{
		{
			Name: "Classic",
			ri: RequestInfo{
				ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			},
			expectedDataset: testServiceName,
		},
		{
			Name: "E&S",
			ri: RequestInfo{
				ApiKey:      "abc123DEF456ghi789jklm",
				Dataset:     "legacy-dataset",
				ContentType: "application/protobuf",
			},
			expectedDataset: testServiceName,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Name, func(t *testing.T) {
			result, err := TranslateLogsRequest(context.Background(), req, tC.ri)
			assert.Nil(t, err)
			assert.Equal(t, proto.Size(req), result.RequestSize)
			assert.Equal(t, 1, len(result.Batches))
			batch := result.Batches[0]
			assert.Equal(t, tC.expectedDataset, batch.Dataset)
			events := batch.Events
			assert.Equal(t, 1, len(events))

			ev := events[0]
			assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
			assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
			assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
			assert.Equal(t, "log", ev.Attributes["meta.signal_type"])
			assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
			assert.Equal(t, uint32(0), ev.Attributes["flags"])
			assert.Equal(t, "test_severity_text", ev.Attributes["severity_text"])
			assert.Equal(t, "debug", ev.Attributes["severity"])
			assert.Equal(t, testServiceName, ev.Attributes["service.name"])
			assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
			assert.Equal(t, int32(100), ev.SampleRate)
			assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
			assert.Equal(t, "event_name", ev.Attributes["event.name"])
		})
	}
}

func TestTranslateHttpLogsRequest(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()

	testServiceName := "my-service"

	req := buildExportLogsServiceRequest(traceID, spanID, startTimestamp, testServiceName)

	testCases := []struct {
		Name            string
		ri              RequestInfo
		expectedDataset string
	}{
		{
			Name: "Classic",
			ri: RequestInfo{
				ApiKey:  "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				Dataset: "legacy-dataset",
			},
			expectedDataset: testServiceName,
		},
		{
			Name: "E&S",
			ri: RequestInfo{
				ApiKey:  "abc123DEF456ghi789jklm",
				Dataset: "legacy-dataset",
			},
			expectedDataset: testServiceName,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.Name, func(t *testing.T) {
			for _, testCaseContentType := range GetSupportedContentTypes() {
				t.Run(testCaseNameForContentType(testCaseContentType), func(t *testing.T) {
					for _, testCaseContentEncoding := range GetSupportedContentEncodings() {
						t.Run(testCaseNameForEncoding(testCaseContentEncoding), func(t *testing.T) {

							tC.ri.ContentType = testCaseContentType
							tC.ri.ContentEncoding = testCaseContentEncoding

							body, err := prepareOtlpRequestHttpBody(req, testCaseContentType, testCaseContentEncoding)
							require.NoError(t, err, "Womp womp. Ought to have been able to turn the OTLP log request into an HTTP body.")

							result, err := TranslateLogsRequestFromReader(context.Background(), io.NopCloser(strings.NewReader(body)), tC.ri)
							require.NoError(t, err)
							assert.Equal(t, proto.Size(req), result.RequestSize)
							assert.Equal(t, 1, len(result.Batches))
							batch := result.Batches[0]
							assert.Equal(t, tC.expectedDataset, batch.Dataset)
							events := batch.Events
							assert.Equal(t, 1, len(events))

							ev := events[0]
							assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
							assert.Equal(t, BytesToTraceID(traceID), ev.Attributes["trace.trace_id"])
							assert.Equal(t, hex.EncodeToString(spanID), ev.Attributes["trace.parent_id"])
							assert.Equal(t, "log", ev.Attributes["meta.signal_type"])
							assert.Equal(t, "span_event", ev.Attributes["meta.annotation_type"])
							assert.Equal(t, uint32(0), ev.Attributes["flags"])
							assert.Equal(t, "test_severity_text", ev.Attributes["severity_text"])
							assert.Equal(t, "debug", ev.Attributes["severity"])
							assert.Equal(t, "my-service", ev.Attributes["service.name"])
							assert.Equal(t, "span_attr_val", ev.Attributes["span_attr"])
							assert.Equal(t, int32(100), ev.SampleRate)
							assert.Equal(t, "resource_attr_val", ev.Attributes["resource_attr"])
							assert.Equal(t, "instr_scope_name", ev.Attributes["library.name"])
							assert.Equal(t, "instr_scope_version", ev.Attributes["library.version"])
							assert.Equal(t, "scope_attr_val", ev.Attributes["scope_attr"])
						})
					}
				})
			}
		})
	}
}

func TestLogs_DetermineDestinationDataset(t *testing.T) {
	traceID := test.RandomBytes(16)
	spanID := test.RandomBytes(8)
	startTimestamp := time.Now()

	for _, protocol := range []string{"GRPC", "HTTP"} {
		t.Run(protocol, func(t *testing.T) {
			environmentTypes := []struct {
				Name   string
				ApiKey string
			}{
				{
					Name:   "old-style/Classic",
					ApiKey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
				},
				{
					Name:   "old-style/E&S",
					ApiKey: "abc123DEF456ghi789jklm",
				},
				{
					Name:   "prefixed/Classic Ingest",
					ApiKey: "hkaic_12345678901234567890123456",
				},
				{
					Name:   "prefixed/E&S Ingest",
					ApiKey: "hkaik_12345678901234567890123456",
				},
			}

			for _, env := range environmentTypes {
				t.Run(env.Name, func(t *testing.T) {
					testCases := []struct {
						desc            string
						datasetHeader   string
						testServiceName string
						expectedDataset string
					}{
						{
							desc:            "when no service.name or dataset header are present, use our fallback",
							datasetHeader:   "",
							testServiceName: "",
							expectedDataset: "unknown_log_source",
						}, {
							desc:            "when service.name is OTel SDK default and no dataset header is present, use our fallback",
							datasetHeader:   "",
							testServiceName: "unknown_service:some_process_name",
							expectedDataset: "unknown_log_source",
						},
						{
							desc:            "when service.name is set to something non-default and there is no dataset header, use the service.name",
							datasetHeader:   "",
							testServiceName: "awesome_service",
							expectedDataset: "awesome_service",
						},
						{
							desc:            "when dataset header is set and there is no service.name, use the dataset header",
							datasetHeader:   "a_dataset_set_for_the_data",
							testServiceName: "",
							expectedDataset: "a_dataset_set_for_the_data",
						},
						{
							desc:            "when both dataset and service.name are set, use the service.name",
							datasetHeader:   "a_dataset_set_for_the_data",
							testServiceName: "awesome_service",
							expectedDataset: "awesome_service",
						},
						{
							desc:            "when dataset header is set and the service.name is OTel SDK default, use the dataset header",
							datasetHeader:   "a_dataset_set_for_the_data",
							testServiceName: "unknown_service:some_process_name",
							expectedDataset: "a_dataset_set_for_the_data",
						},
					}
					for _, tC := range testCases {
						t.Run(tC.desc, func(t *testing.T) {

							ri := RequestInfo{
								ApiKey:      env.ApiKey,
								Dataset:     tC.datasetHeader,
								ContentType: "application/protobuf",
							}

							req := buildExportLogsServiceRequest(traceID, spanID, startTimestamp, tC.testServiceName)

							var result *TranslateOTLPRequestResult
							var err error

							switch protocol {
							case "GRPC":
								result, err = TranslateLogsRequest(context.Background(), req, ri)
								require.NoError(t, err, "Wasn't able to translate that OTLP logs request.")
							case "HTTP":
								body, err := prepareOtlpRequestHttpBody(req, ri.ContentType, "")
								require.NoError(t, err, "Womp womp. Ought to have been able to turn the OTLP log request into an HTTP body.")

								result, err = TranslateLogsRequestFromReader(context.Background(), io.NopCloser(strings.NewReader(body)), ri)
								require.NoError(t, err, "Wasn't able to translate that OTLP logs request.")
							default:
								t.Errorf("lolwut - What kind of protocol is %v?", protocol)
							}

							batch := result.Batches[0]
							assert.Equal(t, tC.expectedDataset, batch.Dataset)
						})
					}
				})
			}
		})
	}
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
						ScopeLogs: []*logs.ScopeLogs{{
							LogRecords: []*logs.LogRecord{{
								SeverityNumber: logs.SeverityNumber(severity),
							}},
						}},
					}},
				}

				result, err := TranslateLogsRequest(context.Background(), req, ri)
				assert.NotNil(t, result)
				assert.Nil(t, err)
				assert.Equal(t, tc.name, result.Batches[0].Events[0].Attributes["severity"])
			}
		})
	}
}

func TestCanExtractBody(t *testing.T) {
	testCases := []struct {
		name          string
		body          *common.AnyValue
		expectedValue interface{}
	}{
		{
			name: "string",
			body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "string_body"}},
			expectedValue: map[string]interface{}{
				"body":             "string_body",
				"flags":            uint32(0),
				"meta.signal_type": "log",
				"severity":         "unspecified",
				"severity_code":    0,
			},
		},
		{
			name: "int",
			body: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 100}},
			expectedValue: map[string]interface{}{
				"body":             int64(100),
				"flags":            uint32(0),
				"meta.signal_type": "log",
				"severity":         "unspecified",
				"severity_code":    0,
			},
		},
		{
			name: "bool",
			body: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}},
			expectedValue: map[string]interface{}{
				"body":             true,
				"flags":            uint32(0),
				"meta.signal_type": "log",
				"severity":         "unspecified",
				"severity_code":    0,
			},
		},
		{
			name: "double",
			body: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 12.34}},
			expectedValue: map[string]interface{}{
				"body":             12.34,
				"flags":            uint32(0),
				"meta.signal_type": "log",
				"severity":         "unspecified",
				"severity_code":    0,
			},
		},
		{
			name: "array",
			body: &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{
				{Value: &common.AnyValue_StringValue{StringValue: "one"}},
				{Value: &common.AnyValue_IntValue{IntValue: 2}},
				{Value: &common.AnyValue_BoolValue{BoolValue: true}},
			},
			}}},
			expectedValue: map[string]interface{}{
				"body":             "[\"one\",2,true]\n",
				"flags":            uint32(0),
				"meta.signal_type": "log",
				"severity":         "unspecified",
				"severity_code":    0,
			},
		},
		{
			name: "kvlist",
			body: &common.AnyValue{Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{
				Values: []*common.KeyValue{
					{Key: "key1", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "value1"}}},
					{Key: "key2", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 2}}},
					{Key: "key3", Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}},
				},
			}}},
			expectedValue: map[string]interface{}{
				"body":             "{\"key1\":\"value1\",\"key2\":2,\"key3\":true}\n",
				"body.key1":        "value1",
				"body.key2":        int64(2),
				"body.key3":        true,
				"flags":            uint32(0),
				"meta.signal_type": "log",
				"severity":         "unspecified",
				"severity_code":    0,
			},
		},
	}
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/protobuf",
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &collectorlogs.ExportLogsServiceRequest{
				ResourceLogs: []*logs.ResourceLogs{{
					ScopeLogs: []*logs.ScopeLogs{{
						LogRecords: []*logs.LogRecord{{
							Body: tc.body,
						}},
					}},
				}},
			}

			result, err := TranslateLogsRequest(context.Background(), req, ri)
			assert.NotNil(t, result)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectedValue, result.Batches[0].Events[0].Attributes)
		})
	}
}

func TestTranslateLogsRequestFromReaderSized(t *testing.T) {
	startTimestamp := time.Now()
	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeLogs: []*logs.ScopeLogs{{
				LogRecords: []*logs.LogRecord{{
					TimeUnixNano:   uint64(startTimestamp.Nanosecond()),
					SeverityText:   "test_severity_text",
					SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_DEBUG,
				}},
			}},
		}},
	}

	bodyBytes, err := proto.Marshal(req)
	assert.Nil(t, err)

	bufValid := new(bytes.Buffer)
	bufValid.Write(bodyBytes)

	bodyValid := io.NopCloser(strings.NewReader(bufValid.String()))
	ri := RequestInfo{
		ApiKey:      "abc123DEF456ghi789jklm",
		Dataset:     "legacy-dataset",
		ContentType: "application/protobuf",
	}

	result, err := TranslateLogsRequestFromReaderSized(context.Background(), bodyValid, ri, int64(len(bodyBytes)*2))
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)

	bufTooLarge := new(bytes.Buffer)
	bufTooLarge.Write(bodyBytes)

	bodyTooLarge := io.NopCloser(strings.NewReader(bufTooLarge.String()))

	_, err = TranslateLogsRequestFromReaderSized(context.Background(), bodyTooLarge, ri, int64(len(bodyBytes)/10))
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, ErrFailedParseBody)
}

func TestLogsRequestWithInvalidContentTypeReturnsError(t *testing.T) {
	req := &collectorlogs.ExportLogsServiceRequest{}
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/binary",
	}

	result, err := TranslateLogsRequest(context.Background(), req, ri)
	assert.Nil(t, result)
	assert.Equal(t, ErrInvalidContentType, err)
}

func TestLogsRequestWithInvalidBodyReturnsError(t *testing.T) {
	bodyBytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	body := io.NopCloser(bytes.NewReader(bodyBytes))
	ri := RequestInfo{
		ApiKey:      "apikey",
		ContentType: "application/protobuf",
	}

	result, err := TranslateLogsRequestFromReader(context.Background(), body, ri)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFailedParseBody)
}

func TestLogsWithoutTraceIdDoesNotGetAnnotationType(t *testing.T) {
	startTimestamp := time.Now()

	ri := RequestInfo{
		ApiKey:      "a1a1a1a1a1a1a1a1a1a1a1",
		ContentType: "application/protobuf",
	}

	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key: "service.name",
					Value: &common.AnyValue{
						Value: &common.AnyValue_StringValue{StringValue: "my-service"},
					},
				}},
			},
			ScopeLogs: []*logs.ScopeLogs{{
				LogRecords: []*logs.LogRecord{{
					TimeUnixNano:   uint64(startTimestamp.Nanosecond()),
					SeverityText:   "test_severity_text",
					SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_DEBUG,
				}},
			}},
		}},
	}

	result, err := TranslateLogsRequest(context.Background(), req, ri)
	assert.Nil(t, err)
	assert.Equal(t, proto.Size(req), result.RequestSize)
	assert.Equal(t, 1, len(result.Batches))
	batch := result.Batches[0]
	assert.Equal(t, "my-service", batch.Dataset)
	events := batch.Events
	assert.Equal(t, 1, len(events))

	ev := events[0]
	assert.Equal(t, startTimestamp.Nanosecond(), ev.Timestamp.Nanosecond())
	assert.Equal(t, "log", ev.Attributes["meta.signal_type"])
	assert.Equal(t, uint32(0), ev.Attributes["flags"])
	assert.Equal(t, "test_severity_text", ev.Attributes["severity_text"])
	assert.Equal(t, "debug", ev.Attributes["severity"])
	assert.Equal(t, "my-service", ev.Attributes["service.name"])

	assert.Nil(t, ev.Attributes["trace.trace_id"])
	assert.Nil(t, ev.Attributes["trace.span_id"])
	assert.Nil(t, ev.Attributes["meta.annotation_type"])
}

// Build an OTel Logs request. Provide a valid OTel traceID and spanID, a time for the log entry, and a service name.
func buildExportLogsServiceRequest(traceID []byte, spanID []byte, startTimestamp time.Time, testServiceName string) *collectorlogs.ExportLogsServiceRequest {
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
						Value: &common.AnyValue_StringValue{StringValue: testServiceName},
					},
				}},
			},
			ScopeLogs: []*logs.ScopeLogs{{
				Scope: &common.InstrumentationScope{
					Name:    "instr_scope_name",
					Version: "instr_scope_version",
					Attributes: []*common.KeyValue{
						{
							Key: "scope_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "scope_attr_val"},
							},
						},
					},
				},
				LogRecords: []*logs.LogRecord{{
					TraceId:        traceID,
					SpanId:         spanID,
					TimeUnixNano:   uint64(startTimestamp.Nanosecond()),
					SeverityText:   "test_severity_text",
					SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_DEBUG,
					EventName:      "event_name",
					Attributes: []*common.KeyValue{
						{
							Key: "span_attr",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "span_attr_val"},
							},
						},
						{
							Key: "sampleRate",
							Value: &common.AnyValue{
								Value: &common.AnyValue_IntValue{IntValue: 100},
							},
						},
					},
				}},
			}},
		}},
	}

	return req
}
