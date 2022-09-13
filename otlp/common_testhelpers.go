package otlp

import (
	"bytes"
	"compress/gzip"
	"errors"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	common "github.com/honeycombio/husky/proto/otlp/common/v1"
	resource "github.com/honeycombio/husky/proto/otlp/resource/v1"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"

	collectorlogs "github.com/honeycombio/husky/proto/otlp/collector/logs/v1"
	logs "github.com/honeycombio/husky/proto/otlp/logs/v1"
)

// The collector<signal>.Export<signal>ServiceRequest structs generated from proto
// definitions each implment the interfaces embedded here. This combined interface
// allows for a single transformer function to receive a request struct
// and return a request serialized in a variety of formats.
type marshalableOtlpRequest interface {
	protoiface.MessageV1      // to protobuf
	protoreflect.ProtoMessage // to protojson
}

// transforms an OTLP signal request proto struct into a supported Content-Type
// and then encoded appropriately for an HTTP request
func prepareOtlpRequestHttpBody(req marshalableOtlpRequest, contentType string, encoding string) (string, error) {
	var bodyBytes []byte
	var err error
	switch contentType {
	case "application/protobuf", "application/x-protobuf":
		bodyBytes, err = proto.Marshal(req)
		if err != nil {
			return "", err
		}
	case "application/json":
		bodyBytes, err = protojson.Marshal(req)
		if err != nil {
			return "", err
		}
	default:
		return "", errors.New("Unknown content-type '" + contentType + "' given for test case. This probably won't go well.")
	}

	body, err := encodeBody(bodyBytes, encoding)
	return body, err
}

// Encode a slice of bytes destined to be the body of an HTTP request
// to a target encoding.
func encodeBody(body []byte, encoding string) (string, error) {
	encodedBytes := new(bytes.Buffer)
	switch encoding {
	case "":
		encodedBytes.Write(body)
	case "gzip":
		w := gzip.NewWriter(encodedBytes)
		w.Write(body)
		w.Close()
	case "zstd":
		w, _ := zstd.NewWriter(encodedBytes)
		w.Write(body)
		w.Close()
	default:
		return "", errors.New("Unknown content-encoding '" + encoding + "' given for test case. This probably won't go well.")
	}
	return encodedBytes.String(), nil
}

// Removes "application/" prefix from Content Type values to reduce the
// nesting of test case name in verbose test output and results.
// e.g. "application/x-protobuf" -> "x-protobuf"
func testCaseNameForContentType(contentType string) string {
	return strings.Split(contentType, "/")[1]
}

// Return a friendlier string for the test cases where Content Encoding
// is ambiguous, e.g. no encoding given is blank, so we give it a
// meaningful name here.
func testCaseNameForEncoding(encoding string) string {
	if encoding == "" {
		return "no encoding given assume uncompressed"
	} else {
		return encoding
	}
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

	return req
}
