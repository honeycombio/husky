package otlp

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	v1logs "go.opentelemetry.io/proto/otlp/logs/v1"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestParseGrpcMetadataIntoRequestInfo(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		apiKeyHeader:    "test-api-key",
		datasetHeader:   "test-dataset",
		userAgentHeader: "test-user-agent",
	}))
	ri := GetRequestInfoFromGrpcMetadata(ctx)

	assert.Equal(t, "test-api-key", ri.ApiKey)
	assert.Equal(t, "test-dataset", ri.Dataset)
	assert.Equal(t, "test-user-agent", ri.UserAgent)
	assert.Equal(t, "application/protobuf", ri.ContentType)
}

func TestParseHttpHeadersIntoRequestInfo(t *testing.T) {
	header := http.Header{}
	header.Set(apiKeyHeader, "test-api-key")
	header.Set(datasetHeader, "test-dataset")
	header.Set(userAgentHeader, "test-user-agent")
	header.Set(contentTypeHeader, "test-content-type")

	ri := GetRequestInfoFromHttpHeaders(header)
	assert.Equal(t, "test-api-key", ri.ApiKey)
	assert.Equal(t, "test-dataset", ri.Dataset)
	assert.Equal(t, "test-user-agent", ri.UserAgent)
	assert.Equal(t, "test-content-type", ri.ContentType)
}

func TestAddAttributesToMap(t *testing.T) {
	testCases := []struct {
		expected  interface{}
		attribute *common.KeyValue
	}{
		{
			expected: map[string]interface{}{"str-attr": "str-value"},
			attribute: &common.KeyValue{
				Key: "str-attr", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "str-value"}},
			},
		},
		{
			expected: map[string]interface{}{"int-attr": int64(123)},
			attribute: &common.KeyValue{
				Key: "int-attr", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 123}},
			},
		},
		{
			expected: map[string]interface{}{"double-attr": float64(12.3)},
			attribute: &common.KeyValue{
				Key: "double-attr", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 12.3}},
			},
		},
		{
			expected: map[string]interface{}{"bool-attr": true},
			attribute: &common.KeyValue{
				Key: "bool-attr", Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}},
			},
		},
		{
			expected: map[string]interface{}{},
			attribute: &common.KeyValue{
				Key: "", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "str-value"}},
			},
		},
		{
			expected: map[string]interface{}{
				"array-attr": "[\"one\",true,3]\n",
			},
			attribute: &common.KeyValue{
				Key: "array-attr", Value: &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{
					Values: []*common.AnyValue{
						{Value: &common.AnyValue_StringValue{StringValue: "one"}},
						{Value: &common.AnyValue_BoolValue{BoolValue: true}},
						{Value: &common.AnyValue_IntValue{IntValue: 3}},
					}}}},
			},
		},
		// Testing single-layer maps is valid but may fail due to map iteration order differences, and
		// that functionality is more completely tested by Test_getValue(). The case of a nested map will fail
		// badly in the way this test is structured, so we don't do maps at all here.
		{
			expected:  map[string]interface{}{},
			attribute: &common.KeyValue{Key: "kv-attr", Value: nil},
		},
	}

	for _, tc := range testCases {
		attrs := map[string]interface{}{}
		AddAttributesToMap(context.Background(), attrs, []*common.KeyValue{tc.attribute})
		assert.Equal(t, tc.expected, attrs)
	}
}

func TestValidateTracesHeaders(t *testing.T) {
	testCases := []struct {
		name        string
		apikey      string
		dataset     string
		contentType string
		err         error
	}{
		{name: "no key, no dataset", apikey: "", dataset: "", contentType: "application/protobuf", err: ErrMissingAPIKeyHeader},
		{name: "no key, dataset present", apikey: "", dataset: "dataset", contentType: "application/protobuf", err: ErrMissingAPIKeyHeader},
		{name: "classic/no dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "application/protobuf", err: ErrMissingDatasetHeader},
		{name: "classic ingest key/no dataset", apikey: "hcxic_1234567890123456789012345678901234567890123456789012345678", dataset: "", contentType: "application/protobuf", err: ErrMissingDatasetHeader},
		{name: "classic/dataset present", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "classic ingest key/dataset present", apikey: "hcxic_1234567890123456789012345678901234567890123456789012345678", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S/no dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S ingest key/no dataset", apikey: "hcxik_1234567890123456789012345678901234567890123456789012345678", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S/dataset present", apikey: "abc123DEF456ghi789jklm", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S ingest key/dataset present", apikey: "hcxik_1234567890123456789012345678901234567890123456789012345678", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "content-type/(missing)", apikey: "apikey", dataset: "dataset", contentType: "", err: ErrInvalidContentType},
		{name: "content-type/javascript", apikey: "apikey", dataset: "dataset", contentType: "application/javascript", err: ErrInvalidContentType},
		{name: "content-type/xml", apikey: "apikey", dataset: "dataset", contentType: "application/xml", err: ErrInvalidContentType},
		{name: "content-type/octet-stream", apikey: "apikey", dataset: "dataset", contentType: "application/octet-stream", err: ErrInvalidContentType},
		{name: "content-type/text-plain", apikey: "apikey", dataset: "dataset", contentType: "text-plain", err: ErrInvalidContentType},
		{name: "content-type/json", apikey: "apikey", dataset: "dataset", contentType: "application/json", err: nil},
		{name: "content-type/protobuf", apikey: "apikey", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "content-type/x-protobuf", apikey: "apikey", dataset: "dataset", contentType: "application/x-protobuf", err: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ri := RequestInfo{ApiKey: tc.apikey, ContentType: tc.contentType, Dataset: tc.dataset}
			err := ri.ValidateTracesHeaders()
			if tc.err != nil {
				assert.EqualError(t, tc.err, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateMetricsHeaders(t *testing.T) {
	testCases := []struct {
		name        string
		apikey      string
		dataset     string
		contentType string
		err         error
	}{
		{name: "no key, no dataset", apikey: "", dataset: "", contentType: "application/protobuf", err: ErrMissingAPIKeyHeader},
		{name: "no key, dataset present", apikey: "", dataset: "dataset", contentType: "application/protobuf", err: ErrMissingAPIKeyHeader},
		// classic environments need to tell us which dataset to put metrics in
		{name: "classic/no dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "application/protobuf", err: ErrMissingDatasetHeader},
		{name: "classic ingest key/no dataset", apikey: "hcxic_1234567890123456789012345678901234567890123456789012345678", dataset: "", contentType: "application/protobuf", err: ErrMissingDatasetHeader},
		{name: "classic/dataset present", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "classic ingest key/dataset present", apikey: "hcxic_1234567890123456789012345678901234567890123456789012345678", dataset: "dataset", contentType: "application/protobuf", err: nil},
		// dataset header not required for E&S, there's a fallback
		{name: "E&S/no dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S ingest key/no dataset", apikey: "hcxik_1234567890123456789012345678901234567890123456789012345678", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S/dataset present", apikey: "abc123DEF456ghi789jklm", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S ingest key/dataset present", apikey: "hcxik_1234567890123456789012345678901234567890123456789012345678", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "content-type/(missing)", apikey: "apikey", dataset: "dataset", contentType: "", err: ErrInvalidContentType},
		{name: "content-type/javascript", apikey: "apikey", dataset: "dataset", contentType: "application/javascript", err: ErrInvalidContentType},
		{name: "content-type/xml", apikey: "apikey", dataset: "dataset", contentType: "application/xml", err: ErrInvalidContentType},
		{name: "content-type/octet-stream", apikey: "apikey", dataset: "dataset", contentType: "application/octet-stream", err: ErrInvalidContentType},
		{name: "content-type/text-plain", apikey: "apikey", dataset: "dataset", contentType: "text-plain", err: ErrInvalidContentType},
		{name: "content-type/json", apikey: "apikey", dataset: "dataset", contentType: "application/json", err: nil},
		{name: "content-type/protobuf", apikey: "apikey", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "content-type/x-protobuf", apikey: "apikey", dataset: "dataset", contentType: "application/x-protobuf", err: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ri := RequestInfo{ApiKey: tc.apikey, ContentType: tc.contentType, Dataset: tc.dataset}
			err := ri.ValidateMetricsHeaders()
			if tc.err != nil {
				assert.EqualError(t, err, tc.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateLogsHeaders(t *testing.T) {
	testCases := []struct {
		name        string
		apikey      string
		dataset     string
		contentType string
		err         error
	}{
		{name: "no key, no dataset", apikey: "", dataset: "", contentType: "application/protobuf", err: ErrMissingAPIKeyHeader},
		{name: "no key, dataset present", apikey: "", dataset: "dataset", contentType: "application/protobuf", err: ErrMissingAPIKeyHeader},
		// logs will use dataset header if present, but log ingest will also use service.name in the data
		// and we will have a sensible default if neither are present, so a missing dataset header is not an error here
		{name: "classic/no dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "classic ingest key/no dataset", apikey: "hcxic_1234567890123456789012345678901234567890123456789012345678", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "classic/dataset present", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "classic ingest key/dataset present", apikey: "hcxic_1234567890123456789012345678901234567890123456789012345678", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S/no dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S ingest key/no dataset", apikey: "hcxik_1234567890123456789012345678901234567890123456789012345678", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S/dataset present", apikey: "abc123DEF456ghi789jklm", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S ingest key/dataset present", apikey: "hcxik_1234567890123456789012345678901234567890123456789012345678", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "content-type/(missing)", apikey: "apikey", dataset: "dataset", contentType: "", err: ErrInvalidContentType},
		{name: "content-type/javascript", apikey: "apikey", dataset: "dataset", contentType: "application/javascript", err: ErrInvalidContentType},
		{name: "content-type/xml", apikey: "apikey", dataset: "dataset", contentType: "application/xml", err: ErrInvalidContentType},
		{name: "content-type/octet-stream", apikey: "apikey", dataset: "dataset", contentType: "application/octet-stream", err: ErrInvalidContentType},
		{name: "content-type/text-plain", apikey: "apikey", dataset: "dataset", contentType: "text-plain", err: ErrInvalidContentType},
		{name: "content-type/json", apikey: "apikey", dataset: "dataset", contentType: "application/json", err: nil},
		{name: "content-type/protobuf", apikey: "apikey", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "content-type/x-protobuf", apikey: "apikey", dataset: "dataset", contentType: "application/x-protobuf", err: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ri := RequestInfo{ApiKey: tc.apikey, ContentType: tc.contentType, Dataset: tc.dataset}
			err := ri.ValidateLogsHeaders()
			if tc.err != nil {
				assert.EqualError(t, err, tc.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetRequestInfoFromGrpcMetadataIsCaseInsensitive(t *testing.T) {
	const (
		apiKeyValue     = "test-apikey"
		datasetValue    = "test-dataset"
		proxyTokenValue = "test-token"
	)

	tests := []struct {
		name             string
		apikeyHeader     string
		datasetHeader    string
		proxyTokenHeader string
	}{
		{
			name:             "lowercase",
			apikeyHeader:     "x-honeycomb-team",
			datasetHeader:    "x-honeycomb-dataset",
			proxyTokenHeader: "x-honeycomb-proxy-token",
		},
		{
			name:             "uppercase",
			apikeyHeader:     "X-HONEYCOMB-TEAM",
			datasetHeader:    "X-HONEYCOMB-DATASET",
			proxyTokenHeader: "X-HONEYCOMB-PROXY-TOKEN",
		},
		{
			name:             "mixed-case",
			apikeyHeader:     "x-HoNeYcOmB-tEaM",
			datasetHeader:    "X-hOnEyCoMb-DaTaSeT",
			proxyTokenHeader: "X-hOnEyCoMb-PrOxY-tOKeN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := metadata.MD{}
			md.Set(tt.apikeyHeader, apiKeyValue)
			md.Set(tt.datasetHeader, datasetValue)

			ctx := metadata.NewIncomingContext(context.Background(), md)
			ri := GetRequestInfoFromGrpcMetadata(ctx)
			assert.Equal(t, apiKeyValue, ri.ApiKey)
			assert.Equal(t, datasetValue, ri.Dataset)
		})
	}
}

func TestGetRequestInfoFromHttpHeadersIsCaseInsensitive(t *testing.T) {
	const (
		apiKeyValue     = "test-apikey"
		datasetValue    = "test-dataset"
		proxyTokenValue = "test-token"
	)

	tests := []struct {
		name             string
		apikeyHeader     string
		datasetHeader    string
		proxyTokenHeader string
	}{
		{
			name:             "lowercase",
			apikeyHeader:     "x-honeycomb-team",
			datasetHeader:    "x-honeycomb-dataset",
			proxyTokenHeader: "x-honeycomb-proxy-token",
		},
		{
			name:             "uppercase",
			apikeyHeader:     "X-HONEYCOMB-TEAM",
			datasetHeader:    "X-HONEYCOMB-DATASET",
			proxyTokenHeader: "X-HONEYCOMB-PROXY-TOKEN",
		},
		{
			name:             "mixed-case",
			apikeyHeader:     "x-HoNeYcOmB-tEaM",
			datasetHeader:    "X-hOnEyCoMb-DaTaSeT",
			proxyTokenHeader: "X-hOnEyCoMb-PrOxY-tOKeN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			header.Set(apiKeyHeader, apiKeyValue)
			header.Set(datasetHeader, datasetValue)

			ri := GetRequestInfoFromHttpHeaders(header)
			assert.Equal(t, apiKeyValue, ri.ApiKey)
			assert.Equal(t, datasetValue, ri.Dataset)
		})
	}
}

func Test_getValue(t *testing.T) {
	tests := []struct {
		name  string
		value *common.AnyValue
		want  interface{}
	}{
		{"string", &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "foo"}}, map[string]interface{}{"body": "foo"}},
		{"int64", &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 123}}, map[string]interface{}{"body": int64(123)}},
		{"bool", &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}, map[string]interface{}{"body": true}},
		{"float64", &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 123}}, map[string]interface{}{"body": float64(123)}},
		{"bytes as b64", &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: []byte{10, 20, 30}}}, map[string]interface{}{"body": `"ChQe"` + "\n"}},
		{"array as mixed-type string", &common.AnyValue{Value: &common.AnyValue_ArrayValue{
			ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{
				{Value: &common.AnyValue_IntValue{IntValue: 123}},
				{Value: &common.AnyValue_DoubleValue{DoubleValue: 45.6}},
				{Value: &common.AnyValue_StringValue{StringValue: "hi mom"}},
			}},
		}}, map[string]interface{}{
			"body": "[123,45.6,\"hi mom\"]\n",
		}},
		{"map as mixed-type string", &common.AnyValue{
			Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{
				Values: []*common.KeyValue{
					{Key: "foo", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 123}}},
					{Key: "bar", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 45.6}}},
					{Key: "mom", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "hi mom"}}},
				},
			}}},
			map[string]interface{}{
				"body.foo": int64(123),
				"body.bar": float64(45.6),
				"body.mom": "hi mom",
			},
		},
		{"nested map as mixed-type string", &common.AnyValue{
			Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{
				Values: []*common.KeyValue{
					{Key: "foo", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 123}}},
					{Key: "bar", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 45.6}}},
					{Key: "nest", Value: &common.AnyValue{
						Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{
							Values: []*common.KeyValue{
								{Key: "foo", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 123}}},
								{Key: "bar", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 45.6}}},
								{Key: "mom", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "hi mom"}}},
							},
						}}}},
				},
			}}},
			map[string]interface{}{
				"body.foo":      int64(123),
				"body.bar":      float64(45.6),
				"body.nest.bar": float64(45.6),
				"body.nest.foo": int64(123),
				"body.nest.mom": "hi mom",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := map[string]interface{}{}
			addAttributeToMap(context.Background(), attrs, "body", tt.value, 0)
			assert.Equal(t, tt.want, attrs)
		})
	}
}

func Test_limitedWriter(t *testing.T) {
	tests := []struct {
		name      string
		max       int
		input     []string
		total     int
		want      string
		wantTrunc int
	}{
		{"no limit", 100, []string{"abcde"}, 5, "abcde", 0},
		{"one write", 5, []string{"abcdefghij"}, 10, "abcde", 5},
		{"two writes", 12, []string{"abcdefghij", "abcdefghij"}, 20, "abcdefghijab", 8},
		{"exact overrun", 10, []string{"abcdefghij", "abcdefghij"}, 20, "abcdefghij", 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLimitedWriter(tt.max)
			total := 0
			for _, s := range tt.input {
				n, err := l.Write([]byte(s))
				if err != nil {
					t.Errorf("limitedWriter.Write() error = %v", err)
					return
				}
				total += n
			}
			if total != tt.total {
				t.Errorf("limitedWriter.Write() total was %v, want %v", total, tt.total)
			}
			s := l.String()
			if s != tt.want {
				t.Errorf("limitedWriter.String() = '%v', want '%v'", s, tt.want)
			}
		})
	}
}

func Test_WriteOtlpHttpFailureResponse(t *testing.T) {
	tests := []struct {
		contentType string
		err         OTLPError
	}{
		{
			contentType: "application/x-protobuf",
			err: OTLPError{
				HTTPStatusCode: http.StatusBadRequest,
				Message:        "test",
			},
		},
		{
			contentType: "application/protobuf",
			err: OTLPError{
				HTTPStatusCode: http.StatusBadRequest,
				Message:        "test",
			},
		},
		{
			contentType: "application/json",
			err: OTLPError{
				HTTPStatusCode: http.StatusBadRequest,
				Message:        "test",
			},
		},
		{
			contentType: "nonsense",
			err: OTLPError{
				HTTPStatusCode: ErrInvalidContentType.HTTPStatusCode,
				Message:        ErrInvalidContentType.Message,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/", nil)
			r.Header.Set("Content-Type", tt.contentType)

			err := WriteOtlpHttpFailureResponse(w, r, tt.err)
			assert.NoError(t, err)

			if IsContentTypeSupported(tt.contentType) {
				assert.Equal(t, tt.contentType, w.Header().Get("Content-Type"))
				assert.Equal(t, tt.err.HTTPStatusCode, w.Code)

				data, err := io.ReadAll(w.Body)
				assert.NoError(t, err)
				var result spb.Status
				if tt.contentType == "application/json" {
					err = protojson.Unmarshal(data, &result)
					assert.NoError(t, err)
				} else {
					err = proto.Unmarshal(data, &result)
					assert.NoError(t, err)
				}
				assert.Equal(t, tt.err.Message, result.Message)
			} else {
				assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
				assert.Equal(t, ErrInvalidContentType.HTTPStatusCode, w.Code)
				assert.Equal(t, ErrInvalidContentType.Message, w.Body.String())
			}
		})
	}
}

func Test_BytesToTraceID(t *testing.T) {
	tests := []struct {
		name    string
		traceID string
		b64     bool
		want    string
	}{
		{
			name:    "64-bit traceID",
			traceID: "cbe4decd12429177",
			want:    "cbe4decd12429177",
		},
		{
			name:    "128-bit zero-padded traceID",
			traceID: "0000000000000000cbe4decd12429177",
			want:    "cbe4decd12429177",
		},
		{
			name:    "128-bit non-zero-padded traceID",
			traceID: "f23b42eac289a0fdcde48fcbe3ab1a32",
			want:    "f23b42eac289a0fdcde48fcbe3ab1a32",
		},
		{
			name:    "Non-hex traceID",
			traceID: "foobar1",
			want:    "666f6f62617231",
		},
		{
			name:    "Longer non-hex traceID",
			traceID: "foobarbaz",
			want:    "666f6f62617262617a",
		},
		{
			name:    "traceID munged by browser",
			traceID: "6e994e8673e93a51200c137330aeddad",
			b64:     true,
			want:    "6e994e8673e93a51200c137330aeddad",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var traceID []byte
			var err error
			if tt.b64 {
				traceID, err = base64.StdEncoding.DecodeString(tt.traceID)
			} else {
				traceID, err = hex.DecodeString(tt.traceID)
			}
			if err != nil {
				traceID = []byte(tt.traceID)
			}
			got := BytesToTraceID(traceID)
			if got != tt.want {
				t.Errorf("got:  %#v\n\twant: %#v", got, tt.want)
			}
		})
	}
}

func Test_WriteOtlpHttpTraceSuccessResponse(t *testing.T) {
	tests := []struct {
		contentType string
	}{
		{
			contentType: "application/x-protobuf",
		},
		{
			contentType: "application/protobuf",
		},
		{
			contentType: "application/json",
		},
		{
			contentType: "nonsense",
		},
	}
	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/", nil)
			r.Header.Set("Content-Type", tt.contentType)

			err := WriteOtlpHttpTraceSuccessResponse(w, r)
			if IsContentTypeSupported(tt.contentType) {
				assert.NoError(t, err)

				assert.Equal(t, tt.contentType, w.Header().Get("Content-Type"))
				assert.Equal(t, http.StatusOK, w.Code)

				data, err := io.ReadAll(w.Body)
				assert.NoError(t, err)
				var result collectortrace.ExportTraceServiceResponse
				if tt.contentType == "application/json" {
					err = protojson.Unmarshal(data, &result)
					assert.NoError(t, err)
				} else {
					err = proto.Unmarshal(data, &result)
					assert.NoError(t, err)
				}
				assert.Nil(t, result.GetPartialSuccess())
			} else {
				assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
				assert.Equal(t, ErrInvalidContentType.HTTPStatusCode, w.Code)
				assert.Equal(t, ErrInvalidContentType.Message, w.Body.String())
			}
		})
	}
}

func Test_WriteOtlpHttpMetricSuccessResponse(t *testing.T) {
	tests := []struct {
		contentType string
	}{
		{
			contentType: "application/x-protobuf",
		},
		{
			contentType: "application/protobuf",
		},
		{
			contentType: "application/json",
		},
		{
			contentType: "nonsense",
		},
	}
	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/", nil)
			r.Header.Set("Content-Type", tt.contentType)

			err := WriteOtlpHttpMetricSuccessResponse(w, r)
			if IsContentTypeSupported(tt.contentType) {
				assert.NoError(t, err)

				assert.Equal(t, tt.contentType, w.Header().Get("Content-Type"))
				assert.Equal(t, http.StatusOK, w.Code)

				data, err := io.ReadAll(w.Body)
				assert.NoError(t, err)
				var result collectormetrics.ExportMetricsServiceResponse
				if tt.contentType == "application/json" {
					err = protojson.Unmarshal(data, &result)
					assert.NoError(t, err)
				} else {
					err = proto.Unmarshal(data, &result)
					assert.NoError(t, err)
				}
				assert.Nil(t, result.GetPartialSuccess())
			} else {
				assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
				assert.Equal(t, ErrInvalidContentType.HTTPStatusCode, w.Code)
				assert.Equal(t, ErrInvalidContentType.Message, w.Body.String())
			}
		})
	}
}

func Test_WriteOtlpHttpLogSuccessResponse(t *testing.T) {
	tests := []struct {
		contentType string
	}{
		{
			contentType: "application/x-protobuf",
		},
		{
			contentType: "application/protobuf",
		},
		{
			contentType: "application/json",
		},
		{
			contentType: "nonsense",
		},
	}
	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/", nil)
			r.Header.Set("Content-Type", tt.contentType)

			err := WriteOtlpHttpLogSuccessResponse(w, r)
			if IsContentTypeSupported(tt.contentType) {
				assert.NoError(t, err)

				assert.Equal(t, tt.contentType, w.Header().Get("Content-Type"))
				assert.Equal(t, http.StatusOK, w.Code)

				data, err := io.ReadAll(w.Body)
				assert.NoError(t, err)
				var result collectorlogs.ExportLogsServiceResponse
				if tt.contentType == "application/json" {
					err = protojson.Unmarshal(data, &result)
					assert.NoError(t, err)
				} else {
					err = proto.Unmarshal(data, &result)
					assert.NoError(t, err)
				}
				assert.Nil(t, result.GetPartialSuccess())
			} else {
				assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))
				assert.Equal(t, ErrInvalidContentType.HTTPStatusCode, w.Code)
				assert.Equal(t, ErrInvalidContentType.Message, w.Body.String())
			}
		})
	}
}

func Test_BytesToSpanID(t *testing.T) {
	tests := []struct {
		name   string
		spanID string
		b64    bool
		want   string
	}{
		{
			name:   "spanID",
			spanID: "890452a577ef2e0f",
			want:   "890452a577ef2e0f",
		},
		{
			name:   "spanID munged by browser (converted in this test)",
			spanID: "890452a577ef2e0f",
			b64:    true,
			want:   "890452a577ef2e0f",
		},
		{
			name:   "spanID munged by browser (from a bad trace)",
			spanID: "e77ddbeb7f7adf77fbd396b9",
			b64:    false,
			want:   "533b639633f705a5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var spanID []byte
			var err error
			if tt.b64 {
				spanID, err = base64.StdEncoding.DecodeString(tt.spanID)
			} else {
				spanID, err = hex.DecodeString(tt.spanID)
			}
			if err != nil {
				spanID = []byte(tt.spanID)
			}
			got := BytesToSpanID(spanID)
			if got != tt.want {
				t.Errorf("got:  %#v\n\twant: %#v", got, tt.want)
			}
		})
	}
}

func Test_ReadOtlpBodyTooLarge(t *testing.T) {
	b, err := proto.Marshal(&collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*v1logs.ResourceLogs{
			{
				SchemaUrl: "test",
			},
		},
	})
	require.NoError(t, err)
	body := io.NopCloser(strings.NewReader(string(b)))
	request := &collectorlogs.ExportLogsServiceRequest{}
	err = parseOtlpRequestBody(body, "application/protobuf", "other", request, 1)
	require.Error(t, err)
}

func TestNoSampleRateKeyReturnOne(t *testing.T) {
	attrs := map[string]interface{}{
		"not_a_sample_rate": 10,
	}
	sampleRate := getSampleRate(attrs, "")
	assert.Equal(t, int32(1), sampleRate)
}

func TestSampleRateKeyVariations(t *testing.T) {
	tests := []struct {
		name  string
		attrs map[string]interface{}
		want  string
	}{
		// ACCEPTED - only accept the two variations we've done in the past
		{"ACCEPTED/camelCase", map[string]interface{}{"sampleRate": 10}, "sampleRate"},
		{"ACCEPTED/PascalCase", map[string]interface{}{"SampleRate": 10}, "SampleRate"},
		// IGNORED - other variations will be ignored
		{"INGORED/lowercase", map[string]interface{}{"samplerate": 10}, ""},
		{"INGORED/UPPERCASE", map[string]interface{}{"SAMPLERATE": 10}, ""},
		{"INGORED/MiXeDcAsE", map[string]interface{}{"SaMpLeRaTe": 10}, ""},
		{"INGORED/snake_case", map[string]interface{}{"sample_rate": 10}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := getSampleRateKey(tt.attrs)
			assert.Equal(t, tt.want, key)
		})

	}
}

func TestGetSampleRateConversions(t *testing.T) {
	testCases := []struct {
		sampleRate interface{}
		expected   int32
	}{
		{sampleRate: nil, expected: 1},
		{sampleRate: "0", expected: 1},
		{sampleRate: "1", expected: 1},
		{sampleRate: "100", expected: 100},
		{sampleRate: "100.0", expected: 100},
		{sampleRate: "100.4", expected: 100},
		{sampleRate: "100.6", expected: 101},
		{sampleRate: "-100", expected: 1},
		{sampleRate: "-100.0", expected: 1},
		{sampleRate: "-100.6", expected: 1},
		{sampleRate: "invalid", expected: 1},
		{sampleRate: strconv.Itoa(math.MaxInt32), expected: math.MaxInt32},
		{sampleRate: strconv.Itoa(math.MaxInt64), expected: math.MaxInt32},

		{sampleRate: 0, expected: 1},
		{sampleRate: 1, expected: 1},
		{sampleRate: 100, expected: 100},
		{sampleRate: 100.0, expected: 100},
		{sampleRate: 100.4, expected: 100},
		{sampleRate: 100.6, expected: 101},
		{sampleRate: -100, expected: 1},
		{sampleRate: -100.0, expected: 1},
		{sampleRate: -100.6, expected: 1},
		{sampleRate: math.MaxInt32, expected: math.MaxInt32},
		{sampleRate: math.MaxInt64, expected: math.MaxInt32},

		{sampleRate: int32(0), expected: 1},
		{sampleRate: int32(1), expected: 1},
		{sampleRate: int32(100), expected: 100},
		{sampleRate: int32(math.MaxInt32), expected: math.MaxInt32},

		{sampleRate: int64(0), expected: 1},
		{sampleRate: int64(1), expected: 1},
		{sampleRate: int64(100), expected: 100},
		{sampleRate: int64(math.MaxInt32), expected: math.MaxInt32},
		{sampleRate: int64(math.MaxInt64), expected: math.MaxInt32},
	}

	for _, tc := range testCases {
		attrs := map[string]interface{}{
			"sampleRate": tc.sampleRate,
		}
		assert.Equal(t, tc.expected, getSampleRate(attrs, ""))
		assert.Equal(t, 0, len(attrs))
	}
}

func TestAddAttributesToMapAreNotHTMLEncoded(t *testing.T) {
	key := "my-html"
	val := "<html><body><h1>hello</h1></body></html>"
	attrs := map[string]interface{}{}
	addAttributeToMapAsJson(attrs, key, &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: val}})
	assert.Equal(t, "\"<html><body><h1>hello</h1></body></html>\"\n", attrs[key])
}

func TestOTelSamplingThreshold(t *testing.T) {
	// the min and max values are to compensate for variations in the
	// sampling probability to sample rate conversion where a float
	// value is rounded to the nearest integer
	tests := []struct {
		name                     string
		probability              float64
		expectedMin, expectedMax int32
	}{
		{"100% - 1/1", 1, 1, 1},
		{"75% - 1/2", 0.75, 1, 2},
		{"50% - 1/2", 0.5, 2, 2},
		{"30% - 1/3", 0.3, 3, 4},
		{"25% - 1/4", 0.25, 4, 4},
		{"20% - 1/5", 0.2, 5, 5},
		{"10% - 1/10", 0.1, 10, 10},
		{"5% - 1/20", 0.05, 20, 20},
		{"1% - 1/100", 0.01, 100, 100},
		{"0.1% - 1/1000", 0.001, 999, 999},
		{"0.01% - 1/10000", 0.0001, 9999, 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threshold, err := sampling.ProbabilityToThreshold(tt.probability)
			assert.NoError(t, err)

			traceState := "th=" + threshold.TValue()

			results := map[int32]int{}
			for range 100 {
				sampleRate, ok := getSampleRateFromOTelSamplingThreshold(traceState)
				assert.True(t, ok)
				assert.GreaterOrEqual(t, sampleRate, tt.expectedMin)
				assert.LessOrEqual(t, sampleRate, tt.expectedMax)
				results[sampleRate]++
			}
			assert.Greater(t, results[tt.expectedMin], 0)
			assert.Greater(t, results[tt.expectedMax], 0)
		})
	}
}

func TestSampleRatePrefersHoneycombAttribute(t *testing.T) {
	attrs := map[string]interface{}{
		"sampleRate": 10,
	}
	sampleRate := getSampleRate(attrs, "th=c") // "th=c" is a sampling threshold of 1/4 or 25%
	assert.Equal(t, int32(10), sampleRate)
}

func TestKnownInstrumentationPrefixesReturnTrue(t *testing.T) {
	tests := []struct {
		name                     string
		libraryName              string
		isInstrumentationLibrary bool
	}{
		{
			name:                     "empty",
			libraryName:              "",
			isInstrumentationLibrary: false,
		},
		{
			name:                     "unknown",
			libraryName:              "unknown",
			isInstrumentationLibrary: false,
		},
		{
			name:                     "java",
			libraryName:              "io.opentelemetry.tomcat-7.0",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "python",
			libraryName:              "opentelemetry.instrumentation.http",
			isInstrumentationLibrary: true,
		},
		{
			name:                     ".net",
			libraryName:              "OpenTelemetry.Instrumentation.AspNetCore",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "ruby",
			libraryName:              "OpenTelemetry::Instrumentation::HTTP",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "go",
			libraryName:              "go.opentelemetry.io/contrib/instrumentation/http",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "js",
			libraryName:              "@opentelemetry/instrumentation/http",
			isInstrumentationLibrary: true,
		},
		{
			name:                     "php",
			libraryName:              "io.opentelemetry.contrib.php.slim",
			isInstrumentationLibrary: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.isInstrumentationLibrary, isInstrumentationLibrary(test.libraryName))
		})
	}
}

func TestIsClassicKey(t *testing.T) {
	testCases := []struct {
		name     string
		key      string
		expected bool
	}{
		// 32-character classic API keys (hex digits only)
		{name: "valid 32-char classic key - all lowercase", key: "a1b2c3d4e5f67890abcdef1234567890", expected: true},
		{name: "valid 32-char classic key - all numbers", key: "12345678901234567890123456789012", expected: true},
		{name: "valid 32-char classic key - all lowercase letters", key: "abcdefabcdefabcdefabcdefabcdefab", expected: true},
		{name: "valid 32-char classic key - mixed hex", key: "0123456789abcdef0123456789abcdef", expected: true},
		{name: "invalid 32-char key - uppercase letters", key: "A1B2C3D4E5F67890ABCDEF1234567890", expected: false},
		{name: "invalid 32-char key - contains g", key: "a1b2c3d4e5f67890abcdefg234567890", expected: false},
		{name: "invalid 32-char key - contains special chars", key: "a1b2c3d4e5f67890abcdef123456789!", expected: false},
		{name: "invalid 32-char key - contains space", key: "a1b2c3d4e5f67890abcdef12345 7890", expected: false},

		// 64-character classic ingest keys (pattern: ^hc[a-z]ic_[0-9a-z]*$)
		{name: "valid 64-char ingest key - hcaic", key: "hcaic_1234567890123456789012345678901234567890123456789012345678", expected: true},
		{name: "valid 64-char ingest key - hcbic", key: "hcbic_abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234", expected: true},
		{name: "valid 64-char ingest key - hczic", key: "hczic_0123456789abcdef0123456789abcdef0123456789abcdef0123456789", expected: true},
		{name: "valid 64-char ingest key - mixed", key: "hcxic_1234567890123456789012345678901234567890123456789012345678", expected: true},
		{name: "invalid 64-char ingest key - wrong prefix", key: "hc1ic_1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - uppercase in prefix", key: "hcAic_1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - missing underscore", key: "hcaic1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - uppercase in suffix", key: "hcaic_1234567890123456789012345678901234567890123456789012345A78", expected: false},
		{name: "invalid 64-char ingest key - special char in suffix", key: "hcaic_123456789012345678901234567890123456789012345678901234567!", expected: false},

		// Edge cases for length
		{name: "empty key", key: "", expected: false},
		{name: "too short - 31 chars", key: "a1b2c3d4e5f67890abcdef123456789", expected: false},
		{name: "too long - 33 chars", key: "a1b2c3d4e5f67890abcdef12345678901", expected: false},
		{name: "too short - 63 chars", key: "hcaic_123456789012345678901234567890123456789012345678901234567", expected: false},
		{name: "too long - 65 chars", key: "hcaic_12345678901234567890123456789012345678901234567890123456789", expected: false},

		// Non-classic keys (E&S keys should return false)
		{name: "E&S key", key: "abc123DEF456ghi789jklm", expected: false},
		{name: "E&S ingest key", key: "hcxik_1234567890123456789012345678901234567890123456789012345678", expected: false},

		// Invalid patterns
		{name: "random string", key: "this-is-not-a-key", expected: false},
		{name: "numbers only but wrong length", key: "123456789012", expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsClassicApiKey(tc.key)
			assert.Equal(t, tc.expected, result, "Expected IsClassicApiKey(%q) to return %v", tc.key, tc.expected)
		})
	}
}

func BenchmarkIsClassicKey(b *testing.B) {
	tests := []struct {
		name string
		key  string
	}{
		{"Valid classic key", "a1b2c3d4e5f67890abcdef1234567890"},
		{"Invalid classic key", "abcdef0123456789abcdef01234567zz"},
		{"Valid ingest key", "hcaic_1234567890123456789012345678901234567890123456789012345678"},
		{"Invalid ingest key", "hcaic_1234567890123456789012345678901234567890123456789012345678"},
	}

	for _, tt := range tests {
		b.Run("current/"+tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = IsClassicApiKey(tt.key)
			}
		})
	}
}
