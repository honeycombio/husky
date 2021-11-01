package otlp

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/metadata"
)

func TestParseGrpcMetadataIntoRequestInfo(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		apiKeyHeader:     "test-api-key",
		datasetHeader:    "test-dataset",
		proxyTokenHeader: "test-proxy-token",
		userAgentHeader:  "test-user-agent",
	}))
	ri := GetRequestInfoFromGrpcMetadata(ctx)

	assert.Equal(t, "test-api-key", ri.ApiKey)
	assert.Equal(t, "test-dataset", ri.Dataset)
	assert.Equal(t, "test-proxy-token", ri.ProxyToken)
	assert.Equal(t, "test-user-agent", ri.UserAgent)
	assert.Equal(t, "application/protobuf", ri.ContentType)
}

func TestParseHttpHeadersIntoRequestInfo(t *testing.T) {
	r, _ := http.NewRequest("POST", "/", nil)
	r.Header.Set(apiKeyHeader, "test-api-key")
	r.Header.Set(datasetHeader, "test-dataset")
	r.Header.Set(proxyTokenHeader, "test-proxy-token")
	r.Header.Set(userAgentHeader, "test-user-agent")
	r.Header.Set(contentTypeHeader, "test-content-type")

	ri := GetRequestInfoFromHttpHeaders(r)
	assert.Equal(t, "test-api-key", ri.ApiKey)
	assert.Equal(t, "test-dataset", ri.Dataset)
	assert.Equal(t, "test-proxy-token", ri.ProxyToken)
	assert.Equal(t, "test-user-agent", ri.UserAgent)
	assert.Equal(t, "test-content-type", ri.ContentType)
}

func TestHasValidContentType(t *testing.T) {
	testCases := []struct {
		contentType string
		exepcted    bool
	}{
		{contentType: "application/protobuf", exepcted: true},
		{contentType: "application/x-protobuf", exepcted: true},
		{contentType: "application/json", exepcted: false},
		{contentType: "application/javascript", exepcted: false},
		{contentType: "application/xml", exepcted: false},
		{contentType: "application/octet-stream", exepcted: false},
		{contentType: "text-plain", exepcted: false},
	}

	for _, tc := range testCases {
		ri := RequestInfo{ContentType: tc.contentType}
		assert.Equal(t, tc.exepcted, ri.HasValidContentType())
	}
}

func TestAddAttributesToMap(t *testing.T) {
	testCases := []struct {
		key       string
		expected  interface{}
		attribute *common.KeyValue
	}{
		{
			key:      "str-attr",
			expected: "str-value",
			attribute: &common.KeyValue{
				Key: "str-attr", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "str-value"}},
			},
		},
		{
			key:      "int-attr",
			expected: int64(123),
			attribute: &common.KeyValue{
				Key: "int-attr", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 123}},
			},
		},
		{
			key:      "double-attr",
			expected: float64(12.3),
			attribute: &common.KeyValue{
				Key: "double-attr", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 12.3}},
			},
		},
		{
			key:      "bool-attr",
			expected: true,
			attribute: &common.KeyValue{
				Key: "bool-attr", Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}},
			},
		},
		{
			key:      "empty-key",
			expected: nil,
			attribute: &common.KeyValue{
				Key: "", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "str-value"}},
			},
		},
		{
			key:      "array-attr", // not supported
			expected: nil,
			attribute: &common.KeyValue{
				Key: "array-attr", Value: &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{
					Values: []*common.AnyValue{
						{Value: &common.AnyValue_StringValue{StringValue: "array-str-value"}},
					}}}},
			},
		},
		{
			key:      "kv-attr",
			expected: nil,
			attribute: &common.KeyValue{
				Key: "kv-attr", Value: &common.AnyValue{
					Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{
						Values: []*common.KeyValue{
							{Key: "kv-attr-str", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{"kv-attr-str-value"}}}},
					}}}},
		},
	}

	for _, tc := range testCases {
		attrs := map[string]interface{}{}
		addAttributesToMap(attrs, []*common.KeyValue{tc.attribute})
		assert.Equal(t, tc.expected, attrs[tc.key])
	}
}

func TestValidateHeaders(t *testing.T) {
	testCases := []struct {
		apikey  string
		dataset string
		err     error
	}{
		{apikey: "", dataset: "", err: ErrMissingAPIKeyHeader},
		{apikey: "apikey", dataset: "", err: ErrMissingDatasetHeader},
		{apikey: "", dataset: "dataset", err: ErrMissingAPIKeyHeader},
		{apikey: "apikey", dataset: "dataset", err: nil},
	}

	for _, tc := range testCases {
		ri := RequestInfo{ApiKey: tc.apikey, Dataset: tc.dataset}
		err := ri.ValidateHeaders()
		assert.Equal(t, tc.err, err)
	}
}
