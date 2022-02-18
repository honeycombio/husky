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
		apiKeyHeader:       "test-api-key",
		datasetHeader:      "test-dataset",
		proxyTokenHeader:   "test-proxy-token",
		proxyVersionHeader: "test-proxy-version",
		userAgentHeader:    "test-user-agent",
	}))
	ri := GetRequestInfoFromGrpcMetadata(ctx)

	assert.Equal(t, "test-api-key", ri.ApiKey)
	assert.Equal(t, "test-dataset", ri.Dataset)
	assert.Equal(t, "test-proxy-token", ri.ProxyToken)
	assert.Equal(t, "test-proxy-version", ri.ProxyVersion)
	assert.Equal(t, "test-user-agent", ri.UserAgent)
	assert.Equal(t, "application/protobuf", ri.ContentType)
}

func TestParseHttpHeadersIntoRequestInfo(t *testing.T) {
	header := http.Header{}
	header.Set(apiKeyHeader, "test-api-key")
	header.Set(datasetHeader, "test-dataset")
	header.Set(proxyTokenHeader, "test-proxy-token")
	header.Set(userAgentHeader, "test-user-agent")
	header.Set(contentTypeHeader, "test-content-type")

	ri := GetRequestInfoFromHttpHeaders(header)
	assert.Equal(t, "test-api-key", ri.ApiKey)
	assert.Equal(t, "test-dataset", ri.Dataset)
	assert.Equal(t, "test-proxy-token", ri.ProxyToken)
	assert.Equal(t, "test-user-agent", ri.UserAgent)
	assert.Equal(t, "test-content-type", ri.ContentType)
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
			expected: "[\"one\",true,3]",
			attribute: &common.KeyValue{
				Key: "array-attr", Value: &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{
					Values: []*common.AnyValue{
						{Value: &common.AnyValue_StringValue{StringValue: "one"}},
						{Value: &common.AnyValue_BoolValue{BoolValue: true}},
						{Value: &common.AnyValue_IntValue{IntValue: 3}},
					}}}},
			},
		},
		{
			key:      "kvlist-attr",
			expected: "[{\"kv-attr-str\":\"kv-attr-str-value\"},{\"kv-attr-int\":1}]",
			attribute: &common.KeyValue{
				Key: "kvlist-attr", Value: &common.AnyValue{
					Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{
						Values: []*common.KeyValue{
							{Key: "kv-attr-str", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "kv-attr-str-value"}}},
							{Key: "kv-attr-int", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 1}}},
						},
					}}}},
		},
		{
			key:       "nil-value-attr",
			expected:  nil,
			attribute: &common.KeyValue{Key: "kv-attr", Value: nil},
		},
	}

	for _, tc := range testCases {
		attrs := map[string]interface{}{}
		addAttributesToMap(attrs, []*common.KeyValue{tc.attribute})
		assert.Equal(t, tc.expected, attrs[tc.key])
	}
}

func TestValidateTracesHeaders(t *testing.T) {
	testCases := []struct {
		apikey      string
		dataset     string
		contentType string
		err         error
	}{
		{apikey: "", dataset: "", contentType: "", err: ErrMissingAPIKeyHeader},
		{apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "", err: ErrMissingDatasetHeader},
		{apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: nil},
		{apikey: "", dataset: "dataset", contentType: "", err: ErrMissingAPIKeyHeader},
		{apikey: "apikey", dataset: "dataset", contentType: "", err: ErrInvalidContentType},
		{apikey: "apikey", dataset: "dataset", contentType: "application/json", err: ErrInvalidContentType},
		{apikey: "apikey", dataset: "dataset", contentType: "application/javascript", err: ErrInvalidContentType},
		{apikey: "apikey", dataset: "dataset", contentType: "application/xml", err: ErrInvalidContentType},
		{apikey: "apikey", dataset: "dataset", contentType: "application/octet-stream", err: ErrInvalidContentType},
		{apikey: "apikey", dataset: "dataset", contentType: "text-plain", err: ErrInvalidContentType},
		{apikey: "apikey", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{apikey: "apikey", dataset: "dataset", contentType: "application/x-protobuf", err: nil},
	}

	for _, tc := range testCases {
		ri := RequestInfo{ApiKey: tc.apikey, ContentType: tc.contentType, Dataset: tc.dataset}
		err := ri.ValidateTracesHeaders()
		assert.Equal(t, tc.err, err)
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
		{name: "missing API key and missing dataset", apikey: "", dataset: "", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "legacy API key and missing dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "", err: ErrMissingDatasetHeader},
		{name: "new API key and missing dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "missing API key", apikey: "", dataset: "dataset", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "missing content-type", apikey: "apikey", dataset: "dataset", contentType: "", err: ErrInvalidContentType},
		{name: "application/json content-type", apikey: "apikey", dataset: "dataset", contentType: "application/json", err: ErrInvalidContentType},
		{name: "application/javascript content-type", apikey: "apikey", dataset: "dataset", contentType: "application/javascript", err: ErrInvalidContentType},
		{name: "application/xml content-type", apikey: "apikey", dataset: "dataset", contentType: "application/xml", err: ErrInvalidContentType},
		{name: "application/octet-stream content-type", apikey: "apikey", dataset: "dataset", contentType: "application/octet-stream", err: ErrInvalidContentType},
		{name: "text-plain content type", apikey: "apikey", dataset: "dataset", contentType: "text-plain", err: ErrInvalidContentType},
		{name: "application/protobuf content-type", apikey: "apikey", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "application/x-protobuf content-type", apikey: "apikey", dataset: "dataset", contentType: "application/x-protobuf", err: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ri := RequestInfo{ApiKey: tc.apikey, ContentType: tc.contentType, Dataset: tc.dataset}
			err := ri.ValidateMetricsHeaders()
			assert.Equal(t, tc.err, err)
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
			md.Set(tt.proxyTokenHeader, proxyTokenValue)

			ctx := metadata.NewIncomingContext(context.Background(), md)
			ri := GetRequestInfoFromGrpcMetadata(ctx)
			assert.Equal(t, apiKeyValue, ri.ApiKey)
			assert.Equal(t, datasetValue, ri.Dataset)
			assert.Equal(t, proxyTokenValue, ri.ProxyToken)
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
			header.Set(proxyTokenHeader, proxyTokenValue)

			ri := GetRequestInfoFromHttpHeaders(header)
			assert.Equal(t, apiKeyValue, ri.ApiKey)
			assert.Equal(t, datasetValue, ri.Dataset)
			assert.Equal(t, proxyTokenValue, ri.ProxyToken)
		})
	}
}
