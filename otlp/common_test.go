package otlp

import (
	"context"
	"net/http"
	"testing"

	common "github.com/honeycombio/husky/proto/otlp/common/v1"
	"github.com/stretchr/testify/assert"
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
		name        string
		apikey      string
		dataset     string
		contentType string
		err         error
	}{
		{name: "no key, no dataset", apikey: "", dataset: "", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "no key, dataset present", apikey: "", dataset: "dataset", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "classic/no dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "", err: ErrMissingDatasetHeader},
		{name: "classic/dataset present", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S/no dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: nil},
		{name: "E&S/dataset present", apikey: "abc123DEF456ghi789jklm", dataset: "dataset", contentType: "application/protobuf", err: nil},
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
		{name: "no key, no dataset", apikey: "", dataset: "", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "no key, dataset present", apikey: "", dataset: "dataset", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "classic/no dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "", err: ErrMissingDatasetHeader},
		{name: "classic/dataset present", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S/no dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: ErrMissingDatasetHeader},
		{name: "E&S/dataset present", apikey: "abc123DEF456ghi789jklm", dataset: "dataset", contentType: "application/protobuf", err: nil},
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
		{name: "no key, no dataset", apikey: "", dataset: "", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "no key, dataset present", apikey: "", dataset: "dataset", contentType: "", err: ErrMissingAPIKeyHeader},
		{name: "classic/no dataset", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "", contentType: "", err: ErrMissingDatasetHeader},
		{name: "classic/dataset present", apikey: "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", dataset: "dataset", contentType: "application/protobuf", err: nil},
		{name: "E&S/no dataset", apikey: "abc123DEF456ghi789jklm", dataset: "", contentType: "application/protobuf", err: ErrMissingDatasetHeader},
		{name: "E&S/dataset present", apikey: "abc123DEF456ghi789jklm", dataset: "dataset", contentType: "application/protobuf", err: nil},
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
