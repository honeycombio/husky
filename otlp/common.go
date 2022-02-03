package otlp

import (
    "context"
    "encoding/json"
    "net/http"
    "regexp"

    common "go.opentelemetry.io/proto/otlp/common/v1"
    "google.golang.org/grpc/metadata"
)

const (
    apiKeyHeader             = "x-honeycomb-team"
    datasetHeader            = "x-honeycomb-dataset"
    proxyTokenHeader         = "x-honeycomb-proxy-token"
    proxyVersionHeader       = "x-basenji-version"
    userAgentHeader          = "user-agent"
    contentTypeHeader        = "content-type"
    contentEncodingHeader    = "content-encoding"
    gRPCAcceptEncodingHeader = "grpc-accept-encoding"
)

var legacyApiKeyPattern = regexp.MustCompile("^[0-9a-f]{32}$")

// RequestInfo represents information parsed from either HTTP headers or gRPC metadata
type RequestInfo struct {
    ApiKey       string
    Dataset      string
    ProxyToken   string
    ProxyVersion string

    UserAgent          string
    ContentType        string
    ContentEncoding    string
    GRPCAcceptEncoding string
}

// ValidateTracesHeaders validates required headers/metadata for a trace OTLP request
func (ri *RequestInfo) ValidateTracesHeaders() error {
    if len(ri.ApiKey) == 0 {
        return ErrMissingAPIKeyHeader
    }
    if isLegacy(ri.ApiKey) && len(ri.Dataset) == 0 {
        return ErrMissingDatasetHeader
    }
    if ri.ContentType != "application/protobuf" && ri.ContentType != "application/x-protobuf" {
        return ErrInvalidContentType
    }
    return nil
}

// ValidateMetricsHeaders validates required headers/metadata for a metric OTLP request
func (ri *RequestInfo) ValidateMetricsHeaders() error {
    if len(ri.ApiKey) == 0 {
        return ErrMissingAPIKeyHeader
    }
    if len(ri.Dataset) == 0 {
        return ErrMissingDatasetHeader
    }
    if ri.ContentType != "application/protobuf" && ri.ContentType != "application/x-protobuf" {
        return ErrInvalidContentType
    }
    return nil
}

// GetRequestInfoFromGrpcMetadata parses relevant gRPC metadata from an incoming request context
func GetRequestInfoFromGrpcMetadata(ctx context.Context) RequestInfo {
    ri := RequestInfo{
        ContentType: "application/protobuf",
    }
    if md, ok := metadata.FromIncomingContext(ctx); ok {
        ri.ApiKey = getValueFromMetadata(md, apiKeyHeader)
        ri.Dataset = getValueFromMetadata(md, datasetHeader)
        ri.ProxyToken = getValueFromMetadata(md, proxyTokenHeader)
        ri.ProxyVersion = getValueFromMetadata(md, proxyVersionHeader)
        ri.UserAgent = getValueFromMetadata(md, userAgentHeader)
        ri.ContentEncoding = getValueFromMetadata(md, contentEncodingHeader)
        ri.GRPCAcceptEncoding = getValueFromMetadata(md, gRPCAcceptEncodingHeader)
    }
    return ri
}

// GetRequestInfoFromHttpHeaders parses relevant incoming HTTP headers
func GetRequestInfoFromHttpHeaders(header http.Header) RequestInfo {
    return RequestInfo{
        ApiKey:             header.Get(apiKeyHeader),
        Dataset:            header.Get(datasetHeader),
        ProxyToken:         header.Get(proxyTokenHeader),
        ProxyVersion:       header.Get(proxyVersionHeader),
        UserAgent:          header.Get(userAgentHeader),
        ContentType:        header.Get(contentTypeHeader),
        ContentEncoding:    header.Get(contentEncodingHeader),
        GRPCAcceptEncoding: header.Get(gRPCAcceptEncodingHeader),
    }
}

func getValueFromMetadata(md metadata.MD, key string) string {
    if vals := md.Get(key); len(vals) > 0 {
        return vals[0]
    }
    return ""
}

func addAttributesToMap(attrs map[string]interface{}, attributes []*common.KeyValue) {
    for _, attr := range attributes {
        // ignore entries if the key is empty or value is nil
        if attr.Key == "" || attr.Value == nil {
            continue
        }
        if val := getValue(attr.Value); val != nil {
            attrs[attr.Key] = val
        }
    }
}

func getValue(value *common.AnyValue) interface{} {
    switch value.Value.(type) {
    case *common.AnyValue_StringValue:
        return value.GetStringValue()
    case *common.AnyValue_BoolValue:
        return value.GetBoolValue()
    case *common.AnyValue_DoubleValue:
        return value.GetDoubleValue()
    case *common.AnyValue_IntValue:
        return value.GetIntValue()
    case *common.AnyValue_ArrayValue:
        items := value.GetArrayValue().Values
        arr := make([]interface{}, len(items))
        for i := 0; i < len(items); i++ {
            arr[i] = getValue(items[i])
        }
        bytes, err := json.Marshal(arr)
        if err == nil {
            return string(bytes)
        }
    case *common.AnyValue_KvlistValue:
        items := value.GetKvlistValue().Values
        arr := make([]map[string]interface{}, len(items))
        for i := 0; i < len(items); i++ {
            arr[i] = map[string]interface{}{
                items[i].Key: getValue(items[i].Value),
            }
        }
        bytes, err := json.Marshal(arr)
        if err == nil {
            return string(bytes)
        }
    }
    return nil
}

func isLegacy(apiKey string) bool {
    return legacyApiKeyPattern.MatchString(apiKey)
}
