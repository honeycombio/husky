package otlp

import (
	"context"
	"encoding/json"
	"net/http"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/metadata"
)

const (
	apiKeyHeader             = "x-honeycomb-team"
	datasetHeader            = "x-honeycomb-dataset"
	proxyTokenHeader         = "x-honeycomb-proxy-token"
	userAgentHeader          = "user-agent"
	contentTypeHeader        = "content-type"
	contentEncodingHeader    = "content-encoding"
	gRPCAcceptEncodingHeader = "grpc-accept-encoding"
)

type RequestInfo struct {
	ApiKey     string
	Dataset    string
	ProxyToken string

	UserAgent          string
	ContentType        string
	ContentEncoding    string
	GRPCAcceptEncoding string
}

func (ri *RequestInfo) HasValidContentType() bool {
	return ri.ContentType == "application/protobuf" || ri.ContentType == "application/x-protobuf"
}

func (ri *RequestInfo) ValidateHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	return nil
}

func GetRequestInfoFromGrpcMetadata(ctx context.Context) RequestInfo {
	ri := RequestInfo{
		ContentType: "application/protobuf",
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ri.ApiKey = getValueFromMetadata(md, apiKeyHeader)
		ri.Dataset = getValueFromMetadata(md, datasetHeader)
		ri.ProxyToken = getValueFromMetadata(md, proxyTokenHeader)
		ri.UserAgent = getValueFromMetadata(md, userAgentHeader)
		ri.ContentEncoding = getValueFromMetadata(md, contentEncodingHeader)
		ri.GRPCAcceptEncoding = getValueFromMetadata(md, gRPCAcceptEncodingHeader)
	}
	return ri
}

func GetRequestInfoFromHttpHeaders(r *http.Request) RequestInfo {
	return RequestInfo{
		ApiKey:             r.Header.Get(apiKeyHeader),
		Dataset:            r.Header.Get(datasetHeader),
		ProxyToken:         r.Header.Get(proxyTokenHeader),
		UserAgent:          r.Header.Get(userAgentHeader),
		ContentType:        r.Header.Get(contentTypeHeader),
		ContentEncoding:    r.Header.Get(contentEncodingHeader),
		GRPCAcceptEncoding: r.Header.Get(gRPCAcceptEncodingHeader),
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
