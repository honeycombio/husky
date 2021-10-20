package otlp

import (
	"context"
	"net/http"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/metadata"
)

const (
	apiKeyKey      = "x-honeycomb-team"
	datasetKey     = "x-honeycomb-dataset"
	proxytokenKey  = "x-honeycomb-proxy-token"
	userAgentKey   = "user-agent"
	contentTypeKey = "content-type"
)

type RequestInfo struct {
	ApiKey     string
	Dataset    string
	ProxyToken string

	UserAgent   string
	ContentType string
}

func (ri *RequestInfo) HasValidContentType() bool {
	return ri.ContentType == "application/protobuf" || ri.ContentType == "application/x-protobuf"
}

func GetRequestInfoFromGrpcMetadata(ctx context.Context) RequestInfo {
	ri := RequestInfo{
		ContentType: "application/protobuf",
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ri.ApiKey = getValueFromMetadata(md, apiKeyKey)
		ri.Dataset = getValueFromMetadata(md, datasetKey)
		ri.ProxyToken = getValueFromMetadata(md, proxytokenKey)
		ri.UserAgent = getValueFromMetadata(md, userAgentKey)
	}
	return ri
}

func GetRequestInfoFromHttpHeaders(r *http.Request) RequestInfo {
	return RequestInfo{
		ApiKey:      r.Header.Get(apiKeyKey),
		Dataset:     r.Header.Get(datasetKey),
		ProxyToken:  r.Header.Get(proxytokenKey),
		UserAgent:   r.Header.Get(userAgentKey),
		ContentType: r.Header.Get(contentTypeKey),
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
		if attr.Key == "" {
			continue
		}
		switch attr.Value.Value.(type) {
		case *common.AnyValue_StringValue:
			attrs[attr.Key] = attr.Value.GetStringValue()
		case *common.AnyValue_BoolValue:
			attrs[attr.Key] = attr.Value.GetBoolValue()
		case *common.AnyValue_DoubleValue:
			attrs[attr.Key] = attr.Value.GetDoubleValue()
		case *common.AnyValue_IntValue:
			attrs[attr.Key] = attr.Value.GetIntValue()
		default:
			// ArrayList and KvList types are not currently handled
			// NOTE: when we move to forwarding encrypted OTLP requests, these will be handled in Shepherd
			// and this func will be redundant
		}
	}
}
