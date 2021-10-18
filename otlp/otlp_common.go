package otlp

import (
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/metadata"
)

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
