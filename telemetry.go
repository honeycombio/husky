package husky

import "context"

// AddTelemetryAttributeFunc is used to provide a function that adds attributes to telemetry controlled
// by users of this package.
// For example, beeline users would use beeline.AddField and OTel users would use span.SetAttributes.
var AddTelemetryAttributeFunc func(ctx context.Context, key string, value any) = nil

// AddTelemetryAttribute is used internally to set attributes using the configured SetAddAttributesFunc.
// This function is not intended to be used directly by consumers of this package.
func AddTelemetryAttribute(ctx context.Context, key string, value any) {
	if AddTelemetryAttributeFunc != nil {
		AddTelemetryAttributeFunc(ctx, key, value)
	}
}

// AddTelemetryAttributes is used internally to set multiple attributes using the configured SetAddAttributesFunc.
// This function is not intended to be used directly by consumers of this package.
func AddTelemetryAttributes(ctx context.Context, attributes map[string]any) {
	for key, value := range attributes {
		AddTelemetryAttribute(ctx, key, value)
	}
}
