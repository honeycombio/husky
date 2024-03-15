package husky

import "context"

// SetAddAttributesFunc is used to provide a function that adds attributes to telemetry controlled
// by users of this package.
// For example, beeline users would use beeline.AddField and OTel users would use span.SetAttributes.
var SetAddAttributesFunc func(ctx context.Context, attributes map[string]any) = nil

// AddAttributes is used internally to set attributes using the configured SetAddAttributesFunc.
// This function is not intended to be used directly by consumers of this package.
func AddAttributes(ctx context.Context, attributes map[string]any) {
	if SetAddAttributesFunc != nil {
		SetAddAttributesFunc(ctx, attributes)
	}
}
