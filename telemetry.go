package husky

import "context"

// SetAttributesFunc is a function that can be used to set attributes in telemetry controlled
// by users of this package.
// For example, beeline users would use beeline.AddField and OTel users would use span.SetAttributes.
var SetAttributesFunc func(ctx context.Context, attributes map[string]any) = nil

// SetAttributes is used internally to set attributes using the configured SetAttributesFunc.
// This function is not intended to be used directly by consumers of this package.
func SetAttributes(ctx context.Context, attributes map[string]any) {
	if SetAttributesFunc != nil {
		SetAttributesFunc(ctx, attributes)
	}
}
