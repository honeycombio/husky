package husky

import "context"

var TracingFunc func(ctx context.Context, attributes map[string]any) = nil

func AddAttributes(ctx context.Context, attributes map[string]any) {
	if TracingFunc != nil {
		TracingFunc(ctx, attributes)
	}
}
