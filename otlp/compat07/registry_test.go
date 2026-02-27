package compat07_test

import (
	"testing"

	// Shadow types from this module
	_ "github.com/honeycombio/husky/otlp/compat07/internal/shadowpb/commonpb"
	_ "github.com/honeycombio/husky/otlp/compat07/internal/shadowpb/metricspb"

	// Upstream 1.x types
	_ "go.opentelemetry.io/proto/otlp/common/v1"
	_ "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestNoRegistryCollision(t *testing.T) {
	// If this test runs at all, the init() functions from both the shadow
	// and upstream protobuf packages registered successfully without panic.
	// This verifies otlp07-compat.AC6.1.
}
