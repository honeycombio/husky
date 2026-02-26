// Package compat07 detects and converts OTLP 0.7 metrics data that arrives
// as protobuf unknown fields when deserialized with OTLP 1.x proto definitions.
package compat07

//go:generate buf generate
