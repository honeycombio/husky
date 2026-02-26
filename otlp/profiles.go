package otlp

import (
	"context"
	"fmt"
	"io"
	"time"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	collectorProfiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// TranslateProfilesRequestFromReader translates an OTLP profiles request into Honeycomb-friendly structure from a reader (eg HTTP body)
// RequestInfo is the parsed information from the HTTP headers
func TranslateProfilesRequestFromReader(ctx context.Context, body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	return TranslateProfilesRequestFromReaderSized(ctx, body, ri, defaultMaxRequestBodySize)
}

// TranslateProfilesRequestFromReaderSized translates an OTLP/HTTP profiles request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
// maxSize is the maximum size of the request body in bytes
func TranslateProfilesRequestFromReaderSized(ctx context.Context, body io.ReadCloser, ri RequestInfo, maxSize int64) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateProfilesHeaders(); err != nil {
		return nil, err
	}
	request := &collectorProfiles.ExportProfilesServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentType, ri.ContentEncoding, request, maxSize); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrFailedParseBody, err)
	}
	return TranslateProfilesRequest(ctx, request, ri)
}

// TranslateProfilesRequest translates an OTLP proto profiles request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
//
// ARCHITECTURE NOTE:
// Husky explodes by (ResourceProfile, ScopeProfile) and hoists attributes:
//  1. Husky: Explode by (Resource, Scope), hoist attributes to columns, create events
//  2. Mutation Producer: Group by partition (trace_id), filter ProfilesData, emit Kafka
//  3. Retriever: Store Dictionary + Profiles[] as blob, attributes as regular columns
func TranslateProfilesRequest(ctx context.Context, request *collectorProfiles.ExportProfilesServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateProfilesHeaders(); err != nil {
		return nil, err
	}

	batches := []Batch{}
	dataset := getProfilesDataset()

	// Explode by (ResourceProfile, ScopeProfile) combination
	// Each combination becomes one event with hoisted attributes
	events := []Event{}

	for _, rp := range request.ResourceProfiles {
		// Extract resource attributes (service.name, host.name, etc.)
		resourceAttrs := extractAttributes(rp.Resource)

		for _, sp := range rp.ScopeProfiles {
			// Extract scope attributes (scope.name, scope.version)
			scopeAttrs := extractAttributes(sp.Scope)

			// Create ProfilesData for this (Resource, Scope) combination
			// Contains: shared Dictionary + only Profiles from this ScopeProfile
			profilesData := &profiles.ProfilesData{
				Dictionary: request.Dictionary, // Shared across all events
				ResourceProfiles: []*profiles.ResourceProfiles{{
					// No Resource field - attributes hoisted to columns
					ScopeProfiles: []*profiles.ScopeProfiles{{
						// No Scope field - attributes hoisted to columns
						Profiles: sp.Profiles,
					}},
				}},
			}

			// Merge resource + scope attributes
			attrs := map[string]interface{}{
				"meta.signal_type": "profile",
				"profile.data":     profilesData,
			}
			for k, v := range resourceAttrs {
				attrs[k] = v
			}
			for k, v := range scopeAttrs {
				attrs[k] = v
			}

			// Use current time as timestamp (profiles have their own timestamps in the proto)
			timestamp := time.Now().UTC()
			sampleRate := int32(1)

			events = append(events, Event{
				Attributes: attrs,
				Timestamp:  timestamp,
				SampleRate: sampleRate,
			})
		}
	}

	batches = append(batches, Batch{
		Dataset: dataset,
		Events:  events,
	})

	return &TranslateOTLPRequestResult{
		RequestSize: proto.Size(request),
		Batches:     batches,
	}, nil
}

// extractAttributes converts OTLP Resource or Scope attributes to map[string]interface{}
func extractAttributes(obj interface{}) map[string]interface{} {
	attrs := make(map[string]interface{})

	// Handle Resource type
	if res, ok := obj.(*resource.Resource); ok && res != nil {
		for _, attr := range res.Attributes {
			if key, val := convertOTLPAttribute(attr); key != "" {
				attrs[key] = val
			}
		}
	}

	// Handle InstrumentationScope type
	if scope, ok := obj.(*common.InstrumentationScope); ok && scope != nil {
		if scope.Name != "" {
			attrs["scope.name"] = scope.Name
		}
		if scope.Version != "" {
			attrs["scope.version"] = scope.Version
		}
		for _, attr := range scope.Attributes {
			if key, val := convertOTLPAttribute(attr); key != "" {
				attrs[key] = val
			}
		}
	}

	return attrs
}

// convertOTLPAttribute converts OTLP KeyValue to (string, interface{})
func convertOTLPAttribute(attr *common.KeyValue) (string, interface{}) {
	if attr == nil || attr.Key == "" {
		return "", nil
	}

	switch v := attr.Value.Value.(type) {
	case *common.AnyValue_StringValue:
		return attr.Key, v.StringValue
	case *common.AnyValue_BoolValue:
		return attr.Key, v.BoolValue
	case *common.AnyValue_IntValue:
		return attr.Key, v.IntValue
	case *common.AnyValue_DoubleValue:
		return attr.Key, v.DoubleValue
	default:
		// Skip complex types (arrays, maps) for now
		return "", nil
	}
}

// getProfilesDataset returns the target dataset for profile data
// Always returns __profiles__ - this is a system dataset, not user-configurable
func getProfilesDataset() string {
	return "__profiles__"
}
