package otlp

import (
	"context"
	"io"
	"time"

	"github.com/honeycombio/husky"
	collectorProfiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
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
		husky.AddTelemetryAttribute(ctx, "error", true)
		husky.AddTelemetryAttribute(ctx, "error_reason", err.Error())
		return nil, err
	}
	request := &collectorProfiles.ExportProfilesServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentType, ri.ContentEncoding, request, maxSize); err != nil {
		husky.AddTelemetryAttribute(ctx, "error", true)
		husky.AddTelemetryAttribute(ctx, "error_reason", err.Error())
		return nil, ErrFailedParseBody
	}
	return TranslateProfilesRequest(ctx, request, ri)
}

// TranslateProfilesRequest translates an OTLP proto profiles request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
//
// ARCHITECTURE NOTE:
// Husky packages ProfilesData as opaque blob. Shepherd handles repacking:
//  1. Husky: Serialize ProfilesData (ResourceProfiles + Dictionary) → pass to Shepherd
//  2. Shepherd: Parse link_table, fan out by partition, create Kafka events
//  3. Retriever: Parse link_table again, fan out by trace_id, write rows
//  4. Finalization: Merge dictionaries, optimize storage
func TranslateProfilesRequest(ctx context.Context, request *collectorProfiles.ExportProfilesServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateProfilesHeaders(); err != nil {
		husky.AddTelemetryAttribute(ctx, "error", true)
		husky.AddTelemetryAttribute(ctx, "error_reason", err.Error())
		return nil, err
	}

	batches := []Batch{}
	dataset := getProfilesDataset()

	// Create ProfilesData from request (ResourceProfiles + Dictionary)
	profilesData := &profiles.ProfilesData{
		ResourceProfiles: request.ResourceProfiles,
		Dictionary:       request.Dictionary,
	}

	// Pass ProfilesData proto directly to Shepherd
	// Shepherd will parse link_table for partition fanout, then serialize for Kafka
	attrs := map[string]interface{}{
		"meta.signal_type": "profile",
		"profile.data":     profilesData, // Pass proto, not bytes
	}

	// Use current time as timestamp (profiles have their own timestamps in the proto)
	timestamp := time.Now().UTC()
	sampleRate := int32(1)

	events := []Event{{
		Attributes: attrs,
		Timestamp:  timestamp,
		SampleRate: sampleRate,
	}}

	batches = append(batches, Batch{
		Dataset: dataset,
		Events:  events,
	})

	return &TranslateOTLPRequestResult{
		RequestSize: proto.Size(request),
		Batches:     batches,
	}, nil
}

// getProfilesDataset returns the target dataset for profile data
// Always returns __profiles__ - this is a system dataset, not user-configurable
func getProfilesDataset() string {
	return "__profiles__"
}
