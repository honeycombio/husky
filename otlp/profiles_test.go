package otlp

import (
	"bytes"
	"context"
	"io"
	"testing"

	collectorProfiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTranslateProfilesRequest(t *testing.T) {
	ctx := context.Background()

	// Create minimal ProfilesData
	request := &collectorProfiles.ExportProfilesServiceRequest{
		ResourceProfiles: []*profiles.ResourceProfiles{
			{
				ScopeProfiles: []*profiles.ScopeProfiles{
					{
						Profiles: []*profiles.Profile{
							{
								TimeUnixNano: 1234567890,
							},
						},
					},
				},
			},
		},
		Dictionary: &profiles.ProfilesDictionary{
			LinkTable: []*profiles.Link{
				{
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
				},
			},
		},
	}

	ri := RequestInfo{
		ApiKey:      "test-key",
		ContentType: "application/protobuf",
	}

	result, err := TranslateProfilesRequest(ctx, request, ri)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify structure
	assert.Greater(t, result.RequestSize, 0)
	assert.Len(t, result.Batches, 1)

	batch := result.Batches[0]
	assert.Equal(t, "__profiles__", batch.Dataset)
	assert.Len(t, batch.Events, 1)

	event := batch.Events[0]
	assert.Equal(t, "profile", event.Attributes["meta.signal_type"])
	assert.NotNil(t, event.Attributes["profile.data"])

	// Verify profile.data is ProfilesData proto (not bytes)
	profileData, ok := event.Attributes["profile.data"].(*profiles.ProfilesData)
	require.True(t, ok)
	assert.Len(t, profileData.ResourceProfiles, 1)
	assert.NotNil(t, profileData.Dictionary)
}

func TestTranslateProfilesRequestFromReader(t *testing.T) {
	ctx := context.Background()

	request := &collectorProfiles.ExportProfilesServiceRequest{
		ResourceProfiles: []*profiles.ResourceProfiles{
			{
				ScopeProfiles: []*profiles.ScopeProfiles{
					{
						Profiles: []*profiles.Profile{
							{
								TimeUnixNano: 1234567890,
							},
						},
					},
				},
			},
		},
	}

	serialized, err := proto.Marshal(request)
	require.NoError(t, err)

	body := io.NopCloser(bytes.NewReader(serialized))

	ri := RequestInfo{
		ApiKey:          "test-key",
		ContentType:     "application/protobuf",
		ContentEncoding: "",
	}

	result, err := TranslateProfilesRequestFromReader(ctx, body, ri)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Len(t, result.Batches, 1)
	assert.Equal(t, "__profiles__", result.Batches[0].Dataset)
}

func TestValidateProfilesHeaders(t *testing.T) {
	tests := []struct {
		name    string
		ri      RequestInfo
		wantErr bool
	}{
		{
			name: "valid headers",
			ri: RequestInfo{
				ApiKey:      "test-key",
				ContentType: "application/protobuf",
			},
			wantErr: false,
		},
		{
			name: "missing API key",
			ri: RequestInfo{
				ContentType: "application/protobuf",
			},
			wantErr: true,
		},
		{
			name: "invalid content type",
			ri: RequestInfo{
				ApiKey:      "test-key",
				ContentType: "text/plain",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ri.ValidateProfilesHeaders()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
