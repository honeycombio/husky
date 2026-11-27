package otlp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func snappyRequestInfo(encoding string) RequestInfo {
	return RequestInfo{
		ApiKey:          "abc123DEF456ghi789jklm",
		Dataset:         "legacy-dataset",
		ContentType:     "application/protobuf",
		ContentEncoding: encoding,
	}
}

func snappyTraceRequest(spanName string) *collectortrace.ExportTraceServiceRequest {
	return &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key:   "service.name",
					Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
					Name:    spanName,
				}},
			}},
		}},
	}
}

// framedSnappy encodes body as a snappy framed stream.
func framedSnappy(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := snappy.NewBufferedWriter(&buf)
	_, err := w.Write(body)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestSnappyBodies(t *testing.T) {
	body, err := proto.Marshal(snappyTraceRequest("snappy-span"))
	require.NoError(t, err)

	bodies := []struct {
		name     string
		encoding string
		payload  []byte
	}{
		{"block", "snappy", snappy.Encode(nil, body)},
		{"framed stream under snappy", "snappy", framedSnappy(t, body)},
		{"framed stream under x-snappy-framed", "x-snappy-framed", framedSnappy(t, body)},
	}

	for _, b := range bodies {
		t.Run(b.name, func(t *testing.T) {
			for _, decoder := range traceDecoders() {
				t.Run(decoder.name, func(t *testing.T) {
					err := decoder.decode(io.NopCloser(bytes.NewReader(b.payload)), snappyRequestInfo(b.encoding), defaultMaxRequestBodySize)
					require.NoError(t, err)
				})
			}
		})
	}
}

func TestSnappyTraceContent(t *testing.T) {
	body, err := proto.Marshal(snappyTraceRequest("snappy-span"))
	require.NoError(t, err)

	result, err := TranslateTraceRequestFromReaderSizedWithMsgp(
		context.Background(),
		io.NopCloser(bytes.NewReader(snappy.Encode(nil, body))),
		snappyRequestInfo("snappy"),
		defaultMaxRequestBodySize,
	)
	require.NoError(t, err)
	require.Len(t, result.Batches, 1)
	require.Len(t, result.Batches[0].Events, 1)
	assert.Equal(t, "my-service", result.Batches[0].Dataset)
}

func TestSnappyLogs(t *testing.T) {
	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key:   "service.name",
					Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}},
				}},
			},
			ScopeLogs: []*logs.ScopeLogs{{
				LogRecords: []*logs.LogRecord{{
					Body: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "hello"}},
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	require.NoError(t, err)

	for _, tc := range []struct {
		encoding string
		payload  []byte
	}{
		{"snappy", snappy.Encode(nil, body)},
		{"x-snappy-framed", framedSnappy(t, body)},
	} {
		t.Run(tc.encoding, func(t *testing.T) {
			result, err := TranslateLogsRequestFromReader(context.Background(), io.NopCloser(bytes.NewReader(tc.payload)), snappyRequestInfo(tc.encoding))
			require.NoError(t, err)
			require.Len(t, result.Batches, 1)
			require.Len(t, result.Batches[0].Events, 1)
		})
	}
}

func TestSnappyBlockDeclaringHugeLengthIsRejected(t *testing.T) {
	// A snappy block starts with the decoded length as a varint. This one
	// declares 1GiB and carries no data, so it must be refused from the
	// declared length alone, before anything is allocated for it.
	payload := binary.AppendUvarint(nil, 1<<30)

	for _, decoder := range traceDecoders() {
		t.Run(decoder.name, func(t *testing.T) {
			var err error
			allocated := allocatedDuring(func() {
				err = decoder.decode(io.NopCloser(bytes.NewReader(payload)), snappyRequestInfo("snappy"), defaultMaxRequestBodySize)
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFailedParseBody)
			assert.Less(t, allocated, uint64(16<<20),
				"a %d byte body allocated %d bytes; the declared length must be checked first", len(payload), allocated)
		})
	}
}

func TestSnappyDecodedBodyLargerThanMaxSizeIsRejected(t *testing.T) {
	body, err := proto.Marshal(snappyTraceRequest(strings.Repeat("a", 1<<20)))
	require.NoError(t, err)

	const maxSize = 64 << 10
	for name, payload := range map[string][]byte{
		"block":  snappy.Encode(nil, body),
		"framed": framedSnappy(t, body),
	} {
		require.Less(t, len(payload), maxSize, "the compressed body must fit under maxSize")
		for _, decoder := range traceDecoders() {
			t.Run(name+"/"+decoder.name, func(t *testing.T) {
				err := decoder.decode(io.NopCloser(bytes.NewReader(payload)), snappyRequestInfo("snappy"), maxSize)
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrFailedParseBody)
			})
		}
	}
}

func TestSnappyCorruptBodyIsRejected(t *testing.T) {
	for _, decoder := range traceDecoders() {
		t.Run(decoder.name, func(t *testing.T) {
			for _, payload := range [][]byte{
				{},
				[]byte("this is not snappy data"),
				snappy.Encode(nil, []byte("some bytes"))[:5],
			} {
				err := decoder.decode(io.NopCloser(bytes.NewReader(payload)), snappyRequestInfo("snappy"), defaultMaxRequestBodySize)
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrFailedParseBody)
			}
		})
	}
}
