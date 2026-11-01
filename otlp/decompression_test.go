package otlp

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// traceDecoder is one of the zstd-decoding entry points for trace requests.
type traceDecoder struct {
	name   string
	decode func(body io.ReadCloser, ri RequestInfo, maxSize int64) error
}

func traceDecoders() []traceDecoder {
	return []traceDecoder{
		{
			name: "TranslateTraceRequestFromReaderSized",
			decode: func(body io.ReadCloser, ri RequestInfo, maxSize int64) error {
				_, err := TranslateTraceRequestFromReaderSized(context.Background(), body, ri, maxSize)
				return err
			},
		},
		{
			name: "TranslateTraceRequestFromReaderSizedWithMsgp",
			decode: func(body io.ReadCloser, ri RequestInfo, maxSize int64) error {
				_, err := TranslateTraceRequestFromReaderSizedWithMsgp(context.Background(), body, ri, maxSize)
				return err
			},
		},
	}
}

func zstdRequestInfo() RequestInfo {
	return RequestInfo{
		ApiKey:          "abc123DEF456ghi789jklm",
		Dataset:         "legacy-dataset",
		ContentType:     "application/protobuf",
		ContentEncoding: "zstd",
	}
}

// zstdFrameDeclaringHugeWindow returns a 9-byte zstd frame that declares a
// 192MiB window and decodes to zero bytes.
//
// A streaming decoder allocates the window a frame declares before it produces
// any output, so a frame this small can force a 192MiB allocation unless the
// decoder sets zstd.WithDecoderMaxWindow. The default maximum window is 512MiB.
func zstdFrameDeclaringHugeWindow() []byte {
	return []byte{
		0x28, 0xb5, 0x2f, 0xfd, // frame magic
		0x00,             // frame header descriptor: no content size, no single segment, no checksum, no dict
		0x8c,             // window descriptor: 2^27 + 4*(2^27/8) = 192MiB
		0x01, 0x00, 0x00, // block header: last block, raw, zero length
	}
}

// allocatedDuring reports the total bytes allocated by f, which is not reduced
// by garbage collection.
func allocatedDuring(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestZstdFrameDeclaringHugeWindowIsRejected(t *testing.T) {
	frame := zstdFrameDeclaringHugeWindow()
	require.Len(t, frame, 9)

	for _, decoder := range traceDecoders() {
		t.Run(decoder.name, func(t *testing.T) {
			var err error
			allocated := allocatedDuring(func() {
				err = decoder.decode(io.NopCloser(bytes.NewReader(frame)), zstdRequestInfo(), defaultMaxRequestBodySize)
			})

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFailedParseBody)
			assert.Less(t, allocated, uint64(16<<20),
				"decoding a %d byte frame allocated %d bytes; the declared window must be bounded", len(frame), allocated)
		})
	}
}

func TestZstdDecompressedBodyLargerThanMaxSizeIsRejected(t *testing.T) {
	// The request is valid protobuf that compresses to a few hundred bytes but
	// decodes to 16MiB, which is well over the maxSize passed below.
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key:   "service.name",
					Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					Name: strings.Repeat("a", 16<<20),
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed := new(bytes.Buffer)
	// A small encoder window keeps the decoder's window allocation out of the
	// measured budget, so the assertion below is about decoded output only.
	w, err := zstd.NewWriter(compressed, zstd.WithWindowSize(128<<10))
	require.NoError(t, err)
	_, err = w.Write(body)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	const maxSize = 4096
	require.Less(t, compressed.Len(), maxSize, "the compressed body must fit under maxSize so the decoder sees all of it")

	for _, decoder := range traceDecoders() {
		t.Run(decoder.name, func(t *testing.T) {
			var err error
			allocated := allocatedDuring(func() {
				err = decoder.decode(io.NopCloser(bytes.NewReader(compressed.Bytes())), zstdRequestInfo(), maxSize)
			})

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFailedParseBody)
			assert.Less(t, allocated, uint64(8<<20),
				"decoding a %d byte body allocated %d bytes; decoded output must be bounded by maxSize", len(body), allocated)
		})
	}
}

// TestZstdDefaultEncoderWindowIsAccepted guards the window bound: a payload
// compressed by a default-configured zstd encoder declares an 8MiB window,
// which is the largest window the encoder emits by default.
func TestZstdDefaultEncoderWindowIsAccepted(t *testing.T) {
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{{
					Key:   "service.name",
					Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "my-service"}},
				}},
			},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{
					Name: strings.Repeat("a", 12<<20),
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed, err := encodeBody(body, "zstd")
	require.NoError(t, err)

	for _, decoder := range traceDecoders() {
		t.Run(decoder.name, func(t *testing.T) {
			err := decoder.decode(io.NopCloser(strings.NewReader(compressed)), zstdRequestInfo(), defaultMaxRequestBodySize)
			require.NoError(t, err)
		})
	}
}
