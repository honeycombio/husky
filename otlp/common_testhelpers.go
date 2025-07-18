package otlp

import (
	"bytes"
	"compress/gzip"
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
)

// The collector<signal>.Export<signal>ServiceRequest structs generated from proto
// definitions each implment the interfaces embedded here. This combined interface
// allows for a single transformer function to receive a request struct
// and return a request serialized in a variety of formats.
type marshalableOtlpRequest interface {
	protoiface.MessageV1      // to protobuf
	protoreflect.ProtoMessage // to protojson
}

// transforms an OTLP signal request proto struct into a supported Content-Type
// and then encoded appropriately for an HTTP request
func prepareOtlpRequestHttpBody(req marshalableOtlpRequest, contentType string, encoding string) (string, error) {
	var bodyBytes []byte
	var err error
	switch contentType {
	case "application/protobuf", "application/x-protobuf":
		bodyBytes, err = proto.Marshal(req)
		if err != nil {
			return "", err
		}
	case "application/json":
		bodyBytes, err = protojson.Marshal(req)
		if err != nil {
			return "", err
		}
	default:
		return "", errors.New("Unknown content-type '" + contentType + "' given for test case. This probably won't go well.")
	}

	body, err := encodeBody(bodyBytes, encoding)
	return body, err
}

// Encode a slice of bytes destined to be the body of an HTTP request
// to a target encoding.
func encodeBody(body []byte, encoding string) (string, error) {
	encodedBytes := new(bytes.Buffer)
	switch encoding {
	case "":
		encodedBytes.Write(body)
	case "gzip":
		w := gzip.NewWriter(encodedBytes)
		w.Write(body)
		w.Close()
	case "zstd":
		w, _ := zstd.NewWriter(encodedBytes)
		w.Write(body)
		w.Close()
	default:
		return "", errors.New("Unknown content-encoding '" + encoding + "' given for test case. This probably won't go well.")
	}
	return encodedBytes.String(), nil
}

// Removes "application/" prefix from Content Type values to reduce the
// nesting of test case name in verbose test output and results.
// e.g. "application/x-protobuf" -> "x-protobuf"
func testCaseNameForContentType(contentType string) string {
	return strings.Split(contentType, "/")[1]
}

// Return a friendlier string for the test cases where Content Encoding
// is ambiguous, e.g. no encoding given is blank, so we give it a
// meaningful name here.
func testCaseNameForEncoding(encoding string) string {
	if encoding == "" {
		return "no encoding given assume uncompressed"
	} else {
		return encoding
	}
}

// decodeMessagePackAttributes unmarshals MessagePack data into a map for testing
func decodeMessagePackAttributes(t testing.TB, data []byte) map[string]any {
	decoded, _, err := msgp.ReadIntfBytes(data)
	require.NoError(t, err)
	if asMap, ok := decoded.(map[string]any); ok {
		return asMap
	}
	t.Fatal("this doesn't look like a msgp map")
	return nil
}
