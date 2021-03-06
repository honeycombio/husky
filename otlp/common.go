package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/klauspost/compress/zstd"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	apiKeyHeader             = "x-honeycomb-team"
	datasetHeader            = "x-honeycomb-dataset"
	proxyTokenHeader         = "x-honeycomb-proxy-token"
	proxyVersionHeader       = "x-basenji-version"
	userAgentHeader          = "user-agent"
	contentTypeHeader        = "content-type"
	contentEncodingHeader    = "content-encoding"
	gRPCAcceptEncodingHeader = "grpc-accept-encoding"
)

var legacyApiKeyPattern = regexp.MustCompile("^[0-9a-f]{32}$")

// TranslateOTLPRequestResult represents an OTLP request translated into Honeycomb-friendly structure
// RequestSize is total byte size of the entire OTLP request
// Batches represent events grouped by their target dataset
type TranslateOTLPRequestResult struct {
	RequestSize int
	Batches     []Batch
}

// Batch represents Honeycomb events grouped by their target dataset
// SizeBytes is the total byte size of the OTLP structure that represents this batch
type Batch struct {
	Dataset   string
	SizeBytes int
	Events    []Event
}

// Event represents a single Honeycomb event
type Event struct {
	Attributes map[string]interface{}
	Timestamp  time.Time
	SampleRate int32
}

// RequestInfo represents information parsed from either HTTP headers or gRPC metadata
type RequestInfo struct {
	ApiKey       string
	Dataset      string
	ProxyToken   string
	ProxyVersion string

	UserAgent          string
	ContentType        string
	ContentEncoding    string
	GRPCAcceptEncoding string
}

// ValidateTracesHeaders validates required headers/metadata for a trace OTLP request
func (ri *RequestInfo) ValidateTracesHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if isLegacy(ri.ApiKey) && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	if ri.ContentType != "application/protobuf" && ri.ContentType != "application/x-protobuf" {
		return ErrInvalidContentType
	}
	return nil
}

// ValidateMetricsHeaders validates required headers/metadata for a metric OTLP request
func (ri *RequestInfo) ValidateMetricsHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if isLegacy(ri.ApiKey) && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	if ri.ContentType != "application/protobuf" && ri.ContentType != "application/x-protobuf" {
		return ErrInvalidContentType
	}
	return nil
}

// ValidateLogsHeaders validates required headers/metadata for a logs OTLP request
func (ri *RequestInfo) ValidateLogsHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if isLegacy(ri.ApiKey) && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	if ri.ContentType != "application/protobuf" && ri.ContentType != "application/x-protobuf" {
		return ErrInvalidContentType
	}
	return nil
}

// GetRequestInfoFromGrpcMetadata parses relevant gRPC metadata from an incoming request context
func GetRequestInfoFromGrpcMetadata(ctx context.Context) RequestInfo {
	ri := RequestInfo{
		ContentType: "application/protobuf",
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ri.ApiKey = getValueFromMetadata(md, apiKeyHeader)
		ri.Dataset = getValueFromMetadata(md, datasetHeader)
		ri.ProxyToken = getValueFromMetadata(md, proxyTokenHeader)
		ri.ProxyVersion = getValueFromMetadata(md, proxyVersionHeader)
		ri.UserAgent = getValueFromMetadata(md, userAgentHeader)
		ri.ContentEncoding = getValueFromMetadata(md, contentEncodingHeader)
		ri.GRPCAcceptEncoding = getValueFromMetadata(md, gRPCAcceptEncodingHeader)
	}
	return ri
}

// GetRequestInfoFromHttpHeaders parses relevant incoming HTTP headers
func GetRequestInfoFromHttpHeaders(header http.Header) RequestInfo {
	return RequestInfo{
		ApiKey:             header.Get(apiKeyHeader),
		Dataset:            header.Get(datasetHeader),
		ProxyToken:         header.Get(proxyTokenHeader),
		ProxyVersion:       header.Get(proxyVersionHeader),
		UserAgent:          header.Get(userAgentHeader),
		ContentType:        header.Get(contentTypeHeader),
		ContentEncoding:    header.Get(contentEncodingHeader),
		GRPCAcceptEncoding: header.Get(gRPCAcceptEncodingHeader),
	}
}

func getValueFromMetadata(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func addAttributesToMap(attrs map[string]interface{}, attributes []*common.KeyValue) {
	for _, attr := range attributes {
		// ignore entries if the key is empty or value is nil
		if attr.Key == "" || attr.Value == nil {
			continue
		}
		if val := getValue(attr.Value); val != nil {
			attrs[attr.Key] = val
		}
	}
}

func getValue(value *common.AnyValue) interface{} {
	switch value.Value.(type) {
	case *common.AnyValue_StringValue:
		return value.GetStringValue()
	case *common.AnyValue_BoolValue:
		return value.GetBoolValue()
	case *common.AnyValue_DoubleValue:
		return value.GetDoubleValue()
	case *common.AnyValue_IntValue:
		return value.GetIntValue()
	case *common.AnyValue_ArrayValue:
		items := value.GetArrayValue().Values
		arr := make([]interface{}, len(items))
		for i := 0; i < len(items); i++ {
			arr[i] = getValue(items[i])
		}
		bytes, err := json.Marshal(arr)
		if err == nil {
			return string(bytes)
		}
	case *common.AnyValue_KvlistValue:
		items := value.GetKvlistValue().Values
		arr := make([]map[string]interface{}, len(items))
		for i := 0; i < len(items); i++ {
			arr[i] = map[string]interface{}{
				items[i].Key: getValue(items[i].Value),
			}
		}
		bytes, err := json.Marshal(arr)
		if err == nil {
			return string(bytes)
		}
	}
	return nil
}

func isLegacy(apiKey string) bool {
	return legacyApiKeyPattern.MatchString(apiKey)
}

func parseOtlpRequestBody(body io.ReadCloser, contentEncoding string, request protoreflect.ProtoMessage) error {
	defer body.Close()
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	bodyReader := bytes.NewReader(bodyBytes)

	var reader io.Reader
	switch contentEncoding {
	case "gzip":
		gzipReader, err := gzip.NewReader(bodyReader)
		defer gzipReader.Close()
		if err != nil {
			return err
		}
		reader = gzipReader
	case "zstd":
		zstdReader, err := zstd.NewReader(bodyReader)
		defer zstdReader.Close()
		if err != nil {
			return err
		}
		reader = zstdReader
	default:
		reader = bodyReader
	}

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(bytes, request)
	if err != nil {
		return err
	}

	return nil
}
