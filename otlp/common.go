package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	apiKeyHeader             = "x-honeycomb-team"
	datasetHeader            = "x-honeycomb-dataset"
	userAgentHeader          = "user-agent"
	contentTypeHeader        = "content-type"
	contentEncodingHeader    = "content-encoding"
	gRPCAcceptEncodingHeader = "grpc-accept-encoding"
	defaultServiceName       = "unknown_service"
	unknownLogSource         = "unknown_log_source"
	resourceAttrsPrefix      = "resource."
	scopeAttrsPrefix         = "scope."
	spanAttrsPrefix          = "span."
	seventAttrsPrefix        = "sevent."
	slinkAttrsPrefix         = "slink."
)

// fieldSizeMax is the maximum size of a field that will be accepted by honeycomb.
// The limit is enforced in retriever (in private honeycomb code), in varstring.go.
const fieldSizeMax = math.MaxUint16

var (
	legacyApiKeyPattern = regexp.MustCompile("^[0-9a-f]{32}$")
	// Incoming OpenTelemetry HTTP Content-Types (e.g. "application/protobuf") we support
	supportedContentTypes = []string{
		"application/protobuf",
		"application/x-protobuf",
		"application/json",
	}
	// Incoming Content-Encodings we support. "" included as a stand in for "not given, assume uncompressed"
	supportedContentEncodings = []string{"", "gzip", "zstd"}

	// Use json-iterator for better performance
	json = jsoniter.ConfigCompatibleWithStandardLibrary

	// List of per-lang instrumentation library prefixes
	instrumentationLibraryPrefixes = []string{
		"io.opentelemetry",                            // Java,
		"opentelemetry.instrumentation",               // Python
		"OpenTelemetry.Instrumentation",               // .NET
		"OpenTelemetry::Instrumentation",              // Ruby
		"go.opentelemetry.io/contrib/instrumentation", // Go
		"@opentelemetry/instrumentation",              // JS
	}
)

// List of HTTP Content Types supported for OTLP ingest.
func GetSupportedContentTypes() []string {
	return supportedContentTypes
}

// Check whether we support a given HTTP Content Type for OTLP.
func IsContentTypeSupported(contentType string) bool {
	for _, supportedType := range supportedContentTypes {
		if contentType == supportedType {
			return true
		}
	}
	return false
}

// List of HTTP Content Encodings supported for OTLP ingest.
func GetSupportedContentEncodings() []string {
	return supportedContentEncodings
}

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
	ApiKey  string
	Dataset string

	UserAgent          string
	ContentType        string
	ContentEncoding    string
	GRPCAcceptEncoding string
}

func (ri RequestInfo) hasLegacyKey() bool {
	return legacyApiKeyPattern.MatchString(ri.ApiKey)
}

// ValidateTracesHeaders validates required headers/metadata for a trace OTLP request
func (ri *RequestInfo) ValidateTracesHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if ri.hasLegacyKey() && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	if !IsContentTypeSupported(ri.ContentType) {
		return ErrInvalidContentType
	}
	return nil // no error, headers passed all the validations
}

// ValidateMetricsHeaders validates required headers/metadata for a metric OTLP request
func (ri *RequestInfo) ValidateMetricsHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if ri.hasLegacyKey() && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	if !IsContentTypeSupported(ri.ContentType) {
		return ErrInvalidContentType
	}
	return nil // no error, headers passed all the validations
}

// ValidateLogsHeaders validates required headers/metadata for a logs OTLP request
func (ri *RequestInfo) ValidateLogsHeaders() error {
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if !IsContentTypeSupported(ri.ContentType) {
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

func addAttributesToMap(prefix string, attrs map[string]interface{}, attributes []*common.KeyValue) {
	for _, attr := range attributes {
		// ignore entries if the key is empty or value is nil
		if attr.Key == "" || attr.Value == nil {
			continue
		}
		if val, truncatedBytes := getValue(attr.Value); val != nil {
			attrs[prefix+attr.Key] = val
			if truncatedBytes != 0 {
				// if we trim a field, add telemetry about it; because we trim at 64K and
				// a whole span can't be more than 100K, this can't happen more than once
				// for a single span. If we ever change those limits, this will need to
				// become additive.
				attrs["meta.truncated_bytes"] = val
				attrs["meta.truncated_field"] = attr.Key
			}
		}
	}
}

func getResourceAttributes(resource *resource.Resource) map[string]interface{} {
	attrs := map[string]interface{}{}
	if resource != nil {
		addAttributesToMap(resourceAttrsPrefix, attrs, resource.Attributes)
	}
	return attrs
}

func getScopeAttributes(scope *common.InstrumentationScope) map[string]interface{} {
	attrs := map[string]interface{}{}
	if scope != nil {
		if scope.Name != "" {
			attrs["library.name"] = scope.Name
			if isInstrumentationLibrary(scope.Name) {
				attrs["telemetry.instrumentation_library"] = true
			}
		}
		if scope.Version != "" {
			attrs["library.version"] = scope.Version
		}
		addAttributesToMap(scopeAttrsPrefix,attrs, scope.Attributes)
	}
	return attrs
}

func isInstrumentationLibrary(libraryName string) bool {
	for _, prefix := range instrumentationLibraryPrefixes {
		if strings.HasPrefix(libraryName, prefix) {
			return true
		}
	}
	return false
}

func getDataset(ri RequestInfo, attrs map[string]interface{}) string {
	var dataset string
	if ri.hasLegacyKey() {
		dataset = ri.Dataset
	} else {
		serviceName, ok := attrs["service.name"].(string)
		if !ok ||
			strings.TrimSpace(serviceName) == "" ||
			strings.HasPrefix(serviceName, "unknown_service") {
			dataset = defaultServiceName
		} else {
			dataset = strings.TrimSpace(serviceName)
		}
	}
	return dataset
}

func getLogsDataset(ri RequestInfo, attrs map[string]interface{}) string {
	var dataset string
	serviceName, ok := attrs["service.name"].(string)
	if !ok || strings.TrimSpace(serviceName) == "" || strings.HasPrefix(serviceName, "unknown_service") {
		if strings.TrimSpace(ri.Dataset) == "" {
			dataset = unknownLogSource
		} else {
			dataset = ri.Dataset
		}
	} else {
		dataset = strings.TrimSpace(serviceName)
	}
	return dataset
}

// limitedWriter is a writer that will stop writing after reaching its max,
// but continue to lie to the caller that it was successful.
// It's a wrapper around strings.Builder for efficiency.
type limitedWriter struct {
	max            int
	w              strings.Builder
	truncatedBytes int
}

func newLimitedWriter(n int) *limitedWriter {
	return &limitedWriter{max: n}
}

func (l *limitedWriter) Write(b []byte) (int, error) {
	n := len(b)
	if n+l.w.Len() > l.max {
		b = b[:l.max-l.w.Len()]
		l.truncatedBytes += n - len(b)
	}
	_, err := l.w.Write(b)
	// return the value that the user sent us
	// so they think we wrote it all
	return n, err
}

func (l *limitedWriter) String() string {
	return l.w.String()
}

// Returns a value that can be marshalled by JSON -- aggregate data structures
// are returned as native Go aggregates (maps and slices), rather than marshalled
// strings (we expect the caller to do the marshalling).
func getMarshallableValue(value *common.AnyValue) interface{} {
	switch value.Value.(type) {
	case *common.AnyValue_StringValue:
		return value.GetStringValue()
	case *common.AnyValue_BoolValue:
		return value.GetBoolValue()
	case *common.AnyValue_DoubleValue:
		return value.GetDoubleValue()
	case *common.AnyValue_IntValue:
		return value.GetIntValue()
	case *common.AnyValue_BytesValue:
		return value.GetBytesValue()
	case *common.AnyValue_ArrayValue:
		items := value.GetArrayValue().Values
		arr := make([]interface{}, len(items))
		for i := 0; i < len(items); i++ {
			arr[i] = getMarshallableValue(items[i])
		}
		return arr
	case *common.AnyValue_KvlistValue:
		items := value.GetKvlistValue().Values
		m := make(map[string]interface{}, len(items))
		for i := 0; i < len(items); i++ {
			m[items[i].GetKey()] = getMarshallableValue(items[i].Value)
		}
		return m
	}
	return nil
}

// This function returns a value that can be handled by Honeycomb -- it must be one of:
// string, int, bool, float. All other values are converted to strings containing JSON.
func getValue(value *common.AnyValue) (result interface{}, truncatedBytes int) {
	switch value.Value.(type) {
	case *common.AnyValue_StringValue:
		return value.GetStringValue(), 0
	case *common.AnyValue_BoolValue:
		return value.GetBoolValue(), 0
	case *common.AnyValue_DoubleValue:
		return value.GetDoubleValue(), 0
	case *common.AnyValue_IntValue:
		return value.GetIntValue(), 0
	// These types are all be marshalled to a string after conversion to Honeycomb-safe values.
	// We use our limitedWriter to ensure that the string can't be bigger than the allowable,
	// and it also minimizes allocations.
	// Note that an Encoder emits JSON with a trailing newline because it's intended for use
	// in streaming. This is correct but sometimes surprising and the tests need to expect it.
	case *common.AnyValue_ArrayValue, *common.AnyValue_KvlistValue, *common.AnyValue_BytesValue:
		arr := getMarshallableValue(value)
		w := newLimitedWriter(fieldSizeMax)
		enc := json.NewEncoder(w)
		err := enc.Encode(arr)
		if err == nil {
			return w.String(), w.truncatedBytes
		}
	}
	return nil, 0
}

func parseOtlpRequestBody(body io.ReadCloser, contentType string, contentEncoding string, request protoreflect.ProtoMessage) error {
	defer body.Close()
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	bodyReader := bytes.NewReader(bodyBytes)

	var reader io.Reader
	switch contentEncoding {
	case "gzip":
		gzipReader, err := gzip.NewReader(bodyReader)
		if err != nil {
			return err
		}
		defer gzipReader.Close()
		reader = gzipReader
	case "zstd":
		zstdReader, err := zstd.NewReader(bodyReader)
		if err != nil {
			return err
		}
		defer zstdReader.Close()
		reader = zstdReader
	default:
		reader = bodyReader
	}

	bytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	switch contentType {
	case "application/protobuf", "application/x-protobuf":
		err = proto.Unmarshal(bytes, request)
	case "application/json":
		err = protojson.Unmarshal(bytes, request)
	default:
		return ErrInvalidContentType
	}
	if err != nil {
		return err
	}

	return nil
}
