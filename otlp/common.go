package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	spb "google.golang.org/genproto/googleapis/rpc/status"
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

	// maxDepth is the maximum depth of a nested kvlist attribute that will be flattened.
	// If the depth is exceeded, the attribute should be added as a JSON string instead.
	maxDepth = 5
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
	if !IsContentTypeSupported(ri.ContentType) {
		return ErrInvalidContentType
	}
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if ri.hasLegacyKey() && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	return nil // no error, headers passed all the validations
}

// ValidateMetricsHeaders validates required headers/metadata for a metric OTLP request
func (ri *RequestInfo) ValidateMetricsHeaders() error {
	if !IsContentTypeSupported(ri.ContentType) {
		return ErrInvalidContentType
	}
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
	}
	if ri.hasLegacyKey() && len(ri.Dataset) == 0 {
		return ErrMissingDatasetHeader
	}
	return nil // no error, headers passed all the validations
}

// ValidateLogsHeaders validates required headers/metadata for a logs OTLP request
func (ri *RequestInfo) ValidateLogsHeaders() error {
	if !IsContentTypeSupported(ri.ContentType) {
		return ErrInvalidContentType
	}
	if len(ri.ApiKey) == 0 {
		return ErrMissingAPIKeyHeader
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

// WriteOtlpHttpFailureResponse is a quick way to write an otlp response for an error.
// It calls WriteOtlpHttpResponse, using the error's HttpStatusCode and building a Status
// using the error's string.
func WriteOtlpHttpFailureResponse(w http.ResponseWriter, r *http.Request, err OTLPError) error {
	return WriteOtlpHttpResponse(w, r, err.HTTPStatusCode, &spb.Status{Message: err.Error()})
}

// WriteOtlpHttpTraceSuccessResponse is a quick way to write an otlp success response for a trace request.
// It calls WriteOtlpHttpResponse, using the 200 status code and an empty ExportTraceServiceResponse
func WriteOtlpHttpTraceSuccessResponse(w http.ResponseWriter, r *http.Request) error {
	return WriteOtlpHttpResponse(w, r, http.StatusOK, &collectortrace.ExportTraceServiceResponse{})
}

// WriteOtlpHttpMetricSuccessResponse is a quick way to write an otlp success response for a metric request.
// It calls WriteOtlpHttpResponse, using the 200 status code and an empty ExportMetricsServiceResponse
func WriteOtlpHttpMetricSuccessResponse(w http.ResponseWriter, r *http.Request) error {
	return WriteOtlpHttpResponse(w, r, http.StatusOK, &collectormetrics.ExportMetricsServiceResponse{})
}

// WriteOtlpHttpLogSuccessResponse is a quick way to write an otlp success response for a trace request.
// It calls WriteOtlpHttpResponse, using the 200 status code and an empty ExportLogsServiceResponse
func WriteOtlpHttpLogSuccessResponse(w http.ResponseWriter, r *http.Request) error {
	return WriteOtlpHttpResponse(w, r, http.StatusOK, &collectorlogs.ExportLogsServiceResponse{})
}

// WriteOtlpHttpResponse writes a compliant OTLP HTTP response to the given http.ResponseWriter
// based on the provided `contentType`. If an error occurs while marshalling to either json or proto it is returned
// before the http.ResponseWriter is updated. If an error occurs while writing to the http.ResponseWriter it is ignored.
// If an invalid content type is provided, a 415 Unsupported Media Type via text/plain is returned.
func WriteOtlpHttpResponse(w http.ResponseWriter, r *http.Request, statusCode int, m proto.Message) error {
	if r == nil {
		return fmt.Errorf("nil Request")
	}

	contentType := r.Header.Get("Content-Type")
	var body []byte
	var serializationError error
	switch contentType {
	case "application/json":
		body, serializationError = protojson.Marshal(m)
	case "application/x-protobuf", "application/protobuf":
		body, serializationError = proto.Marshal(m)
	default:
		// If the content type is not supported, return a 415 Unsupported Media Type via text/plain
		body = []byte(ErrInvalidContentType.Message)
		contentType = "text/plain"
		statusCode = ErrInvalidContentType.HTTPStatusCode
	}
	if serializationError != nil {
		return serializationError
	}

	// At this point we're committed
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
	return nil
}

func getValueFromMetadata(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// AddAttributesToMap adds attributes to a map, extracting the underlying attribute data type.
// Supported types are string, bool, double, int, bytes, array, and kvlist.
// kvlist attributes are flattened to a depth of (maxDepth), if the depth is exceeded, the attribute is added as a JSON string.
// Bytes and array values are always added as JSON strings.
func AddAttributesToMap(attrs map[string]interface{}, attributes []*common.KeyValue) {
	for _, attr := range attributes {
		// ignore entries if the key is empty or value is nil
		if attr.Key == "" || attr.Value == nil {
			continue
		}
		addAttributeToMap(attrs, attr.Key, attr.Value, 0)
	}
}

func getResourceAttributes(resource *resource.Resource) map[string]interface{} {
	attrs := map[string]interface{}{}
	if resource != nil {
		AddAttributesToMap(attrs, resource.Attributes)
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
		AddAttributesToMap(attrs, scope.Attributes)
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

// addAttributeToMap adds an attribute to a map, extracting the underlying attribute data type.
// Supported types are string, bool, double, int, bytes, array, and kvlist.
// kvlist attributes are flattened to a depth of (maxDepth), if the depth is exceeded, the attribute is added as a JSON string.
// Bytes and array values are always added as JSON strings.
func addAttributeToMap(result map[string]interface{}, key string, value *common.AnyValue, depth int) {
	switch value.Value.(type) {
	case *common.AnyValue_StringValue:
		result[key] = value.GetStringValue()
	case *common.AnyValue_BoolValue:
		result[key] = value.GetBoolValue()
	case *common.AnyValue_DoubleValue:
		result[key] = value.GetDoubleValue()
	case *common.AnyValue_IntValue:
		result[key] = value.GetIntValue()
	case *common.AnyValue_BytesValue, *common.AnyValue_ArrayValue:
		addAttributeToMapAsJson(result, key, value)
	case *common.AnyValue_KvlistValue:
		for _, entry := range value.GetKvlistValue().Values {
			k := key + "." + entry.Key
			if depth < maxDepth {
				addAttributeToMap(result, k, entry.Value, depth+1)
			} else {
				addAttributeToMapAsJson(result, k, entry.Value)
			}
		}
	}
}

// addAttributeToMapAsJson adds an attribute to a map as a JSON string.
// Uses limitedWriter to ensure that the string can't be bigger than the maximum field size and
// helps reduce allocation and copying.
// Note that an Encoder emits JSON with a trailing newline because it's intended for use
// in streaming. This is correct but sometimes surprising and the tests need to expect it.
func addAttributeToMapAsJson(attrs map[string]interface{}, key string, value *common.AnyValue) int {
	val := getMarshallableValue(value)
	w := newLimitedWriter(fieldSizeMax)
	if err := json.NewEncoder(w).Encode(val); err != nil {
		// TODO: log error or report error when we have a way to do so
		return 0
	}
	attrs[key] = w.String()
	return w.truncatedBytes
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

// BytesToTraceID returns an ID suitable for use for spans and traces. Before
// encoding the bytes as a hex string, we want to handle cases where we are
// given 128-bit IDs with zero padding, e.g. 0000000000000000f798a1e7f33c8af6.
// There are many ways to achieve this, but careful benchmarking and testing
// showed the below as the most performant, avoiding memory allocations
// and the use of flexible but expensive library functions. As this is hot code,
// it seemed worthwhile to do it this way.
func BytesToTraceID(traceID []byte) string {
	var encoded []byte
	switch len(traceID) {
	case traceIDLongLength: // 16 bytes, trim leading 8 bytes if all 0's
		if shouldTrimTraceId(traceID) {
			encoded = make([]byte, 16)
			traceID = traceID[traceIDShortLength:]
		} else {
			encoded = make([]byte, 32)
		}
		hex.Encode(encoded, traceID)
	case traceIDShortLength: // 8 bytes
		encoded = make([]byte, 16)
		hex.Encode(encoded, traceID)
	case traceIDb64Length: // 24 bytes
		// The spec says that traceID and spanID should be encoded as hex, but
		// the protobuf system is interpreting them as b64, so we need to
		// reverse them back to b64 which gives us the original hex.
		encoded = make([]byte, base64.StdEncoding.EncodedLen(len(traceID)))
		base64.StdEncoding.Encode(encoded, traceID)
	default:
		encoded = make([]byte, len(traceID)*2)
		hex.Encode(encoded, traceID)
	}
	return string(encoded)
}

func BytesToSpanID(spanID []byte) string {
	var encoded []byte
	switch len(spanID) {
	case spanIDb64Length: // 12 bytes
		// The spec says that traceID and spanID should be encoded as hex, but
		// the protobuf system is interpreting them as b64, so we need to
		// reverse them back to b64 which gives us the original hex.
		encoded = make([]byte, base64.StdEncoding.EncodedLen(len(spanID)))
		base64.StdEncoding.Encode(encoded, spanID)
	default:
		encoded = make([]byte, len(spanID)*2)
		hex.Encode(encoded, spanID)
	}
	return string(encoded)
}

func shouldTrimTraceId(traceID []byte) bool {
	for i := 0; i < 8; i++ {
		if traceID[i] != 0 {
			return false
		}
	}
	return true
}
