package otlp

// OTLP Trace Message Hierarchy and Honeycomb Translation:
//
// ExportTraceServiceRequest
// └── ResourceSpans (repeated) -> Each ResourceSpans creates events grouped by dataset
//     ├── Resource
//     │   └── attributes: KeyValue (repeated) -> Copied to all events from this resource
//     │                                          Special: "service.name" determines dataset for non-classic keys
//     ├── ScopeSpans (repeated)
//     │   ├── InstrumentationScope
//     │   │   ├── name: string -> "library.name", sets "telemetry.instrumentation_library"=true if recognized
//     │   │   ├── version: string -> "library.version"
//     │   │   └── attributes: KeyValue (repeated) -> Merged into all spans from this scope
//     │   └── Span (repeated) -> Each span becomes an event, plus events for span events/links
//     │       ├── trace_id: bytes -> "trace.trace_id" (hex encoded)
//     │       ├── span_id: bytes -> "trace.span_id" (hex encoded)
//     │       ├── parent_span_id: bytes -> "trace.parent_id" (hex encoded, if present)
//     │       ├── name: string -> "name"
//     │       ├── kind: Span.SpanKind (enum) -> "span.kind" and "type" (string: client/server/producer/consumer/internal)
//     │       ├── start_time_unix_nano: fixed64 -> Event.Timestamp
//     │       ├── end_time_unix_nano: fixed64 -> Used to calculate "duration_ms" = (end - start) / 1e6
//     │       ├── attributes: KeyValue (repeated) -> Merged into span event
//     │       │                                      Special: "sampleRate" determines Event.SampleRate
//     │       ├── events: Span.Event (repeated) -> Each becomes a separate event
//     │       │   ├── time_unix_nano: fixed64 -> Event.Timestamp
//     │       │   ├── name: string -> "name"
//     │       │   └── attributes: KeyValue (repeated) -> Merged with resource/scope attrs
//     │       │                                          Additional fields:
//     │       │                                          - "trace.trace_id" (from parent span)
//     │       │                                          - "trace.parent_id" (parent span's span_id)
//     │       │                                          - "parent_name" (parent span's name)
//     │       │                                          - "meta.annotation_type" = "span_event"
//     │       │                                          - "meta.signal_type" = "trace"
//     │       │                                          - "meta.time_since_span_start_ms" = (event_time - span_start) / 1e6
//     │       │                                          - "error" = true (if parent span has error status)
//     │       │                                          Exception events: attrs copied to parent span if name="exception"
//     │       ├── links: Span.Link (repeated) -> Each becomes a separate event
//     │       │   ├── trace_id: bytes -> "trace.link.trace_id" (hex encoded)
//     │       │   ├── span_id: bytes -> "trace.link.span_id" (hex encoded)
//     │       │   ├── trace_state: string -> (merged into attributes)
//     │       │   └── attributes: KeyValue (repeated) -> Merged with resource/scope attrs
//     │       │                                          Additional fields:
//     │       │                                          - "trace.trace_id" (from parent span)
//     │       │                                          - "trace.parent_id" (parent span's span_id)
//     │       │                                          - "parent_name" (parent span's name)
//     │       │                                          - "meta.annotation_type" = "link"
//     │       │                                          - "meta.signal_type" = "trace"
//     │       │                                          - "error" = true (if parent span has error status)
//     │       ├── status: Status
//     │       │   ├── code: Status.StatusCode (enum) -> "status_code" (int), "status.code" (string)
//     │       │   │                                     Sets "error"=true if code=2 (ERROR)
//     │       │   └── message: string -> "status_message" and "status.message"
//     │       └── trace_state: string -> "trace.trace_state", used for sampleRate calculation
//     └── schema_url: string -> (ignored)
//
// Additional span event attributes:
// - "span.num_events": Count of span events
// - "span.num_links": Count of span links
// - "meta.signal_type": Always "trace"
// - "meta.invalid_duration": true if duration < 0
// - "meta.invalid_time_since_span_start": true if span event time < span start time
//
// Common types:
// - KeyValue: { key: string, value: AnyValue }
// - AnyValue: oneof { string_value, bool_value, int_value, double_value, array_value, kvlist_value, bytes_value }
//             (array_value and bytes_value are JSON-encoded, kvlist_value is flattened with dot notation)

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/honeycombio/husky"
	"github.com/tinylib/msgp/msgp"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

var (
	ErrInvalidLength        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflow          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroup = fmt.Errorf("proto: unexpected end of group")
)

type TranslateOTLPRequestResultMsgp struct {
	RequestSize int
	Batches     []BatchMsgp
}

// BatchMsgp represents Honeycomb events grouped by their target dataset
type BatchMsgp struct {
	Dataset string
	Events  []EventMsgp
}

// Event represents a single Honeycomb event
type EventMsgp struct {
	// Here, Attributes is a map[string]any which is messagepack-encoded.
	Attributes []byte
	Timestamp  time.Time
	SampleRate int32
}

// Holds messagepack-encode keyvalues, without a header.
type msgpAttributes struct {
	buf   []byte
	count int

	// memoized metadata fields we'll need internally
	serviceName string
	sampleRate  int32
	isError     bool
}

func (b *msgpAttributes) addAny(key []byte, value any) error {
	var err error
	b.count++
	b.buf = msgp.AppendStringFromBytes(b.buf, key)
	b.buf, err = msgp.AppendIntf(b.buf, value)
	return err
}

func (b *msgpAttributes) addString(key []byte, value []byte) {
	b.buf = msgp.AppendStringFromBytes(b.buf, key)
	b.buf = msgp.AppendStringFromBytes(b.buf, value)
	b.count++
}

// addInt64 adds a string key with int64 value
func (b *msgpAttributes) addInt64(key []byte, value int64) {
	b.buf = msgp.AppendStringFromBytes(b.buf, key)
	b.buf = msgp.AppendInt64(b.buf, value)
	b.count++
}

// addFloat64 adds a string key with float64 value
func (b *msgpAttributes) addFloat64(key []byte, value float64) {
	b.buf = msgp.AppendStringFromBytes(b.buf, key)
	b.buf = msgp.AppendFloat64(b.buf, value)
	b.count++
}

// addBool adds a string key with bool value
func (b *msgpAttributes) addBool(key []byte, value bool) {
	b.buf = msgp.AppendStringFromBytes(b.buf, key)
	b.buf = msgp.AppendBool(b.buf, value)
	b.count++
}

// Wraps a msgpAttributes, but includes a map header indicating the value count.
// Used to produce complete Attributes payloads.
type msgpAttributesMap struct {
	msgpAttributes
}

func newMsgpMap() msgpAttributesMap {
	var m msgpAttributesMap
	m.buf = append(m.buf, 0xde, 0, 0)
	return m
}

func (m *msgpAttributesMap) addAttributes(add *msgpAttributes) {
	m.buf = append(m.buf, add.buf...)
	m.count += add.count
	if !m.isError {
		m.isError = add.isError
	}
	if m.serviceName == "" {
		m.serviceName = add.serviceName
	}
	if m.sampleRate == 0 {
		m.sampleRate = add.sampleRate
	}
}

// Returns serialized msgp map including the header, suitable for transmission.
func (m *msgpAttributesMap) finalize() ([]byte, error) {
	if m.count > 65536 {
		return nil, errors.New("too many attributes")
	}
	m.buf[1] = byte(m.count >> 8)
	m.buf[2] = byte(m.count)
	return m.buf, nil
}

// UnmarshalTraceRequestDirectMsgp translates a serialized OTLP trace request directly
// into Honeycomb-friendly structure without creating intermediate proto structs.
// This is a performance optimization that avoids the overhead of unmarshaling
// into proto structs first.
// Why does the code look like this? Because it's derived from gogo's generated
// code, and carries over some of the style conventions so that it will hopefully
// be relatively easy to update it in future, should that be necessary.
// Fortunately this part of OTLP is marked as "stable" so we don't expect changes.
func UnmarshalTraceRequestDirectMsgp(ctx context.Context, data []byte, ri RequestInfo) (*TranslateOTLPRequestResultMsgp, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}

	result := &TranslateOTLPRequestResultMsgp{
		RequestSize: len(data),
		Batches:     []BatchMsgp{},
	}

	// Parse the protobuf wire format
	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return nil, err
		}
		if wireType == 4 {
			return nil, fmt.Errorf("proto: ExportTraceServiceRequest: wiretype end group for non-group")
		}

		switch fieldNum {
		case 1: // ResourceSpans
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}

			// Parse the ResourceSpans
			err = unmarshalResourceSpans(ctx, slice, ri, result)
			if err != nil {
				return nil, err
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	if iNdEx > l {
		return nil, io.ErrUnexpectedEOF
	}

	return result, nil
}

// unmarshalResourceSpans parses a ResourceSpans message
func unmarshalResourceSpans(
	ctx context.Context,
	data []byte,
	ri RequestInfo,
	result *TranslateOTLPRequestResultMsgp,
) error {
	var resourceAttrs msgpAttributes
	var dataset string

	l := len(data)
	iNdEx := 0

	// We need to parse the resource before the spans, but they're not gauranteed
	// to arrive in this order. So, walk the data twice to get each field.
loop:
	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // resource
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse resource
			err = unmarshalResource(ctx, slice, &resourceAttrs)
			if err != nil {
				return err
			}
			break loop
		default:
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Determine dataset from resource attributes
	dataset = getDatasetFromMsgpAttr(ri, &resourceAttrs)

	// Now parse the spans.
	iNdEx = 0
	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 2: // scope_spans, repeated
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			err = unmarshalScopeSpans(ctx, slice, ri, &resourceAttrs, dataset, result)
			if err != nil {
				return err
			}

		default:
			// Note field 3 is schema_url, but we don't use it.
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalResource parses a Resource message
func unmarshalResource(ctx context.Context, data []byte, attrs *msgpAttributes) error {
	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // attributes
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse KeyValue
			err = unmarshalKeyValue(ctx, slice, attrs, 0)
			if err != nil {
				return err
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// parseKeyValue parses a KeyValue message and returns the key and value
func parseKeyValue(ctx context.Context, data []byte) ([]byte, any, error) {
	var key []byte
	var value any

	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return nil, nil, err
		}

		switch fieldNum {
		case 1: // key
			key, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, nil, err
			}

		case 2: // value
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, nil, err
			}

			// Parse AnyValue
			val, err := unmarshalAnyValue(ctx, slice)
			if err != nil {
				return nil, nil, err
			}
			value = val

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, nil, err
			}
		}
	}

	return key, value, nil
}

// unmarshalKeyValue parses a KeyValue message and adds it to msgpAttributes
func unmarshalKeyValue(ctx context.Context, data []byte, attrs *msgpAttributes, depth int) error {
	key, value, err := parseKeyValue(ctx, data)
	if err != nil {
		return err
	}

	// KeyValue messages should have both key and value
	if len(key) > 0 && value != nil {
		// We don't include sample rates in the attribute payload, so use the value
		// and then return here.
		if bytes.Equal(key, []byte("sampleRate")) {
			attrs.sampleRate = getSampleRateFromAnyValue(value)
			return nil
		}
		if bytes.Equal(key, []byte("SampleRate")) {
			// if both sample rate keys exist, "sampleRate" should win. So this
			// one defers to any existing value.
			if attrs.sampleRate == 0 {
				attrs.sampleRate = getSampleRateFromAnyValue(value)
			}
			return nil
		}

		// Handle different value types to match legacy behavior
		switch v := value.(type) {
		case []byte:
			// Bytes are JSON encoded - match the legacy behavior
			// This telemetry attribute tracks when we receive byte array attributes
			husky.AddTelemetryAttribute(ctx, "received_bytes_attr_type", true)
			err = attrs.addAny(key, addAttributeToMapAsJsonDirect(v))
			if err != nil {
				return err
			}
		case []any:
			// Arrays are JSON encoded
			husky.AddTelemetryAttribute(ctx, "received_array_attr_type", true)
			err = attrs.addAny(key, addAttributeToMapAsJsonDirect(v))
			if err != nil {
				return err
			}
		case map[string]any:
			// Kvlists are flattened with dot notation
			husky.AddTelemetryAttributes(ctx, map[string]any{
				"received_kvlist_attr_type": true,
				"kvlist_max_depth":          depth,
			})
			if depth < maxDepth {
				// Flatten the kvlist
				for k, v := range v {
					flatKey := append(key, '.')
					flatKey = append(flatKey, k...)

					// Nested complex types (arrays, bytes, nested kvlists) are JSON encoded
					switch v.(type) {
					case []byte, []any, map[string]any:
						err = attrs.addAny(flatKey, addAttributeToMapAsJsonDirect(v))
						if err != nil {
							return err
						}
					default:
						err = attrs.addAny(flatKey, v)
						if err != nil {
							return err
						}
					}
				}
			} else {
				// Max depth exceeded, JSON encode the whole thing
				err = attrs.addAny(key, addAttributeToMapAsJsonDirect(v))
				if err != nil {
					return err
				}
			}
		default:
			// Simple types - just encode directly
			err = attrs.addAny(key, v)
		}

		// If this is the service name, note it.
		if asStr, ok := value.(string); ok && bytes.Equal(key, []byte("service.name")) {
			attrs.serviceName = asStr
		}
	}

	return nil
}

// unmarshalAnyValue parses an AnyValue message
func unmarshalAnyValue(ctx context.Context, data []byte) (any, error) {
	l := len(data)
	iNdEx := 0
	var result any

	// Handle empty message
	if l == 0 {
		return nil, nil
	}

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return nil, err
		}

		switch fieldNum {
		case 1: // string_value
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			result = string(slice)

		case 2: // bool_value
			if wireType != 0 {
				return nil, fmt.Errorf("proto: wrong wireType = %d for field BoolValue", wireType)
			}
			v := int(decodeVarint(data, &iNdEx))
			result = v != 0

		case 3: // int_value
			if wireType != 0 {
				return nil, fmt.Errorf("proto: wrong wireType = %d for field IntValue", wireType)
			}
			v := int64(decodeVarint(data, &iNdEx))
			result = v

		case 4: // double_value
			v, err := decodeWireType1(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			result = math.Float64frombits(v)

		case 5: // array_value
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			// Parse ArrayValue message and return as []any
			arr, err := unmarshalArrayValue(ctx, slice)
			if err != nil {
				return nil, err
			}
			result = arr

		case 6: // kvlist_value
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			// Parse KeyValueList message and return as map[string]any
			m, err := unmarshalKvlistValue(ctx, slice)
			if err != nil {
				return nil, err
			}
			result = m

		case 7: // bytes_value
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			// Match the behavior of the legacy code - return as []byte which will be JSON encoded later
			// Make a copy of the slice to avoid issues with the underlying buffer
			b := make([]byte, len(slice))
			copy(b, slice)
			result = b

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// unmarshalArrayValue parses an ArrayValue message and returns []any
func unmarshalArrayValue(ctx context.Context, data []byte) ([]any, error) {
	var values []any
	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			// If we get EOF at the start of an empty message, return empty array
			if err == io.ErrUnexpectedEOF && len(values) == 0 && iNdEx == 0 {
				return values, nil
			}
			return nil, err
		}

		switch fieldNum {
		case 1: // values (repeated)
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			// Parse AnyValue
			val, err := unmarshalAnyValue(ctx, slice)
			if err != nil {
				return nil, err
			}
			if val != nil {
				values = append(values, val)
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return values, nil
}

// unmarshalKeyValueToMap parses a KeyValue message and adds it to a map
func unmarshalKeyValueToMap(ctx context.Context, data []byte, result map[string]any) error {
	key, value, err := parseKeyValue(ctx, data)
	if err != nil {
		return err
	}

	// KeyValue messages should have both key and value
	if len(key) > 0 && value != nil {
		result[string(key)] = value
	}

	return nil
}

// unmarshalKvlistValue parses a KeyValueList message and returns map[string]any
func unmarshalKvlistValue(ctx context.Context, data []byte) (map[string]any, error) {
	result := make(map[string]any)
	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			// If we get EOF at the start of an empty message, return empty map
			if err == io.ErrUnexpectedEOF && len(result) == 0 && iNdEx == 0 {
				return result, nil
			}
			return nil, err
		}

		switch fieldNum {
		case 1: // values (repeated KeyValue)
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			// Parse KeyValue and add to map
			if err := unmarshalKeyValueToMap(ctx, slice, result); err != nil {
				return nil, err
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// unmarshalScopeSpans parses a ScopeSpans message
func unmarshalScopeSpans(
	ctx context.Context,
	data []byte,
	ri RequestInfo,
	resourceAttrs *msgpAttributes,
	dataset string,
	result *TranslateOTLPRequestResultMsgp,
) error {
	// Get the instrumentation scope first
	var scopeAttrs msgpAttributes
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // scope
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse InstrumentationScope
			err = unmarshalInstrumentationScope(ctx, slice, &scopeAttrs)
			if err != nil {
				return err
			}
		default:
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	iNdEx = 0
	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 2: // spans
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse Span
			err = unmarshalSpan(ctx, slice, ri, resourceAttrs, &scopeAttrs, dataset, result)
			if err != nil {
				return err
			}

		default:
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalInstrumentationScope parses an InstrumentationScope message
func unmarshalInstrumentationScope(ctx context.Context, data []byte, attrs *msgpAttributes) error {
	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // name
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			if len(slice) > 0 {
				attrs.addString([]byte("library.name"), slice)
				if isInstrumentationLibrary(string(slice)) {
					attrs.addBool([]byte("telemetry.instrumentation_library"), true)
				}
			}

		case 2: // version
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			if len(slice) > 0 {
				attrs.addString([]byte("library.version"), slice)
			}

		case 3: // attributes
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse KeyValue
			err = unmarshalKeyValue(ctx, slice, attrs, 0)
			if err != nil {
				return err
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalSpan parses a Span message and creates an event
func unmarshalSpan(
	ctx context.Context,
	data []byte,
	ri RequestInfo,
	resourceAttrs,
	scopeAttrs *msgpAttributes,
	dataset string,
	result *TranslateOTLPRequestResultMsgp,
) error {
	// Find or create the batch for this dataset
	var batch *BatchMsgp
	for i := range result.Batches {
		if result.Batches[i].Dataset == dataset {
			batch = &result.Batches[i]
			break
		}
	}
	if batch == nil {
		result.Batches = append(result.Batches, BatchMsgp{
			Dataset: dataset,
			Events:  []EventMsgp{},
		})
		batch = &result.Batches[len(result.Batches)-1]
	}

	eventAttr := newMsgpMap()

	var eventsData, linksData [][]byte
	var name, traceID, spanID []byte
	var statusCode int64
	var startTimeUnixNano, endTimeUnixNano uint64
	sampleRate := defaultSampleRate

	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // trace_id
			traceID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			if len(traceID) > 0 {
				eventAttr.addString([]byte("trace.trace_id"), []byte(BytesToTraceID(traceID)))
			}

		case 2: // span_id
			spanID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			if len(spanID) > 0 {
				eventAttr.addString([]byte("trace.span_id"), []byte(BytesToSpanID(spanID)))
			}

		case 3: // trace_state
			traceState, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			if len(traceState) > 0 {
				eventAttr.addString([]byte("trace.trace_state"), traceState)

				rate, ok := getSampleRateFromOTelSamplingThreshold(string(traceState))
				if ok {
					sampleRate = rate
				}
			}

		case 4: // parent_span_id
			parentSpanID, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			if len(parentSpanID) > 0 {
				eventAttr.addString([]byte("trace.parent_id"), []byte(BytesToSpanID(parentSpanID)))
			}

		case 5: // name
			name, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Will add name below, to make sure it's always added.

		case 6: // kind
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Kind", wireType)
			}
			kind := trace.Span_SpanKind(decodeVarint(data, &iNdEx))
			kindStr := getSpanKind(kind)
			eventAttr.addString([]byte("span.kind"), []byte(kindStr))
			eventAttr.addString([]byte("type"), []byte(kindStr)) // Also add "type" for backward compatibility

		case 7: // start_time_unix_nano
			v, err := decodeWireType1(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			startTimeUnixNano = v

		case 8: // end_time_unix_nano
			v, err := decodeWireType1(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			endTimeUnixNano = v

		case 9: // attributes
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Parse KeyValue attribute
			err = unmarshalKeyValue(ctx, slice, &eventAttr.msgpAttributes, 0)
			if err != nil {
				return err
			}

		case 11: // events
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Collect event data to process later
			eventsData = append(eventsData, slice)

		case 13: // links
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Collect link data to process later
			linksData = append(linksData, slice)

		case 15: // status
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Parse status inline
			sl := len(slice)
			siNdEx := 0
			for siNdEx < sl {
				sPreIndex := siNdEx
				sfieldNum, swireType, serr := decodeField(slice, &siNdEx)
				if serr != nil {
					return serr
				}

				switch sfieldNum {
				case 2: // message
					messageSlice, err := decodeWireType2(slice, &siNdEx, sl, swireType)
					if err != nil {
						return err
					}
					if len(messageSlice) > 0 {
						eventAttr.addString([]byte("status_message"), messageSlice)
					}

				case 3: // code
					if swireType != 0 {
						return fmt.Errorf("proto: wrong wireType = %d for field Code", swireType)
					}
					statusCode = int64(decodeVarint(slice, &siNdEx))
					// Check if this is an error status
					// Error is only set here for the span, then propagated to child events
					if statusCode == 2 { // STATUS_CODE_ERROR
						eventAttr.addBool([]byte("error"), true)
						eventAttr.isError = true
					}

				default:
					// Skip unknown fields
					if err := skipField(slice, &siNdEx, sPreIndex, sl); err != nil {
						return err
					}
				}
			}

		default:
			// Skip unknown fields and other fields we don't process yet
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Add fields which are always expected, even when they have no encoded value.
	eventAttr.addString([]byte("name"), name)
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	eventAttr.addInt64([]byte("status_code"), statusCode)
	eventAttr.addInt64([]byte("span.num_events"), int64(len(eventsData)))
	eventAttr.addInt64([]byte("span.num_links"), int64(len(linksData)))

	eventAttr.addAttributes(resourceAttrs)
	eventAttr.addAttributes(scopeAttrs)

	// Calculate duration
	duration := float64(0)
	if startTimeUnixNano > 0 && endTimeUnixNano > 0 {
		if endTimeUnixNano >= startTimeUnixNano {
			durationNs := float64(endTimeUnixNano - startTimeUnixNano)
			duration = durationNs / float64(time.Millisecond)
		} else {
			// Negative duration - endTime is before startTime
			duration = 0
			eventAttr.addBool([]byte("meta.invalid_duration"), true)
		}
	}
	eventAttr.addFloat64([]byte("duration_ms"), duration)

	timestamp := timestampFromUnixNano(startTimeUnixNano)

	// Get the final sample rate and isError state
	if eventAttr.sampleRate != 0 {
		// Prefer Honeycomb's sampleRate attribute if it exists
		sampleRate = eventAttr.sampleRate
	}

	// Process span events first (before the main span)
	var firstExceptionAttrs *msgpAttributes
	for _, eventData := range eventsData {
		exceptionAttrs, err := unmarshalSpanEvent(ctx, eventData, traceID, spanID, name, startTimeUnixNano, resourceAttrs, scopeAttrs, sampleRate, eventAttr.isError, batch)
		if err != nil {
			return err
		}
		// Only keep the first exception's attributes
		if exceptionAttrs != nil && firstExceptionAttrs == nil {
			firstExceptionAttrs = exceptionAttrs
		}
	}

	// Add exception attributes from the first exception event to the parent span
	if firstExceptionAttrs != nil {
		eventAttr.addAttributes(firstExceptionAttrs)
	}

	// Process span links next
	for _, linkData := range linksData {
		err := unmarshalSpanLink(ctx, linkData, traceID, spanID, name, timestamp, resourceAttrs, scopeAttrs, sampleRate, eventAttr.isError, batch)
		if err != nil {
			return err
		}
	}

	// Add the span event last (matching the order of regular unmarshaling)
	attrBuf, err := eventAttr.finalize()
	if err != nil {
		return err
	}
	event := EventMsgp{
		Attributes: attrBuf,
		SampleRate: sampleRate,
		Timestamp:  timestamp,
	}
	batch.Events = append(batch.Events, event)

	return nil
}

// unmarshalSpanEvent parses a Span.Event message and creates an event
// Returns exception attributes if this is an exception event, nil otherwise
func unmarshalSpanEvent(
	ctx context.Context,
	data []byte,
	traceID, parentSpanID, parentName []byte,
	spanStartTime uint64,
	resourceAttrs, scopeAttrs *msgpAttributes,
	sampleRate int32,
	isError bool,
	batch *BatchMsgp,
) (*msgpAttributes, error) {
	eventAttr := newMsgpMap()

	// Set trace info
	if len(traceID) > 0 {
		eventAttr.addString([]byte("trace.trace_id"), []byte(BytesToTraceID(traceID)))
	}
	if len(parentSpanID) > 0 {
		eventAttr.addString([]byte("trace.parent_id"), []byte(BytesToSpanID(parentSpanID)))
	}

	// Don't generate a span ID for span events - they only have parent_id

	// Mark as span event
	eventAttr.addString([]byte("meta.annotation_type"), []byte("span_event"))
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	if len(parentName) > 0 {
		eventAttr.addString([]byte("parent_name"), parentName)
	}

	// Parse event fields
	var name []byte
	var timeUnixNano uint64

	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return nil, err
		}

		switch fieldNum {
		case 1: // time_unix_nano
			v, err := decodeWireType1(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			timeUnixNano = v

		case 2: // name
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			name = slice

		case 3: // attributes
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			// Parse KeyValue attribute
			err = unmarshalKeyValue(ctx, slice, &eventAttr.msgpAttributes, 0)
			if err != nil {
				return nil, err
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	// Set event name
	eventAttr.addString([]byte("name"), name)

	// Calculate duration relative to span start
	if timeUnixNano > 0 && spanStartTime > 0 {
		if timeUnixNano >= spanStartTime {
			timeSinceSpanStart := float64(timeUnixNano-spanStartTime) / float64(time.Millisecond)
			eventAttr.addFloat64([]byte("meta.time_since_span_start_ms"), timeSinceSpanStart)
		} else {
			// Event time is before span start time
			eventAttr.addFloat64([]byte("meta.time_since_span_start_ms"), float64(0))
			eventAttr.addBool([]byte("meta.invalid_time_since_span_start"), true)
		}
	}

	// Add error status from parent span if applicable
	if isError {
		eventAttr.addBool([]byte("error"), true)
	}

	// Add resource and scope attributes
	eventAttr.addAttributes(resourceAttrs)
	eventAttr.addAttributes(scopeAttrs)

	// Set timestamp
	timestamp := timestampFromUnixNano(timeUnixNano)

	// Create exception attributes to return if this is an exception event
	var exceptionAttrs *msgpAttributes
	if bytes.Equal(name, []byte("exception")) {
		exceptionAttrs = &msgpAttributes{}
		// We need to parse attributes again to extract exception-specific ones
		l := len(data)
		iNdEx := 0
		for iNdEx < l {
			preIndex := iNdEx
			fieldNum, wireType, err := decodeField(data, &iNdEx)
			if err != nil {
				return nil, err
			}

			switch fieldNum {
			case 3: // attributes
				slice, err := decodeWireType2(data, &iNdEx, l, wireType)
				if err != nil {
					return nil, err
				}
				// Parse each attribute to check if it's an exception attribute
				err = parseExceptionAttributesForReturn(ctx, slice, exceptionAttrs)
				if err != nil {
					return nil, err
				}
			default:
				// Skip other fields
				if err := skipField(data, &iNdEx, preIndex, l); err != nil {
					return nil, err
				}
			}
		}
	}

	attrBuf, err := eventAttr.finalize()
	if err != nil {
		return nil, err
	}
	event := EventMsgp{
		Attributes: attrBuf,
		SampleRate: sampleRate,
		Timestamp:  timestamp,
	}
	batch.Events = append(batch.Events, event)
	return exceptionAttrs, nil
}

// parseExceptionAttributesForReturn parses KeyValue attributes and adds exception-specific ones to msgpAttributes
func parseExceptionAttributesForReturn(ctx context.Context, data []byte, exceptionAttrs *msgpAttributes) error {
	key, value, err := parseKeyValue(ctx, data)
	if err != nil {
		return err
	}

	if len(key) > 0 && value != nil {
		// Check if this is an exception attribute we want to copy
		switch string(key) {
		case "exception.message", "exception.type", "exception.stacktrace":
			if str, ok := value.(string); ok {
				exceptionAttrs.addString(key, []byte(str))
			}
		case "exception.escaped":
			if b, ok := value.(bool); ok {
				exceptionAttrs.addBool(key, b)
			}
		}
	}

	return nil
}

// unmarshalSpanLink parses a Span.Link message and creates an event
func unmarshalSpanLink(
	ctx context.Context,
	data []byte,
	traceID, parentSpanID, parentName []byte,
	parentTimestamp time.Time,
	resourceAttrs, scopeAttrs *msgpAttributes,
	sampleRate int32,
	isError bool,
	batch *BatchMsgp,
) error {
	eventAttr := newMsgpMap()

	// Set trace info
	if len(traceID) > 0 {
		eventAttr.addString([]byte("trace.trace_id"), []byte(BytesToTraceID(traceID)))
	}
	if len(parentSpanID) > 0 {
		eventAttr.addString([]byte("trace.parent_id"), []byte(BytesToSpanID(parentSpanID)))
	}

	// Don't generate a span ID for span links - they only have parent_id

	// Mark as link
	eventAttr.addString([]byte("meta.annotation_type"), []byte("link"))
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	if len(parentName) > 0 {
		eventAttr.addString([]byte("parent_name"), parentName)
	}

	// Parse link fields
	var linkedTraceID, linkedSpanID []byte

	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // trace_id
			var err error
			linkedTraceID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

		case 2: // span_id
			var err error
			linkedSpanID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

		case 3: // trace_state
			// Skip trace_state - original implementation doesn't add it
			_, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

		case 4: // attributes
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Parse KeyValue attribute
			err = unmarshalKeyValue(ctx, slice, &eventAttr.msgpAttributes, 0)
			if err != nil {
				return err
			}

		default:
			// Skip unknown fields
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Set link fields
	if len(linkedTraceID) > 0 {
		eventAttr.addString([]byte("trace.link.trace_id"), []byte(BytesToTraceID(linkedTraceID)))
	}
	if len(linkedSpanID) > 0 {
		eventAttr.addString([]byte("trace.link.span_id"), []byte(hex.EncodeToString(linkedSpanID)))
	}
	// Note: The original implementation doesn't add trace.link.trace_state
	// even though it's part of the OTLP spec, so we skip it for compatibility

	// Add error status from parent span if applicable
	if isError {
		eventAttr.addBool([]byte("error"), true)
	}

	// Add resource and scope attributes
	eventAttr.addAttributes(resourceAttrs)
	eventAttr.addAttributes(scopeAttrs)

	attrBuf, err := eventAttr.finalize()
	if err != nil {
		return err
	}
	event := EventMsgp{
		Attributes: attrBuf,
		SampleRate: sampleRate,
		Timestamp:  parentTimestamp, // use timestamp from parent span
	}
	batch.Events = append(batch.Events, event)
	return nil
}

// timestampFromUnixNano converts unix nano timestamp to time.Time
func timestampFromUnixNano(unixNano uint64) time.Time {
	return time.Unix(0, int64(unixNano)).UTC()
}

// decodeWireType2 decodes a length-delimited field (wire type 2) and returns
// a sub-slice of the original data without copying. The iNdEx is advanced past
// the field data. It also validates that the wire type is correct.
func decodeWireType2(data []byte, iNdEx *int, l int, wireType int) ([]byte, error) {
	if wireType != 2 {
		return nil, fmt.Errorf("proto: wrong wireType = %d for field", wireType)
	}
	msglen := int(decodeVarint(data, iNdEx))
	if msglen < 0 {
		return nil, ErrInvalidLength
	}
	postIndex := *iNdEx + msglen
	if postIndex < 0 {
		return nil, ErrInvalidLength
	}
	if postIndex > l {
		return nil, io.ErrUnexpectedEOF
	}
	slice := data[*iNdEx:postIndex]
	*iNdEx = postIndex
	return slice, nil
}

// decodeWireType1 decodes a 64-bit fixed field (wire type 1) and returns the
// value as uint64. The iNdEx is advanced by 8 bytes. It also validates that
// the wire type is correct.
func decodeWireType1(data []byte, iNdEx *int, l int, wireType int) (uint64, error) {
	if wireType != 1 {
		return 0, fmt.Errorf("proto: wrong wireType = %d for field", wireType)
	}
	if (*iNdEx + 8) > l {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint64(data[*iNdEx])
	v |= uint64(data[*iNdEx+1]) << 8
	v |= uint64(data[*iNdEx+2]) << 16
	v |= uint64(data[*iNdEx+3]) << 24
	v |= uint64(data[*iNdEx+4]) << 32
	v |= uint64(data[*iNdEx+5]) << 40
	v |= uint64(data[*iNdEx+6]) << 48
	v |= uint64(data[*iNdEx+7]) << 56
	*iNdEx += 8
	return v, nil
}

// skipField skips an unknown field in the protobuf wire format.
func skipField(data []byte, iNdEx *int, preIndex int, l int) error {
	*iNdEx = preIndex
	depth := 0
	for *iNdEx < l {
		_, wireType, err := decodeField(data, iNdEx)
		if err != nil {
			return err
		}
		switch wireType {
		case 0:
			_ = decodeVarint(data, iNdEx)
		case 1:
			*iNdEx += 8
		case 2:
			_, err := decodeWireType2(data, iNdEx, l, wireType)
			if err != nil {
				return err
			}
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return ErrUnexpectedEndOfGroup
			}
			depth--
		case 5:
			*iNdEx += 4
		default:
			return fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if *iNdEx < 0 {
			return ErrInvalidLength
		}
		if depth == 0 {
			return nil
		}
	}
	return io.ErrUnexpectedEOF
}

func decodeVarint(data []byte, iNdEx *int) uint64 {
	var res uint64
	startIdx := *iNdEx
	for shift := uint(0); shift < 64; shift += 7 {
		if *iNdEx >= len(data) {
			// Reset index to start position if we can't read a complete varint
			*iNdEx = startIdx
			return 0
		}
		b := data[*iNdEx]
		*iNdEx++
		res |= (uint64(b) & 0x7F) << shift
		if (b & 0x80) == 0 {
			return res
		}
	}

	// The number is too large to represent in a 64-bit value.
	return 0
}

func decodeField(data []byte, iNdEx *int) (fieldNum int32, wireType int, err error) {
	preIndex := *iNdEx
	wire := decodeVarint(data, iNdEx)
	if *iNdEx == preIndex {
		return 0, 0, io.ErrUnexpectedEOF
	}
	fieldNum = int32(wire >> 3)
	wireType = int(wire & 0x7)

	if fieldNum <= 0 {
		return 0, 0, fmt.Errorf("proto: illegal tag %d (wire type %d)", fieldNum, wireType)
	}
	return
}

// addAttributeToMapAsJsonDirect converts a value to JSON string, matching the behavior
// of the legacy addAttributeToMapAsJson function
func addAttributeToMapAsJsonDirect(val any) string {
	// Convert bytes to base64 if needed
	if bytes, ok := val.([]byte); ok {
		val = base64.StdEncoding.EncodeToString(bytes)
	}

	// Use json-iterator for consistency with legacy code
	// fieldSizeMax = math.MaxUint16 from common.go
	w := newLimitedWriter(math.MaxUint16)
	e := json.NewEncoder(w)
	e.SetEscapeHTML(false)
	if err := e.Encode(val); err != nil {
		// Return empty string on error to match legacy behavior
		return ""
	}
	return w.String()
}

func getDatasetFromMsgpAttr(ri RequestInfo, attrs *msgpAttributes) string {
	if ri.hasClassicKey() {
		return ri.Dataset
	}

	serviceName := strings.TrimSpace(attrs.serviceName)
	if serviceName == "" || strings.HasPrefix(serviceName, defaultServiceName) {
		return defaultServiceName
	}
	return serviceName
}
