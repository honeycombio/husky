package otlp

// Translates OTLP traces into honeycomb's event format, including a serialized
// messagepack attribute map per event, instead of a fully deserialized one.

// OTLP Trace Protobuf Structure:
//
// ExportTraceServiceRequest
// └── ResourceSpans (repeated)
//     ├── Resource
//     │   └── attributes: KeyValue (repeated)
//     ├── ScopeSpans (repeated)
//     │   ├── InstrumentationScope
//     │   │   ├── name: string
//     │   │   ├── version: string
//     │   │   └── attributes: KeyValue (repeated)
//     │   └── Span (repeated)
//     │       ├── trace_id: bytes
//     │       ├── span_id: bytes
//     │       ├── parent_span_id: bytes
//     │       ├── name: string
//     │       ├── kind: Span.SpanKind (enum)
//     │       ├── start_time_unix_nano: fixed64
//     │       ├── end_time_unix_nano: fixed64
//     │       ├── attributes: KeyValue (repeated)
//     │       ├── events: Span.Event (repeated)
//     │       │   ├── time_unix_nano: fixed64
//     │       │   ├── name: string
//     │       │   └── attributes: KeyValue (repeated)
//     │       ├── links: Span.Link (repeated)
//     │       │   ├── trace_id: bytes
//     │       │   ├── span_id: bytes
//     │       │   ├── trace_state: string
//     │       │   └── attributes: KeyValue (repeated)
//     │       ├── status: Status
//     │       │   ├── code: Status.StatusCode (enum)
//     │       │   └── message: string
//     │       └── trace_state: string
//     └── schema_url: string
//
// Field Mappings to Honeycomb Events:
//
// Resource attributes → All events
// - "service.name" → Dataset name (for non-classic API keys)
// - attributes → All events
//
// InstrumentationScope → All events
// - name → "library.name" (sets "telemetry.instrumentation_library"=true if recognized)
// - version → "library.version"
// - attributes → All events
//
// Span → Main span event
// - trace_id → "trace.trace_id" (hex, trimmed if leading zeros)
// - span_id → "trace.span_id" (hex)
// - parent_span_id → "trace.parent_id" (hex)
// - name → "name"
// - kind → "span.kind", "type" (client/server/producer/consumer/internal)
// - start_time_unix_nano → Event.Timestamp
// - end_time_unix_nano → "duration_ms" = (end - start) / 1e6
// - attributes → Event (special: "sampleRate" → Event.SampleRate)
// - status.code → "status_code", "error"=true if ERROR
// - status.message → "status_message"
// - trace_state → "trace.trace_state"
//
// Span.Event → Separate event per span event
// - Inherits resource, scope, and first exception attrs
// - time_unix_nano → Event.Timestamp
// - name → "name"
// - attributes → Event
// - Added: "trace.trace_id", "trace.parent_id", "parent_name",
//   "meta.annotation_type"="span_event", "meta.signal_type"="trace",
//   "meta.time_since_span_start_ms", "error" (if parent errored)
//
// Span.Link → Separate event per link
// - Inherits resource and scope attrs
// - trace_id → "trace.link.trace_id" (hex)
// - span_id → "trace.link.span_id" (hex)
// - attributes → Event
// - Added: "trace.trace_id", "trace.parent_id", "parent_name",
//   "meta.annotation_type"="link", "meta.signal_type"="trace",
//   "error" (if parent errored)

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/honeycombio/husky"

	"github.com/dgryski/go-wyhash"
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

var msgpAttributesPool = sync.Pool{
	New: func() any {
		return new(msgpAttributes)
	},
}

func recycleMsgpAttributes(m *msgpAttributes) {
	if m != nil {
		m.reset()
		msgpAttributesPool.Put(m)
	}
}

// Holds a list of messagepack-encoded key-value pairs, without a map header.
type msgpAttributes struct {
	buf       []byte
	keyHashes map[uint64]struct{}

	// memoized metadata fields we'll need internally
	serviceName string
	sampleRate  int32
	isError     bool
}

func (m *msgpAttributes) reset() {
	clear(m.keyHashes)
	*m = msgpAttributes{
		buf:       m.buf[:0],
		keyHashes: m.keyHashes,
	}
}

func (m *msgpAttributes) addAny(key []byte, value any) error {
	var err error
	m.buf = msgp.AppendStringFromBytes(m.buf, key)
	m.buf, err = msgp.AppendIntf(m.buf, value)
	return err
}

func (m *msgpAttributes) addString(key []byte, value []byte) {
	m.buf = msgp.AppendStringFromBytes(m.buf, key)
	m.buf = msgp.AppendStringFromBytes(m.buf, value)
}

func (m *msgpAttributes) addInt64(key []byte, value int64) {
	m.buf = msgp.AppendStringFromBytes(m.buf, key)
	m.buf = msgp.AppendInt64(m.buf, value)
}

func (m *msgpAttributes) addFloat64(key []byte, value float64) {
	m.buf = msgp.AppendStringFromBytes(m.buf, key)
	m.buf = msgp.AppendFloat64(m.buf, value)
}

func (m *msgpAttributes) addBool(key []byte, value bool) {
	m.buf = msgp.AppendStringFromBytes(m.buf, key)
	m.buf = msgp.AppendBool(m.buf, value)
}

// Adds the contents of the given msgpAttributes to this msgpAttributes.
// When finalize() is called, it will prefer the first occurence of any duplicates,
// so it's important to call this first with highest-precedence data, then the least.
func (m *msgpAttributes) addAttributes(add *msgpAttributes) {
	m.buf = append(m.buf, add.buf...)
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

var keyHashPool = sync.Pool{
	New: func() any {
		return make(map[uint64]struct{}, 64)
	},
}

// Returns serialized msgp map including the header, suitable for transmission.
// The result is on fresh heap, so the underlying msgpAttributes can be recycled.
// Suppresses any duplicates in the input, prefering the FIRST occurence of each value.
func (m *msgpAttributes) finalize() ([]byte, error) {
	// Messagepack has 3 map header formats for different maximum counts,
	// 1, 3, and 5 bytes long. Here we'll just always use the 3-byte form,
	// which can represent counts up to 2^16-1. This is slightly wasteful
	// if there are few enough values that we could have used the 1-byte form,
	// and will cause an error if there are too many, but honeycomb doesn't
	// support column counts that large anyway. With a fixed-length header,
	// we can allocate space for it now, then set the later 2 bytes with the
	// real count once we're finished encoding.
	result := make([]byte, 3, len(m.buf)+3)
	result[0] = 0xde // map16 format

	keyHashes := keyHashPool.Get().(map[uint64]struct{})
	defer func() {
		clear(keyHashes)
		keyHashPool.Put(keyHashes)
	}()

	// Parse the messagepack data directly
	data := m.buf
	for len(data) > 0 {
		// Read the key using the zero-copy method
		keyBytes, remaining, err := msgp.ReadMapKeyZC(data)
		if err != nil {
			return nil, err
		}
		keyHash := wyhash.Hash(keyBytes, 0)

		// Find where the value ends by skipping it
		afterValue, err := msgp.Skip(remaining)
		if err != nil {
			return nil, err
		}
		if _, exists := keyHashes[keyHash]; !exists {
			keyHashes[keyHash] = struct{}{}

			// Copy the entire key-value pair to result
			result = append(result, data[:len(data)-len(afterValue)]...)
		}

		// Move to the next key-value pair
		data = afterValue
	}

	uniqueCount := len(keyHashes)
	if uniqueCount > 65535 {
		return nil, errors.New("too many attributes")
	}

	// Update the count in the header
	result[1] = byte(uniqueCount >> 8)
	result[2] = byte(uniqueCount)

	return result, nil
}

// addTraceID adds a trace ID, truncating the empty prefix if required,
// and encoding it as hex without allocating.
func (m *msgpAttributes) addTraceID(key []byte, traceID []byte) {
	if shouldTrimTraceId(traceID) {
		traceID = traceID[traceIDShortLength:]
	}

	m.addHexID(key, traceID)
}

// addHexID adds a hexidecimal ID, encoding it without allocating
func (m *msgpAttributes) addHexID(key []byte, spanID []byte) {
	m.buf = msgp.AppendStringFromBytes(m.buf, key)

	// Calculate the encoded length
	encodedLen := len(spanID) * 2

	// Write string header
	if encodedLen <= 31 {
		// fixstr format
		m.buf = append(m.buf, byte(0xa0|encodedLen))
	} else {
		// str8 format (up to 255 bytes)
		m.buf = append(m.buf, 0xd9, byte(encodedLen))
	}

	// Encode the ID directly into the buffer
	m.buf = hex.AppendEncode(m.buf, spanID)
}

// unmarshalTraceRequestDirectMsgp translates a serialized OTLP trace request directly
// into a Honeycomb-friendly structure without creating intermediate proto structs,
// which is EXTREMELY expensive.
// Why does the code look like this? Because it's derived from gogo's generated
// code, and carries over some of the style conventions so that it will hopefully
// be relatively easy to update it in future, should that be necessary.
// Fortunately this part of OTLP is marked as "stable" so we don't expect many changes.
// However, as of this writing the otel folks are working on making this even more
// complex than it already is, by adding a new EntityRef field to Resource.
// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/resource/v1/resource.proto#L43
// When this is finalized we'll presumably have to add support here.
func unmarshalTraceRequestDirectMsgp(ctx context.Context, data []byte, ri RequestInfo) (*TranslateOTLPRequestResultMsgp, error) {
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
	var dataset string

	resourceAttrs := msgpAttributesPool.Get().(*msgpAttributes)
	defer recycleMsgpAttributes(resourceAttrs)

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
			err = unmarshalResource(ctx, slice, resourceAttrs)
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
	dataset = getDatasetFromMsgpAttr(ri, resourceAttrs)

	// Create a new batch for this resource. Note this may create multiple
	// batches for the same dataset, which matches the behavior of the legacy
	// implementation. In future we may want to change this to combine all
	// events from the dataset into a single batch.
	// Find or create the batch for this dataset
	result.Batches = append(result.Batches, BatchMsgp{
		Dataset: dataset,
		Events:  []EventMsgp{},
	})
	batch := &result.Batches[len(result.Batches)-1]

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

			// Note, the logic here will probably need to change once the Entity
			// system is finalized.
			err = unmarshalScopeSpans(ctx, slice, resourceAttrs, batch)
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
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// parseKeyValue parses a KeyValue message and returns the key and raw value bytes
func parseKeyValue(data []byte) ([]byte, []byte, error) {
	var key []byte
	var valueBytes []byte

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
			valueBytes, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, nil, err
			}

		default:
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, nil, err
			}
		}
	}

	return key, valueBytes, nil
}

// processValueDirect handles a value recursively, tracking depth for proper flattening
func processValueDirect(ctx context.Context, key []byte, value any, attrs *msgpAttributes, depth int) error {
	var err error

	// Handle different value types to match legacy behavior
	switch v := value.(type) {
	case []byte:
		// Bytes are JSON encoded - match the legacy behavior
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
				// Tricky: we can't just append to key, since key most likely
				// came from the input buffer, so appending to it will write
				// into the input. Instead construct a new key string.
				// This does allocate garbage and in theory it could be factored
				// out by writing directly into the messagepack buffer, but we
				// expect this to be an uncommon case.
				flatKey := make([]byte, 0, len(key)+1+len(k))
				flatKey = append(flatKey, key...)
				flatKey = append(flatKey, '.')
				flatKey = append(flatKey, k...)

				// Process the nested value recursively
				err = processValueDirect(ctx, flatKey, v, attrs, depth+1)
				if err != nil {
					return err
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
		if err != nil {
			return err
		}
	}

	return nil
}

func sampleRateFromFloat(f float64) int32 {
	if f > math.MaxInt32 {
		return math.MaxInt32
	}

	rate := int32(f + 0.5) // Round to nearest int

	// Check this AFTER converting to int, since oddities like NaN will become 0
	if rate < defaultSampleRate {
		return defaultSampleRate
	}
	return rate
}

// unmarshalKeyValue parses a KeyValue message and adds it to msgpAttributes
func unmarshalKeyValue(ctx context.Context, data []byte, attrs *msgpAttributes, depth int) error {
	key, valueBytes, err := parseKeyValue(data)
	if err != nil {
		return err
	}

	// KeyValue messages should have both key and value
	if len(key) > 0 && len(valueBytes) > 0 {
		// Parse the AnyValue directly to avoid allocations for scalar types
		l := len(valueBytes)
		iNdEx := 0

		// We only need to parse the first field since AnyValue is a oneof
		if iNdEx < l {
			fieldNum, wireType, err := decodeField(valueBytes, &iNdEx)
			if err != nil {
				return err
			}

			// Handle scalar types directly without allocating
			switch fieldNum {
			case 1: // string_value
				slice, err := decodeWireType2(valueBytes, &iNdEx, l, wireType)
				if err != nil {
					return err
				}

				if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
					// Convert string to float for sample rate
					if f, err := strconv.ParseFloat(string(slice), 64); err == nil {
						if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
							attrs.sampleRate = sampleRateFromFloat(f)
						}
					}
					return nil
				}

				// If this is the service name, note it
				if bytes.Equal(key, []byte("service.name")) {
					attrs.serviceName = string(slice)
				}

				attrs.addString(key, slice)
				return nil

			case 2: // bool_value
				if wireType != 0 {
					return fmt.Errorf("proto: wrong wireType = %d for field BoolValue", wireType)
				}
				v := int(decodeVarint(valueBytes, &iNdEx))
				attrs.addBool(key, v != 0)
				return nil

			case 3: // int_value
				if wireType != 0 {
					return fmt.Errorf("proto: wrong wireType = %d for field IntValue", wireType)
				}
				v := int64(decodeVarint(valueBytes, &iNdEx))

				if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
					v = min(v, math.MaxInt32)
					if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
						attrs.sampleRate = max(defaultSampleRate, int32(v))
					}
					return nil
				}

				attrs.addInt64(key, v)
				return nil

			case 4: // double_value
				v, err := decodeWireType1(valueBytes, &iNdEx, l, wireType)
				if err != nil {
					return err
				}
				floatVal := math.Float64frombits(v)

				if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
					if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
						attrs.sampleRate = sampleRateFromFloat(floatVal)
					}
					return nil
				}

				attrs.addFloat64(key, floatVal)
				return nil

			default:
				// For complex types (array, kvlist, bytes), fall back to unmarshalAnyValue
				value, err := unmarshalAnyValue(ctx, valueBytes)
				if err != nil {
					return err
				}

				// Process the value recursively with depth tracking
				return processValueDirect(ctx, key, value, attrs, depth)
			}
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
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return values, nil
}

// unmarshalKeyValueToMap parses a KeyValue message and adds it to a map
func unmarshalKeyValueToMap(ctx context.Context, data []byte, result map[string]any) error {
	key, valueBytes, err := parseKeyValue(data)
	if err != nil {
		return err
	}

	// KeyValue messages should have both key and value
	if len(key) > 0 && len(valueBytes) > 0 {
		// Parse the value from bytes
		value, err := unmarshalAnyValue(ctx, valueBytes)
		if err != nil {
			return err
		}

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
	resourceAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {
	// Get the instrumentation scope first
	scopeAttrs := msgpAttributesPool.Get().(*msgpAttributes)
	defer recycleMsgpAttributes(scopeAttrs)

	l := len(data)
	iNdEx := 0
loop:
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
			err = unmarshalInstrumentationScope(ctx, slice, scopeAttrs)
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
			err = unmarshalSpan(ctx, slice, resourceAttrs, scopeAttrs, batch)
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
	resourceAttrs,
	scopeAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {
	// Collect span-specific attributes separately first
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)
	defer recycleMsgpAttributes(eventAttr)

	var eventsData, linksData [][]byte
	var name, traceID, spanID []byte
	var statusCode int64
	var startTimeUnixNano, endTimeUnixNano uint64
	kindStr := "unspecified"
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

		case 2: // span_id
			spanID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
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
				eventAttr.addHexID([]byte("trace.parent_id"), parentSpanID)
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
			kindStr = getSpanKind(kind)

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
			err = unmarshalKeyValue(ctx, slice, eventAttr, 0)
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
					if err := skipField(slice, &siNdEx, sPreIndex, sl); err != nil {
						return err
					}
				}
			}

		default:
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Add fields which are always expected, even when they have no encoded value.
	eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	eventAttr.addHexID([]byte("trace.span_id"), spanID)
	eventAttr.addString([]byte("name"), name)
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	eventAttr.addInt64([]byte("status_code"), statusCode)
	eventAttr.addInt64([]byte("span.num_events"), int64(len(eventsData)))
	eventAttr.addInt64([]byte("span.num_links"), int64(len(linksData)))
	eventAttr.addString([]byte("span.kind"), []byte(kindStr))
	eventAttr.addString([]byte("type"), []byte(kindStr))

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

	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

	// Process span events first (before the main span)
	var firstExceptionAttrs *msgpAttributes
	for _, eventData := range eventsData {
		exceptionAttrs, err := unmarshalSpanEvent(ctx,
			eventData,
			traceID,
			spanID,
			name,
			startTimeUnixNano,
			resourceAttrs,
			scopeAttrs,
			sampleRate,
			eventAttr.isError,
			batch,
		)
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
		err := unmarshalSpanLink(
			ctx,
			linkData,
			traceID,
			spanID,
			name,
			timestamp,
			resourceAttrs,
			scopeAttrs,
			sampleRate,
			eventAttr.isError,
			batch,
		)
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
	// Collect event-specific attributes separately
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)
	defer recycleMsgpAttributes(eventAttr)

	// Set trace info
	if len(traceID) > 0 {
		eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	}
	if len(parentSpanID) > 0 {
		eventAttr.addHexID([]byte("trace.parent_id"), parentSpanID)
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
			err = unmarshalKeyValue(ctx, slice, eventAttr, 0)
			if err != nil {
				return nil, err
			}

		default:
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

	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

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
	key, valueBytes, err := parseKeyValue(data)
	if err != nil {
		return err
	}

	if len(key) > 0 && len(valueBytes) > 0 {
		// Parse the value from bytes
		value, err := unmarshalAnyValue(ctx, valueBytes)
		if err != nil {
			return err
		}

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
	// Collect link-specific attributes separately
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)
	defer recycleMsgpAttributes(eventAttr)

	// Set trace info
	if len(traceID) > 0 {
		eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	}
	if len(parentSpanID) > 0 {
		eventAttr.addHexID([]byte("trace.parent_id"), parentSpanID)
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
			err = unmarshalKeyValue(ctx, slice, eventAttr, 0)
			if err != nil {
				return err
			}

		default:
			if err := skipField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Set link fields
	if len(linkedTraceID) > 0 {
		eventAttr.addTraceID([]byte("trace.link.trace_id"), linkedTraceID)
	}
	if len(linkedSpanID) > 0 {
		eventAttr.addHexID([]byte("trace.link.span_id"), linkedSpanID)
	}
	// Note: The original implementation doesn't add trace.link.trace_state
	// even though it's part of the OTLP spec, so we skip it for compatibility

	// Add error status from parent span if applicable
	if isError {
		eventAttr.addBool([]byte("error"), true)
	}

	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

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
// BE CAREFUL: the []byte returned here is a reference to a subslice of the input
// data, which is fine as long as it's read-only. Writing to it or appending to
// it will corrupt the input data. If you need to modify it, make a copy first.
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
	if *iNdEx >= len(data) {
		return 0, 0, io.ErrUnexpectedEOF
	}

	// Fast path: single-byte field header (common case)
	b := data[*iNdEx]
	if b < 0x80 {
		*iNdEx++
		fieldNum = int32(b >> 3)
		wireType = int(b & 0x7)

		if fieldNum <= 0 {
			return 0, 0, fmt.Errorf("proto: illegal tag %d (wire type %d)", fieldNum, wireType)
		}
		return
	}

	// Slow path: multi-byte varint (for large field numbers)
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
