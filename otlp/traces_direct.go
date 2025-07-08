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
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/honeycombio/husky"
)

var (
	ErrInvalidLength        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflow          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroup = fmt.Errorf("proto: unexpected end of group")
)

// UnmarshalTraceRequestDirect translates a serialized OTLP trace request directly
// into Honeycomb-friendly structure without creating intermediate proto structs.
// This is a performance optimization that avoids the overhead of unmarshaling
// into proto structs first.
// Why does the code look like this? Because it's derived from gogo's generated
// code, and carries over some of the style conventions so that it will hopefully
// be relatively easy to update it in future, should that be necessary.
// Fortunately this part of OTLP is marked as "stable" so we don't expect changes.
func UnmarshalTraceRequestDirect(ctx context.Context, data []byte, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}

	result := &TranslateOTLPRequestResult{
		RequestSize: len(data),
		Batches:     []Batch{},
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
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
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
func unmarshalResourceSpans(ctx context.Context, data []byte, ri RequestInfo, result *TranslateOTLPRequestResult) error {
	resourceAttrs := make(map[string]any)
	dataset := ""
	var scopeSpansData [][]byte

	l := len(data)
	iNdEx := 0

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

		case 2: // scope_spans
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Collect ScopeSpans data to process later
			scopeSpansData = append(scopeSpansData, slice)

		case 3: // schema_url
			_, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Skip the string value

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Determine dataset from resource attributes
	dataset = getDataset(ri, resourceAttrs)

	// Now process the collected ScopeSpans with the determined dataset
	for _, scopeSpansBytes := range scopeSpansData {
		err := unmarshalScopeSpans(ctx, scopeSpansBytes, ri, resourceAttrs, dataset, result)
		if err != nil {
			return err
		}
	}

	return nil
}

// unmarshalResource parses a Resource message
func unmarshalResource(ctx context.Context, data []byte, attrs map[string]any) error {
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
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalKeyValue parses a KeyValue message
func unmarshalKeyValue(ctx context.Context, data []byte, attrs map[string]any, depth int) error {
	var key string
	var value any

	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // key
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			key = string(slice)

		case 2: // value
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse AnyValue
			val, err := unmarshalAnyValue(ctx, slice)
			if err != nil {
				return err
			}
			value = val

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// KeyValue messages should have both key and value
	if key != "" && value != nil {
		// Handle different value types to match legacy behavior
		switch v := value.(type) {
		case []byte:
			// Bytes are JSON encoded - match the legacy behavior
			husky.AddTelemetryAttribute(ctx, "received_bytes_attr_type", true)
			attrs[key] = addAttributeToMapAsJsonDirect(v)
		case []any:
			// Arrays are JSON encoded
			husky.AddTelemetryAttribute(ctx, "received_array_attr_type", true)
			attrs[key] = addAttributeToMapAsJsonDirect(v)
		case map[string]any:
			// Kvlists are flattened with dot notation
			husky.AddTelemetryAttributes(ctx, map[string]any{
				"received_kvlist_attr_type": true,
				"kvlist_max_depth":          depth,
			})
			if depth < maxDepth {
				// Flatten the kvlist
				for k, v := range v {
					flatKey := key + "." + k
					switch vv := v.(type) {
					case []byte:
						attrs[flatKey] = addAttributeToMapAsJsonDirect(vv)
					case []any:
						attrs[flatKey] = addAttributeToMapAsJsonDirect(vv)
					case map[string]any:
						// Nested kvlist - would need recursive flattening
						// For now, JSON encode it if we hit max depth
						attrs[flatKey] = addAttributeToMapAsJsonDirect(vv)
					default:
						attrs[flatKey] = vv
					}
				}
			} else {
				// Max depth exceeded, JSON encode the whole thing
				attrs[key] = addAttributeToMapAsJsonDirect(v)
			}
		default:
			// Simple types - just assign directly
			attrs[key] = value
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
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
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
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return values, nil
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
			if err := unmarshalKeyValue(ctx, slice, result, 0); err != nil {
				return nil, err
			}

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// unmarshalScopeSpans parses a ScopeSpans message
func unmarshalScopeSpans(ctx context.Context, data []byte, ri RequestInfo, resourceAttrs map[string]any, dataset string, result *TranslateOTLPRequestResult) error {
	scopeAttrs := make(map[string]any)

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
			err = unmarshalInstrumentationScope(ctx, slice, scopeAttrs)
			if err != nil {
				return err
			}

		case 2: // spans
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

			// Parse Span
			err = unmarshalSpan(ctx, slice, ri, resourceAttrs, scopeAttrs, dataset, result)
			if err != nil {
				return err
			}

		case 3: // schema_url
			_, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Skip the string value

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalInstrumentationScope parses an InstrumentationScope message
func unmarshalInstrumentationScope(ctx context.Context, data []byte, attrs map[string]any) error {
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
			name := string(slice)
			if name != "" {
				attrs["library.name"] = name
				if isInstrumentationLibrary(name) {
					attrs["telemetry.instrumentation_library"] = true
				}
			}

		case 2: // version
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			version := string(slice)
			if version != "" {
				attrs["library.version"] = version
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
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalSpan parses a Span message and creates an event
func unmarshalSpan(ctx context.Context, data []byte, ri RequestInfo, resourceAttrs, scopeAttrs map[string]any, dataset string, result *TranslateOTLPRequestResult) error {
	// Find or create the batch for this dataset
	var batch *Batch
	for i := range result.Batches {
		if result.Batches[i].Dataset == dataset {
			batch = &result.Batches[i]
			break
		}
	}
	if batch == nil {
		newBatch := Batch{
			Dataset: dataset,
			Events:  []Event{},
		}
		result.Batches = append(result.Batches, newBatch)
		batch = &result.Batches[len(result.Batches)-1]
	}

	// Create event with resource and scope attributes
	event := Event{
		Attributes: make(map[string]any),
		SampleRate: defaultSampleRate,
	}

	// Add resource attributes
	for k, v := range resourceAttrs {
		event.Attributes[k] = v
	}

	// Add scope attributes
	for k, v := range scopeAttrs {
		event.Attributes[k] = v
	}

	// Parse span fields
	var traceID, spanID, parentSpanID []byte
	var name, traceState string
	var kind int32
	var startTimeUnixNano, endTimeUnixNano uint64
	var eventsData [][]byte // Collect event data to process later
	var linksData [][]byte  // Collect link data to process later
	var statusCode int      // Default to 0

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
			traceID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

		case 2: // span_id
			var err error
			spanID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

		case 3: // trace_state
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			traceState = string(slice)

		case 4: // parent_span_id
			var err error
			parentSpanID, err = decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}

		case 5: // name
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			name = string(slice)

		case 6: // kind
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Kind", wireType)
			}
			kind = int32(decodeVarint(data, &iNdEx))

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
			err = unmarshalKeyValue(ctx, slice, event.Attributes, 0)
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
			// Parse status
			status, err := unmarshalStatus(ctx, slice)
			if err != nil {
				return err
			}
			// Parse status and update statusCode
			if status != nil {
				statusCode = int(status.code)
				if status.message != "" {
					event.Attributes["status_message"] = status.message
				}
				// Check if this is an error status
				if status.code == 2 { // STATUS_CODE_ERROR
					event.Attributes["error"] = true
				}
			}

		default:
			// Skip unknown fields and other fields we don't process yet
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Set span fields in the event
	if len(traceID) > 0 {
		event.Attributes["trace.trace_id"] = BytesToTraceID(traceID)
	}
	if len(spanID) > 0 {
		event.Attributes["trace.span_id"] = BytesToSpanID(spanID)
	}
	if len(parentSpanID) > 0 {
		event.Attributes["trace.parent_id"] = BytesToSpanID(parentSpanID)
	}
	event.Attributes["name"] = name
	if traceState != "" {
		event.Attributes["trace.trace_state"] = traceState
	}

	// Convert span kind
	kindStr := spanKindToString(kind)
	event.Attributes["span.kind"] = kindStr
	event.Attributes["type"] = kindStr // Also add "type" for backward compatibility

	// Add meta fields
	event.Attributes["meta.signal_type"] = "trace"

	// Add status code (always present, default 0)
	event.Attributes["status_code"] = statusCode

	// Add span event/link counts
	event.Attributes["span.num_events"] = len(eventsData)
	event.Attributes["span.num_links"] = len(linksData)

	// Set timestamp to start time (default to epoch if not set)
	event.Timestamp = timestampFromUnixNano(startTimeUnixNano)

	// Calculate duration
	duration := float64(0)
	if startTimeUnixNano > 0 && endTimeUnixNano > 0 {
		durationNs := float64(endTimeUnixNano - startTimeUnixNano)
		duration = durationNs / float64(time.Millisecond)
		if duration < 0 {
			duration = 0
			event.Attributes["meta.invalid_duration"] = true
		}
	}
	event.Attributes["duration_ms"] = duration

	// Get sample rate from trace state
	event.SampleRate = getSampleRate(event.Attributes, traceState)

	// Check if span has error status for propagation to events/links
	isError := false
	if errorVal, ok := event.Attributes["error"]; ok {
		isError, _ = errorVal.(bool)
	}

	// Process span events first (before the main span)
	for _, eventData := range eventsData {
		err := unmarshalSpanEvent(ctx, eventData, traceID, spanID, name, startTimeUnixNano, resourceAttrs, scopeAttrs, event.SampleRate, isError, batch, event)
		if err != nil {
			return err
		}
	}

	// Process span links next
	for _, linkData := range linksData {
		err := unmarshalSpanLink(ctx, linkData, traceID, spanID, name, event.Timestamp, resourceAttrs, scopeAttrs, event.SampleRate, isError, batch)
		if err != nil {
			return err
		}
	}

	// Add the span event last (matching the order of regular unmarshaling)
	batch.Events = append(batch.Events, event)

	return nil
}

// unmarshalSpanEvent parses a Span.Event message and creates an event
func unmarshalSpanEvent(ctx context.Context, data []byte, traceID, parentSpanID []byte, parentName string, spanStartTime uint64, resourceAttrs, scopeAttrs map[string]any, sampleRate int32, isError bool, batch *Batch, parentSpan Event) error {
	// Create event with resource and scope attributes
	event := Event{
		Attributes: make(map[string]any),
		SampleRate: sampleRate,
	}

	// Add resource attributes
	for k, v := range resourceAttrs {
		event.Attributes[k] = v
	}

	// Add scope attributes
	for k, v := range scopeAttrs {
		event.Attributes[k] = v
	}

	// Set trace info
	if len(traceID) > 0 {
		event.Attributes["trace.trace_id"] = BytesToTraceID(traceID)
	}
	if len(parentSpanID) > 0 {
		event.Attributes["trace.parent_id"] = BytesToSpanID(parentSpanID)
	}

	// Don't generate a span ID for span events - they only have parent_id

	// Mark as span event
	event.Attributes["meta.annotation_type"] = "span_event"
	event.Attributes["meta.signal_type"] = "trace"
	if parentName != "" {
		event.Attributes["parent_name"] = parentName
	}

	// Parse event fields
	var name string
	var timeUnixNano uint64

	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		fieldNum, wireType, err := decodeField(data, &iNdEx)
		if err != nil {
			return err
		}

		switch fieldNum {
		case 1: // time_unix_nano
			v, err := decodeWireType1(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			timeUnixNano = v

		case 2: // name
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			name = string(slice)

		case 3: // attributes
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return err
			}
			// Parse KeyValue attribute
			err = unmarshalKeyValue(ctx, slice, event.Attributes, 0)
			if err != nil {
				return err
			}

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Set event name
	event.Attributes["name"] = name

	// Set timestamp
	if timeUnixNano > 0 {
		event.Timestamp = timestampFromUnixNano(timeUnixNano)
	}

	// Calculate duration relative to span start
	if timeUnixNano > 0 && spanStartTime > 0 {
		timeSinceSpanStart := float64(timeUnixNano-spanStartTime) / float64(time.Millisecond)
		if timeSinceSpanStart < 0 {
			timeSinceSpanStart = 0
			event.Attributes["meta.invalid_time_since_span_start"] = true
		}
		event.Attributes["meta.time_since_span_start_ms"] = timeSinceSpanStart
	}

	// Add error status from parent span if applicable
	if isError {
		event.Attributes["error"] = true
	}

	// Handle exception events specially
	if name == "exception" {
		// Copy exception attributes to parent span
		for k, v := range event.Attributes {
			switch k {
			case "exception.message", "exception.type", "exception.stacktrace", "exception.escaped":
				// Don't overwrite if the value is already on the span
				if _, present := parentSpan.Attributes[k]; !present {
					parentSpan.Attributes[k] = v
				}
			}
		}
	}

	batch.Events = append(batch.Events, event)
	return nil
}

// unmarshalSpanLink parses a Span.Link message and creates an event
func unmarshalSpanLink(ctx context.Context, data []byte, traceID, parentSpanID []byte, parentName string, parentTimestamp time.Time, resourceAttrs, scopeAttrs map[string]any, sampleRate int32, isError bool, batch *Batch) error {
	// Create event with resource and scope attributes
	event := Event{
		Attributes: make(map[string]any),
		SampleRate: sampleRate,
		Timestamp:  parentTimestamp, // use timestamp from parent span
	}

	// Add resource attributes
	for k, v := range resourceAttrs {
		event.Attributes[k] = v
	}

	// Add scope attributes
	for k, v := range scopeAttrs {
		event.Attributes[k] = v
	}

	// Set trace info
	if len(traceID) > 0 {
		event.Attributes["trace.trace_id"] = BytesToTraceID(traceID)
	}
	if len(parentSpanID) > 0 {
		event.Attributes["trace.parent_id"] = BytesToSpanID(parentSpanID)
	}

	// Don't generate a span ID for span links - they only have parent_id

	// Mark as link
	event.Attributes["meta.annotation_type"] = "link"
	event.Attributes["meta.signal_type"] = "trace"
	if parentName != "" {
		event.Attributes["parent_name"] = parentName
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
			err = unmarshalKeyValue(ctx, slice, event.Attributes, 0)
			if err != nil {
				return err
			}

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return err
			}
		}
	}

	// Set link fields
	if len(linkedTraceID) > 0 {
		event.Attributes["trace.link.trace_id"] = BytesToTraceID(linkedTraceID)
	}
	if len(linkedSpanID) > 0 {
		event.Attributes["trace.link.span_id"] = hex.EncodeToString(linkedSpanID)
	}
	// Note: The original implementation doesn't add trace.link.trace_state
	// even though it's part of the OTLP spec, so we skip it for compatibility

	// Add error status from parent span if applicable
	if isError {
		event.Attributes["error"] = true
	}

	batch.Events = append(batch.Events, event)
	return nil
}

var eventCounter uint64

// generateSpanID generates a unique 8-byte span ID for events
func generateSpanID() []byte {
	eventCounter++
	spanID := make([]byte, 8)
	// Simple deterministic ID based on counter
	spanID[0] = 0xff // Marker to distinguish from regular span IDs
	spanID[1] = byte(eventCounter >> 40)
	spanID[2] = byte(eventCounter >> 32)
	spanID[3] = byte(eventCounter >> 24)
	spanID[4] = byte(eventCounter >> 16)
	spanID[5] = byte(eventCounter >> 8)
	spanID[6] = byte(eventCounter)
	spanID[7] = 0x00
	return spanID
}

// spanStatus represents the status of a span
type spanStatus struct {
	code    int32
	message string
}

// spanKindToString converts a span kind enum to string
func spanKindToString(kind int32) string {
	switch kind {
	case 0:
		return "unspecified" // SPAN_KIND_UNSPECIFIED
	case 1:
		return "internal"
	case 2:
		return "server"
	case 3:
		return "client"
	case 4:
		return "producer"
	case 5:
		return "consumer"
	default:
		return "unspecified"
	}
}

// unmarshalStatus parses a Status message
func unmarshalStatus(ctx context.Context, data []byte) (*spanStatus, error) {
	var status spanStatus

	l := len(data)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		wire := decodeVarint(data, &iNdEx)
		if iNdEx == preIndex {
			return nil, io.ErrUnexpectedEOF
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)

		switch fieldNum {
		case 2: // message
			slice, err := decodeWireType2(data, &iNdEx, l, wireType)
			if err != nil {
				return nil, err
			}
			status.message = string(slice)

		case 3: // code
			if wireType != 0 {
				return nil, fmt.Errorf("proto: wrong wireType = %d for field Code", wireType)
			}
			status.code = int32(decodeVarint(data, &iNdEx))

		default:
			// Skip unknown fields
			if err := skipUnknownField(data, &iNdEx, preIndex, l); err != nil {
				return nil, err
			}
		}
	}

	return &status, nil
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

// skipUnknownField skips an unknown field in the protobuf wire format.
// TODO we need a test which actually contains serialized unknown fields.
func skipUnknownField(data []byte, iNdEx *int, preIndex int, l int) error {
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
