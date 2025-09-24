package otlp

// JSON unmarshaling for OTLP traces, providing direct translation to Honeycomb's
// event format with serialized messagepack attribute maps, avoiding intermediate
// allocations.

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"time"

	"github.com/honeycombio/husky"

	"github.com/valyala/fastjson"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

var parserPool fastjson.ParserPool

// unmarshalTraceRequestDirectMsgpJSON translates a JSON-encoded OTLP trace request directly
// into a Honeycomb-friendly structure without creating intermediate proto structs.
// This code makes extensive use of fastjson's Visit() method, which is efficient
// but offers no way to return an error or abort iteration. Until this is modernized
// or replaced, we end up having to use pretty awkward closure-based error handling.
func unmarshalTraceRequestDirectMsgpJSON(
	ctx context.Context,
	data []byte,
	ri RequestInfo,
) (*TranslateOTLPRequestResultMsgp, error) {
	if err := ri.ValidateTracesHeaders(); err != nil {
		return nil, err
	}

	result := &TranslateOTLPRequestResultMsgp{
		RequestSize: len(data),
		Batches:     []BatchMsgp{},
	}

	// Parse the JSON
	parser := parserPool.Get()
	defer parserPool.Put(parser)

	v, err := parser.ParseBytes(data)
	if err != nil {
		return nil, err
	}

	// Process root object
	obj, err := v.Object()
	if err != nil {
		return nil, err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "resourceSpans":
			resourceSpansArray := v.GetArray()
			for _, resourceSpansVal := range resourceSpansArray {
				if err = unmarshalResourceSpansJSON(ctx, resourceSpansVal, ri, result); err != nil {
					return
				}
			}
		}
	})

	return result, err
}

// unmarshalResourceSpansJSON parses a ResourceSpans message from JSON
func unmarshalResourceSpansJSON(
	ctx context.Context,
	v *fastjson.Value,
	ri RequestInfo,
	result *TranslateOTLPRequestResultMsgp,
) error {
	var dataset string
	resourceAttrs := msgpAttributesPool.Get().(*msgpAttributes)
	defer resourceAttrs.recycle()

	obj, err := v.Object()
	if err != nil {
		return err
	}

	var scopeSpansArray []*fastjson.Value
	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "resource":
			err = unmarshalResourceJSON(ctx, v, resourceAttrs)
		case "scopeSpans":
			scopeSpansArray = v.GetArray()
		}
	})

	if err != nil {
		return err
	}

	// Determine dataset from resource attributes
	dataset = getDatasetFromMsgpAttr(ri, resourceAttrs)

	// Create a new batch for this resource
	result.Batches = append(result.Batches, BatchMsgp{
		Dataset: dataset,
		Events:  []EventMsgp{},
	})
	batch := &result.Batches[len(result.Batches)-1]

	// Process scopeSpans
	for _, scopeSpansVal := range scopeSpansArray {
		if err := unmarshalScopeSpansJSON(ctx, scopeSpansVal, resourceAttrs, batch); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalResourceJSON parses a Resource message from JSON
func unmarshalResourceJSON(
	ctx context.Context,
	v *fastjson.Value,
	attrs *msgpAttributes,
) error {
	obj, err := v.Object()
	if err != nil {
		return nil // Treat as empty resource
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "attributes":
			attributes := v.GetArray()
			if len(attributes) > 0 {
				if err = unmarshalKeyValueArrayJSON(ctx, attributes, attrs, 0); err != nil {
					return
				}
			}
		}
	})

	return err
}

// unmarshalKeyValueArrayJSON parses an array of KeyValue from JSON
func unmarshalKeyValueArrayJSON(
	ctx context.Context,
	values []*fastjson.Value,
	attrs *msgpAttributes,
	depth int,
) error {
	for i := range values {
		if err := unmarshalKeyValueJSON(ctx, values[i], attrs, depth); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalKeyValueJSON parses a KeyValue message from JSON and adds it to msgpAttributes
func unmarshalKeyValueJSON(
	ctx context.Context,
	v *fastjson.Value,
	attrs *msgpAttributes,
	depth int,
) error {
	obj, err := v.Object()
	if err != nil {
		return nil // Skip invalid KeyValue
	}

	var keyBytes []byte
	var valueObj *fastjson.Value
	obj.Visit(func(k []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(k) {
		case "key":
			// CAUTION: GetStringBytes() returns references to the input buffer.
			// This is only ok as long as we don't retain or modify it.
			keyBytes = v.GetStringBytes()
		case "value":
			valueObj = v
		}
	})

	if len(keyBytes) == 0 || valueObj == nil {
		return nil
	}

	return unmarshalAnyValueIntoAttrsJSON(ctx, valueObj, keyBytes, attrs, depth)
}

// unmarshalAnyValueIntoAttrsJSON parses an AnyValue from JSON and adds it
// directly to msgpAttributes.
// Similar to protobuf, JSON OTLP encodes an "AnyValue" as an object with one of
// several potential keys, depending on the type. In this way, it can re-implent
// JSON's inherent type agnosticism, with the advantage of being far more expensive
// to generate, transmit, and parse.
func unmarshalAnyValueIntoAttrsJSON(
	ctx context.Context,
	v *fastjson.Value,
	key []byte,
	attrs *msgpAttributes,
	depth int,
) error {
	obj, err := v.Object()
	if err != nil {
		return nil // Skip invalid AnyValue
	}

	// Visit the object once to find the value type
	obj.Visit(func(k []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}

		switch string(k) {
		case "stringValue":
			value := v.GetStringBytes()

			// Special handling for sample rate
			if isSampleRateKey(key) && trySetSampleRate(key, string(value), attrs) {
				return
			}

			// Check for service name
			if bytes.Equal(key, []byte("service.name")) {
				attrs.serviceName = string(value)
			}

			attrs.addString(key, value)

		case "boolValue":
			attrs.addBool(key, v.GetBool())

		case "intValue":
			// In JSON, code can be either string or integer
			var val int64
			switch v.Type() {
			case fastjson.TypeNumber:
				val = int64(v.GetInt())
			case fastjson.TypeString:
				strBytes := v.GetStringBytes()
				val, err = strconv.ParseInt(string(strBytes), 10, 64)
				if err != nil {
					return
				}
			default:
				err = errors.New("parse error: expected value as a string or a number.")
				return
			}

			// Special handling for sample rate
			if isSampleRateKey(key) && trySetSampleRate(key, val, attrs) {
				return
			}

			attrs.addInt64(key, val)

		case "doubleValue":
			floatVal := v.GetFloat64()

			// Special handling for sample rate
			if isSampleRateKey(key) && trySetSampleRate(key, floatVal, attrs) {
				return
			}

			attrs.addFloat64(key, floatVal)

		case "bytesValue":
			// Bytes are base64 encoded in JSON. We don't need to decode it since
			// we're just turning it back into JSON. Note if we pass this as []byte
			// instead of string, the JSON marshaller will double-encode it.
			strBytes := v.GetStringBytes()
			attrs.addString(key, []byte(marshalAnyToJSON(string(strBytes))))

			husky.AddTelemetryAttribute(ctx, "received_bytes_attr_type", true)

		case "arrayValue":
			// For arrays, we need to parse and JSON encode
			husky.AddTelemetryAttribute(ctx, "received_array_attr_type", true)
			var arr []any
			arr, err = unmarshalArrayValueJSON(ctx, v)
			if err != nil {
				return
			}
			attrs.addString(key, marshalAnyToJSON(arr))

		case "kvlistValue":
			// For kvlists, handle flattening
			husky.AddTelemetryAttributes(ctx, map[string]any{
				"received_kvlist_attr_type": true,
				"kvlist_max_depth":          depth,
			})

			if depth < maxDepth {
				// Flatten the kvlist
				if err = unmarshalKvlistValueFlattenJSON(ctx, v, key, attrs, depth+1); err != nil {
					return
				}
			} else {
				// Max depth exceeded, JSON encode the whole thing
				var m map[string]any
				m, err = unmarshalKvlistValueJSON(ctx, v)
				if err != nil {
					return
				}
				attrs.addString(key, marshalAnyToJSON(m))
			}
		}
	})

	return err
}

// unmarshalArrayValueJSON parses an ArrayValue from JSON and returns []any
// Only used when we're going to re-marshal the values as JSON.
func unmarshalArrayValueJSON(ctx context.Context, v *fastjson.Value) ([]any, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, nil
	}

	var values []any

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "values":
			valuesArray := v.GetArray()
			if len(valuesArray) > 0 {
				values = make([]any, 0, len(valuesArray))
				for _, valObj := range valuesArray {
					var val any
					val, err = unmarshalAnyValueJSON(ctx, valObj)
					if err != nil {
						return
					}
					if val != nil {
						values = append(values, val)
					}
				}
			}
		}
	})

	return values, err
}

// unmarshalAnyValueJSON parses an AnyValue from JSON and returns the value
// Only used when we're going to re-marshal the values as JSON.
func unmarshalAnyValueJSON(ctx context.Context, v *fastjson.Value) (any, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, nil
	}

	var result any
	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "stringValue":
			result = string(v.GetStringBytes())

		case "boolValue":
			result = v.GetBool()

		case "intValue":
			// JSON encodes int64 as string
			strBytes := v.GetStringBytes()
			var val int64
			val, err = strconv.ParseInt(string(strBytes), 10, 64)
			if err == nil {
				result = val
			}

		case "doubleValue":
			result = v.GetFloat64()

		case "bytesValue":
			// Bytes are base64 encoded in JSON
			strBytes := v.GetStringBytes()
			var b []byte
			b, err = base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				result = b
			}

		case "arrayValue":
			var arr []any
			arr, err = unmarshalArrayValueJSON(ctx, v)
			if err == nil {
				result = arr
			}

		case "kvlistValue":
			var m map[string]any
			m, err = unmarshalKvlistValueJSON(ctx, v)
			if err == nil {
				result = m
			}
		}
	})

	return result, err
}

// unmarshalKvlistValueJSON parses a KeyValueList from JSON and returns map[string]any
// Only used when we're going to re-marshal the values as JSON.
func unmarshalKvlistValueJSON(ctx context.Context, v *fastjson.Value) (map[string]any, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, nil
	}

	result := make(map[string]any)
	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "values":
			valuesArray := v.GetArray()
			for _, value := range valuesArray {
				var valueObject *fastjson.Object
				valueObject, err = value.Object()
				if err != nil {
					return
				}

				var keyStr string
				var valueObj *fastjson.Value

				valueObject.Visit(func(k []byte, v *fastjson.Value) {
					switch string(k) {
					case "key":
						keyStr = string(v.GetStringBytes())
					case "value":
						valueObj = v
					}
				})

				if keyStr != "" && valueObj != nil {
					var val any
					val, err = unmarshalAnyValueJSON(ctx, valueObj)
					if err != nil {
						return
					}
					if val != nil {
						result[keyStr] = val
					}
				}
			}
		}
	})

	return result, err
}

// unmarshalKvlistValueFlattenJSON parses a KeyValueList from JSON and flattens it
// into msgpAttributes
func unmarshalKvlistValueFlattenJSON(
	ctx context.Context,
	v *fastjson.Value,
	keyPrefix []byte,
	attrs *msgpAttributes,
	depth int,
) error {
	obj, err := v.Object()
	if err != nil {
		return nil
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "values":
			valuesArray := v.GetArray()
			for _, value := range valuesArray {
				var valueObject *fastjson.Object
				valueObject, err = value.Object()
				if err != nil {
					return
				}

				var keyBytes []byte
				var valueObj *fastjson.Value

				valueObject.Visit(func(k []byte, v *fastjson.Value) {
					if err != nil {
						return
					}
					switch string(k) {
					case "key":
						keyBytes = v.GetStringBytes()
					case "value":
						valueObj = v
					}
				})

				if len(keyBytes) == 0 || valueObj == nil {
					continue
				}

				// Create flattened key
				// Create a new buffer to avoid corrupting the input
				flatKey := make([]byte, 0, len(keyPrefix)+1+len(keyBytes))
				flatKey = append(flatKey, keyPrefix...)
				flatKey = append(flatKey, '.')
				flatKey = append(flatKey, keyBytes...)

				// Process the value
				if err = unmarshalAnyValueIntoAttrsJSON(ctx, valueObj, flatKey, attrs, depth); err != nil {
					return
				}
			}
		}
	})

	return err
}

// unmarshalScopeSpansJSON parses a ScopeSpans message from JSON
func unmarshalScopeSpansJSON(
	ctx context.Context,
	v *fastjson.Value,
	resourceAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {
	scopeAttrs := msgpAttributesPool.Get().(*msgpAttributes)
	defer scopeAttrs.recycle()

	obj, err := v.Object()
	if err != nil {
		return err
	}

	var spansArray []*fastjson.Value
	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "scope":
			if err = unmarshalInstrumentationScopeJSON(ctx, v, scopeAttrs); err != nil {
				return
			}
		case "spans":
			spansArray = v.GetArray()
		}
	})
	if err != nil {
		return err
	}

	// Process spans
	for _, spanVal := range spansArray {
		if err := unmarshalSpanJSON(ctx, spanVal, resourceAttrs, scopeAttrs, batch); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalInstrumentationScopeJSON parses an InstrumentationScope message from JSON
func unmarshalInstrumentationScopeJSON(
	ctx context.Context,
	v *fastjson.Value,
	attrs *msgpAttributes,
) error {
	obj, err := v.Object()
	if err != nil {
		return nil // Treat as empty scope
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "name":
			nameBytes := v.GetStringBytes()
			if len(nameBytes) > 0 {
				attrs.addString([]byte("library.name"), nameBytes)
				if isInstrumentationLibrary(string(nameBytes)) {
					attrs.addBool([]byte("telemetry.instrumentation_library"), true)
				}
			}

		case "version":
			versionBytes := v.GetStringBytes()
			if len(versionBytes) > 0 {
				attrs.addString([]byte("library.version"), versionBytes)
			}

		case "attributes":
			attributes := v.GetArray()
			if len(attributes) > 0 {
				if err = unmarshalKeyValueArrayJSON(ctx, attributes, attrs, 0); err != nil {
					return
				}
			}
		}
	})

	return err
}

// unmarshalSpanJSON parses a Span message from JSON and creates an event
func unmarshalSpanJSON(
	ctx context.Context,
	v *fastjson.Value,
	resourceAttrs *msgpAttributes,
	scopeAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)
	defer eventAttr.recycle()

	var traceState string
	var startTimeUnixNano, endTimeUnixNano uint64
	sampleRate := defaultSampleRate
	var eventsArray, linksArray []*fastjson.Value

	// Initialize span attributes struct
	fields := spanFields{
		commonFields: commonFields{
			metaSignalType: "trace",
		},
		spanKind: "unspecified",
	}

	obj, err := v.Object()
	if err != nil {
		return err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}

		switch string(key) {
		case "traceId":
			// Trace ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			var b []byte
			b, err = base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				fields.traceID = b
			}

		case "spanId":
			// Span ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			var b []byte
			b, err = base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				fields.spanID = b
			}

		case "traceState":
			traceStateBytes := v.GetStringBytes()
			traceState = string(traceStateBytes)
			if traceState != "" {
				fields.traceState = traceStateBytes

				rate, ok := getSampleRateFromOTelSamplingThreshold(traceState)
				if ok {
					sampleRate = rate
				}
			}

		case "parentSpanId":
			// Parent span ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			var b []byte
			b, err = base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil && len(b) > 0 {
				fields.parentID = b
			}

		case "name":
			fields.name = v.GetStringBytes()

		case "kind":
			// In JSON, kind can be either string enum name or integer
			if v.Type() == fastjson.TypeString {
				// String enum like "SPAN_KIND_SERVER"
				enumBytes := v.GetStringBytes()
				switch string(enumBytes) {
				case "SPAN_KIND_UNSPECIFIED":
					fields.spanKind = "unspecified"
				case "SPAN_KIND_INTERNAL":
					fields.spanKind = "internal"
				case "SPAN_KIND_SERVER":
					fields.spanKind = "server"
				case "SPAN_KIND_CLIENT":
					fields.spanKind = "client"
				case "SPAN_KIND_PRODUCER":
					fields.spanKind = "producer"
				case "SPAN_KIND_CONSUMER":
					fields.spanKind = "consumer"
				default:
					fields.spanKind = "unspecified"
				}
			} else {
				// Integer value
				kind := trace.Span_SpanKind(v.GetInt())
				fields.spanKind = getSpanKind(kind)
			}

		case "startTimeUnixNano":
			// Time is encoded as string in JSON
			strBytes := v.GetStringBytes()
			var val uint64
			val, err = strconv.ParseUint(string(strBytes), 10, 64)
			if err == nil {
				startTimeUnixNano = val
			}

		case "endTimeUnixNano":
			// Time is encoded as string in JSON
			strBytes := v.GetStringBytes()
			var val uint64
			val, err = strconv.ParseUint(string(strBytes), 10, 64)
			if err == nil {
				endTimeUnixNano = val
			}

		case "attributes":
			attributes := v.GetArray()
			if len(attributes) > 0 {
				if err = unmarshalKeyValueArrayJSON(ctx, attributes, eventAttr, 0); err != nil {
					return
				}
			}

		case "events":
			eventsArray = v.GetArray()

		case "links":
			linksArray = v.GetArray()

		case "status":
			var statusObj *fastjson.Object
			statusObj, err = v.Object()
			if err != nil {
				return
			}
			statusObj.Visit(func(k []byte, v *fastjson.Value) {
				// Closure-based error handling
				// if err is set, all subsequent visits are no-ops
				if err != nil {
					return
				}
				switch string(k) {
				case "message":
					messageBytes := v.GetStringBytes()
					if len(messageBytes) > 0 {
						fields.statusMessage = messageBytes
					}

				case "code":
					switch v.Type() {
					case fastjson.TypeNumber:
						fields.statusCode = int64(v.GetInt())
						// Check if this is an error status
						if fields.statusCode == 2 { // STATUS_CODE_ERROR
							fields.hasError = true
						}
					case fastjson.TypeString:
						// String enum like "STATUS_CODE_OK"
						codeBytes := v.GetStringBytes()
						switch string(codeBytes) {
						case "STATUS_CODE_UNSET":
							fields.statusCode = 0
						case "STATUS_CODE_OK":
							fields.statusCode = 1
						case "STATUS_CODE_ERROR":
							fields.statusCode = 2
							fields.hasError = true
						default:
							fields.statusCode = 0
						}
					default:
						err = errors.New("parse error: expected value as a string or a number.")
						return
					}
				}
			})
		}
	})
	if err != nil {
		return err
	}

	// Calculate duration directly in spanAttrs
	if startTimeUnixNano > 0 && endTimeUnixNano > 0 {
		if endTimeUnixNano >= startTimeUnixNano {
			durationNs := float64(endTimeUnixNano - startTimeUnixNano)
			fields.durationMs = durationNs / float64(time.Millisecond)
		} else {
			// Negative duration
			fields.durationMs = 0
			fields.hasInvalidDuration = true
		}
	}

	// Populate span attributes struct with remaining fields
	fields.spanNumEvents = int64(len(eventsArray))
	fields.spanNumLinks = int64(len(linksArray))

	// Add all span attributes to eventAttr
	fields.addToMsgpAttributes(eventAttr)

	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

	timestamp := timestampFromUnixNano(startTimeUnixNano)

	// Get the final sample rate
	if eventAttr.sampleRate != 0 {
		sampleRate = eventAttr.sampleRate
	}

	// Process span events first
	var firstExceptionAttrs *msgpAttributes
	for _, eventVal := range eventsArray {
		exceptionAttrs, err := unmarshalSpanEventJSON(
			ctx,
			eventVal,
			fields.traceID,
			fields.spanID,
			fields.name,
			startTimeUnixNano,
			resourceAttrs,
			scopeAttrs,
			sampleRate,
			fields.hasError,
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
		firstExceptionAttrs.recycle()
	}

	// Process span links next
	for _, linkVal := range linksArray {
		err := unmarshalSpanLinkJSON(
			ctx,
			linkVal,
			fields.traceID,
			fields.spanID,
			fields.name,
			timestamp,
			resourceAttrs,
			scopeAttrs,
			sampleRate,
			fields.hasError,
			batch,
		)
		if err != nil {
			return err
		}
	}

	// Add the span event last
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

// unmarshalSpanEventJSON parses a Span.Event message from JSON and creates an event
// Returns exception attributes if this is an exception event, nil otherwise
func unmarshalSpanEventJSON(
	ctx context.Context,
	v *fastjson.Value,
	traceID []byte,
	parentSpanID []byte,
	parentName []byte,
	spanStartTime uint64,
	resourceAttrs *msgpAttributes,
	scopeAttrs *msgpAttributes,
	sampleRate int32,
	isError bool,
	batch *BatchMsgp,
) (*msgpAttributes, error) {
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)

	// Initialize span event fields struct
	fields := spanEventFields{
		commonFields: commonFields{
			traceID:            traceID,
			parentID:           parentSpanID,
			parentName:         parentName,
			metaAnnotationType: "span_event",
			metaSignalType:     "trace",
			hasError:           isError,
		},
	}

	// Parse event fields
	var timeUnixNano uint64
	var exceptionAttrs *msgpAttributes
	var attributes []*fastjson.Value

	obj, err := v.Object()
	if err != nil {
		return nil, err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}
		switch string(key) {
		case "timeUnixNano":
			// Time is encoded as string in JSON
			strBytes := v.GetStringBytes()
			var val uint64
			val, err = strconv.ParseUint(string(strBytes), 10, 64)
			if err == nil {
				timeUnixNano = val
			}

		case "name":
			fields.name = v.GetStringBytes()

		case "attributes":
			attributes = v.GetArray()
		}
	})
	if err != nil {
		return nil, err
	}

	if len(attributes) > 0 {
		if err = unmarshalKeyValueArrayJSON(ctx, attributes, eventAttr, 0); err != nil {
			return nil, err
		}

		// If this is an exception event, also collect exception-specific attributes
		if bytes.Equal(fields.name, []byte("exception")) {
			exceptionAttrs = msgpAttributesPool.Get().(*msgpAttributes)

			// Extract exception attributes
			for _, kv := range attributes {
				kvObj, err := kv.Object()
				if err != nil {
					continue
				}

				var keyStr string
				var valueObj *fastjson.Value

				kvObj.Visit(func(k []byte, v *fastjson.Value) {
					switch string(k) {
					case "key":
						keyStr = string(v.GetStringBytes())
					case "value":
						valueObj = v
					}
				})

				if keyStr == "" || valueObj == nil {
					continue
				}

				switch keyStr {
				case "exception.message", "exception.type", "exception.stacktrace":
					valObj, err := valueObj.Object()
					if err == nil {
						valObj.Visit(func(k []byte, v *fastjson.Value) {
							if string(k) == "stringValue" {
								valBytes := v.GetStringBytes()
								exceptionAttrs.addString([]byte(keyStr), valBytes)
							}
						})
					}
				case "exception.escaped":
					valObj, err := valueObj.Object()
					if err == nil {
						valObj.Visit(func(k []byte, v *fastjson.Value) {
							if string(k) == "boolValue" {
								exceptionAttrs.addBool([]byte(keyStr), v.GetBool())
							}
						})
					}
				}
			}
		}
	}

	// Calculate duration relative to span start
	if timeUnixNano > 0 && spanStartTime > 0 {
		if timeUnixNano >= spanStartTime {
			fields.timeSinceSpanStartMs = float64(timeUnixNano-spanStartTime) / float64(time.Millisecond)
		} else {
			// Event time is before span start time
			fields.timeSinceSpanStartMs = float64(0)
			fields.hasInvalidTimeSinceSpanStart = true
		}
	}

	// Add all span event fields to eventAttr
	fields.addToMsgpAttributes(eventAttr)

	// Add resource and scope attributes
	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

	// Set timestamp
	timestamp := timestampFromUnixNano(timeUnixNano)

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

// unmarshalSpanLinkJSON parses a Span.Link message from JSON and creates an event
func unmarshalSpanLinkJSON(
	ctx context.Context,
	v *fastjson.Value,
	traceID []byte,
	parentSpanID []byte,
	parentName []byte,
	parentTimestamp time.Time,
	resourceAttrs *msgpAttributes,
	scopeAttrs *msgpAttributes,
	sampleRate int32,
	isError bool,
	batch *BatchMsgp,
) error {
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)

	// Initialize span link fields struct
	fields := spanLinkFields{
		commonFields: commonFields{
			traceID:            traceID,
			parentID:           parentSpanID,
			parentName:         parentName,
			metaAnnotationType: "link",
			metaSignalType:     "trace",
			hasError:           isError,
		},
	}

	// Parse link fields

	obj, err := v.Object()
	if err != nil {
		return err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		// Closure-based error handling
		// if err is set, all subsequent visits are no-ops
		if err != nil {
			return
		}

		switch string(key) {
		case "traceId":
			// Trace ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			var b []byte
			b, err = base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				fields.linkedTraceID = b
			}

		case "spanId":
			// Span ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			var b []byte
			b, err = base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				fields.linkedSpanID = b
			}

		case "attributes":
			attributes := v.GetArray()
			if len(attributes) > 0 {
				if err = unmarshalKeyValueArrayJSON(ctx, attributes, eventAttr, 0); err != nil {
					return
				}
			}
		}
	})
	if err != nil {
		return err
	}

	// Add all span link fields to eventAttr
	fields.addToMsgpAttributes(eventAttr)

	// Add resource and scope attributes
	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

	attrBuf, err := eventAttr.finalize()
	if err != nil {
		return err
	}
	event := EventMsgp{
		Attributes: attrBuf,
		SampleRate: sampleRate,
		Timestamp:  parentTimestamp,
	}
	batch.Events = append(batch.Events, event)
	return nil
}
