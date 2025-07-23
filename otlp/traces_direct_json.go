package otlp

// JSON unmarshaling for OTLP traces, providing direct translation to Honeycomb's
// event format with serialized messagepack attribute maps, avoiding intermediate
// allocations.

import (
	"bytes"
	"context"
	"encoding/base64"
	"math"
	"strconv"
	"time"

	"github.com/honeycombio/husky"

	"github.com/valyala/fastjson"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

var parserPool fastjson.ParserPool

// reusableParser is a wrapper to safely return parser to pool
type reusableParser struct {
	p *fastjson.Parser
}

func (rp *reusableParser) Release() {
	if rp.p != nil {
		parserPool.Put(rp.p)
		rp.p = nil
	}
}

func getParser() *reusableParser {
	return &reusableParser{p: parserPool.Get()}
}

// bytesEqual compares a byte slice with a string without allocation
func bytesEqual(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

// unmarshalTraceRequestDirectMsgpJSON translates a JSON-encoded OTLP trace request directly
// into a Honeycomb-friendly structure without creating intermediate proto structs.
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
	rp := getParser()
	defer rp.Release()

	v, err := rp.p.ParseBytes(data)
	if err != nil {
		return nil, err
	}

	// Process root object
	obj, err := v.Object()
	if err != nil {
		return nil, err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		if bytesEqual(key, "resourceSpans") {
			resourceSpansArray := v.GetArray()
			if resourceSpansArray != nil {
				for _, resourceSpansVal := range resourceSpansArray {
					if err := unmarshalResourceSpansJSON(ctx, resourceSpansVal, ri, result); err != nil {
						// Store error but continue processing
						return
					}
				}
			}
		}
	})

	return result, nil
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
		switch string(key) {
		case "resource":
			if err := unmarshalResourceJSON(ctx, v, resourceAttrs); err != nil {
				// Store error but continue
				return
			}
		case "scopeSpans":
			scopeSpansArray = v.GetArray()
		}
	})

	// Determine dataset from resource attributes
	dataset = getDatasetFromMsgpAttr(ri, resourceAttrs)

	// Create a new batch for this resource
	result.Batches = append(result.Batches, BatchMsgp{
		Dataset: dataset,
		Events:  []EventMsgp{},
	})
	batch := &result.Batches[len(result.Batches)-1]

	// Process scopeSpans
	if scopeSpansArray != nil {
		for _, scopeSpansVal := range scopeSpansArray {
			if err := unmarshalScopeSpansJSON(ctx, scopeSpansVal, resourceAttrs, batch); err != nil {
				return err
			}
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
		if bytesEqual(key, "attributes") {
			attributes := v.GetArray()
			if attributes != nil {
				if err := unmarshalKeyValueArrayJSON(ctx, attributes, attrs, 0); err != nil {
					// Store error but continue
					return
				}
			}
		}
	})

	return nil
}

// unmarshalKeyValueArrayJSON parses an array of KeyValue from JSON
func unmarshalKeyValueArrayJSON(
	ctx context.Context,
	values []*fastjson.Value,
	attrs *msgpAttributes,
	depth int,
) error {
	for i := 0; i < len(values); i++ {
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

// unmarshalAnyValueIntoAttrsJSON parses an AnyValue from JSON and adds it directly to msgpAttributes
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
		switch string(k) {
		case "stringValue":
			value := v.GetStringBytes()

			// Special handling for sample rate
			if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
				if f, err := strconv.ParseFloat(string(value), 64); err == nil {
					if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
						attrs.sampleRate = sampleRateFromFloat(f)
					}
				}
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
			// JSON encodes int64 as string
			strBytes := v.GetStringBytes()
			val, err := strconv.ParseInt(string(strBytes), 10, 64)
			if err != nil {
				return
			}

			// Special handling for sample rate
			if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
				val = min(val, math.MaxInt32)
				if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
					attrs.sampleRate = max(defaultSampleRate, int32(val))
				}
				return
			}

			attrs.addInt64(key, val)

		case "doubleValue":
			floatVal := v.GetFloat64()

			// Special handling for sample rate
			if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
				if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
					attrs.sampleRate = sampleRateFromFloat(floatVal)
				}
				return
			}

			attrs.addFloat64(key, floatVal)

		case "bytesValue":
			// Bytes are base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err != nil {
				return
			}

			husky.AddTelemetryAttribute(ctx, "received_bytes_attr_type", true)
			attrs.addAny(key, addAttributeToMapAsJsonDirect(b))

		case "arrayValue":
			// For arrays, we need to parse and JSON encode
			husky.AddTelemetryAttribute(ctx, "received_array_attr_type", true)
			arr, err := unmarshalArrayValueJSON(ctx, v)
			if err != nil {
				return
			}
			attrs.addAny(key, addAttributeToMapAsJsonDirect(arr))

		case "kvlistValue":
			// For kvlists, handle flattening
			husky.AddTelemetryAttributes(ctx, map[string]any{
				"received_kvlist_attr_type": true,
				"kvlist_max_depth":          depth,
			})

			if depth < maxDepth {
				// Flatten the kvlist
				if err := unmarshalKvlistValueFlattenJSON(ctx, v, key, attrs, depth+1); err != nil {
					return
				}
			} else {
				// Max depth exceeded, JSON encode the whole thing
				m, err := unmarshalKvlistValueJSON(ctx, v)
				if err != nil {
					return
				}
				attrs.addAny(key, addAttributeToMapAsJsonDirect(m))
			}
		}
	})

	return nil
}

// unmarshalArrayValueJSON parses an ArrayValue from JSON and returns []any
func unmarshalArrayValueJSON(ctx context.Context, v *fastjson.Value) ([]any, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, nil
	}

	var values []any

	obj.Visit(func(key []byte, v *fastjson.Value) {
		if bytesEqual(key, "values") {
			valuesArray := v.GetArray()
			if valuesArray != nil {
				values = make([]any, 0, len(valuesArray))
				for _, valObj := range valuesArray {
					val, err := unmarshalAnyValueJSON(ctx, valObj)
					if err != nil {
						continue
					}
					if val != nil {
						values = append(values, val)
					}
				}
			}
		}
	})

	return values, nil
}

// unmarshalAnyValueJSON parses an AnyValue from JSON and returns the value
func unmarshalAnyValueJSON(ctx context.Context, v *fastjson.Value) (any, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, nil
	}

	var result any

	obj.Visit(func(key []byte, v *fastjson.Value) {
		switch string(key) {
		case "stringValue":
			result = string(v.GetStringBytes())

		case "boolValue":
			result = v.GetBool()

		case "intValue":
			// JSON encodes int64 as string
			strBytes := v.GetStringBytes()
			val, err := strconv.ParseInt(string(strBytes), 10, 64)
			if err == nil {
				result = val
			}

		case "doubleValue":
			result = v.GetFloat64()

		case "bytesValue":
			// Bytes are base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				result = b
			}

		case "arrayValue":
			arr, err := unmarshalArrayValueJSON(ctx, v)
			if err == nil {
				result = arr
			}

		case "kvlistValue":
			m, err := unmarshalKvlistValueJSON(ctx, v)
			if err == nil {
				result = m
			}
		}
	})

	return result, nil
}

// unmarshalKvlistValueFlattenJSON parses a KeyValueList from JSON and flattens it into msgpAttributes
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
		if bytesEqual(key, "values") {
			valuesArray := v.GetArray()
			if valuesArray != nil {
				for _, kvObj := range valuesArray {
					kvObjObj, err := kvObj.Object()
					if err != nil {
						continue
					}

					var keyBytes []byte
					var valueObj *fastjson.Value

					kvObjObj.Visit(func(k []byte, v *fastjson.Value) {
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
					if err := unmarshalAnyValueIntoAttrsJSON(ctx, valueObj, flatKey, attrs, depth); err != nil {
						continue
					}
				}
			}
		}
	})

	return nil
}

// unmarshalKvlistValueJSON parses a KeyValueList from JSON and returns map[string]any
func unmarshalKvlistValueJSON(ctx context.Context, v *fastjson.Value) (map[string]any, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, nil
	}

	result := make(map[string]any)

	obj.Visit(func(key []byte, v *fastjson.Value) {
		if bytesEqual(key, "values") {
			valuesArray := v.GetArray()
			if valuesArray != nil {
				for _, kvObj := range valuesArray {
					kvObjObj, err := kvObj.Object()
					if err != nil {
						continue
					}

					var keyStr string
					var valueObj *fastjson.Value

					kvObjObj.Visit(func(k []byte, v *fastjson.Value) {
						switch string(k) {
						case "key":
							keyStr = string(v.GetStringBytes())
						case "value":
							valueObj = v
						}
					})

					if keyStr != "" && valueObj != nil {
						val, err := unmarshalAnyValueJSON(ctx, valueObj)
						if err == nil && val != nil {
							result[keyStr] = val
						}
					}
				}
			}
		}
	})

	return result, nil
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
		switch string(key) {
		case "scope":
			if err := unmarshalInstrumentationScopeJSON(ctx, v, scopeAttrs); err != nil {
				// Store error but continue
				return
			}
		case "spans":
			spansArray = v.GetArray()
		}
	})

	// Process spans
	if spansArray != nil {
		for _, spanVal := range spansArray {
			if err := unmarshalSpanJSON(ctx, spanVal, resourceAttrs, scopeAttrs, batch); err != nil {
				return err
			}
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
			if attributes != nil {
				if err := unmarshalKeyValueArrayJSON(ctx, attributes, attrs, 0); err != nil {
					// Store error but continue
					return
				}
			}
		}
	})

	return nil
}

// unmarshalSpanJSON parses a Span message from JSON and creates an event
func unmarshalSpanJSON(
	ctx context.Context,
	v *fastjson.Value,
	resourceAttrs,
	scopeAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)
	defer eventAttr.recycle()

	var name, traceID, spanID []byte
	var traceState string
	var statusCode int64
	var startTimeUnixNano, endTimeUnixNano uint64
	kindStr := "unspecified"
	sampleRate := defaultSampleRate
	var eventsArray, linksArray []*fastjson.Value

	obj, err := v.Object()
	if err != nil {
		return err
	}

	// Process all span fields in one pass
	obj.Visit(func(key []byte, v *fastjson.Value) {
		switch string(key) {
		case "traceId":
			// Trace ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				traceID = b
			}

		case "spanId":
			// Span ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				spanID = b
			}

		case "traceState":
			traceStateBytes := v.GetStringBytes()
			traceState = string(traceStateBytes)
			if traceState != "" {
				eventAttr.addString([]byte("trace.trace_state"), traceStateBytes)

				rate, ok := getSampleRateFromOTelSamplingThreshold(traceState)
				if ok {
					sampleRate = rate
				}
			}

		case "parentSpanId":
			// Parent span ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil && len(b) > 0 {
				eventAttr.addHexID([]byte("trace.parent_id"), b)
			}

		case "name":
			name = v.GetStringBytes()

		case "kind":
			// In JSON, kind can be either string enum name or integer
			if v.Type() == fastjson.TypeString {
				// String enum like "SPAN_KIND_SERVER"
				enumBytes := v.GetStringBytes()
				switch string(enumBytes) {
				case "SPAN_KIND_UNSPECIFIED":
					kindStr = "unspecified"
				case "SPAN_KIND_INTERNAL":
					kindStr = "internal"
				case "SPAN_KIND_SERVER":
					kindStr = "server"
				case "SPAN_KIND_CLIENT":
					kindStr = "client"
				case "SPAN_KIND_PRODUCER":
					kindStr = "producer"
				case "SPAN_KIND_CONSUMER":
					kindStr = "consumer"
				default:
					kindStr = "unspecified"
				}
			} else {
				// Integer value
				kind := trace.Span_SpanKind(v.GetInt())
				kindStr = getSpanKind(kind)
			}

		case "startTimeUnixNano":
			// Time is encoded as string in JSON
			strBytes := v.GetStringBytes()
			val, err := strconv.ParseUint(string(strBytes), 10, 64)
			if err == nil {
				startTimeUnixNano = val
			}

		case "endTimeUnixNano":
			// Time is encoded as string in JSON
			strBytes := v.GetStringBytes()
			val, err := strconv.ParseUint(string(strBytes), 10, 64)
			if err == nil {
				endTimeUnixNano = val
			}

		case "attributes":
			attributes := v.GetArray()
			if attributes != nil {
				if err := unmarshalKeyValueArrayJSON(ctx, attributes, eventAttr, 0); err != nil {
					// Store error but continue
					return
				}
			}

		case "events":
			eventsArray = v.GetArray()

		case "links":
			linksArray = v.GetArray()

		case "status":
			statusObj, err := v.Object()
			if err == nil {
				statusObj.Visit(func(k []byte, v *fastjson.Value) {
					switch string(k) {
					case "message":
						messageBytes := v.GetStringBytes()
						if len(messageBytes) > 0 {
							eventAttr.addString([]byte("status_message"), messageBytes)
						}

					case "code":
						// In JSON, code can be either string enum or integer
						if v.Type() == fastjson.TypeString {
							// String enum like "STATUS_CODE_OK"
							codeBytes := v.GetStringBytes()
							switch string(codeBytes) {
							case "STATUS_CODE_UNSET":
								statusCode = 0
							case "STATUS_CODE_OK":
								statusCode = 1
							case "STATUS_CODE_ERROR":
								statusCode = 2
								eventAttr.addBool([]byte("error"), true)
								eventAttr.isError = true
							default:
								statusCode = 0
							}
						} else {
							// Integer value
							statusCode = int64(v.GetInt())
							// Check if this is an error status
							if statusCode == 2 { // STATUS_CODE_ERROR
								eventAttr.addBool([]byte("error"), true)
								eventAttr.isError = true
							}
						}
					}
				})
			}
		}
	})

	// Add fields which are always expected
	eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	eventAttr.addHexID([]byte("trace.span_id"), spanID)
	eventAttr.addString([]byte("name"), name)
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	eventAttr.addInt64([]byte("status_code"), statusCode)
	eventAttr.addInt64([]byte("span.num_events"), int64(len(eventsArray)))
	eventAttr.addInt64([]byte("span.num_links"), int64(len(linksArray)))
	eventAttr.addString([]byte("span.kind"), []byte(kindStr))
	eventAttr.addString([]byte("type"), []byte(kindStr))

	eventAttr.addAttributes(scopeAttrs)
	eventAttr.addAttributes(resourceAttrs)

	// Calculate duration
	duration := float64(0)
	if startTimeUnixNano > 0 && endTimeUnixNano > 0 {
		if endTimeUnixNano >= startTimeUnixNano {
			durationNs := float64(endTimeUnixNano - startTimeUnixNano)
			duration = durationNs / float64(time.Millisecond)
		} else {
			// Negative duration
			duration = 0
			eventAttr.addBool([]byte("meta.invalid_duration"), true)
		}
	}
	eventAttr.addFloat64([]byte("duration_ms"), duration)

	timestamp := timestampFromUnixNano(startTimeUnixNano)

	// Get the final sample rate
	if eventAttr.sampleRate != 0 {
		sampleRate = eventAttr.sampleRate
	}

	// Process span events first
	var firstExceptionAttrs *msgpAttributes
	if eventsArray != nil {
		for _, eventVal := range eventsArray {
			exceptionAttrs, err := unmarshalSpanEventJSON(ctx, eventVal, traceID, spanID, name, startTimeUnixNano, resourceAttrs, scopeAttrs, sampleRate, eventAttr.isError, batch)
			if err != nil {
				return err
			}
			// Only keep the first exception's attributes
			if exceptionAttrs != nil && firstExceptionAttrs == nil {
				firstExceptionAttrs = exceptionAttrs
			}
		}
	}

	// Add exception attributes from the first exception event to the parent span
	if firstExceptionAttrs != nil {
		eventAttr.addAttributes(firstExceptionAttrs)
		firstExceptionAttrs.recycle()
	}

	// Process span links next
	if linksArray != nil {
		for _, linkVal := range linksArray {
			err := unmarshalSpanLinkJSON(ctx, linkVal, traceID, spanID, name, timestamp, resourceAttrs, scopeAttrs, sampleRate, eventAttr.isError, batch)
			if err != nil {
				return err
			}
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
	traceID,
	parentSpanID,
	parentName []byte,
	spanStartTime uint64,
	resourceAttrs,
	scopeAttrs *msgpAttributes,
	sampleRate int32,
	isError bool,
	batch *BatchMsgp,
) (*msgpAttributes, error) {
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)

	// Set trace info
	if len(traceID) > 0 {
		eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	}
	if len(parentSpanID) > 0 {
		eventAttr.addHexID([]byte("trace.parent_id"), parentSpanID)
	}

	// Mark as span event
	eventAttr.addString([]byte("meta.annotation_type"), []byte("span_event"))
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	if len(parentName) > 0 {
		eventAttr.addString([]byte("parent_name"), parentName)
	}

	// Parse event fields
	var name []byte
	var timeUnixNano uint64
	var exceptionAttrs *msgpAttributes
	var attributes []*fastjson.Value

	obj, err := v.Object()
	if err != nil {
		return nil, err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		switch string(key) {
		case "timeUnixNano":
			// Time is encoded as string in JSON
			strBytes := v.GetStringBytes()
			val, err := strconv.ParseUint(string(strBytes), 10, 64)
			if err == nil {
				timeUnixNano = val
			}

		case "name":
			name = v.GetStringBytes()

		case "attributes":
			attributes = v.GetArray()
		}
	})

	// Process attributes
	if attributes != nil {
		if err := unmarshalKeyValueArrayJSON(ctx, attributes, eventAttr, 0); err != nil {
			return nil, err
		}

		// If this is an exception event, also collect exception-specific attributes
		if bytes.Equal(name, []byte("exception")) {
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
	traceID,
	parentSpanID,
	parentName []byte,
	parentTimestamp time.Time,
	resourceAttrs,
	scopeAttrs *msgpAttributes,
	sampleRate int32,
	isError bool,
	batch *BatchMsgp,
) error {
	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)

	// Set trace info
	if len(traceID) > 0 {
		eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	}
	if len(parentSpanID) > 0 {
		eventAttr.addHexID([]byte("trace.parent_id"), parentSpanID)
	}

	// Mark as link
	eventAttr.addString([]byte("meta.annotation_type"), []byte("link"))
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	if len(parentName) > 0 {
		eventAttr.addString([]byte("parent_name"), parentName)
	}

	// Parse link fields
	var linkedTraceID, linkedSpanID []byte

	obj, err := v.Object()
	if err != nil {
		return err
	}

	obj.Visit(func(key []byte, v *fastjson.Value) {
		switch string(key) {
		case "traceId":
			// Trace ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				linkedTraceID = b
			}

		case "spanId":
			// Span ID is base64 encoded in JSON
			strBytes := v.GetStringBytes()
			b, err := base64.StdEncoding.DecodeString(string(strBytes))
			if err == nil {
				linkedSpanID = b
			}

		case "attributes":
			attributes := v.GetArray()
			if attributes != nil {
				if err := unmarshalKeyValueArrayJSON(ctx, attributes, eventAttr, 0); err != nil {
					// Store error but continue
					return
				}
			}
		}
	})

	// Set link fields
	if len(linkedTraceID) > 0 {
		eventAttr.addTraceID([]byte("trace.link.trace_id"), linkedTraceID)
	}
	if len(linkedSpanID) > 0 {
		eventAttr.addHexID([]byte("trace.link.span_id"), linkedSpanID)
	}

	// Add error status from parent span if applicable
	if isError {
		eventAttr.addBool([]byte("error"), true)
	}

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
