package otlp

// JSON unmarshaling for OTLP traces, providing direct translation to Honeycomb's
// event format with serialized messagepack attribute maps, avoiding intermediate
// allocations.

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/honeycombio/husky"

	jsoniter "github.com/json-iterator/go"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

var jsonConfig = jsoniter.Config{
	EscapeHTML:                    false,
	ObjectFieldMustBeSimpleString: true,
}.Froze()

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
	iter := jsoniter.ParseBytes(jsonConfig, data)

	// Read the root object
	rootType := iter.WhatIsNext()
	if rootType != jsoniter.ObjectValue {
		return nil, fmt.Errorf("expected object at root, got %v", rootType)
	}

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "resourceSpans":
			if err := unmarshalResourceSpansArrayJSON(ctx, iter, ri, result); err != nil {
				iter.ReportError("unmarshalResourceSpansArrayJSON", err.Error())
				return false
			}
		default:
			iter.Skip()
		}
		return true
	})

	if iter.Error != nil {
		return nil, iter.Error
	}

	return result, nil
}

// unmarshalResourceSpansArrayJSON parses an array of ResourceSpans from JSON
func unmarshalResourceSpansArrayJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	ri RequestInfo,
	result *TranslateOTLPRequestResultMsgp,
) error {
	// Handle null
	if iter.ReadNil() {
		return nil
	}

	// Read array
	for iter.ReadArray() {
		if err := unmarshalResourceSpansJSON(ctx, iter, ri, result); err != nil {
			return err
		}
	}

	return iter.Error
}

// unmarshalResourceSpansJSON parses a ResourceSpans message from JSON
func unmarshalResourceSpansJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	ri RequestInfo,
	result *TranslateOTLPRequestResultMsgp,
) error {
	var dataset string
	resourceAttrs := msgpAttributesPool.Get().(*msgpAttributes)
	defer resourceAttrs.recycle()

	// We need to read resource first, then scopeSpans
	// Store scopeSpans data to process after we have resource
	var scopeSpansData [][]byte

	// Read the ResourceSpans object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "resource":
			if err := unmarshalResourceJSON(ctx, iter, resourceAttrs); err != nil {
				iter.ReportError("unmarshalResourceJSON", err.Error())
				return false
			}
		case "scopeSpans":
			// Capture the raw JSON for scopeSpans to process later
			if iter.ReadNil() {
				return true
			}

			for iter.ReadArray() {
				raw := iter.SkipAndReturnBytes()
				scopeSpansData = append(scopeSpansData, raw)
			}

			if iter.Error != nil {
				return false
			}
		default:
			iter.Skip()
		}
		return true
	})

	// Determine dataset from resource attributes
	dataset = getDatasetFromMsgpAttr(ri, resourceAttrs)

	// Create a new batch for this resource
	result.Batches = append(result.Batches, BatchMsgp{
		Dataset: dataset,
		Events:  []EventMsgp{},
	})
	batch := &result.Batches[len(result.Batches)-1]

	// Now process scopeSpans with the resource attributes
	for _, scopeSpansJSON := range scopeSpansData {
		scopeIter := jsoniter.ParseBytes(jsonConfig, scopeSpansJSON)
		if err := unmarshalScopeSpansJSON(ctx, scopeIter, resourceAttrs, batch); err != nil {
			return err
		}
		if scopeIter.Error != nil {
			return scopeIter.Error
		}
	}

	return nil
}

// unmarshalResourceJSON parses a Resource message from JSON
func unmarshalResourceJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	attrs *msgpAttributes,
) error {
	if iter.ReadNil() {
		return nil
	}

	// Read the Resource object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "attributes":
			if err := unmarshalKeyValueArrayJSON(ctx, iter, attrs, 0); err != nil {
				iter.ReportError("unmarshalKeyValueArrayJSON", err.Error())
				return false
			}
		default:
			iter.Skip()
		}
		return true
	})

	return iter.Error
}

// unmarshalKeyValueArrayJSON parses an array of KeyValue from JSON
func unmarshalKeyValueArrayJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	attrs *msgpAttributes, depth int,
) error {
	if iter.ReadNil() {
		return nil
	}

	// Read array
	for iter.ReadArray() {
		if err := unmarshalKeyValueJSON(ctx, iter, attrs, depth); err != nil {
			return err
		}
	}

	return iter.Error
}

// unmarshalKeyValueJSON parses a KeyValue message from JSON and adds it to msgpAttributes
func unmarshalKeyValueJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	attrs *msgpAttributes, depth int,
) error {
	var keyBytes []byte
	var valueProcessed bool

	// Read the KeyValue object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "key":
			// Read key and store as bytes
			key := iter.ReadString()
			keyBytes = make([]byte, len(key))
			copy(keyBytes, key)
		case "value":
			if len(keyBytes) > 0 && !valueProcessed {
				if err := unmarshalAnyValueIntoAttrsJSON(ctx, iter, keyBytes, attrs, depth); err != nil {
					iter.ReportError("unmarshalAnyValueIntoAttrsJSON", err.Error())
					return false
				}
				valueProcessed = true
			} else {
				iter.Skip()
			}
		default:
			iter.Skip()
		}
		return true
	})

	return iter.Error
}

// unmarshalAnyValueIntoAttrsJSON parses an AnyValue from JSON and adds it directly to msgpAttributes
func unmarshalAnyValueIntoAttrsJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	key []byte,
	attrs *msgpAttributes,
	depth int,
) error {
	if iter.ReadNil() {
		return nil
	}

	// Read the AnyValue object to determine its type
	// JSON encoding uses field names like "stringValue", "intValue", etc.
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "stringValue":
			value := iter.ReadString()

			// Special handling for sample rate
			if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
				if f, err := strconv.ParseFloat(value, 64); err == nil {
					if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
						attrs.sampleRate = sampleRateFromFloat(f)
					}
				}
				return true // Continue processing other fields even though we've handled sampleRate
			}

			// Check for service name
			if bytes.Equal(key, []byte("service.name")) {
				attrs.serviceName = value
			}

			attrs.addString(key, []byte(value))

		case "boolValue":
			attrs.addBool(key, iter.ReadBool())

		case "intValue":
			// JSON encodes int64 as string
			strVal := iter.ReadString()
			v, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				iter.ReportError("ParseInt", fmt.Sprintf("failed to parse intValue: %v", err))
				return false
			}

			// Special handling for sample rate
			if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
				v = min(v, math.MaxInt32)
				if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
					attrs.sampleRate = max(defaultSampleRate, int32(v))
				}
				return true // Continue processing other fields
			}

			attrs.addInt64(key, v)

		case "doubleValue":
			floatVal := iter.ReadFloat64()

			// Special handling for sample rate
			if bytes.Equal(key, []byte("sampleRate")) || bytes.Equal(key, []byte("SampleRate")) {
				if attrs.sampleRate == 0 || bytes.Equal(key, []byte("sampleRate")) {
					attrs.sampleRate = sampleRateFromFloat(floatVal)
				}
				return true // Continue processing other fields
			}

			attrs.addFloat64(key, floatVal)

		case "bytesValue":
			// Bytes are base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode bytesValue: %v", err))
				return false
			}

			husky.AddTelemetryAttribute(ctx, "received_bytes_attr_type", true)
			err = attrs.addAny(key, addAttributeToMapAsJsonDirect(b))
			if err != nil {
				iter.ReportError("addAny", err.Error())
				return false
			}

		case "arrayValue":
			// For arrays, we need to parse and JSON encode
			// We can't skip unmarshal/marshal because we need to transform from OTLP format
			// (with AnyValue wrappers) to simplified JSON format
			husky.AddTelemetryAttribute(ctx, "received_array_attr_type", true)
			arr, err := unmarshalArrayValueJSON(ctx, iter)
			if err != nil {
				iter.ReportError("unmarshalArrayValueJSON", err.Error())
				return false
			}
			err = attrs.addAny(key, addAttributeToMapAsJsonDirect(arr))
			if err != nil {
				iter.ReportError("addAny", err.Error())
				return false
			}

		case "kvlistValue":
			// For kvlists, handle flattening
			husky.AddTelemetryAttributes(ctx, map[string]any{
				"received_kvlist_attr_type": true,
				"kvlist_max_depth":          depth,
			})

			if depth < maxDepth {
				// Flatten the kvlist
				if err := unmarshalKvlistValueFlattenJSON(ctx, iter, key, attrs, depth+1); err != nil {
					iter.ReportError("unmarshalKvlistValueFlattenJSON", err.Error())
					return false
				}
			} else {
				// Max depth exceeded, JSON encode the whole thing
				m, err := unmarshalKvlistValueJSON(ctx, iter)
				if err != nil {
					iter.ReportError("unmarshalKvlistValueJSON", err.Error())
					return false
				}
				err = attrs.addAny(key, addAttributeToMapAsJsonDirect(m))
				if err != nil {
					iter.ReportError("addAny", err.Error())
					return false
				}
			}

		default:
			iter.Skip()
		}
		return true
	})

	return iter.Error
}

// unmarshalArrayValueJSON parses an ArrayValue from JSON and returns []any
func unmarshalArrayValueJSON(ctx context.Context, iter *jsoniter.Iterator) ([]any, error) {
	var values []any

	// Read the ArrayValue object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "values":
			if !iter.ReadNil() {
				for iter.ReadArray() {
					val, err := unmarshalAnyValueJSON(ctx, iter)
					if err != nil {
						iter.ReportError("unmarshalAnyValueJSON", err.Error())
						return false
					}
					if val != nil {
						values = append(values, val)
					}
				}
			}
		default:
			iter.Skip()
		}
		return true
	})

	return values, iter.Error
}

// unmarshalAnyValueJSON parses an AnyValue from JSON and returns the value
func unmarshalAnyValueJSON(ctx context.Context, iter *jsoniter.Iterator) (any, error) {
	if iter.ReadNil() {
		return nil, nil
	}

	var result any

	// Read the AnyValue object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "stringValue":
			result = iter.ReadString()

		case "boolValue":
			result = iter.ReadBool()

		case "intValue":
			// JSON encodes int64 as string
			strVal := iter.ReadString()
			v, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				iter.ReportError("ParseInt", fmt.Sprintf("failed to parse intValue: %v", err))
				return false
			}
			result = v

		case "doubleValue":
			result = iter.ReadFloat64()

		case "bytesValue":
			// Bytes are base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode bytesValue: %v", err))
				return false
			}
			result = b

		case "arrayValue":
			arr, err := unmarshalArrayValueJSON(ctx, iter)
			if err != nil {
				iter.ReportError("unmarshalArrayValueJSON", err.Error())
				return false
			}
			result = arr

		case "kvlistValue":
			m, err := unmarshalKvlistValueJSON(ctx, iter)
			if err != nil {
				iter.ReportError("unmarshalKvlistValueJSON", err.Error())
				return false
			}
			result = m

		default:
			iter.Skip()
		}
		return true
	})

	return result, iter.Error
}

// unmarshalKvlistValueFlattenJSON parses a KeyValueList from JSON and flattens it into msgpAttributes
func unmarshalKvlistValueFlattenJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	keyPrefix []byte,
	attrs *msgpAttributes,
	depth int,
) error {
	// Read the KeyValueList object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "values":
			if !iter.ReadNil() {
				for iter.ReadArray() {
					// Parse each KeyValue in the list
					var key string
					var valueProcessed bool

					iter.ReadObjectCB(func(iter *jsoniter.Iterator, kvField string) bool {
						switch kvField {
						case "key":
							key = iter.ReadString()
						case "value":
							if key != "" && !valueProcessed {
								// Create flattened key
								// Create a new buffer to avoid corrupting the input
								flatKey := make([]byte, 0, len(keyPrefix)+1+len(key))
								flatKey = append(flatKey, keyPrefix...)
								flatKey = append(flatKey, '.')
								flatKey = append(flatKey, key...)

								// Process the value with AnyValue unmarshaling
								if err := unmarshalAnyValueIntoAttrsJSON(ctx, iter, flatKey, attrs, depth); err != nil {
									iter.ReportError("unmarshalAnyValueIntoAttrsJSON", err.Error())
									return false
								}
								valueProcessed = true
							} else {
								iter.Skip()
							}
						default:
							iter.Skip()
						}
						return true
					})

					if iter.Error != nil {
						return false
					}
				}
			}
		default:
			iter.Skip()
		}
		return true
	})

	return iter.Error
}

// unmarshalKvlistValueJSON parses a KeyValueList from JSON and returns map[string]any
func unmarshalKvlistValueJSON(ctx context.Context, iter *jsoniter.Iterator) (map[string]any, error) {
	result := make(map[string]any)

	// Read the KeyValueList object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "values":
			if !iter.ReadNil() {
				for iter.ReadArray() {
					// Parse each KeyValue in the list
					var key string
					var val any

					iter.ReadObjectCB(func(iter *jsoniter.Iterator, kvField string) bool {
						switch kvField {
						case "key":
							key = iter.ReadString()
						case "value":
							v, err := unmarshalAnyValueJSON(ctx, iter)
							if err != nil {
								iter.ReportError("unmarshalAnyValueJSON", err.Error())
								return false
							}
							val = v
						default:
							iter.Skip()
						}
						return true
					})

					if iter.Error != nil {
						return false
					}

					if key != "" && val != nil {
						result[key] = val
					}
				}
			}
		default:
			iter.Skip()
		}
		return true
	})

	return result, iter.Error
}

// unmarshalScopeSpansJSON parses a ScopeSpans message from JSON
func unmarshalScopeSpansJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	resourceAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {
	scopeAttrs := msgpAttributesPool.Get().(*msgpAttributes)
	defer scopeAttrs.recycle()

	// Store spans data to process after we have scope
	var spansData [][]byte

	// Read the ScopeSpans object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "scope":
			if err := unmarshalInstrumentationScopeJSON(ctx, iter, scopeAttrs); err != nil {
				iter.ReportError("unmarshalInstrumentationScopeJSON", err.Error())
				return false
			}
		case "spans":
			// Capture the raw JSON for spans to process later
			if !iter.ReadNil() {
				for iter.ReadArray() {
					raw := iter.SkipAndReturnBytes()
					spansData = append(spansData, raw)
				}
			}
		default:
			iter.Skip()
		}
		return true
	})

	if iter.Error != nil {
		return iter.Error
	}

	// Now process spans with both resource and scope attributes
	for _, spanJSON := range spansData {
		spanIter := jsoniter.ParseBytes(jsonConfig, spanJSON)
		if err := unmarshalSpanJSON(ctx, spanIter, resourceAttrs, scopeAttrs, batch); err != nil {
			return err
		}
		if spanIter.Error != nil {
			return spanIter.Error
		}
	}

	return nil
}

// unmarshalInstrumentationScopeJSON parses an InstrumentationScope message from JSON
func unmarshalInstrumentationScopeJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	attrs *msgpAttributes,
) error {
	if iter.ReadNil() {
		return nil
	}

	// Read the InstrumentationScope object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "name":
			name := iter.ReadString()
			if name != "" {
				attrs.addString([]byte("library.name"), []byte(name))
				if isInstrumentationLibrary(name) {
					attrs.addBool([]byte("telemetry.instrumentation_library"), true)
				}
			}
		case "version":
			version := iter.ReadString()
			if version != "" {
				attrs.addString([]byte("library.version"), []byte(version))
			}
		case "attributes":
			if err := unmarshalKeyValueArrayJSON(ctx, iter, attrs, 0); err != nil {
				iter.ReportError("unmarshalKeyValueArrayJSON", err.Error())
				return false
			}
		default:
			iter.Skip()
		}
		return true
	})

	return iter.Error
}

// unmarshalSpanJSON parses a Span message from JSON and creates an event
func unmarshalSpanJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
	resourceAttrs,
	scopeAttrs *msgpAttributes,
	batch *BatchMsgp,
) error {

	eventAttr := msgpAttributesPool.Get().(*msgpAttributes)
	defer eventAttr.recycle()

	var eventsData, linksData [][]byte
	var name, traceID, spanID []byte
	var traceState string
	var statusCode int64
	var startTimeUnixNano, endTimeUnixNano uint64
	kindStr := "unspecified"
	sampleRate := defaultSampleRate

	// Read the Span object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "traceId":
			// Trace ID is base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode traceId: %v", err))
				return false
			}
			traceID = b

		case "spanId":
			// Span ID is base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode spanId: %v", err))
				return false
			}
			spanID = b

		case "traceState":
			traceState = iter.ReadString()
			if traceState != "" {
				eventAttr.addString([]byte("trace.trace_state"), []byte(traceState))

				rate, ok := getSampleRateFromOTelSamplingThreshold(traceState)
				if ok {
					sampleRate = rate
				}
			}

		case "parentSpanId":
			// Parent span ID is base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode parentSpanId: %v", err))
				return false
			}
			if len(b) > 0 {
				eventAttr.addHexID([]byte("trace.parent_id"), b)
			}

		case "name":
			nameStr := iter.ReadString()
			name = []byte(nameStr)

		case "kind":
			// In JSON, kind can be either string enum name or integer
			if iter.WhatIsNext() == jsoniter.StringValue {
				// String enum like "SPAN_KIND_SERVER"
				enumStr := iter.ReadString()
				// Map from protobuf enum string to our string representation
				switch enumStr {
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
				kind := trace.Span_SpanKind(iter.ReadInt())
				kindStr = getSpanKind(kind)
			}

		case "startTimeUnixNano":
			// Time is encoded as string in JSON
			strVal := iter.ReadString()
			v, err := strconv.ParseUint(strVal, 10, 64)
			if err != nil {
				iter.ReportError("ParseUint", fmt.Sprintf("failed to parse startTimeUnixNano: %v", err))
				return false
			}
			startTimeUnixNano = v

		case "endTimeUnixNano":
			// Time is encoded as string in JSON
			strVal := iter.ReadString()
			v, err := strconv.ParseUint(strVal, 10, 64)
			if err != nil {
				iter.ReportError("ParseUint", fmt.Sprintf("failed to parse endTimeUnixNano: %v", err))
				return false
			}
			endTimeUnixNano = v

		case "attributes":
			if err := unmarshalKeyValueArrayJSON(ctx, iter, eventAttr, 0); err != nil {
				iter.ReportError("unmarshalKeyValueArrayJSON", err.Error())
				return false
			}

		case "events":
			// Capture the raw JSON for events to process later
			if !iter.ReadNil() {
				for iter.ReadArray() {
					raw := iter.SkipAndReturnBytes()
					eventsData = append(eventsData, raw)
				}
			}

		case "links":
			// Capture the raw JSON for links to process later
			if !iter.ReadNil() {
				for iter.ReadArray() {
					raw := iter.SkipAndReturnBytes()
					linksData = append(linksData, raw)
				}
			}

		case "status":
			if !iter.ReadNil() {
				// Parse status object
				iter.ReadObjectCB(func(iter *jsoniter.Iterator, statusField string) bool {
					switch statusField {
					case "message":
						message := iter.ReadString()
						if message != "" {
							eventAttr.addString([]byte("status_message"), []byte(message))
						}
					case "code":
						// In JSON, code can be either string enum or integer
						if iter.WhatIsNext() == jsoniter.StringValue {
							// String enum like "STATUS_CODE_OK"
							codeStr := iter.ReadString()
							switch codeStr {
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
							statusCode = int64(iter.ReadInt())
							// Check if this is an error status
							if statusCode == 2 { // STATUS_CODE_ERROR
								eventAttr.addBool([]byte("error"), true)
								eventAttr.isError = true
							}
						}
					default:
						iter.Skip()
					}
					return true
				})
			}

		default:
			iter.Skip()
		}
		return true
	})

	if iter.Error != nil {
		return iter.Error
	}

	// Add fields which are always expected
	eventAttr.addTraceID([]byte("trace.trace_id"), traceID)
	eventAttr.addHexID([]byte("trace.span_id"), spanID)
	eventAttr.addString([]byte("name"), name)
	eventAttr.addString([]byte("meta.signal_type"), []byte("trace"))
	eventAttr.addInt64([]byte("status_code"), statusCode)
	eventAttr.addInt64([]byte("span.num_events"), int64(len(eventsData)))
	eventAttr.addInt64([]byte("span.num_links"), int64(len(linksData)))
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
	for _, eventJSON := range eventsData {
		eventIter := jsoniter.ParseBytes(jsonConfig, eventJSON)
		exceptionAttrs, err := unmarshalSpanEventJSON(ctx, eventIter, traceID, spanID, name, startTimeUnixNano, resourceAttrs, scopeAttrs, sampleRate, eventAttr.isError, batch)
		if err != nil {
			return err
		}
		if eventIter.Error != nil {
			return eventIter.Error
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
	for _, linkJSON := range linksData {
		linkIter := jsoniter.ParseBytes(jsonConfig, linkJSON)
		err := unmarshalSpanLinkJSON(ctx, linkIter, traceID, spanID, name, timestamp, resourceAttrs, scopeAttrs, sampleRate, eventAttr.isError, batch)
		if err != nil {
			return err
		}
		if linkIter.Error != nil {
			return linkIter.Error
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

	return iter.Error
}

// unmarshalSpanEventJSON parses a Span.Event message from JSON and creates an event
// Returns exception attributes if this is an exception event, nil otherwise
func unmarshalSpanEventJSON(
	ctx context.Context,
	iter *jsoniter.Iterator,
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
	var attrData []byte

	// Read the Event object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "timeUnixNano":
			// Time is encoded as string in JSON
			strVal := iter.ReadString()
			v, err := strconv.ParseUint(strVal, 10, 64)
			if err != nil {
				iter.ReportError("ParseUint", fmt.Sprintf("failed to parse timeUnixNano: %v", err))
				return false
			}
			timeUnixNano = v

		case "name":
			nameStr := iter.ReadString()
			name = []byte(nameStr)

		case "attributes":
			// Store raw attributes data to process after we know the event name
			attrData = iter.SkipAndReturnBytes()

		default:
			iter.Skip()
		}
		return true
	})

	if iter.Error != nil {
		return nil, iter.Error
	}

	// Now process attributes if we have them
	if len(attrData) > 0 {
		// Parse attributes into event
		attrIter := jsoniter.ParseBytes(jsonConfig, attrData)
		if err := unmarshalKeyValueArrayJSON(ctx, attrIter, eventAttr, 0); err != nil {
			return nil, err
		}

		// If this is an exception event, also collect exception-specific attributes
		if bytes.Equal(name, []byte("exception")) {
			exceptionAttrs = msgpAttributesPool.Get().(*msgpAttributes)

			// Re-parse to extract exception attributes
			attrIter2 := jsoniter.ParseBytes(jsonConfig, attrData)
			for attrIter2.ReadArray() {
				var key string
				attrIter2.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
					switch field {
					case "key":
						key = iter.ReadString()
					case "value":
						// Only process if it's an exception attribute
						switch key {
						case "exception.message", "exception.type", "exception.stacktrace":
							iter.ReadObjectCB(func(iter *jsoniter.Iterator, valueField string) bool {
								if valueField == "stringValue" {
									val := iter.ReadString()
									exceptionAttrs.addString([]byte(key), []byte(val))
								} else {
									iter.Skip()
								}
								return true
							})
						case "exception.escaped":
							iter.ReadObjectCB(func(iter *jsoniter.Iterator, valueField string) bool {
								if valueField == "boolValue" {
									val := iter.ReadBool()
									exceptionAttrs.addBool([]byte(key), val)
								} else {
									iter.Skip()
								}
								return true
							})
						default:
							iter.Skip()
						}
					default:
						iter.Skip()
					}
					return true
				})
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
	iter *jsoniter.Iterator,
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

	// Read the Link object
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "traceId":
			// Trace ID is base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode link traceId: %v", err))
				return false
			}
			linkedTraceID = b

		case "spanId":
			// Span ID is base64 encoded in JSON
			strVal := iter.ReadString()
			b, err := base64.StdEncoding.DecodeString(strVal)
			if err != nil {
				iter.ReportError("DecodeString", fmt.Sprintf("failed to decode link spanId: %v", err))
				return false
			}
			linkedSpanID = b

		case "traceState":
			// Skip trace_state - original implementation doesn't add it
			iter.Skip()

		case "attributes":
			if err := unmarshalKeyValueArrayJSON(ctx, iter, eventAttr, 0); err != nil {
				iter.ReportError("unmarshalKeyValueArrayJSON", err.Error())
				return false
			}

		default:
			iter.Skip()
		}
		return true
	})

	if iter.Error != nil {
		return iter.Error
	}

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
