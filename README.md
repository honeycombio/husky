[![OSS Lifecycle](https://img.shields.io/osslifecycle/honeycombio/husky)](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)
[![Build Status](https://circleci.com/gh/honeycombio/husky.svg?style=shield)](https://circleci.com/gh/honeycombio/husky)

# Husky

A place to store translation modules to convert custom wire formats to Honeycomb formatted data structures.

IMPORTANT NOTE: most consumers of husky will probably want to use a backwards-compatible version
of the opentelemetry protobufs. If this applies to you, add the following to your go.mod file:

```go.mod
replace go.opentelemetry.io/proto/otlp => github.com/honeycombio/opentelemetry-proto-go/otlp v0.19.0
```

- [OTLP](./otlp/README.md)


## Data Transformations

List of transformations that Husky performs while translating from OTLP to Honeycomb event format. All of these transformation happen before handling the resource, scope and attributes. They will be overwritten by any attributes with matching keys. The order of precedence is (with 1 being highest):

1. Item (span, span even, span link, log record) attributes
2. Instrumentation scope attributes
3. Resource attributes
4. These transformations.


<!-- HTML tables are used so that the tables support cell merging.) -->
## Spans

<table>
  <tr>
    <th>OTLP Field</th>
    <th>Husky Honeycomb Event Field</th>
    <th>Notes</th>
  </tr>
  <tr>
    <td>trace_id</td>
    <td>trace.trace_id</td>
    <td></td>
  </tr>
  <tr>
    <td>span_id</td>
    <td>trace.span_id</td>
    <td></td>
  </tr>
  <tr>
    <td>name</td>
    <td>name</td>
    <td>The span's name becomes the event name</td>
  </tr>
  <tr>
    <td rowspan="2">kind</td>
    <td>type</td>
    <td rowspan="2">kind is used to set both these fields. Also, Husky converts the OTLP numeric enum to a string</td>
  </tr>
  <tr>
    <td>span.kind</td>
  </tr>
  <tr>
    <td rowspan="2">status</td>
    <td>status_code</td>
    <td rowspan="2">status in OTLP is a Status, which is an object with a string message and a numeric enum status_code.</td>
  </tr>
  <tr>
    <td>status_message</td>
  </tr>
  <tr>
    <td></td>
    <td>span.num_events</td>
    <td>Calculated using number of span events</td>
  </tr>
  <tr>
    <td></td>
    <td>span.num_links</td>
    <td>Calculated using number of span links</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.signal_type</td>
    <td>Added by husky based on otlp signal</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.invalid_duration</td>
    <td>Set if the span's duration is less than 0</td>
  </tr>
  <tr>
    <td></td>
    <td>duration_ms</td>
    <td>Calculated as difference betweeen end time and start time. Converted from nanoseconds to milliseconds</td>
  </tr>
  <tr>
    <td>parent_span_id</td>
    <td>trace.parent_id</td>
    <td></td>
  </tr>
  <tr>
    <td></td>
    <td>error</td>
    <td>Set if the status_code was STATUS_CODE_ERROR</td>
  </tr>
  <tr>
    <td>trace_state</td>
    <td>trace.trace_state</td>
    <td></td>
  </tr>
  <tr>
    <td rowspan="4"></td>
    <td>exception.message</td>
    <td rowspan="4">Attributes from a span event with name "exception" are copied on to the honeycomb event's fields.</td>
  </tr>
  <tr>
    <td>exception.type</td>
  </tr>
  <tr>
    <td>exception.stacktrace</td>
  </tr>
  <tr>
    <td>exception.escaped</td>
  </tr>
  <tr>
    <td>instrumentation_scope.name</td>
    <td>library.name</td>
    <td>Specifically the instrumentation scope name, not the span name</td>
  </tr>
  <tr>
    <td></td>
    <td>telemetry.instrumentation_library</td>
    <td>Set if the instrumentation scope name matches a fixed list of instrumentation libraries</td>
  </tr>
</table>

## Span Events

<table>
  <tr>
    <th>OTLP Field</th>
    <th>Husky Honeycomb Event Field</th>
    <th>Notes</th>
  </tr>
  <tr>
    <td>trace_id</td>
    <td>trace.trace_id</td>
    <td></td>
  </tr>
  <tr>
    <td>span_id</td>
    <td>trace.span_id</td>
    <td></td>
  </tr>
  <tr>
    <td>name</td>
    <td>name</td>
    <td>The span event's name becomes the event name</td>
  </tr>
  <tr>
    <td></td>
    <td>parent_name</td>
    <td>Husky uses the span's name to set this field on all its span events</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.annotation_type</td>
    <td>Added by husky based on object type (span event or span link)</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.signal_type</td>
    <td>Added by husky based on otlp signal</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.invalid_time_since_span_start</td>
    <td>Set if the span events's time happened before the span's start time.</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.time_since_span_start_ms</td>
    <td>Calculated as difference betweeen spen event time and span start time. Converted from nanoseconds to milliseconds</td>
  </tr>
  <tr>
    <td></td>
    <td>error</td>
    <td>Set if the span's status_code was STATUS_CODE_ERROR</td>
  </tr>
  <tr>
    <td rowspan="4"></td>
    <td>exception.message</td>
    <td rowspan="4">Attributes from a span event with name "exception" are copied on to the honeycomb event's fields.</td>
  </tr>
  <tr>
    <td>exception.type</td>
  </tr>
  <tr>
    <td>exception.stacktrace</td>
  </tr>
  <tr>
    <td>exception.escaped</td>
  </tr>
</table>

## Span Links

<table>
  <tr>
    <th>OTLP Field</th>
    <th>Husky Honeycomb Event Field</th>
    <th>Notes</th>
  </tr>
  <tr>
    <td rowspan="2"></td>
    <td>trace.trace_id</td>
    <td rowspan="2">For a span link, the trace.trace_id and trace.span_id fields come from the trace_id and span_id of the associated span. The Link's actual trace_id and span_id fields, which in OTLP reference the span this Link points to, are recorded as trace.link.trace_id and trace.link.span_id</td>
  </tr>
  <tr>
    <td>trace.span_id</td>
  </tr>
  <tr>
    <td>trace_id</td>
    <td>trace.link.trace_id</td>
    <td></td>
  </tr>
  <tr>
    <td>span_id</td>
    <td>trace.link.span_id</td>
    <td></td>
  </tr>
  <tr>
    <td></td>
    <td>parent_name</td>
    <td>Husky uses the span's name to set this field on all the span's Span Links</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.annotation_type</td>
    <td>Added by husky based on object type (span event or span link)</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.signal_type</td>
    <td>Added by husky based on otlp signal</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.invalid_time_since_span_start</td>
    <td>Set if the span link's time happened before the span's start time.</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.time_since_span_start_ms</td>
    <td>Calculated as difference betweeen span link time and span start time. Converted from nanoseconds to milliseconds</td>
  </tr>
  <tr>
    <td></td>
    <td>error</td>
    <td>Set if the span's status_code was STATUS_CODE_ERROR</td>
  </tr>
</table>

## Log Records

<table>
  <tr>
    <th>OTLP Field</th>
    <th>Husky Honeycomb Event Field</th>
    <th>Notes</th>
  </tr>
  <tr>
    <td>trace_id</td>
    <td>trace.trace_id</td>
    <td></td>
  </tr>
  <tr>
    <td>span_id</td>
    <td>trace.parent_id</td>
    <td rowspan="2">Husky uses the log's span id to set trace.parent_id instead of trace.span_id. This manipulation is what makes log records on a trace appear on the trace waterfall as Span Events in the UI. If trace.span_id was set, the log would look like a root span to our trace waterfall logic and Refinery. The meta.annotation_type field is only added when the log has a trace id. Set to span_event.</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.annotation_type</td>
  </tr>
  <tr>
    <td></td>
    <td>meta.signal_type</td>
    <td>Added by husky based on otlp signal</td>
  </tr>
  <tr>
    <td></td>
    <td>severity</td>
    <td>A string representation of the log's severity_code</td>
  </tr>
  <tr>
    <td>severity_number</td>
    <td>severity_code</td>
    <td></td>
  </tr>
  <tr>
    <td>body</td>
    <td>body</td>
    <td>We use the field "body" to record the body when it is a string. If the body is a Map in the OTLP payload, we flatten it into the honeycomb event. It flattens to a dept of 5, using the original "body" field as a prefix for the key, and then appending each subsequent field name to the key, delimited with a dot ("."). Anything deeper is converted to a json string and used as the final value for that field. After flattening, if the field "body" does not exist, we set the field with a json string representation of the map. This was Husky's original behavior for OTLP maps and was kept to ensure that the addition of this flattening feature was not a breaking change.</td>
  </tr>
</table>
