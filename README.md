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

## Spans

| OTLP Field	     | Husky Honeycomb Event Field	       | Notes	                                                                                                 |
|-----------------|------------------------------------|--------------------------------------------------------------------------------------------------------|
| trace_id	       | trace.trace_id	                    | 	                                                                                                      |
| span_id	        | trace.span_id	                     | 	                                                                                                      |
| kind	           | type	                              | kind is used to set both these fields. Also, Husky converts the OTLP numeric enum to a string	         |
| kind	           | span.kind	                         |
| status	         | status_code	                       | status in OTLP is a Status, which is an object with a string message and a numeric enum status_code. 	 |
| status	         | status_message	                    |
| 	               | span.num_events	                   | Calculated using number of span events 	                                                               |
| 	               | span.num_links	                    | Calculated using number of span links	                                                                 |
| 	               | meta.signal_type	                  | Added by husky based on otlp signal	                                                                   |
| 	               | meta.invalid_duration	             | Set if the span's duration is less than 0	                                                             |
| 	               | duration_ms	                       | Calculated as difference betweeen end time and start time. Converted from nanoseconds to milliseconds	 |
| parent_span_id	 | trace.parent_id	                   | 	                                                                                                      |
| 	               | error	                             | Set if the status_code was STATUS_CODE_ERROR	                                                          |
| trace_state	    | trace.trace_state	                 | 	                                                                                                      |
| 	               | exception.message	                 | Attributes from a span event with name "exception" are copied on to the honeycomb event's fields. 	    |
| 	               | exception.type	                    |
| 	               | exception.stacktrace	              |
| 	               | exception.escaped	                 |
| name	           | library.name	                      | Specifically the instrumentation scope name, not the span name	                                        |
| 	               | telemetry.instrumentation_library	 | Set if the instrumentation scope name matches a fixed list of instrumentation libraries	               |

## Span Events

| OTLP Field	 | Husky Honeycomb Event Field	        | Notes	                                                                                                             |
|-------------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| trace_id	   | trace.trace_id	                     | 	                                                                                                                  |
| span_id	    | trace.span_id	                      | 	                                                                                                                  |
| 	           | parent_name	                        | Husky uses the span's name to set this field on all its span events	                                               |
| 	           | meta.annotation_type	               | Added by husky based 	                                                                                             |
| 	           | meta.signal_type	                   | Added by husky based on otlp signal	                                                                               |
| 	           | meta.invalid_time_since_span_start	 | Set if the span events's time happened before the span's start time.	                                              |
| 	           | meta.time_since_span_start_ms	      | Calculated as difference betweeen spen event time and span start time. Converted from nanoseconds to milliseconds	 |
| 	           | error	                              | Set if the span's status_code was STATUS_CODE_ERROR	                                                               |
| 	           | exception.message	                  | Attributes from a span event with name "exception" are copied on to the honeycomb event's fields. 	                |
| 	           | exception.type	                     |
| 	           | exception.stacktrace	               |
| 	           | exception.escaped	                  |

## Span Links

| OTLP Field	 | Husky Honeycomb Event Field	        | Notes	                                                                                                                                                                                                                                                                                    |
|-------------|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 	           | trace.trace_id	                     | For a span link, the trace.trace_id and trace.span_id fields come from the trace_id and span_id of the associated span. The Link's actual trace_id and span_id fields, which in OTLP reference the span this Link points to, are recorded as trace.link.trace_id and trace.link.span_id 	 |
| 	           | trace.span_id	                      |
| trace_id	   | trace.link.trace_id	                |
| span_id	    | trace.link.span_id	                 |
| 	           | parent_name	                        | Husky uses the span's name to set this field on all the span's Span Links	                                                                                                                                                                                                                |
| 	           | meta.annotation_type	               | Added by husky based 	                                                                                                                                                                                                                                                                    |
| 	           | meta.signal_type	                   | Added by husky based on otlp signal	                                                                                                                                                                                                                                                      |
| 	           | meta.invalid_time_since_span_start	 | Set if the span link's time happened before the span's start time.	                                                                                                                                                                                                                       |
| 	           | meta.time_since_span_start_ms	      | Calculated as difference betweeen spen link time and span start time. Converted from nanoseconds to milliseconds	                                                                                                                                                                         |
| 	           | error	                              | Set if the span's status_code was STATUS_CODE_ERROR	                                                                                                                                                                                                                                      |

## Log Records

| OTLP Field	      | Husky Honeycomb Event Field	 | Notes	                                                                                                                                                                                                                                                                                                                                                                                               |
|------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| trace_id	        | trace.trace_id	              | 	                                                                                                                                                                                                                                                                                                                                                                                                    |
| span_id	         | trace.parent_id	             | Husky uses the log's span id to set trace.parent_id instead of trace.span_id. This manipulation is what makes log records on a trace appear on the trace waterfall as Span Events in the UI. If trace.span_id was set, the log would look like a root span to our trace waterfall logic and Refinery. The meta.annotation_type field is only added when the log has a trace id. Set to span_event. 	 |
| 	                | meta.annotation_type	        |
| 	                | meta.signal_type	            | Added by husky based on otlp signal	                                                                                                                                                                                                                                                                                                                                                                 |
| 	                | severity	                    | A string representation of the log's severity_code	                                                                                                                                                                                                                                                                                                                                                  |
| severity_number	 | severity_code	               | 	                                                                                                                                                                                                                                                                                                                                                                                                    |
| body	            | body	                        | We use the field "body" to record the body when it is a string. If the body is a Map in the OTLP payload, we flatten it into the honeycomb event. After flattening if the field "body" does not exist, we set the field with a json string representation of the map.	                                                                                                                               |
