# OTLP Translator

This module provides an easy to use way of converting OTLP requests into easily ingestible data structures (eg `[]map[string]interface{}`).
This makes consuming the OTLP wire format easier and more consistent.

### Traces

You can either provide the OTLP trace request directly or a HTTP request object that contains the request in the body.

```
// HTTP Request
res, err := TranslateHttpTraceRequest(req *http.Request)

// OTLP Trace gRPC
res, err := TranslateGrpcTraceRequest(request *collectorTrace.ExportTraceServiceRequest)
```

### Common

The library also includes generic ways to extract request information (API Key, Dataset, etc).

```
// HTTP request
requestInfo := GetRequestInfoFromHttpHeaders(r *http.Request)

// gRPC request context
requestInfo := GetRequestInfoFromGrpcMetadata(ctx context.Context)
```
