# OTLP Translator

This module provides an easy to use way of converting OTLP requests into easily ingestible data structures (eg `[]map[string]interface{}`).
This makes consuming the OTLP wire format easier and more consistent.

### Traces

You can either provide the OTLP trace request directly or a HTTP request object that contains the request in the body.

```go
// HTTP Request
ri := GetRequestInfoFromHttpHeaders(request.header) // (request.header http.Header)
res, err := TranslateHttpTraceRequest(request.body, ri) //(request.body io.Reader, ri RequestInfo)

// OTLP Trace gRPC
res, err := TranslateGrpcTraceRequest(request) // (request *collectorTrace.ExportTraceServiceRequest)
```

### Common

The library also includes generic ways to extract request information (API Key, Dataset, etc).

```go
// HTTP request
requestInfo := GetRequestInfoFromHttpHeaders(header) // (header http.Header)

// gRPC request context
requestInfo := GetRequestInfoFromGrpcMetadata(ctx) // (ctx context.Context)
```
