# OTLP Translator

This module provides an easy to use way of converting OTLP requests into easily ingestible data structures (eg `[]map[string]{}`).
This makes consuming the OTLP wire format easier and more consistent.

### Traces

You can either provide the OTLP trace request directly or a HTTP request object that contains the request in the body.

```
// HTTP Request (with zstdDecoders)
res, err := TranslateHttpTraceRequest(req *http.Request, zstdDecoders chan *zstd.Decoder)

// OTLP Trace gRPC
res, err := TranslateGrpcTraceRequest(request *collectorTrace.ExportTraceServiceRequest)
```

NOTES:
- The HTTP translator requires a channel of zstd Decoders (This may change in future)

### Common

The library also includes generic ways to extract request information (API Key, Dataset, etc).

```
// HTTP request
requestInfo := GetRequestInfoFromHttpHeaders(r *http.Request)

// gRPC request context
requestInfo := GetRequestInfoFromGrpcMetadata(ctx context.Context)
```
