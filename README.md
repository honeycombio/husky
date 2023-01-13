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
