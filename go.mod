module github.com/honeycombio/husky

go 1.25.0

require (
	github.com/dgryski/go-wyhash v0.0.0-20191203203029-c4841ae36371
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.18.4
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling v0.146.0
	github.com/stretchr/testify v1.11.1
	github.com/tinylib/msgp v1.6.3
	github.com/valyala/fastjson v1.6.10
	go.opentelemetry.io/proto/otlp v1.9.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251222181119-0a764e51fe1b
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.52.0 // indirect
	go.opentelemetry.io/collector/pdata v1.52.0 // indirect
	go.opentelemetry.io/proto/otlp/collector/profiles/v1development v0.2.0
	go.opentelemetry.io/proto/otlp/profiles/v1development v0.2.0
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/proto/otlp => github.com/honeycombio/opentelemetry-proto-go/otlp v1.9.0-compat
