module github.com/honeycombio/husky

go 1.23.0

require (
	github.com/dgryski/go-wyhash v0.0.0-20191203203029-c4841ae36371
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.18.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling v0.128.0
	github.com/stretchr/testify v1.10.0
	github.com/tinylib/msgp v1.3.0
	github.com/valyala/fastjson v1.6.4
	go.opentelemetry.io/proto/otlp v0.19.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250512202823-5a2f75b736a9
	google.golang.org/grpc v1.72.2
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/collector/pdata v1.34.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250218202821-56aae31c358a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/proto/otlp => github.com/honeycombio/opentelemetry-proto-go/otlp v0.19.0-compat
