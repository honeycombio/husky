module github.com/honeycombio/husky

go 1.22.7

require (
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.17.11
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/proto/otlp v0.19.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241118233622-e639e219e697
	google.golang.org/grpc v1.69.2
	google.golang.org/protobuf v1.36.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241015192408-796eee8c2d53 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/proto/otlp => github.com/honeycombio/opentelemetry-proto-go/otlp v0.19.0-compat
