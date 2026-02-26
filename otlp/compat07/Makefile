.PHONY: all build test test-race lint generate generate-fixtures tidy clean

#: Build, lint, and test (default)
all: build lint test

#: Compile the package
build:
	go build ./...

#: Run tests with verbose output
test:
	go test -v ./...

#: Run tests with race detector
test-race:
	go test -race -count=1 ./...

#: Run go vet
lint:
	go vet ./...

#: Regenerate shadow proto Go code — run after updating buf or protoc-gen-go versions
generate:
	buf generate

#: Regenerate .binpb test fixtures — run after adding new scenarios or updating protobuf tooling
generate-fixtures:
	cd testdata/generate && go run .

#: Tidy go.mod for both the main and generator modules
tidy:
	go mod tidy
	cd testdata/generate && go mod tidy

#: Remove compiled binaries from testdata/generate
clean:
	rm -f testdata/generate/generate
