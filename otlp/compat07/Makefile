.PHONY: generate generate-fixtures

#: Regenerate shadow proto Go code — run after updating buf or protoc-gen-go versions
generate:
	buf generate

#: Regenerate .binpb test fixtures — run after adding new scenarios or updating protobuf tooling
generate-fixtures:
	cd testdata/generate && go run .
