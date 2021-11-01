package otlp

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OTLPError struct {
	message        string
	httpStatusCode int
	grpcStatusCode codes.Code
}

var (
	ErrInvalidContentType   = OTLPError{"invalid content-type - only 'application/protobuf' is supported", http.StatusNotImplemented, codes.Unimplemented}
	ErrFailedParseBody      = OTLPError{"failed to parse OTLP request body", http.StatusBadRequest, codes.Internal}
	ErrMissingAPIKeyHeader  = OTLPError{"missing 'x-honeycomb-team' header", http.StatusUnauthorized, codes.Unauthenticated}
	ErrMissingDatasetHeader = OTLPError{"missing 'x-honeycomb-dataset' header", http.StatusUnauthorized, codes.Unauthenticated}
)

func (e OTLPError) Error() string {
	return e.message
}

func AsJson(e error) string {
	return fmt.Sprintf(`{"message":"%s"}`, e.Error())
}

func AsGRPCError(e error) error {
	if otlpErr, ok := e.(OTLPError); ok {
		return status.Error(otlpErr.grpcStatusCode, otlpErr.message)
	}
	return status.Error(codes.Internal, "")
}
