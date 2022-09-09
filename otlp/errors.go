package otlp

import (
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OTLPError struct {
	Message        string
	HTTPStatusCode int
	GRPCStatusCode codes.Code
}

var (
	ErrInvalidContentType   = OTLPError{"unsupported content-type, valid types are: " + strings.Join(GetSupportedContentTypes(), ", "), http.StatusUnsupportedMediaType, codes.Unimplemented}
	ErrFailedParseBody      = OTLPError{"failed to parse OTLP request body", http.StatusBadRequest, codes.Internal}
	ErrMissingAPIKeyHeader  = OTLPError{"missing 'x-honeycomb-team' header", http.StatusUnauthorized, codes.Unauthenticated}
	ErrMissingDatasetHeader = OTLPError{"missing 'x-honeycomb-dataset' header", http.StatusUnauthorized, codes.Unauthenticated}
)

func (e OTLPError) Error() string {
	return e.Message
}

func AsJson(e error) string {
	return fmt.Sprintf(`{"message":"%s"}`, e.Error())
}

func AsGRPCError(e error) error {
	if otlpErr, ok := e.(OTLPError); ok {
		return status.Error(otlpErr.GRPCStatusCode, otlpErr.Message)
	}
	return status.Error(codes.Internal, "")
}
