package otlp

import (
	"errors"
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
	ErrInvalidContentType   = OTLPError{Message: "unsupported content-type, valid types are: " + strings.Join(GetSupportedContentTypes(), ", "), HTTPStatusCode: http.StatusUnsupportedMediaType, GRPCStatusCode: codes.Unimplemented}
	ErrFailedParseBody      = OTLPError{Message: "failed to parse OTLP request body", HTTPStatusCode: http.StatusBadRequest, GRPCStatusCode: codes.Internal}
	ErrMissingAPIKeyHeader  = OTLPError{Message: "missing 'x-honeycomb-team' header", HTTPStatusCode: http.StatusUnauthorized, GRPCStatusCode: codes.Unauthenticated}
	ErrMissingDatasetHeader = OTLPError{Message: "missing 'x-honeycomb-dataset' header", HTTPStatusCode: http.StatusUnauthorized, GRPCStatusCode: codes.Unauthenticated}
)

func (e OTLPError) Error() string {
	return e.Message
}

func AsJson(e error) string {
	return fmt.Sprintf(`{"message":"%s"}`, e.Error())
}

func AsGRPCError(e error) error {
	var otlpErr OTLPError
	if errors.As(e, &otlpErr) {
		return status.Error(otlpErr.GRPCStatusCode, e.Error())
	}
	return status.Error(codes.Internal, "")
}
