package otlp

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestErrorsReturnJson(t *testing.T) {
	err := OTLPError{message: "test-message"}
	assert.Equal(t, `{"message":"test-message"}`, AsJson(err))
}

func TestAsGRPCError(t *testing.T) {
	err := OTLPError{message: "otlp-error", grpcStatusCode: codes.InvalidArgument}
	assert.Equal(t, "rpc error: code = InvalidArgument desc = otlp-error", AsGRPCError(err).Error())
}

func TestNonOTLPErrorAsGRPCError(t *testing.T) {
	err := errors.New("base-error")
	assert.Equal(t, "rpc error: code = Internal desc = ", AsGRPCError(err).Error())
}
