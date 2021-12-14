package husky

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionIsPopulated(t *testing.T) {
	assert.NotEmpty(t, Version)
}
