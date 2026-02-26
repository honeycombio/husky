package compat07

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// buildWireBytes constructs raw wire-format bytes from a sequence of
// (field number, payload) pairs, all as length-delimited fields.
func buildWireBytes(fields ...struct {
	num     protowire.Number
	payload []byte
}) []byte {
	var b []byte
	for _, f := range fields {
		b = protowire.AppendTag(b, f.num, protowire.BytesType)
		b = protowire.AppendBytes(b, f.payload)
	}
	return b
}

func TestExtractField(t *testing.T) {
	tests := []struct {
		name               string
		raw                []byte
		fieldNum           protowire.Number
		expectValues       [][]byte
		expectRemaining    []byte
		expectErr          bool
		expectErrSubstring string
	}{
		{
			name: "field present (single occurrence)",
			raw: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 4, payload: []byte{0x01, 0x02}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x03}},
			),
			fieldNum:     4,
			expectValues: [][]byte{{0x01, 0x02}},
			expectRemaining: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x03}},
			),
			expectErr:    false,
		},
		{
			name: "field absent",
			raw: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x03}},
			),
			fieldNum:     4,
			expectValues: nil,
			expectRemaining: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x03}},
			),
			expectErr:    false,
		},
		{
			name: "multiple occurrences (repeated field)",
			raw: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 1, payload: []byte{0x01}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 1, payload: []byte{0x02}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 1, payload: []byte{0x03}},
			),
			fieldNum:     1,
			expectValues: [][]byte{{0x01}, {0x02}, {0x03}},
			expectRemaining: nil,
			expectErr:    false,
		},
		{
			name: "mixed fields with multiple occurrences",
			raw: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 1, payload: []byte{0x01}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x04}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 1, payload: []byte{0x02}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 9, payload: []byte{0x05}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 1, payload: []byte{0x03}},
			),
			fieldNum:     1,
			expectValues: [][]byte{{0x01}, {0x02}, {0x03}},
			expectRemaining: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x04}},
				struct {
					num     protowire.Number
					payload []byte
				}{num: 9, payload: []byte{0x05}},
			),
			expectErr:    false,
		},
		{
			name:          "empty input",
			raw:           nil,
			fieldNum:      4,
			expectValues:  nil,
			expectErr:     false,
		},
		{
			name:               "malformed input (truncated tag)",
			raw:                []byte{0x80},
			fieldNum:           4,
			expectErr:          true,
			expectErrSubstring: "invalid tag",
		},
		{
			name: "malformed input (truncated value)",
			raw: func() []byte {
				// Construct a valid tag for field 4 (BytesType) followed by a length
				// prefix claiming 100 bytes but only 2 bytes present.
				var b []byte
				b = protowire.AppendTag(b, 4, protowire.BytesType)
				// Append a varint length (100) but then only 2 bytes of payload
				b = append(b, byte(100)) // 100 as a single-byte varint
				b = append(b, 0x01, 0x02)
				return b
			}(),
			fieldNum:           4,
			expectErr:          true,
			expectErrSubstring: "invalid field value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, remaining, err := extractField(tt.raw, tt.fieldNum)

			if tt.expectErr {
				require.Error(t, err)
				if tt.expectErrSubstring != "" {
					assert.Contains(t, err.Error(), tt.expectErrSubstring)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectValues, values)
				if tt.expectRemaining != nil {
					assert.Equal(t, tt.expectRemaining, remaining)
				} else {
					assert.True(t, remaining == nil || len(remaining) == 0, "expected remaining to be nil or empty")
				}
			}
		})
	}
}

func TestHasField(t *testing.T) {
	tests := []struct {
		name       string
		raw        []byte
		fieldNum   protowire.Number
		expectHas  bool
	}{
		{
			name: "field present",
			raw: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 4, payload: []byte{0x01, 0x02}},
			),
			fieldNum:  4,
			expectHas: true,
		},
		{
			name: "field absent",
			raw: buildWireBytes(
				struct {
					num     protowire.Number
					payload []byte
				}{num: 7, payload: []byte{0x03}},
			),
			fieldNum:  4,
			expectHas: false,
		},
		{
			name:       "empty input",
			raw:        nil,
			fieldNum:   4,
			expectHas:  false,
		},
		{
			name:       "malformed input",
			raw:        []byte{0x80},
			fieldNum:   4,
			expectHas:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			has := hasField(tt.raw, tt.fieldNum)
			assert.Equal(t, tt.expectHas, has)
		})
	}
}
