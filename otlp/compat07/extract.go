package compat07

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// extractField extracts all occurrences of fieldNum from raw unknown field
// bytes. For each occurrence of a length-delimited field (wire type 2), it
// returns the inner message bytes (without tag or length prefix). The remaining
// bytes (all fields that were NOT extracted) are also returned, suitable for
// passing back to SetUnknown.
//
// Returns an error if the raw bytes are malformed (invalid tag or field value).
// Returns (nil, nil, nil) if raw is empty.
func extractField(raw []byte, fieldNum protowire.Number) (values [][]byte, remaining []byte, err error) {
	b := raw
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return nil, nil, fmt.Errorf("invalid tag in unknown fields: %w", protowire.ParseError(tagLen))
		}

		valLen := protowire.ConsumeFieldValue(num, typ, b[tagLen:])
		if valLen < 0 {
			return nil, nil, fmt.Errorf("invalid field value for field %d: %w", num, protowire.ParseError(valLen))
		}

		totalLen := tagLen + valLen

		if num == fieldNum {
			if typ != protowire.BytesType {
				return nil, nil, fmt.Errorf("expected length-delimited (wire type 2) for field %d, got wire type %d", fieldNum, typ)
			}
			// ConsumeBytes parses the varint length prefix and returns the inner bytes.
			innerBytes, _ := protowire.ConsumeBytes(b[tagLen:])
			values = append(values, innerBytes)
		} else {
			remaining = append(remaining, b[:totalLen]...)
		}

		b = b[totalLen:]
	}
	return values, remaining, nil
}

// hasField checks whether fieldNum appears anywhere in the raw unknown field
// bytes. This is cheaper than extractField when you only need a boolean answer
// (e.g., for Has07Data).
func hasField(raw []byte, fieldNum protowire.Number) bool {
	b := raw
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return false // malformed; caller should use extractField for error detail
		}

		valLen := protowire.ConsumeFieldValue(num, typ, b[tagLen:])
		if valLen < 0 {
			return false
		}

		if num == fieldNum {
			return true
		}

		b = b[tagLen+valLen:]
	}
	return false
}
