package otlp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

func TestKeyHashSet(t *testing.T) {
	tests := []struct {
		name      string
		adds      []uint64
		wantAdded []bool
		wantLen   int
	}{
		{"duplicates", []uint64{5, 9, 5, 9, 5}, []bool{true, true, false, false, false}, 2},
		{"zero hash is an ordinary member", []uint64{0, 3, 0}, []bool{true, true, false}, 2},
		{"colliding home slots", []uint64{1, 1 + keyHashSetMinSlots, 1 + 2*keyHashSetMinSlots, 1 + keyHashSetMinSlots}, []bool{true, true, true, false}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := newKeyHashSet()
			for i, hash := range tt.adds {
				assert.Equal(t, tt.wantAdded[i], set.add(hash), "add #%d", i)
			}
			assert.Equal(t, tt.wantLen, set.len())
		})
	}
}

func TestKeyHashSetGrowAndReset(t *testing.T) {
	set := newKeyHashSet()
	const count = 5 * keyHashSetMinSlots
	for i := range uint64(count) {
		require.True(t, set.add(i*0x9e3779b97f4a7c15), "first add of %d", i)
	}
	assert.Greater(t, len(set.slots), keyHashSetMinSlots)
	assert.Equal(t, count, set.len())
	for i := range uint64(count) {
		require.False(t, set.add(i*0x9e3779b97f4a7c15), "second add of %d", i)
	}

	set.reset()
	assert.Equal(t, 0, set.len())
	assert.True(t, set.add(0))
	assert.True(t, set.add(7))
}

func TestFinalizeDedupsAcrossGrowth(t *testing.T) {
	const unique = 300
	var attrs msgpAttributes
	for round := range 2 {
		for i := range unique {
			attrs.addString([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("round%d", round)))
		}
	}

	out, err := attrs.finalize()
	require.NoError(t, err)
	size, rest, err := msgp.ReadMapHeaderBytes(out)
	require.NoError(t, err)
	assert.EqualValues(t, unique, size)

	for i := range unique {
		var key, value string
		key, rest, err = msgp.ReadStringBytes(rest)
		require.NoError(t, err)
		value, rest, err = msgp.ReadStringBytes(rest)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("key%d", i), key)
		assert.Equal(t, "round0", value, "first occurrence wins")
	}
	assert.Empty(t, rest)
}

func BenchmarkMsgpAttributesFinalize(b *testing.B) {
	for _, count := range []int{8, 32, 100} {
		b.Run(fmt.Sprintf("attrs=%d", count), func(b *testing.B) {
			var attrs msgpAttributes
			for i := range count {
				attrs.addString([]byte(fmt.Sprintf("attribute.key.%d", i)), []byte("some value"))
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := attrs.finalize(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
