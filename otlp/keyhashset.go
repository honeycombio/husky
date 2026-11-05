package otlp

import "math/rand/v2"

// keyHashSeed seeds the attribute key hashes. It is random per process so a
// sender cannot choose keys whose hashes collide in the low bits and force long
// probe chains in keyHashSet, or collide fully and have attributes dropped as
// duplicates.
var keyHashSeed = rand.Uint64()

// keyHashSet is an open-addressed set of 64-bit key hashes with linear
// probing. Keys must already be uniformly distributed (a seeded wyhash
// output), so the low bits pick the home slot with no further mixing. Hash 0
// marks an empty slot and is tracked out of band.
type keyHashSet struct {
	slots       []uint64
	mask        uint64
	size        int
	zeroPresent bool
}

const (
	keyHashSetMinSlots = 128
	// A set grown past this is dropped instead of pooled, so one span with a
	// huge attribute count doesn't make every later reset clear a large table.
	keyHashSetMaxPooledSlots = 512
)

func newKeyHashSet() *keyHashSet {
	return &keyHashSet{
		slots: make([]uint64, keyHashSetMinSlots),
		mask:  keyHashSetMinSlots - 1,
	}
}

// add inserts hash and reports whether it was absent.
func (s *keyHashSet) add(hash uint64) bool {
	if hash == 0 {
		added := !s.zeroPresent
		s.zeroPresent = true
		return added
	}
	for idx := hash & s.mask; ; idx = (idx + 1) & s.mask {
		switch s.slots[idx] {
		case hash:
			return false
		case 0:
			if s.size*4 >= len(s.slots)*3 {
				s.grow()
				return s.add(hash)
			}
			s.slots[idx] = hash
			s.size++
			return true
		}
	}
}

func (s *keyHashSet) len() int {
	if s.zeroPresent {
		return s.size + 1
	}
	return s.size
}

func (s *keyHashSet) reset() {
	clear(s.slots)
	s.size = 0
	s.zeroPresent = false
}

func (s *keyHashSet) grow() {
	old := s.slots
	s.slots = make([]uint64, len(old)*2)
	s.mask = uint64(len(s.slots) - 1)
	s.size = 0
	for _, hash := range old {
		if hash != 0 {
			s.add(hash)
		}
	}
}
