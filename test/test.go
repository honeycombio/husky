package test

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// RandomByte generates a random byte slice of specified length
func RandomBytes(length int) []byte {
	SeedRand()
	b := make([]byte, length)
	rand.Read(b)
	return b
}

// RandomString generate a random string of specified length
func RandomString(length int) string {
	b := RandomBytes(length / 2)
	return fmt.Sprintf("%x", b)
}

func SeedRand() {
	once := &sync.Once{}
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
	})
}
