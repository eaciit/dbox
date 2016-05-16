package driver

import "math/rand"

type Rand struct {
	src rand.Source
}

var globalRand = &Rand{src: rand.NewSource(1)}

func (r *Rand) Read(p []byte) (n int, err error) {
	for i := 0; i < len(p); i += 7 {
		val := r.src.Int63()
		for j := 0; i+j < len(p) && j < 7; j++ {
			p[i+j] = byte(val)
			val >>= 8
		}
	}
	return len(p), nil
}

func randRead(p []byte) (n int, err error) {
	return globalRand.Read(p)
}
