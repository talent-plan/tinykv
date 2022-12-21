// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
)

// Fatalf prints the message and exits the program.
func Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Println("")
	os.Exit(1)
}

// Fatal prints the message and exits the program.
func Fatal(args ...interface{}) {
	fmt.Fprint(os.Stderr, args...)
	fmt.Println("")
	os.Exit(1)
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandBytes fills the bytes with alphabetic characters randomly
func RandBytes(r *rand.Rand, b []byte) {
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
}

// BufPool is a bytes.Buffer pool
type BufPool struct {
	p *sync.Pool
}

// NewBufPool creates a buffer pool.
func NewBufPool() *BufPool {
	p := &sync.Pool{
		New: func() interface{} {
			return []byte(nil)
		},
	}
	return &BufPool{
		p: p,
	}
}

// Get gets a buffer.
func (b *BufPool) Get() []byte {
	buf := b.p.Get().([]byte)
	buf = buf[:0]
	return buf
}

// Put returns a buffer.
func (b *BufPool) Put(buf []byte) {
	b.p.Put(buf)
}
