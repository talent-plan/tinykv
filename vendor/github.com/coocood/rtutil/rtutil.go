package rtutil

import (
	"unsafe"
)

// NanoTime returns the current time in nanoseconds from a monotonic clock.
//go:linkname NanoTime runtime.nanotime
func NanoTime() int64

// CPUTicks is a faster alternative to NanoTime to measure time duration.
//go:linkname CPUTicks runtime.cputicks
func CPUTicks() int64

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname aeshash runtime.aeshash
func aeshash(p unsafe.Pointer, h, s uintptr) uintptr

// AESHash is the hash function used by map, it utilize available hardware instructions.
func AESHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(aeshash(ss.str, 0, uintptr(ss.len)))
}

// FastRand is a fast thread local random function.
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

