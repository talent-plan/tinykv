// +build amd64

package surf

import "github.com/klauspost/cpuid"

var hasBMI2 = cpuid.CPU.BMI2()

// go:noescape
func select64(x uint64, k int64) int64
