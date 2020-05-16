// +build !amd64

package surf

func select64(x uint64, k int64) int64 {
	return select64Broadword(x, k)
}
