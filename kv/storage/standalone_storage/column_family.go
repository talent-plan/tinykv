package standalone_storage

import (
	"fmt"
	"strings"
)

//func createIndexKey(indexPrefixKey byte, value []byte, startTime uint64, traceID model.TraceID) []byte {
//	// KEY: indexKey<indexValue><startTime><traceId> (traceId is last 16 bytes of the key)
//	key := make([]byte, 1+len(value)+8+sizeOfTraceID)
//	key[0] = (indexPrefixKey & indexKeyRange) | spanKeyPrefix
//	pos := len(value) + 1
//	copy(key[1:pos], value)
//	binary.BigEndian.PutUint64(key[pos:], startTime)
//	pos += 8 // sizeOfTraceID / 2
//	binary.BigEndian.PutUint64(key[pos:], traceID.High)
//	pos += 8 // sizeOfTraceID / 2
//	binary.BigEndian.PutUint64(key[pos:], traceID.Low)
//	return key
//}

// just use simple rule to build a key represented in store
func createIndexKey(cf string, key string) string {
	return fmt.Sprintf("cf:%s:key:%s", cf, key)
}

func keyWithoutPrefix(storageKey string) string {
	ss := strings.Split(storageKey, ":")
	return ss[len(ss)-1]
}

func createCF(cf string) string {
	return fmt.Sprintf("cf:%s", cf)
}
