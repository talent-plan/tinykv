package kvstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeKey(t *testing.T) {
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0, 247, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, EncodeKey([]byte{}, 0))
	assert.Equal(t, []byte{42, 0, 0, 0, 0, 0, 0, 0, 248, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, EncodeKey([]byte{42}, 0))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 0, 250, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, EncodeKey([]byte{42, 0, 5}, 0))
	assert.Equal(t, []byte{42, 0, 0, 0, 0, 0, 0, 0, 248, 0, 0, 39, 154, 52, 120, 65, 255}, EncodeKey([]byte{42}, ^uint64(43543258743295)))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 5, 226, 221, 76}, EncodeKey([]byte{42, 0, 5}, ^uint64(98753868)))

	// Test that encoded keys are in descending order.
	assert.True(t, bytes.Compare(EncodeKey([]byte{42}, 238), EncodeKey([]byte{200}, 0)) < 0)
	assert.True(t, bytes.Compare(EncodeKey([]byte{42}, 238), EncodeKey([]byte{42, 0}, 0)) < 0)
}

func TestDecodeKey(t *testing.T) {
	assert.Equal(t, []byte{}, decodeUserKey(EncodeKey([]byte{}, 0)))
	assert.Equal(t, []byte{42}, decodeUserKey(EncodeKey([]byte{42}, 0)))
	assert.Equal(t, []byte{42, 0, 5}, decodeUserKey(EncodeKey([]byte{42, 0, 5}, 0)))
	assert.Equal(t, []byte{42}, decodeUserKey(EncodeKey([]byte{42}, 2342342355436234)))
	assert.Equal(t, []byte{42, 0, 5}, decodeUserKey(EncodeKey([]byte{42, 0, 5}, 234234)))
}
