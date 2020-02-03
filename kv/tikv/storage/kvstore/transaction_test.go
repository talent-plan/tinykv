package kvstore

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAppendTS(t *testing.T) {
	assert.Equal(t, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, AppendTS([]byte{}, 0))
	assert.Equal(t, []byte{42, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, AppendTS([]byte{42}, 0))
	assert.Equal(t, []byte{42, 0, 5, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, AppendTS([]byte{42, 0, 5}, 0))
	assert.Equal(t, []byte{42, 0, 0, 39, 154, 52, 120, 65, 255}, AppendTS([]byte{42}, ^uint64(43543258743295)))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 5, 226, 221, 76}, AppendTS([]byte{42, 0, 5}, ^uint64(98753868)))

	// Test that encoded keys are in descending order.
	assert.True(t, bytes.Compare(AppendTS([]byte{42}, 238), AppendTS([]byte{200}, 0)) < 0)
}
