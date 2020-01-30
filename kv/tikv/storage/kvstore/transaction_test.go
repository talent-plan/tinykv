package kvstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAppendTS(t *testing.T) {
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, AppendTS([]byte{}, 0))
	assert.Equal(t, []byte{42, 0, 0, 0, 0, 0, 0, 0, 0}, AppendTS([]byte{42}, 0))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0}, AppendTS([]byte{42, 0, 5}, 0))
	assert.Equal(t, []byte{42, 0, 0, 39, 154, 52, 120, 65, 255}, AppendTS([]byte{42}, 43543258743295))
	assert.Equal(t, []byte{42, 0, 5, 0, 0, 0, 0, 5, 226, 221, 76}, AppendTS([]byte{42, 0, 5}, 98753868))
}
