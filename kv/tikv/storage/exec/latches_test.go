package exec

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestAcquireLatches(t *testing.T) {
	l := latches{
		latchMap: make(map[string]*sync.WaitGroup),
	}

	// Acquiring a new latch is ok.
	wg := l.acquireLatches([][]byte{{}, {3}, {3, 0, 42}})
	assert.Nil(t, wg)

	// Can only acquire once.
	wg = l.acquireLatches([][]byte{{}})
	assert.NotNil(t, wg)
	wg = l.acquireLatches([][]byte{{3, 0, 42}})
	assert.NotNil(t, wg)

	// Release then acquire is ok.
	l.releaseLatches([][]byte{{3}, {3, 0, 43}})
	wg = l.acquireLatches([][]byte{{3}})
	assert.Nil(t, wg)
	wg = l.acquireLatches([][]byte{{3, 0, 42}})
	assert.NotNil(t, wg)
}
