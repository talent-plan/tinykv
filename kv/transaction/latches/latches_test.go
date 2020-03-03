package latches

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestAcquireLatches(t *testing.T) {
	l := Latches{
		latchMap: make(map[string]*sync.WaitGroup),
	}

	// Acquiring a new latch is ok.
	wg := l.AcquireLatches([][]byte{{}, {3}, {3, 0, 42}})
	assert.Nil(t, wg)

	// Can only acquire once.
	wg = l.AcquireLatches([][]byte{{}})
	assert.NotNil(t, wg)
	wg = l.AcquireLatches([][]byte{{3, 0, 42}})
	assert.NotNil(t, wg)

	// Release then acquire is ok.
	l.ReleaseLatches([][]byte{{3}, {3, 0, 43}})
	wg = l.AcquireLatches([][]byte{{3}})
	assert.Nil(t, wg)
	wg = l.AcquireLatches([][]byte{{3, 0, 42}})
	assert.NotNil(t, wg)
}
