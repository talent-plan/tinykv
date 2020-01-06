// Copyright 2017 PingCAP, Inc.
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

package codec

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestTable(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct{}

func (s *testCodecSuite) TestDecodeBytes(c *C) {
	key := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < len(key); i++ {
		_, k, err := DecodeBytes(EncodeBytes([]byte(key[:i])))
		c.Assert(err, IsNil)
		c.Assert(string(k), Equals, key[:i])
	}
}

func (s *testCodecSuite) TestTableID(c *C) {
	key := EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff"))
	c.Assert(key.TableID(), Equals, int64(0xff))

	key = EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff_i\x01\x02"))
	c.Assert(key.TableID(), Equals, int64(0xff))

	key = []byte("t\x80\x00\x00\x00\x00\x00\x00\xff")
	c.Assert(key.TableID(), Equals, int64(0))

	key = EncodeBytes([]byte("T\x00\x00\x00\x00\x00\x00\x00\xff"))
	c.Assert(key.TableID(), Equals, int64(0))

	key = EncodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\xff"))
	c.Assert(key.TableID(), Equals, int64(0))
}
