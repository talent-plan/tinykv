// Copyright 2019 PingCAP, Inc.
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

package util

import (
	"bytes"
	"testing"
)

func TestCodec(t *testing.T) {
	colIDs := []int64{1, 4, 7, 2, 5, 8}
	cols := [][]byte{[]byte("147"), []byte("258"), []byte("147258"), []byte(""), []byte("258147"), []byte("369")}

	buf, err := EncodeRow(cols, colIDs, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, err := DecodeRow(buf)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range colIDs {
		if !bytes.Equal(cols[i], row[id]) {
			t.Fatalf("id:%v, before:%q, after:%q", id, cols[i], row[id])
		}
	}
}
