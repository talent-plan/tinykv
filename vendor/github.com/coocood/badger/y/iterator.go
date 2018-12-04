/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Meta     byte
	UserMeta []byte
	Value    []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() uint16 {
	return uint16(len(v.Value) + len(v.UserMeta) + 2) // meta
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = nil
	userMetaEnd := 2 + b[1]
	if b[1] != 0 {
		v.UserMeta = b[2:userMetaEnd]
	}
	v.Value = b[userMetaEnd:]
	// Reset the Version because *ValueStruct may be reused.
	v.Version = 0
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) {
	b[0] = v.Meta
	b[1] = byte(len(v.UserMeta))
	copy(b[2:], v.UserMeta)
	copy(b[2+len(v.UserMeta):], v.Value)
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) EncodeTo(buf []byte) []byte {
	buf = append(buf, v.Meta, byte(len(v.UserMeta)))
	buf = append(buf, v.UserMeta...)
	buf = append(buf, v.Value...)
	return buf
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	FillValue(vs *ValueStruct)
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}
