package util

import (
	"reflect"
	"testing"
)

func TestFieldPair(t *testing.T) {
	m := map[string][]byte{
		"f2": []byte("b"),
		"f1": []byte("a"),
	}

	p := NewFieldPairs(m)

	check := FieldPairs{
		{"f1", []byte("a")},
		{"f2", []byte("b")},
	}

	if !reflect.DeepEqual(p, check) {
		t.Errorf("want %v, but got %v", check, p)
	}
}
