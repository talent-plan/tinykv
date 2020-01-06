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

package placement

import (
	"math/rand"

	. "github.com/pingcap/check"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct{}

func (s *testRuleSuite) TestPrepareRulesForApply(c *C) {
	rules := []*Rule{
		{GroupID: "g1", Index: 0, ID: "id5"},
		{GroupID: "g1", Index: 0, ID: "id6"},
		{GroupID: "g1", Index: 1, ID: "id4"},
		{GroupID: "g1", Index: 99, ID: "id3"},

		{GroupID: "g2", Index: 0, ID: "id1"},
		{GroupID: "g2", Index: 0, ID: "id2"},
		{GroupID: "g2", Index: 1, ID: "id0"},

		{GroupID: "g3", Index: 0, ID: "id6"},
		{GroupID: "g3", Index: 2, ID: "id1", Override: true},
		{GroupID: "g3", Index: 2, ID: "id2"},
		{GroupID: "g3", Index: 3, ID: "id0"},

		{GroupID: "g4", Index: 0, ID: "id9", Override: true},
		{GroupID: "g4", Index: 1, ID: "id8", Override: true},
		{GroupID: "g4", Index: 2, ID: "id7", Override: true},
	}
	expected := [][2]string{
		{"g1", "id5"}, {"g1", "id6"}, {"g1", "id4"}, {"g1", "id3"},
		{"g2", "id1"}, {"g2", "id2"}, {"g2", "id0"},
		{"g3", "id1"}, {"g3", "id2"}, {"g3", "id0"},
		{"g4", "id7"},
	}

	rand.Shuffle(len(rules), func(i, j int) { rules[i], rules[j] = rules[j], rules[i] })
	rules = prepareRulesForApply(rules)

	c.Assert(len(rules), Equals, len(expected))
	for i := range rules {
		c.Assert(rules[i].Key(), Equals, expected[i])
	}
}
