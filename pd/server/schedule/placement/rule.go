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

import "sort"

// PeerRoleType is the expected peer type of the placement rule.
type PeerRoleType string

const (
	// Voter can either match a leader peer or follower peer
	Voter PeerRoleType = "voter"
	// Leader matches a leader.
	Leader PeerRoleType = "leader"
	// Follower matches a follower.
	Follower PeerRoleType = "follower"
	// Learner matches a learner.
	Learner PeerRoleType = "learner"
)

func validateRole(s PeerRoleType) bool {
	return s == Voter || s == Leader || s == Follower || s == Learner
}

// Rule is the placement rule that can be checked against a region. When
// applying rules (apply means schedule regions to match selected rules), the
// apply order is defined by the tuple [GroupID, Index, ID].
type Rule struct {
	GroupID          string            `json:"group_id"`                    // mark the source that add the rule
	ID               string            `json:"id"`                          // unique ID within a group
	Index            int               `json:"index,omitempty"`             // rule apply order in a group, rule with less ID is applied first when indexes are equal
	Override         bool              `json:"override,omitempty"`          // when it is true, all rules with less indexes are disabled
	StartKey         []byte            `json:"-"`                           // range start key
	StartKeyHex      string            `json:"start_key"`                   // hex format start key, for marshal/unmarshal
	EndKey           []byte            `json:"-"`                           // range end key
	EndKeyHex        string            `json:"end_key"`                     // hex format end key, for marshal/unmarshal
	Role             PeerRoleType      `json:"role"`                        // expected role of the peers
	Count            int               `json:"count"`                       // expected count of the peers
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"` // used to select stores to place peers
	LocationLabels   []string          `json:"location_labels,omitempty"`   // used to make peers isolated physically
}

// Key returns (groupID, ID) as the global unique key of a rule.
func (r Rule) Key() [2]string {
	return [2]string{r.GroupID, r.ID}
}

// Rules are ordered by (GroupID, Index, ID).
func compareRule(a, b *Rule) int {
	switch {
	case a.GroupID < b.GroupID:
		return -1
	case a.GroupID > b.GroupID:
		return 1
	case a.Index < b.Index:
		return -1
	case a.Index > b.Index:
		return 1
	case a.ID < b.ID:
		return -1
	case a.ID > b.ID:
		return 1
	default:
		return 0
	}
}

func sortRules(rules []*Rule) {
	sort.Slice(rules, func(i, j int) bool { return compareRule(rules[i], rules[j]) < 0 })
}

// Sort Rules, trim concealed rules.
func prepareRulesForApply(rules []*Rule) []*Rule {
	sortRules(rules)
	res := rules[:0]
	var i, j int
	for i = 1; i < len(rules); i++ {
		if rules[j].GroupID != rules[i].GroupID {
			res = append(res, rules[j:i]...)
			j = i
		}
		if rules[i].Override {
			j = i
		}
	}
	return append(res, rules[j:]...)
}
