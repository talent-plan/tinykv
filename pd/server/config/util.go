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

package config

import (
	"regexp"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pkg/errors"
)

const matchRule = "^[A-Za-z0-9]([-A-Za-z0-9_./]*[A-Za-z0-9])?$"

// ValidateLabelString checks the legality of the label string.
// The valid label consists of alphanumeric characters, '-', '_', '.' or '/',
// and must start and end with an alphanumeric character.
func ValidateLabelString(s string) error {
	isValid, _ := regexp.MatchString(matchRule, s)
	if !isValid {
		return errors.Errorf("invalid label: %s", s)
	}
	return nil
}

// ValidateLabels checks the legality of the labels.
func ValidateLabels(labels []*metapb.StoreLabel) error {
	for _, label := range labels {
		err := ValidateLabelString(label.Key)
		if err != nil {
			return err
		}
		err = ValidateLabelString(label.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
