// Copyright 2018 PingCAP, Inc.
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

// Package core defines core characteristics of the server.
// This file uses the errcode packate to define PD specific error codes.
// Probably this should be a different package.
package core

import (
	"fmt"
	"net/http"

	"github.com/pingcap/errcode"
)

var (
	// Parent for other errors
	storeStateCode = errcode.StateCode.Child("state.store")

	// StoreBlockedCode is an error due to requesting an operation that is invalid due to a store being in a blocked state
	StoreBlockedCode = storeStateCode.Child("state.store.blocked")

	// StoreTombstonedCode is an invalid operation was attempted on a store which is in a removed state.
	StoreTombstonedCode = storeStateCode.Child("state.store.tombstoned").SetHTTP(http.StatusGone)
)

var _ errcode.ErrorCode = (*StoreTombstonedErr)(nil) // assert implements interface
var _ errcode.ErrorCode = (*StoreBlockedErr)(nil)    // assert implements interface

// StoreErr can be newtyped or embedded in your own error
type StoreErr struct {
	StoreID uint64 `json:"storeId"`
}

// StoreTombstonedErr is an invalid operation was attempted on a store which is in a removed state.
type StoreTombstonedErr StoreErr

func (e StoreTombstonedErr) Error() string {
	return fmt.Sprintf("The store %020d has been removed", e.StoreID)
}

// Code returns StoreTombstonedCode
func (e StoreTombstonedErr) Code() errcode.Code { return StoreTombstonedCode }

// StoreBlockedErr has a Code() of StoreBlockedCode
type StoreBlockedErr StoreErr

func (e StoreBlockedErr) Error() string {
	return fmt.Sprintf("store %v is blocked", e.StoreID)
}

// Code returns StoreBlockedCode
func (e StoreBlockedErr) Code() errcode.Code { return StoreBlockedCode }
