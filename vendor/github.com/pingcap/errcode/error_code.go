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

// Package errcode facilitates standardized API error codes.
// The goal is that clients can reliably understand errors by checking against immutable error codes
//
// This godoc documents usage. For broader context, see https://github.com/pingcap/errcode/tree/master/README.md
//
// Error codes are represented as strings by CodeStr (see CodeStr documentation).
//
// This package is designed to have few opinions and be a starting point for how you want to do errors in your project.
// The main requirement is to satisfy the ErrorCode interface by attaching a Code to an Error.
// See the documentation of ErrorCode.
// Additional optional interfaces HasClientData, HasOperation, Causer, and StackTracer are provided for extensibility
// in creating structured error data representations.
//
// Hierarchies are supported: a Code can point to a parent.
// This is used in the HTTPCode implementation to inherit HTTP codes found with MetaDataFromAncestors.
// The hierarchy is present in the Code's string representation with a dot separation.
//
// A few generic top-level error codes are provided (see the variables section of the doc).
// You are encouraged to create your own error codes customized to your application rather than solely using generic errors.
//
// See NewJSONFormat for an opinion on how to send back meta data about errors with the error data to a client.
// JSONFormat includes a body of response data (the "data field") that is by default the data from the Error
// serialized to JSON.
//
// Stack traces are automatically added by NewInternalErr and show up as the Stack field in JSONFormat.
// Errors can be grouped with Combine() and ungrouped via Errors() which show up as the Others field in JSONFormat.
//
// To extract any ErrorCodes from an error, use CodeChain().
// This extracts error codes without information loss (using ChainContext).
package errcode

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
)

// CodeStr is the name of the error code.
// It is a representation of the type of a particular error.
// The underlying type is string rather than int.
// This enhances both extensibility (avoids merge conflicts) and user-friendliness.
// A CodeStr can have dot separators indicating a hierarchy.
//
// Generally a CodeStr should never be modified once used by clients.
// Instead a new CodeStr should be created.
type CodeStr string

func (str CodeStr) String() string { return string(str) }

// A Code has a CodeStr representation.
// It is attached to a Parent to find metadata from it.
type Code struct {
	// codeStr does not include parent paths
	// The full code (with parent paths) is accessed with CodeStr
	codeStr CodeStr
	Parent  *Code
}

// CodeStr gives the full dot-separted path.
// This is what should be used for equality comparison.
func (code Code) CodeStr() CodeStr {
	if code.Parent == nil {
		return code.codeStr
	}
	return (*code.Parent).CodeStr() + "." + code.codeStr
}

// NewCode creates a new top-level code.
// A top-level code must not contain any dot separators: that will panic
// Most codes should be created from hierachry with the Child method.
func NewCode(codeRep CodeStr) Code {
	code := Code{codeStr: codeRep}
	if err := code.checkCodePath(); err != nil {
		panic(err)
	}
	return code
}

// Child creates a new code from a parent.
// For documentation purposes, a childStr may include the parent codes with dot-separation.
// An incorrect parent reference in the string panics.
func (code Code) Child(childStr CodeStr) Code {
	child := Code{codeStr: childStr, Parent: &code}
	if err := child.checkCodePath(); err != nil {
		panic(err)
	}
	// Don't store parent paths, those are re-constructed in CodeStr()
	paths := strings.Split(child.codeStr.String(), ".")
	child.codeStr = CodeStr(paths[len(paths)-1])
	return child
}

// FindAncestor looks for an ancestor satisfying the given test function.
func (code Code) findAncestor(test func(Code) bool) *Code {
	if test(code) {
		return &code
	}
	if code.Parent == nil {
		return nil
	}
	return (*code.Parent).findAncestor(test)
}

// IsAncestor looks for the given code in its ancestors.
func (code Code) IsAncestor(ancestorCode Code) bool {
	return nil != code.findAncestor(func(an Code) bool { return an == ancestorCode })
}

// MetaData is used in a pattern for attaching meta data to codes and inheriting it from a parent.
// See MetaDataFromAncestors.
// This is used to attach an HTTP code to a Code as meta data.
type MetaData map[CodeStr]interface{}

// MetaDataFromAncestors looks for meta data starting at the current code.
// If not found, it traverses up the hierarchy
// by looking for the first ancestor with the given metadata key.
// This is used in the HTTPCode implementation to inherit the HTTP Code from ancestors.
func (code Code) MetaDataFromAncestors(metaData MetaData) interface{} {
	if existing, ok := metaData[code.CodeStr()]; ok {
		return existing
	}
	if code.Parent == nil {
		return nil
	}
	return (*code.Parent).MetaDataFromAncestors(metaData)
}

var httpMetaData = make(MetaData)

// SetHTTP adds an HTTP code to the meta data
func (code Code) SetHTTP(httpCode int) Code {
	if existingCode, ok := httpMetaData[code.CodeStr()]; ok {
		panic(fmt.Sprintf("http already exists %v for %+v", existingCode, code))
	}
	httpMetaData[code.CodeStr()] = httpCode
	return code
}

// HTTPCode retrieves the HTTP code for a code or its first ancestor with an HTTP code.
// If none are specified, it defaults to 400 BadRequest
func (code Code) HTTPCode() int {
	httpCode := code.MetaDataFromAncestors(httpMetaData)
	if httpCode == nil {
		return http.StatusBadRequest
	}
	return httpCode.(int)
}

// ErrorCode is the interface that ties an error and RegisteredCode together.
//
// Note that there are additional interfaces (HasClientData, HasOperation, please see the docs)
// that can be defined by an ErrorCode to customize finding structured data for the client.
//
// ErrorCode allows error codes to be defined
// without being forced to use a particular struct such as CodedError.
// CodedError is convenient for generic errors that wrap many different errors with similar codes.
// Please see the docs for CodedError.
// For an application specific error with a 1:1 mapping between a go error structure and a RegisteredCode,
// You probably want to use this interface directly. Example:
//
// 	// First define a normal error type
//	type PathBlocked struct {
//		start     uint64 `json:"start"`
//		end       uint64 `json:"end"`
//		obstacle  uint64 `json:"end"`
//	}
//
//	func (e PathBlocked) Error() string {
//		return fmt.Sprintf("The path %d -> %d has obstacle %d", e.start, e.end, e.obstacle)
//	}
//
//	// Now define the code
//	var PathBlockedCode = errcode.StateCode.Child("state.blocked")
//
// 	// Now attach the code to the error type
//	func (e PathBlocked) Code() Code {
//		return PathBlockedCode
//	}
type ErrorCode interface {
	Error() string // The Error interface
	Code() Code
}

// Causer allows the abstract retrieval of the underlying error.
// This is the interface that pkg/errors does not export but is considered part of the stable public API.
// TODO: export this from pkg/errors
//
// Types that wrap errors should implement this to allow viewing of the underlying error.
// Generally you would use this via pkg/errors.Cause or pkg/errors.Unwrap.
type Causer interface {
	Cause() error
}

// HasClientData is used to defined how to retrieve the data portion of an ErrorCode to be returned to the client.
// Otherwise the struct itself will be assumed to be all the data by the ClientData method.
// This is provided for exensibility, but may be unnecessary for you.
// Data should be retrieved with the ClientData method.
type HasClientData interface {
	GetClientData() interface{}
}

// ClientData retrieves data from a structure that implements HasClientData
// If HasClientData is not defined it will use the given ErrorCode object.
// Normally this function is used rather than GetClientData.
func ClientData(errCode ErrorCode) interface{} {
	var data interface{} = errCode
	if hasData, ok := errCode.(HasClientData); ok {
		data = hasData.GetClientData()
	}
	return data
}

// JSONFormat is an opinion on how to serialize an ErrorCode to JSON.
// * Code is the error code string (CodeStr)
// * Msg is the string from Error() and should be friendly to end users.
// * Data is the ad-hoc data filled in by GetClientData and should be consumable by clients.
// * Operation is the high-level operation that was happening at the time of the error.
// The Operation field may be missing, and the Data field may be empty.
//
// The rest of the fields may be populated sparsely depending on the application:
// * Stack is a stack trace. This is only given for internal errors.
// * Others gives other errors that occurred (perhaps due to parallel requests).
type JSONFormat struct {
	Code      CodeStr           `json:"code"`
	Msg       string            `json:"msg"`
	Data      interface{}       `json:"data"`
	Operation string            `json:"operation,omitempty"`
	Stack     errors.StackTrace `json:"stack,omitempty"`
	Others    []JSONFormat      `json:"others,omitempty"`
}

// OperationClientData gives the results of both the ClientData and Operation functions.
// The Operation function is applied to the original ErrorCode.
// If that does not return an operation, it is applied to the result of ClientData.
// This function is used by NewJSONFormat to fill JSONFormat.
func OperationClientData(errCode ErrorCode) (string, interface{}) {
	op := Operation(errCode)
	data := ClientData(errCode)
	if op == "" {
		op = Operation(data)
	}
	return op, data
}

// NewJSONFormat turns an ErrorCode into a JSONFormat.
// If you use ErrorCodeChain first, you will ensure proper population of the Others field.
func NewJSONFormat(errCode ErrorCode) JSONFormat {
	// Gather up multiple errors.
	// We discard any that are not ErrorCode.
	errorCodes := ErrorCodes(errCode)
	others := make([]JSONFormat, len(errorCodes)-1)
	for i, err := range errorCodes[1:] {
		others[i] = NewJSONFormat(err)
	}

	op, data := OperationClientData(errCode)

	var stack errors.StackTrace
	if errCode.Code().IsAncestor(InternalCode) {
		stack = StackTrace(errCode)
	}

	return JSONFormat{
		Data:      data,
		Msg:       errCode.Error(),
		Code:      errCode.Code().CodeStr(),
		Operation: op,
		Stack:     stack,
		Others:    others,
	}
}

// checkCodePath checks that the given code string either
// contains no dots or extends the parent code string
func (code Code) checkCodePath() error {
	paths := strings.Split(code.codeStr.String(), ".")
	if len(paths) == 1 {
		return nil
	}
	if code.Parent == nil {
		if len(paths) > 1 {
			return fmt.Errorf("expected no parent paths: %#v", code.codeStr)
		}
	} else {
		parent := *code.Parent
		parentPath := paths[len(paths)-2]
		if parentPath != parent.codeStr.String() {
			return fmt.Errorf("got %#v but expected a path to parent %#v for %#v", parentPath, parent.codeStr, code.codeStr)
		}
	}
	return nil
}
