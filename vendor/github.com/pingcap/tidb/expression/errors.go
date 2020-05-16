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

package expression

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount = terror.ClassExpression.New(mysql.ErrWrongParamcountToNativeFct, mysql.MySQLErrName[mysql.ErrWrongParamcountToNativeFct])
	ErrDivisionByZero          = terror.ClassExpression.New(mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
	ErrRegexp                  = terror.ClassExpression.New(mysql.ErrRegexp, mysql.MySQLErrName[mysql.ErrRegexp])
	ErrOperandColumns          = terror.ClassExpression.New(mysql.ErrOperandColumns, mysql.MySQLErrName[mysql.ErrOperandColumns])
	ErrCutValueGroupConcat     = terror.ClassExpression.New(mysql.ErrCutValueGroupConcat, mysql.MySQLErrName[mysql.ErrCutValueGroupConcat])
	ErrFunctionsNoopImpl       = terror.ClassExpression.New(mysql.ErrNotSupportedYet, "function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
	ErrIncorrectType           = terror.ClassExpression.New(mysql.ErrIncorrectType, mysql.MySQLErrName[mysql.ErrIncorrectType])

	// All the un-exported errors are defined here:
	errFunctionNotExists = terror.ClassExpression.New(mysql.ErrSpDoesNotExist, mysql.MySQLErrName[mysql.ErrSpDoesNotExist])
	errNonUniq           = terror.ClassExpression.New(mysql.ErrNonUniq, mysql.MySQLErrName[mysql.ErrNonUniq])
)

func init() {
	expressionMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrWrongParamcountToNativeFct:        mysql.ErrWrongParamcountToNativeFct,
		mysql.ErrDivisionByZero:                    mysql.ErrDivisionByZero,
		mysql.ErrSpDoesNotExist:                    mysql.ErrSpDoesNotExist,
		mysql.ErrNotSupportedYet:                   mysql.ErrNotSupportedYet,
		mysql.ErrZlibZData:                         mysql.ErrZlibZData,
		mysql.ErrZlibZBuf:                          mysql.ErrZlibZBuf,
		mysql.ErrWrongArguments:                    mysql.ErrWrongArguments,
		mysql.ErrUnknownCharacterSet:               mysql.ErrUnknownCharacterSet,
		mysql.ErrInvalidDefault:                    mysql.ErrInvalidDefault,
		mysql.ErrWarnDeprecatedSyntaxNoReplacement: mysql.ErrWarnDeprecatedSyntaxNoReplacement,
		mysql.ErrOperandColumns:                    mysql.ErrOperandColumns,
		mysql.ErrCutValueGroupConcat:               mysql.ErrCutValueGroupConcat,
		mysql.ErrRegexp:                            mysql.ErrRegexp,
		mysql.ErrWarnAllowedPacketOverflowed:       mysql.ErrWarnAllowedPacketOverflowed,
		mysql.WarnOptionIgnored:                    mysql.WarnOptionIgnored,
		mysql.ErrTruncatedWrongValue:               mysql.ErrTruncatedWrongValue,
		mysql.ErrUnknownLocale:                     mysql.ErrUnknownLocale,
		mysql.ErrBadField:                          mysql.ErrBadField,
		mysql.ErrNonUniq:                           mysql.ErrNonUniq,
		mysql.ErrIncorrectType:                     mysql.ErrIncorrectType,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExpression] = expressionMySQLErrCodes
}

// handleDivisionByZeroError reports error or warning depend on the context.
func handleDivisionByZeroError(ctx sessionctx.Context) error {
	sc := ctx.GetSessionVars().StmtCtx
	if sc.InInsertStmt || sc.InDeleteStmt {
		if !ctx.GetSessionVars().SQLMode.HasErrorForDivisionByZeroMode() {
			return nil
		}
		if ctx.GetSessionVars().StrictSQLMode && !sc.DividedByZeroAsWarning {
			return ErrDivisionByZero
		}
	}
	sc.AppendWarning(ErrDivisionByZero)
	return nil
}
