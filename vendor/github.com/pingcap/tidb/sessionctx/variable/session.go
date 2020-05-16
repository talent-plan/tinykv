// Copyright 2015 PingCAP, Inc.
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

package variable

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
)

// Error instances.
var (
	ErrCantSetToNull  = terror.ClassVariable.New(mysql.ErrCantSetToNull, mysql.MySQLErrName[mysql.ErrCantSetToNull])
	ErrSnapshotTooOld = terror.ClassVariable.New(mysql.ErrSnapshotTooOld, mysql.MySQLErrName[mysql.ErrSnapshotTooOld])
)

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	forUpdateTS   uint64
	DirtyDB       interface{}
	InfoSchema    interface{}
	History       interface{}
	SchemaVersion int64
	StartTS       uint64
	Shard         *int64
	TableDeltaMap map[int64]TableDelta

	CreateTime     time.Time
	StatementCount int
}

// UpdateDeltaForTable updates the delta info for some table.
func (tc *TransactionContext) UpdateDeltaForTable(tableID int64, delta int64, count int64, colSize map[int64]int64) {
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[tableID]
	if item.ColSize == nil && colSize != nil {
		item.ColSize = make(map[int64]int64)
	}
	item.Delta += delta
	item.Count += count
	for key, val := range colSize {
		item.ColSize[key] += val
	}
	tc.TableDeltaMap[tableID] = item
}

// Cleanup clears up transaction info that no longer use.
func (tc *TransactionContext) Cleanup() {
	// tc.InfoSchema = nil; we cannot do it now, because some operation like handleFieldList depend on this.
	tc.DirtyDB = nil
	tc.History = nil
	tc.TableDeltaMap = nil
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.TableDeltaMap = nil
}

// GetForUpdateTS returns the ts for update.
func (tc *TransactionContext) GetForUpdateTS() uint64 {
	if tc.forUpdateTS > tc.StartTS {
		return tc.forUpdateTS
	}
	return tc.StartTS
}

// SetForUpdateTS sets the ts for update.
func (tc *TransactionContext) SetForUpdateTS(forUpdateTS uint64) {
	if forUpdateTS > tc.forUpdateTS {
		tc.forUpdateTS = forUpdateTS
	}
}

// WriteStmtBufs can be used by insert/replace/delete/update statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by tablecodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// BufStore stores temp KVs for a row when executing insert statement.
	// We could reuse a BufStore for multiple rows of a session to reduce memory allocations.
	BufStore *kv.BufferStore
	// AddRowValues use to store temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Datum

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Datum
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	ib.BufStore = nil
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	Concurrency
	BatchSize
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]string
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16

	//  TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// KVVars is the variables for KV storage.
	KVVars *kv.Variables

	// TxnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	TxnIsolationLevelOneShot struct {
		// State 0 means default
		// State 1 means it's set in current transaction.
		// State 2 means it should be used in current transaction.
		State int
		Value string
	}

	// Status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID int64

	// CurrentDB is the default database of this session.
	CurrentDB string

	// StrictSQLMode indicates if the session is in strict mode.
	StrictSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this session.
	CommonGlobalLoaded bool

	// InRestrictedSQL indicates if the session is handling restricted SQL execution.
	InRestrictedSQL bool

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowWriteRowID can be set to false to forbid write data to _tidb_rowid.
	// This variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// CorrelationThreshold is the guard to enable row count estimation using column order correlation.
	CorrelationThreshold float64

	// CorrelationExpFactor is used to control the heuristic approach of row count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// CPUFactor is the CPU cost of processing one expression for one row.
	CPUFactor float64
	// CopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	CopCPUFactor float64
	// NetworkFactor is the network cost of transferring 1 byte data.
	NetworkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on TiKV.
	ScanFactor float64
	// DescScanFactor is the IO cost of scanning 1 byte data on TiKV in desc order.
	DescScanFactor float64
	// SeekFactor is the IO cost of seeking the start value of a range in TiKV.
	SeekFactor float64
	// MemoryFactor is the memory cost of storing one tuple.
	MemoryFactor float64
	// DiskFactor is the IO cost of reading/writing one byte to temporary disk.
	DiskFactor float64
	// ConcurrencyFactor is the CPU cost of additional one goroutine.
	ConcurrencyFactor float64

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	/* TiDB system variables */

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// IDAllocator is provided by kvEncoder, if it is provided, we will use it to alloc auto id instead of using
	// Table.alloc.
	IDAllocator autoid.Allocator

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DDLReorgPriority is the operation priority of adding indices.
	DDLReorgPriority int

	// WaitSplitRegionFinish defines the split region behaviour is sync or async.
	WaitSplitRegionFinish bool

	// WaitSplitRegionTimeout defines the split region timeout.
	WaitSplitRegionTimeout uint64

	writeStmtBufs WriteStmtBufs

	// EnableRadixJoin indicates whether to use radix hash join to execute
	// HashJoin.
	EnableRadixJoin bool

	// ConstraintCheckInPlace indicates whether to check the constraint when the SQL executing.
	ConstraintCheckInPlace bool

	// CommandValue indicates which command current session is doing.
	CommandValue uint32

	// TiDBOptJoinReorderThreshold defines the minimal number of join nodes
	// to use the greedy join reorder algorithm.
	TiDBOptJoinReorderThreshold int

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	SlowQueryFile string

	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the value is 0, timeouts are not enabled.
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_execution_time
	MaxExecutionTime uint64

	// Killed is a flag to indicate that this query is killed.
	Killed uint32

	// ConnectionInfo indicates current connection info used by current session, only be lazy assigned by plugin.
	ConnectionInfo *ConnectionInfo

	// use noop funcs or not
	EnableNoopFuncs bool

	// StartTime is the start time of the last query.
	StartTime time.Time

	// DurationParse is the duration of parsing SQL string to AST of the last query.
	DurationParse time.Duration

	// DurationCompile is the duration of compiling AST to execution plan of the last query.
	DurationCompile time.Duration

	// PrevStmt is used to store the previous executed statement in the current session.
	PrevStmt fmt.Stringer

	// AllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	AllowRemoveAutoInc bool

	// Unexported fields should be accessed and set through interfaces like GetReplicaRead() and SetReplicaRead().

	// allowInSubqToJoinAndAgg can be set to false to forbid rewriting the semi join to inner join with agg.
	allowInSubqToJoinAndAgg bool

	// replicaRead is used for reading data from replicas, only follower is supported at this time.
	replicaRead kv.ReplicaReadType

	// RowEncoder is reused in session for encode row data.
	RowEncoder rowcodec.Encoder
}

// ConnectionInfo present connection used by audit.
type ConnectionInfo struct {
	ConnectionID      uint32
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
	ServerPort        int
	Duration          float64
	User              string
	ServerOSLoginUser string
	OSVersion         string
	ClientVersion     string
	ServerVersion     string
	SSLVersion        string
	PID               int
	DB                string
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	vars := &SessionVars{
		Users:                       make(map[string]string),
		systems:                     make(map[string]string),
		TxnCtx:                      &TransactionContext{},
		KVVars:                      kv.NewVariables(),
		StrictSQLMode:               true,
		Status:                      mysql.ServerStatusAutocommit,
		StmtCtx:                     new(stmtctx.StatementContext),
		AllowAggPushDown:            false,
		DDLReorgPriority:            kv.PriorityLow,
		allowInSubqToJoinAndAgg:     DefOptInSubqToJoinAndAgg,
		CorrelationThreshold:        DefOptCorrelationThreshold,
		CorrelationExpFactor:        DefOptCorrelationExpFactor,
		CPUFactor:                   DefOptCPUFactor,
		CopCPUFactor:                DefOptCopCPUFactor,
		NetworkFactor:               DefOptNetworkFactor,
		ScanFactor:                  DefOptScanFactor,
		DescScanFactor:              DefOptDescScanFactor,
		SeekFactor:                  DefOptSeekFactor,
		MemoryFactor:                DefOptMemoryFactor,
		DiskFactor:                  DefOptDiskFactor,
		ConcurrencyFactor:           DefOptConcurrencyFactor,
		EnableRadixJoin:             false,
		EnableVectorizedExpression:  DefEnableVectorizedExpression,
		CommandValue:                uint32(mysql.ComSleep),
		TiDBOptJoinReorderThreshold: DefTiDBOptJoinReorderThreshold,
		WaitSplitRegionFinish:       DefTiDBWaitSplitRegionFinish,
		WaitSplitRegionTimeout:      DefWaitSplitRegionTimeout,
		EnableNoopFuncs:             DefTiDBEnableNoopFuncs,
		replicaRead:                 kv.ReplicaReadLeader,
		AllowRemoveAutoInc:          DefTiDBAllowRemoveAutoInc,
	}
	vars.Concurrency = Concurrency{
		IndexLookupConcurrency:     DefIndexLookupConcurrency,
		IndexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		IndexLookupJoinConcurrency: DefIndexLookupJoinConcurrency,
		HashJoinConcurrency:        DefTiDBHashJoinConcurrency,
		ProjectionConcurrency:      DefTiDBProjectionConcurrency,
		DistSQLScanConcurrency:     DefDistSQLScanConcurrency,
		HashAggPartialConcurrency:  DefTiDBHashAggPartialConcurrency,
		HashAggFinalConcurrency:    DefTiDBHashAggFinalConcurrency,
	}
	vars.BatchSize = BatchSize{
		IndexLookupSize: DefIndexLookupSize,
		InitChunkSize:   DefInitChunkSize,
		MaxChunkSize:    DefMaxChunkSize,
	}
	return vars
}

// GetAllowInSubqToJoinAndAgg get AllowInSubqToJoinAndAgg from sql hints and SessionVars.allowInSubqToJoinAndAgg.
func (s *SessionVars) GetAllowInSubqToJoinAndAgg() bool {
	if s.StmtCtx.HasAllowInSubqToJoinAndAggHint {
		return s.StmtCtx.AllowInSubqToJoinAndAgg
	}
	return s.allowInSubqToJoinAndAgg
}

// SetAllowInSubqToJoinAndAgg set SessionVars.allowInSubqToJoinAndAgg.
func (s *SessionVars) SetAllowInSubqToJoinAndAgg(val bool) {
	s.allowInSubqToJoinAndAgg = val
}

// GetReplicaRead get ReplicaRead from sql hints and SessionVars.replicaRead.
func (s *SessionVars) GetReplicaRead() kv.ReplicaReadType {
	if s.StmtCtx.HasReplicaReadHint {
		return kv.ReplicaReadType(s.StmtCtx.ReplicaRead)
	}
	return s.replicaRead
}

// SetReplicaRead set SessionVars.replicaRead.
func (s *SessionVars) SetReplicaRead(val kv.ReplicaReadType) {
	s.replicaRead = val
}

// GetWriteStmtBufs get pointer of SessionVars.writeStmtBufs.
func (s *SessionVars) GetWriteStmtBufs() *WriteStmtBufs {
	return &s.writeStmtBufs
}

// GetSplitRegionTimeout gets split region timeout.
func (s *SessionVars) GetSplitRegionTimeout() time.Duration {
	return time.Duration(s.WaitSplitRegionTimeout) * time.Second
}

// CleanBuffers cleans the temporary bufs
func (s *SessionVars) CleanBuffers() {
	s.GetWriteStmtBufs().clean()
}

// AllocPlanColumnID allocates column id for plan.
func (s *SessionVars) AllocPlanColumnID() int64 {
	s.PlanColumnID++
	return s.PlanColumnID
}

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
	charset = s.systems[CharacterSetConnection]
	collation = s.systems[CollationConnection]
	return
}

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.StmtCtx.LastInsertID = insertID
}

// SetStatusFlag sets the session server status variable.
// If on is ture sets the flag in session status,
// otherwise removes the flag.
func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
	if on {
		s.Status |= flag
		return
	}
	s.Status &= ^flag
}

// GetStatusFlag gets the session server status variable, returns true if it is on.
func (s *SessionVars) GetStatusFlag(flag uint16) bool {
	return s.Status&flag > 0
}

// InTxn returns if the session is in transaction.
func (s *SessionVars) InTxn() bool {
	return s.GetStatusFlag(mysql.ServerStatusInTrans)
}

// IsAutocommit returns if the session is set to autocommit.
func (s *SessionVars) IsAutocommit() bool {
	return s.GetStatusFlag(mysql.ServerStatusAutocommit)
}

// Location returns the value of time_zone session variable. If it is nil, then return time.Local.
func (s *SessionVars) Location() *time.Location {
	loc := s.TimeZone
	if loc == nil {
		loc = time.Local
	}
	return loc
}

// GetSystemVar gets the string value of a system variable.
func (s *SessionVars) GetSystemVar(name string) (string, bool) {
	if name == WarningCount {
		return strconv.Itoa(s.SysWarningCount), true
	} else if name == ErrorCount {
		return strconv.Itoa(int(s.SysErrorCount)), true
	}
	val, ok := s.systems[name]
	return val, ok
}

func (s *SessionVars) setDDLReorgPriority(val string) {
	val = strings.ToLower(val)
	switch val {
	case "priority_low":
		s.DDLReorgPriority = kv.PriorityLow
	case "priority_normal":
		s.DDLReorgPriority = kv.PriorityNormal
	case "priority_high":
		s.DDLReorgPriority = kv.PriorityHigh
	default:
		s.DDLReorgPriority = kv.PriorityLow
	}
}

// SetSystemVar sets the value of a system variable.
func (s *SessionVars) SetSystemVar(name string, val string) error {
	switch name {
	case TxnIsolationOneShot:
		switch val {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			skipIsolationLevelCheck, err := GetSessionSystemVar(s, TiDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(val)
			if err != nil {
				returnErr = err
			}
			if !TiDBOptOn(skipIsolationLevelCheck) || err != nil {
				return returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			if name == "transaction_isolation" {
				s.StmtCtx.AppendWarning(returnErr)
			}
		}
		s.TxnIsolationLevelOneShot.State = 1
		s.TxnIsolationLevelOneShot.Value = val
	case SQLModeVar:
		val = mysql.FormatSQLModeStr(val)
		// Modes is a list of different modes separated by commas.
		sqlMode, err2 := mysql.GetSQLMode(val)
		if err2 != nil {
			return errors.Trace(err2)
		}
		s.StrictSQLMode = sqlMode.HasStrictMode()
		s.SQLMode = sqlMode
		s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
	case AutoCommit:
		isAutocommit := TiDBOptOn(val)
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetStatusFlag(mysql.ServerStatusInTrans, false)
		}
	case MaxExecutionTime:
		timeoutMS := tidbOptPositiveInt32(val, 0)
		s.MaxExecutionTime = uint64(timeoutMS)
	case TiDBSkipUTF8Check:
		s.SkipUTF8Check = TiDBOptOn(val)
	case TiDBOptAggPushDown:
		s.AllowAggPushDown = TiDBOptOn(val)
	case TiDBOptWriteRowID:
		s.AllowWriteRowID = TiDBOptOn(val)
	case TiDBOptInSubqToJoinAndAgg:
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
	case TiDBOptCorrelationThreshold:
		s.CorrelationThreshold = tidbOptFloat64(val, DefOptCorrelationThreshold)
	case TiDBOptCorrelationExpFactor:
		s.CorrelationExpFactor = int(tidbOptInt64(val, DefOptCorrelationExpFactor))
	case TiDBOptCPUFactor:
		s.CPUFactor = tidbOptFloat64(val, DefOptCPUFactor)
	case TiDBOptCopCPUFactor:
		s.CopCPUFactor = tidbOptFloat64(val, DefOptCopCPUFactor)
	case TiDBOptNetworkFactor:
		s.NetworkFactor = tidbOptFloat64(val, DefOptNetworkFactor)
	case TiDBOptScanFactor:
		s.ScanFactor = tidbOptFloat64(val, DefOptScanFactor)
	case TiDBOptDescScanFactor:
		s.DescScanFactor = tidbOptFloat64(val, DefOptDescScanFactor)
	case TiDBOptSeekFactor:
		s.SeekFactor = tidbOptFloat64(val, DefOptSeekFactor)
	case TiDBOptMemoryFactor:
		s.MemoryFactor = tidbOptFloat64(val, DefOptMemoryFactor)
	case TiDBOptDiskFactor:
		s.DiskFactor = tidbOptFloat64(val, DefOptDiskFactor)
	case TiDBOptConcurrencyFactor:
		s.ConcurrencyFactor = tidbOptFloat64(val, DefOptConcurrencyFactor)
	case TiDBIndexLookupConcurrency:
		s.IndexLookupConcurrency = tidbOptPositiveInt32(val, DefIndexLookupConcurrency)
	case TiDBIndexLookupJoinConcurrency:
		s.IndexLookupJoinConcurrency = tidbOptPositiveInt32(val, DefIndexLookupJoinConcurrency)
	case TiDBIndexLookupSize:
		s.IndexLookupSize = tidbOptPositiveInt32(val, DefIndexLookupSize)
	case TiDBHashJoinConcurrency:
		s.HashJoinConcurrency = tidbOptPositiveInt32(val, DefTiDBHashJoinConcurrency)
	case TiDBProjectionConcurrency:
		s.ProjectionConcurrency = tidbOptInt64(val, DefTiDBProjectionConcurrency)
	case TiDBHashAggPartialConcurrency:
		s.HashAggPartialConcurrency = tidbOptPositiveInt32(val, DefTiDBHashAggPartialConcurrency)
	case TiDBHashAggFinalConcurrency:
		s.HashAggFinalConcurrency = tidbOptPositiveInt32(val, DefTiDBHashAggFinalConcurrency)
	case TiDBDistSQLScanConcurrency:
		s.DistSQLScanConcurrency = tidbOptPositiveInt32(val, DefDistSQLScanConcurrency)
	case TiDBIndexSerialScanConcurrency:
		s.IndexSerialScanConcurrency = tidbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
	case TiDBBackoffLockFast:
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, kv.DefBackoffLockFast)
	case TiDBBackOffWeight:
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, kv.DefBackOffWeight)
	case TiDBConstraintCheckInPlace:
		s.ConstraintCheckInPlace = TiDBOptOn(val)
	case TiDBCurrentTS, TiDBConfig:
		return ErrReadOnly
	case TiDBMaxChunkSize:
		s.MaxChunkSize = tidbOptPositiveInt32(val, DefMaxChunkSize)
	case TiDBInitChunkSize:
		s.InitChunkSize = tidbOptPositiveInt32(val, DefInitChunkSize)
	case TiDBGeneralLog:
		atomic.StoreUint32(&ProcessGeneralLog, uint32(tidbOptPositiveInt32(val, DefTiDBGeneralLog)))
	case TiDBEnableCascadesPlanner:
		s.EnableCascadesPlanner = TiDBOptOn(val)
	case TiDBDDLReorgPriority:
		s.setDDLReorgPriority(val)
	case TiDBEnableRadixJoin:
		s.EnableRadixJoin = TiDBOptOn(val)
	case TiDBEnableVectorizedExpression:
		s.EnableVectorizedExpression = TiDBOptOn(val)
	case TiDBOptJoinReorderThreshold:
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, DefTiDBOptJoinReorderThreshold)
	case TiDBSlowQueryFile:
		s.SlowQueryFile = val
	case TiDBWaitSplitRegionFinish:
		s.WaitSplitRegionFinish = TiDBOptOn(val)
	case TiDBWaitSplitRegionTimeout:
		s.WaitSplitRegionTimeout = uint64(tidbOptPositiveInt32(val, DefWaitSplitRegionTimeout))
	case TiDBEnableNoopFuncs:
		s.EnableNoopFuncs = TiDBOptOn(val)
	case TiDBReplicaRead:
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(kv.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(kv.ReplicaReadLeader)
		}
	case TiDBAllowRemoveAutoInc:
		s.AllowRemoveAutoInc = TiDBOptOn(val)
	// It's a global variable, but it also wants to be cached in server.
	case TiDBMaxDeltaSchemaCount:
		SetMaxDeltaSchemaCount(tidbOptInt64(val, DefTiDBMaxDeltaSchemaCount))
	}
	s.systems[name] = val
	return nil
}

// SetLocalSystemVar sets values of the local variables which in "server" scope.
func SetLocalSystemVar(name string, val string) {
	switch name {
	case TiDBDDLReorgWorkerCount:
		SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgWorkerCount)))
	case TiDBDDLReorgBatchSize:
		SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgBatchSize)))
	case TiDBDDLErrorCountLimit:
		SetDDLErrorCountLimit(tidbOptInt64(val, DefTiDBDDLErrorCountLimit))
	}
}

// special session variables.
const (
	SQLModeVar           = "sql_mode"
	CharacterSetResults  = "character_set_results"
	MaxAllowedPacket     = "max_allowed_packet"
	TimeZone             = "time_zone"
	TxnIsolation         = "tx_isolation"
	TransactionIsolation = "transaction_isolation"
	TxnIsolationOneShot  = "tx_isolation_one_shot"
	MaxExecutionTime     = "max_execution_time"
)

// these variables are useless for TiDB, but still need to validate their values for some compatible issues.
// TODO: some more variables need to be added here.
const (
	serverReadOnly = "read_only"
)

var (
	// TxIsolationNames are the valid values of the variable "tx_isolation" or "transaction_isolation".
	TxIsolationNames = map[string]struct{}{
		"READ-UNCOMMITTED": {},
		"READ-COMMITTED":   {},
		"REPEATABLE-READ":  {},
		"SERIALIZABLE":     {},
	}
)

// TableDelta stands for the changed count for one table.
type TableDelta struct {
	Delta    int64
	Count    int64
	ColSize  map[int64]int64
	InitTime time.Time // InitTime is the time that this delta is generated.
}

// Concurrency defines concurrency values.
type Concurrency struct {
	// IndexLookupConcurrency is the number of concurrent index lookup worker.
	IndexLookupConcurrency int

	// IndexLookupJoinConcurrency is the number of concurrent index lookup join inner worker.
	IndexLookupJoinConcurrency int

	// DistSQLScanConcurrency is the number of concurrent dist SQL scan worker.
	DistSQLScanConcurrency int

	// HashJoinConcurrency is the number of concurrent hash join outer worker.
	HashJoinConcurrency int

	// ProjectionConcurrency is the number of concurrent projection worker.
	ProjectionConcurrency int64

	// HashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	HashAggPartialConcurrency int

	// HashAggFinalConcurrency is the number of concurrent hash aggregation final worker.
	HashAggFinalConcurrency int

	// IndexSerialScanConcurrency is the number of concurrent index serial scan worker.
	IndexSerialScanConcurrency int
}

// BatchSize defines batch size values.
type BatchSize struct {

	// IndexLookupSize is the number of handles for an index lookup task in index double read executor.
	IndexLookupSize int

	// InitChunkSize defines init row count of a Chunk during query execution.
	InitChunkSize int

	// MaxChunkSize defines max row count of a Chunk during query execution.
	MaxChunkSize int
}
