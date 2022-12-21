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

package pg

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	// pg package
	_ "github.com/lib/pq"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// pg properties
const (
	pgHost     = "pg.host"
	pgPort     = "pg.port"
	pgUser     = "pg.user"
	pgPassword = "pg.password"
	pgDBName   = "pg.db"
	pdSSLMode  = "pg.sslmode"
	// TODO: support batch and auto commit
)

type pgCreator struct {
}

type pgDB struct {
	p       *properties.Properties
	db      *sql.DB
	verbose bool

	bufPool *util.BufPool

	dbName string
}

type contextKey string

const stateKey = contextKey("pgDB")

type pgState struct {
	// Do we need a LRU cache here?
	stmtCache map[string]*sql.Stmt

	conn *sql.Conn
}

func (c pgCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(pgDB)
	d.p = p

	host := p.GetString(pgHost, "127.0.0.1")
	port := p.GetInt(pgPort, 5432)
	user := p.GetString(pgUser, "root")
	password := p.GetString(pgPassword, "")
	dbName := p.GetString(pgDBName, "test")
	sslMode := p.GetString(pdSSLMode, "disable")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", user, password, host, port, dbName, sslMode)
	var err error
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Printf("open pg failed %v", err)
		return nil, err
	}

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxIdleConns(threadCount + 1)
	db.SetMaxOpenConns(threadCount * 2)

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.db = db
	d.dbName = dbName

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *pgDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		if _, err := db.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			return err
		}
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (YCSB_KEY VARCHAR(64) PRIMARY KEY", tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR(%d)", i, fieldLength))
	}

	buf.WriteString(");")

	if db.verbose {
		fmt.Println(buf.String())
	}

	_, err := db.db.Exec(buf.String())
	return err
}

func (db *pgDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *pgDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to create db conn %v", err))
	}

	state := &pgState{
		stmtCache: make(map[string]*sql.Stmt),
		conn:      conn,
	}

	return context.WithValue(ctx, stateKey, state)
}

func (db *pgDB) CleanupThread(ctx context.Context) {
	state := ctx.Value(stateKey).(*pgState)

	for _, stmt := range state.stmtCache {
		stmt.Close()
	}
	state.conn.Close()
}

func (db *pgDB) getAndCacheStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	state := ctx.Value(stateKey).(*pgState)

	if stmt, ok := state.stmtCache[query]; ok {
		return stmt, nil
	}

	stmt, err := state.conn.PrepareContext(ctx, query)
	if err == sql.ErrConnDone {
		// Try build the connection and prepare again
		if state.conn, err = db.db.Conn(ctx); err == nil {
			stmt, err = state.conn.PrepareContext(ctx, query)
		}
	}

	if err != nil {
		return nil, err
	}

	state.stmtCache[query] = stmt
	return stmt, nil
}

func (db *pgDB) clearCacheIfFailed(ctx context.Context, query string, err error) {
	if err == nil {
		return
	}

	state := ctx.Value(stateKey).(*pgState)
	if stmt, ok := state.stmtCache[query]; ok {
		stmt.Close()
	}
	delete(state.stmtCache, query)
}

func (db *pgDB) queryRows(ctx context.Context, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vs := make([]map[string][]byte, 0, count)
	for rows.Next() {
		m := make(map[string][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			v := new([]byte)
			dest[i] = v
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}

		for i, v := range dest {
			m[cols[i]] = *v.(*[]byte)
		}

		vs = append(vs, m)
	}

	return vs, rows.Err()
}

func (db *pgDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = $1`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = $1`, strings.Join(fields, ","), table)
	}

	rows, err := db.queryRows(ctx, query, 1, key)
	db.clearCacheIfFailed(ctx, query, err)

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *pgDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= $1 LIMIT $2`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= $1 LIMIT $2`, strings.Join(fields, ","), table)
	}

	rows, err := db.queryRows(ctx, query, count, startKey, count)
	db.clearCacheIfFailed(ctx, query, err)

	return rows, err
}

func (db *pgDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, args...)
	db.clearCacheIfFailed(ctx, query, err)
	return err
}

func (db *pgDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")
	firstField := true
	args := make([]interface{}, 0, len(values)+1)
	placeHolderIndex := 1
	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		if firstField {
			firstField = false
		} else {
			buf.WriteString(", ")
		}

		buf.WriteString(fmt.Sprintf("%s = $%d", p.Field, placeHolderIndex))
		args = append(args, p.Value)
		placeHolderIndex++
	}
	buf.WriteString(fmt.Sprintf(" WHERE YCSB_KEY = $%d", placeHolderIndex))

	args = append(args, key)

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *pgDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("INSERT INTO ")
	buf.WriteString(table)
	buf.WriteString(" (YCSB_KEY")
	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(" ,")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES ($1")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(fmt.Sprintf(" ,$%d", i+2))
	}

	buf.WriteString(") ON CONFLICT DO NOTHING")

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *pgDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = $1`, table)

	return db.execQuery(ctx, query, key)
}

func init() {
	ycsb.RegisterDBCreator("pg", pgCreator{})
	ycsb.RegisterDBCreator("postgresql", pgCreator{})
	ycsb.RegisterDBCreator("cockroach", pgCreator{})
	ycsb.RegisterDBCreator("cdb", pgCreator{})
}
