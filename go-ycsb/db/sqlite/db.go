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

//go:build libsqlite3

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"
	// sqlite package
	"github.com/mattn/go-sqlite3"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Sqlite properties
const (
	sqliteDBPath              = "sqlite.db"
	sqliteMode                = "sqlite.mode"
	sqliteJournalMode         = "sqlite.journalmode"
	sqliteCache               = "sqlite.cache"
	sqliteMaxOpenConns        = "sqlite.maxopenconns"
	sqliteMaxIdleConns        = "sqlite.maxidleconns"
	sqliteOptimistic          = "sqlite.optimistic"
	sqliteOptimisticBackoffMs = "sqlite.optimistic_backoff_ms"
)

type sqliteCreator struct {
}

type sqliteDB struct {
	p          *properties.Properties
	db         *sql.DB
	verbose    bool
	optimistic bool
	backoffMs  int

	bufPool *util.BufPool
}

func (c sqliteCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(sqliteDB)
	d.p = p

	dbPath := p.GetString(sqliteDBPath, "/tmp/sqlite.db")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dbPath)
	}

	mode := p.GetString(sqliteMode, "rwc")
	journalMode := p.GetString(sqliteJournalMode, "WAL")
	cache := p.GetString(sqliteCache, "shared")
	maxOpenConns := p.GetInt(sqliteMaxOpenConns, 1)
	maxIdleConns := p.GetInt(sqliteMaxIdleConns, 2)

	v := url.Values{}
	v.Set("cache", cache)
	v.Set("mode", mode)
	v.Set("_journal_mode", journalMode)
	dsn := fmt.Sprintf("file:%s?%s", dbPath, v.Encode())
	var err error
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)

	d.optimistic = p.GetBool(sqliteOptimistic, false)
	d.backoffMs = p.GetInt(sqliteOptimisticBackoffMs, 5)
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.db = db

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *sqliteDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

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

func (db *sqliteDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *sqliteDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *sqliteDB) CleanupThread(ctx context.Context) {

}

func (db *sqliteDB) optimisticTx(ctx context.Context, f func(tx *sql.Tx) error) error {
	for {
		tx, err := db.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if err = f(tx); err != nil {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err != nil && db.optimistic {
			if err, ok := err.(sqlite3.Error); ok && (err.Code == sqlite3.ErrBusy ||
				err.ExtendedCode == sqlite3.ErrIoErrUnlock) {
				time.Sleep(time.Duration(db.backoffMs) * time.Millisecond)
				continue
			}
		}
		return err
	}
}

func (db *sqliteDB) doQueryRows(ctx context.Context, tx *sql.Tx, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
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

func (db *sqliteDB) doRead(ctx context.Context, tx *sql.Tx, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = ?`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = ?`, strings.Join(fields, ","), table)
	}

	rows, err := db.doQueryRows(ctx, tx, query, 1, key)

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *sqliteDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var output map[string][]byte
	err := db.optimisticTx(ctx, func(tx *sql.Tx) error {
		res, err := db.doRead(ctx, tx, table, key, fields)
		output = res
		return err
	})
	return output, err
}

func (db *sqliteDB) doScan(ctx context.Context, tx *sql.Tx, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= ? LIMIT ?`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= ? LIMIT ?`, strings.Join(fields, ","), table)
	}

	rows, err := db.doQueryRows(ctx, tx, query, count, startKey, count)

	return rows, err
}

func (db *sqliteDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var output []map[string][]byte
	err := db.optimisticTx(ctx, func(tx *sql.Tx) error {
		res, err := db.doScan(ctx, tx, table, startKey, count, fields)
		output = res
		return err
	})
	return output, err
}

func (db *sqliteDB) doUpdate(ctx context.Context, tx *sql.Tx, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")
	firstField := true
	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for _, p := range pairs {
		if firstField {
			firstField = false
		} else {
			buf.WriteString(", ")
		}

		buf.WriteString(p.Field)
		buf.WriteString(`= ?`)
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")

	args = append(args, key)

	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (db *sqliteDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		return db.doUpdate(ctx, tx, table, key, values)
	})
}

func (db *sqliteDB) doInsert(ctx context.Context, tx *sql.Tx, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("INSERT OR IGNORE INTO ")
	buf.WriteString(table)
	buf.WriteString(" (YCSB_KEY")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(" ,")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(" ,?")
	}

	buf.WriteByte(')')

	_, err := tx.ExecContext(ctx, buf.String(), args...)
	if err != nil && db.verbose {
		fmt.Printf("error(doInsert): %s: %+v\n", buf.String(), err)
	}
	return err
}

func (db *sqliteDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error { return db.doInsert(ctx, tx, table, key, values) })
}

func (db *sqliteDB) doDelete(ctx context.Context, tx *sql.Tx, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = ?`, table)
	_, err := tx.ExecContext(ctx, query, key)
	return err
}

func (db *sqliteDB) Delete(ctx context.Context, table string, key string) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error { return db.doDelete(ctx, tx, table, key) })
}

func (db *sqliteDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			err := db.doInsert(ctx, tx, table, keys[i], values[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *sqliteDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	var output []map[string][]byte
	err := db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			res, err := db.doRead(ctx, tx, table, keys[i], fields)
			if err != nil {
				return err
			}
			output = append(output, res)
		}
		return nil
	})
	return output, err
}

func (db *sqliteDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			err := db.doUpdate(ctx, tx, table, keys[i], values[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *sqliteDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			err := db.doDelete(ctx, tx, table, keys[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func init() {
	ycsb.RegisterDBCreator("sqlite", sqliteCreator{})
}

var _ ycsb.BatchDB = (*sqliteDB)(nil)
