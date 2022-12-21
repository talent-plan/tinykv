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

package spanner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"

	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	spannerDBName      = "spanner.db"
	spannerCredentials = "spanner.credentials"
)

type spannerCreator struct {
}

type spannerDB struct {
	p       *properties.Properties
	client  *spanner.Client
	verbose bool
}

type contextKey string

const stateKey = contextKey("spannerDB")

type spannerState struct {
}

func (c spannerCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(spannerDB)
	d.p = p

	credentials := p.GetString(spannerCredentials, "")
	if len(credentials) == 0 {
		// no credentials provided, try using ~/.spanner/credentials.json"
		usr, err := user.Current()
		if err != nil {
			return nil, err
		}
		credentials = path.Join(usr.HomeDir, ".spanner/credentials.json")
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentials)

	ctx := context.Background()

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	dbName := p.GetString(spannerDBName, "")
	if len(dbName) == 0 {
		return nil, fmt.Errorf("must provide a database like projects/xxxx/instances/xxxx/databases/xxx")
	}

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)

	_, err = d.createDatabase(ctx, adminClient, dbName)
	if err != nil {
		return nil, err
	}

	client, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		return nil, err
	}
	d.client = client

	if err = d.createTable(ctx, adminClient, dbName); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *spannerDB) createDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbName string) (string, error) {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(dbName)
	if matches == nil || len(matches) != 3 {
		return "", fmt.Errorf("Invalid database id %s", dbName)
	}

	database, err := adminClient.GetDatabase(ctx, &adminpb.GetDatabaseRequest{
		Name: dbName,
	})
	if err != nil {
		return "", err
	}

	if database.State == adminpb.Database_STATE_UNSPECIFIED {
		op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:          matches[1],
			CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
			ExtraStatements: []string{},
		})
		if err != nil {
			return "", err
		}
		if _, err := op.Wait(ctx); err != nil {
			return "", err
		}
	}

	return matches[2], nil
}

func (db *spannerDB) tableExisted(ctx context.Context, table string) (bool, error) {
	stmt := spanner.NewStatement(`SELECT t.table_name FROM information_schema.tables AS t 
	WHERE t.table_catalog = '' AND t.table_schema = '' AND t.table_name = @name`)
	stmt.Params["name"] = table
	iter := db.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	found := false
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return false, err
		}
		found = true
		break
	}

	return found, nil
}

func (db *spannerDB) createTable(ctx context.Context, adminClient *database.DatabaseAdminClient, dbName string) error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)
	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	existed, err := db.tableExisted(ctx, tableName)
	if err != nil {
		return err
	}

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) && existed {
		op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database: dbName,
			Statements: []string{
				fmt.Sprintf("DROP TABLE %s", tableName),
			},
		})
		if err != nil {
			return err
		}

		if err = op.Wait(ctx); err != nil {
			return err
		}
		existed = false
	}

	if existed {
		return nil
	}

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE  %s (YCSB_KEY STRING(%d)", tableName, fieldLength)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d STRING(%d)", i, fieldLength))
	}

	buf.WriteString(") PRIMARY KEY (YCSB_KEY)")

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: dbName,
		Statements: []string{
			buf.String(),
		},
	})
	if err != nil {
		return err
	}

	if err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func (db *spannerDB) Close() error {
	if db.client == nil {
		return nil
	}

	db.client.Close()
	return nil
}

func (db *spannerDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := &spannerState{}

	return context.WithValue(ctx, stateKey, state)
}

func (db *spannerDB) CleanupThread(ctx context.Context) {
	//	state := ctx.Value(stateKey).(*spanner)
}

func (db *spannerDB) queryRows(ctx context.Context, stmt spanner.Statement, count int) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", stmt.SQL, stmt.Params)
	}

	iter := db.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	vs := make([]map[string][]byte, 0, count)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		rowSize := row.Size()
		m := make(map[string][]byte, rowSize)
		dest := make([]interface{}, rowSize)
		for i := 0; i < rowSize; i++ {
			v := new(spanner.NullString)
			dest[i] = v
		}

		if err := row.Columns(dest...); err != nil {
			return nil, err
		}

		for i := 0; i < rowSize; i++ {
			v := dest[i].(*spanner.NullString)
			if v.Valid {
				m[row.ColumnName(i)] = util.Slice(v.StringVal)
			}
		}

		vs = append(vs, m)
	}

	return vs, nil
}

func (db *spannerDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = @key`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = @key`, strings.Join(fields, ","), table)
	}

	stmt := spanner.NewStatement(query)
	stmt.Params["key"] = key

	rows, err := db.queryRows(ctx, stmt, 1)

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *spannerDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= @key LIMIT @limit`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= @key LIMIT @limit`, strings.Join(fields, ","), table)
	}

	stmt := spanner.NewStatement(query)
	stmt.Params["key"] = startKey
	stmt.Params["limit"] = count

	rows, err := db.queryRows(ctx, stmt, count)

	return rows, err
}

func createMutations(key string, mutations map[string][]byte) ([]string, []interface{}) {
	keys := make([]string, 0, 1+len(mutations))
	values := make([]interface{}, 0, 1+len(mutations))
	keys = append(keys, "YCSB_KEY")
	values = append(values, key)

	for key, value := range mutations {
		keys = append(keys, key)
		values = append(values, util.String(value))
	}

	return keys, values
}

func (db *spannerDB) Update(ctx context.Context, table string, key string, mutations map[string][]byte) error {
	keys, values := createMutations(key, mutations)
	m := spanner.Update(table, keys, values)
	_, err := db.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}

func (db *spannerDB) Insert(ctx context.Context, table string, key string, mutations map[string][]byte) error {
	keys, values := createMutations(key, mutations)
	m := spanner.InsertOrUpdate(table, keys, values)
	_, err := db.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}

func (db *spannerDB) Delete(ctx context.Context, table string, key string) error {
	m := spanner.Delete(table, spanner.Key{key})
	_, err := db.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}

func init() {
	ycsb.RegisterDBCreator("spanner", spannerCreator{})
}
