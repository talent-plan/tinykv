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

package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/spf13/cobra"
)

func newShellCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "shell db",
		Short: "YCSB Command Line Client",
		Args:  cobra.MinimumNArgs(1),
		Run:   runShellCommandFunc,
	}
	m.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Spefify a property file")
	m.Flags().StringSliceVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	return m
}

var shellContext context.Context

func runShellCommandFunc(cmd *cobra.Command, args []string) {
	dbName := args[0]
	initialGlobal(dbName, nil)

	shellContext = globalWorkload.InitThread(globalContext, 0, 1)
	shellContext = globalDB.InitThread(shellContext, 0, 1)

	shellLoop()

	globalDB.CleanupThread(shellContext)
	globalWorkload.CleanupThread(shellContext)
}

func runShellCommand(args []string) {
	cmd := &cobra.Command{
		Use:   "shell",
		Short: "YCSB shell command",
	}

	cmd.SetArgs(args)
	cmd.ParseFlags(args)

	cmd.AddCommand(
		&cobra.Command{
			Use:                   "read key [field0 field1 field2 ...]",
			Short:                 "Read a record",
			Args:                  cobra.MinimumNArgs(1),
			Run:                   runShellReadCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "scan key recordcount [field0 field1 field2 ...]",
			Short:                 "Scan starting at key",
			Args:                  cobra.MinimumNArgs(2),
			Run:                   runShellScanCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "insert key field0=value0 [field1=value1 ...]",
			Short:                 "Insert a record",
			Args:                  cobra.MinimumNArgs(2),
			Run:                   runShellInsertCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "update key field0=value0 [field1=value1 ...]",
			Short:                 "Update a record",
			Args:                  cobra.MinimumNArgs(2),
			Run:                   runShellUpdateCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "delete key",
			Short:                 "Delete a record",
			Args:                  cobra.MinimumNArgs(1),
			Run:                   runShellDeleteCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "table [tablename]",
			Short:                 "Get or [set] the name of the table",
			Args:                  cobra.MaximumNArgs(1),
			Run:                   runShellTableCommand,
			DisableFlagsInUseLine: true,
		},
	)

	if err := cmd.Execute(); err != nil {
		fmt.Println(cmd.UsageString())
	}
}

func runShellReadCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	fields := args[1:]
	row, err := globalDB.Read(shellContext, tableName, key, fields)
	if err != nil {
		fmt.Printf("Read %s failed %v\n", key, err)
		return
	}

	if row == nil {
		fmt.Printf("Read empty for %s\n", key)
		return
	}

	fmt.Printf("Read %s ok\n", key)
	for key, value := range row {
		fmt.Printf("%s=%q\n", key, value)
	}
}

func runShellScanCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	recordCount, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Printf("invalid record count %s for scan\n", args[1])
		return
	}
	fields := args[2:]

	rows, err := globalDB.Scan(shellContext, tableName, key, recordCount, fields)
	if err != nil {
		fmt.Printf("Scan from %s with %d failed %v\n", key, recordCount, err)
		return
	}

	if len(rows) == 0 {
		fmt.Println("0 records")
		return
	}

	fmt.Println("--------------------------------")
	for i, row := range rows {
		fmt.Printf("Record %d\n", i+1)
		for key, value := range row {
			fmt.Printf("%s=%q\n", key, value)
		}
	}
	fmt.Println("--------------------------------")
}

func runShellInsertCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	values := make(map[string][]byte, len(args[1:]))

	for _, arg := range args[1:] {
		sep := strings.SplitN(arg, "=", 2)
		values[sep[0]] = []byte(sep[1])
	}

	if err := globalDB.Insert(shellContext, tableName, key, values); err != nil {
		fmt.Printf("Insert %s failed %v\n", key, err)
		return
	}

	fmt.Printf("Insert %s ok\n", key)
}

func runShellUpdateCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	values := make(map[string][]byte, len(args[1:]))

	for _, arg := range args[1:] {
		sep := strings.SplitN(arg, "=", 2)
		values[sep[0]] = []byte(sep[1])
	}

	if err := globalDB.Update(shellContext, tableName, key, values); err != nil {
		fmt.Printf("Update %s failed %v\n", key, err)
		return
	}

	fmt.Printf("Update %s ok\n", key)
}

func runShellDeleteCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	if err := globalDB.Delete(shellContext, tableName, key); err != nil {
		fmt.Printf("Delete %s failed %v\n", key, err)
		return
	}

	fmt.Printf("Delete %s ok\n", key)
}

func runShellTableCommand(cmd *cobra.Command, args []string) {
	if len(args) == 1 {
		tableName = args[0]
	}
	fmt.Printf("Using table %s\n", tableName)
}

func shellLoop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		util.Fatal(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				return
			} else if err == io.EOF {
				return
			}
			continue
		}
		if line == "exit" {
			return
		}
		args := strings.Split(strings.TrimSpace(line), " ")
		runShellCommand(args)
	}
}
