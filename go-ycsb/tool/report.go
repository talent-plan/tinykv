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

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

var (
	logsPath   = "./logs"
	operations = map[string]struct{}{
		"INSERT":            struct{}{},
		"READ":              struct{}{},
		"UPDATE":            struct{}{},
		"SCAN":              struct{}{},
		"READ_MODIFY_WRITE": struct{}{},
		"DELETE":            struct{}{},
	}
	workloads = map[string]struct{}{
		"load":      struct{}{},
		"workloada": struct{}{},
		"workloadb": struct{}{},
		"workloadc": struct{}{},
		"workloadd": struct{}{},
		"workloade": struct{}{},
		"workloadf": struct{}{},
	}
)

type stat struct {
	OPS float64
	P99 float64
}

func statFieldFunc(c rune) bool {
	return c == ':' || c == ','
}

func newStat(line string) (*stat, error) {
	kvs := strings.FieldsFunc(line, statFieldFunc)
	s := stat{}
	if len(kvs)%2 != 0 {
		println(line)
	}
	for i := 0; i < len(kvs); i += 2 {
		v, err := strconv.ParseFloat(strings.TrimSpace(kvs[i+1]), 64)
		if err != nil {
			return nil, err
		}
		switch strings.TrimSpace(kvs[i]) {
		case "OPS":
			s.OPS = v
		case "99th(us)":
			s.P99 = v
		default:
		}
	}
	return &s, nil
}

type dbStat struct {
	db       string
	workload string
	summary  map[string]*stat
	// progress map[string][]*stat
}

func parseDBStat(pathName string) (*dbStat, error) {
	// check db and workload from file name, the name format is:
	// 	1. db_load.log
	// 	2. db_run_workloadx.log
	s := new(dbStat)
	s.summary = make(map[string]*stat, 1)
	// s.progress = make(map[string][]*stat, 1)

	fileName := path.Base(pathName)
	seps := strings.Split(fileName, "_")

	if len(seps) != 2 {
		return nil, nil
	}

	s.db = seps[0]
	workload := strings.TrimSuffix(seps[1], ".log")
	if _, ok := workloads[workload]; !ok {
		return nil, nil
	}

	s.workload = workload
	file, err := os.Open(pathName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	handleSummary := false

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Run finished") {
			handleSummary = true
			continue
		}

		seps := strings.Split(line, "-")
		op := strings.TrimSpace(seps[0])
		if _, ok := operations[op]; !ok {
			continue
		}

		stat, err := newStat(strings.TrimSpace(seps[1]))
		if err != nil {
			return nil, err
		}

		if handleSummary {
			// handle summary logs
			s.summary[op] = stat
		}

		// TODO  handle progress logs
		// s.progress[op] = append(s.progress[op], stat)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return s, nil
}

type dbStats []*dbStat

func (a dbStats) Len() int           { return len(a) }
func (a dbStats) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a dbStats) Less(i, j int) bool { return a[i].db < a[j].db }

func reportDBStats(logsPath string, workload string, stats dbStats) error {
	dir := path.Join(logsPath, "report")
	os.MkdirAll(dir, 0755)

	fileName := path.Join(dir, fmt.Sprintf("%s_summary.csv", workload))
	sort.Sort(stats)

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	var fields []string
	switch workload {
	case "load":
		fields = []string{"INSERT"}
	case "workloada":
		fields = []string{"READ", "UPDATE"}
	case "workloadb":
		fields = []string{"READ", "UPDATE"}
	case "workloadc":
		fields = []string{"READ"}
	case "workloadd":
		fields = []string{"READ", "INSERT"}
	case "workloade":
		fields = []string{"SCAN", "INSERT"}
	case "workloadf":
		fields = []string{"READ_MODIFY_WRITE"}
	default:
	}

	fmt.Fprintf(file, "DB")
	for _, field := range fields {
		fmt.Fprintf(file, ",%s OPS,%s P99(us)", field, field)
	}
	fmt.Fprint(file, "\n")

	for _, stat := range stats {
		fmt.Fprintf(file, "%s", stat.db)
		for _, field := range fields {
			s := stat.summary[field]
			if s == nil {
				fmt.Fprintf(file, ",0.0")
			} else {
				fmt.Fprintf(file, ",%.f,%.f", s.OPS, s.P99)
			}
		}
		fmt.Fprintf(file, "\n")
	}

	return nil
}

func main() {
	if len(os.Args) >= 2 {
		logsPath = os.Args[1]
	}

	files, err := ioutil.ReadDir(logsPath)
	if err != nil {
		println(err.Error())
		return
	}

	stats := make(map[string][]*dbStat)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		s, err := parseDBStat(path.Join(logsPath, file.Name()))

		if err != nil {
			fmt.Printf("parse %s failed %v\n", file.Name(), err.Error())
		}

		if s == nil {
			continue
		}

		stats[s.workload] = append(stats[s.workload], s)
	}

	for workload, s := range stats {
		if err := reportDBStats(logsPath, workload, s); err != nil {
			fmt.Printf("report %s failed %v\n", workload, err)
		}
	}
}
