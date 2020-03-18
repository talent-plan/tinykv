// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	clientTimeout  = 3 * time.Second
	requestTimeout = etcdutil.DefaultRequestTimeout
)

// Version information.
var (
	PDReleaseVersion = "None"
	PDBuildTS        = "None"
	PDGitHash        = "None"
	PDGitBranch      = "None"
)

// dialClient used to dail http request.
var dialClient = &http.Client{
	Timeout: clientTimeout,
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

// LogPDInfo prints the PD version information.
func LogPDInfo() {
	log.Info("Welcome to Placement Driver (PD)")
	log.Info("PD", zap.String("release-version", PDReleaseVersion))
	log.Info("PD", zap.String("git-hash", PDGitHash))
	log.Info("PD", zap.String("git-branch", PDGitBranch))
	log.Info("PD", zap.String("utc-build-time", PDBuildTS))
}

// PrintPDInfo prints the PD version information without log info.
func PrintPDInfo() {
	fmt.Println("Release Version:", PDReleaseVersion)
	fmt.Println("Git Commit Hash:", PDGitHash)
	fmt.Println("Git Branch:", PDGitBranch)
	fmt.Println("UTC Build Time: ", PDBuildTS)
}

// PrintConfigCheckMsg prints the message about configuration checks.
func PrintConfigCheckMsg(cfg *config.Config) {
	if len(cfg.WarningMsgs) == 0 {
		fmt.Println("config check successful")
		return
	}

	for _, msg := range cfg.WarningMsgs {
		fmt.Println(msg)
	}
}

func initOrGetClusterID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), requestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
	value := typeutil.Uint64ToBytes(clusterID)

	// Multiple PDs may try to init the cluster ID at the same time.
	// Only one PD can commit this transaction, then other PDs can get
	// the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errors.Errorf("txn returns empty response: %v", resp)
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.Errorf("txn returns invalid range response: %v", resp)
	}

	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

// GetMembers return a slice of Members.
func GetMembers(etcdClient *clientv3.Client) ([]*schedulerpb.Member, error) {
	listResp, err := etcdutil.ListEtcdMembers(etcdClient)
	if err != nil {
		return nil, err
	}

	members := make([]*schedulerpb.Member, 0, len(listResp.Members))
	for _, m := range listResp.Members {
		info := &schedulerpb.Member{
			Name:       m.Name,
			MemberId:   m.ID,
			ClientUrls: m.ClientURLs,
			PeerUrls:   m.PeerURLs,
		}
		members = append(members, info)
	}

	return members, nil
}

// InitHTTPClient initials a http client.
func InitHTTPClient(svr *Server) error {
	tlsConfig, err := svr.GetSecurityConfig().ToTLSConfig()
	if err != nil {
		return err
	}

	dialClient = &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			TLSClientConfig:   tlsConfig,
			DisableKeepAlives: true,
		},
	}
	return nil
}
