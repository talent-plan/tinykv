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

package join

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap-incubator/tinykv/pd/pkg/etcdutil"
	"github.com/pingcap-incubator/tinykv/pd/server/config"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

const (
	// privateFileMode grants owner to read/write a file.
	privateFileMode = 0600
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// listMemberRetryTimes is the retry times of list member.
var listMemberRetryTimes = 20

// PrepareJoinCluster sends MemberAdd command to PD cluster,
// and returns the initial configuration of the PD cluster.
//
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//        and it is not a member of cluster, join does MemberAdd, it returns an
//        error if PD tries to join itself, missing data or join a duplicated PD.
//
// Etcd automatically re-joins the cluster if there is a data directory. So
// first it checks if there is a data directory or not. If there is, it returns
// an empty string (etcd will get the correct configurations from the data
// directory.)
//
// If there is no data directory, there are following cases:
//
//  - A new PD joins an existing cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//
//  - A failed PD re-joins the previous cluster.
//      What join does: return an error. (etcd reports: raft log corrupted,
//                      truncated, or lost?)
//
//  - A deleted PD joins to previous cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//                      (it is not in the member list and there is no data, so
//                       we can treat it as a new PD.)
//
// If there is a data directory, there are following special cases:
//
//  - A failed PD tries to join the previous cluster but it has been deleted
//    during its downtime.
//      What join does: return "" (etcd will connect to other peers and find
//                      that the PD itself has been removed.)
//
//  - A deleted PD joins the previous cluster.
//      What join does: return "" (as etcd will read data directory and find
//                      that the PD itself has been removed, so an empty string
//                      is fine.)
func PrepareJoinCluster(cfg *config.Config) error {
	// - A PD tries to join itself.
	if cfg.Join == "" {
		return nil
	}

	if cfg.Join == cfg.AdvertiseClientUrls {
		return errors.New("join self is forbidden")
	}

	filePath := path.Join(cfg.DataDir, "join")
	// Read the persist join config
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		s, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatal("read the join config meet error", zap.Error(err))
		}
		cfg.InitialCluster = strings.TrimSpace(string(s))
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	initialCluster := ""
	// Cases with data directory.
	if isDataExist(path.Join(cfg.DataDir, "member")) {
		cfg.InitialCluster = initialCluster
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// Below are cases without data directory.
	tlsConfig, err := cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Join, ","),
		DialTimeout: etcdutil.DefaultDialTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer client.Close()

	listResp, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return err
	}

	existed := false
	for _, m := range listResp.Members {
		if len(m.Name) == 0 {
			return errors.New("there is a member that has not joined successfully")
		}
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// - A failed PD re-joins the previous cluster.
	if existed {
		return errors.New("missing data or join a duplicated pd")
	}

	var addResp *clientv3.MemberAddResponse

	failpoint.Inject("add-member-failed", func() {
		listMemberRetryTimes = 2
		failpoint.Goto("LabelSkipAddMember")
	})
	// - A new PD joins an existing cluster.
	// - A deleted PD joins to previous cluster.
	{
		// First adds member through the API
		addResp, err = etcdutil.AddEtcdMember(client, []string{cfg.AdvertisePeerUrls})
		if err != nil {
			return err
		}
	}
	failpoint.Label("LabelSkipAddMember")

	var (
		pds      []string
		listSucc bool
	)

	for i := 0; i < listMemberRetryTimes; i++ {
		listResp, err = etcdutil.ListEtcdMembers(client)
		if err != nil {
			return err
		}

		pds = []string{}
		for _, memb := range listResp.Members {
			n := memb.Name
			if addResp != nil && memb.ID == addResp.Member.ID {
				n = cfg.Name
				listSucc = true
			}
			if len(n) == 0 {
				return errors.New("there is a member that has not joined successfully")
			}
			for _, m := range memb.PeerURLs {
				pds = append(pds, fmt.Sprintf("%s=%s", n, m))
			}
		}

		if listSucc {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !listSucc {
		return errors.Errorf("join failed, adds the new member %s may failed", cfg.Name)
	}

	initialCluster = strings.Join(pds, ",")
	cfg.InitialCluster = initialCluster
	cfg.InitialClusterState = embed.ClusterStateFlagExisting
	err = os.MkdirAll(cfg.DataDir, privateDirMode)
	if err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}

	err = ioutil.WriteFile(filePath, []byte(cfg.InitialCluster), privateFileMode)
	return errors.WithStack(err)
}

func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		log.Error("failed to open directory", zap.Error(err))
		return false
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		log.Error("failed to list directory", zap.Error(err))
		return false
	}
	return len(names) != 0
}
