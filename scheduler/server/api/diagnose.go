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

package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	"github.com/pingcap-incubator/tinykv/scheduler/server"
	"github.com/pkg/errors"
	"github.com/unrolled/render"
)

type diagnoseType int

// Recommendation contains a potential problem and possible way to deal with it.
type Recommendation struct {
	Module      string `json:"module"`
	Level       string `json:"level"`
	Description string `json:"description"`
	Instruction string `json:"instruction"`
}

//lint:file-ignore U1000 document available levels and modules
const (
	// analyze levels
	levelWarning  = "Warning"
	levelMinor    = "Minor"
	levelMajor    = "Major"
	levelCritical = "Critical"

	// analyze modules
	modMember  = "member"
	modTiKV    = "TiKV"
	modDefault = "Default"

	memberOneInstance diagnoseType = iota
	memberEvenInstance
	memberLostPeers
	memberLostPeersMoreThanHalf
	memberLeaderChanged
	tikvCap70
	tikvCap80
	tikvCap90
	tikvLostPeers
	tikvLostPeersLongTime
)

var (
	diagnoseMap = map[diagnoseType]Recommendation{
		memberOneInstance:           {modMember, levelWarning, "only one PD instance is running.", "please add PD instance."},
		memberEvenInstance:          {modMember, levelMinor, "PD instances is even number.", "the recommended number of PD's instances is odd."},
		memberLostPeers:             {modMember, levelMajor, "some PD instances is down.", "please check host load and traffic."},
		memberLostPeersMoreThanHalf: {modMember, levelCritical, "more than half PD instances is down.", "please check host load and traffic."},
		memberLeaderChanged:         {modMember, levelMinor, "PD cluster leader is changed.", "please check host load and traffic."},
		tikvCap70:                   {modTiKV, levelWarning, "some TiKV storage used more than 70%.", "please add TiKV node."},
		tikvCap80:                   {modTiKV, levelMinor, "some TiKV storage used more than 80%.", "please add TiKV node."},
		tikvCap90:                   {modTiKV, levelMajor, "some TiKV storage used more than 90%.", "please add TiKV node."},
		tikvLostPeers:               {modTiKV, levelWarning, "some TiKV lost connect.", "please check network."},
		tikvLostPeersLongTime:       {modTiKV, levelMajor, "some TiKV lost connect more than 1h.", "please check network."},
	}
)

type diagnoseHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newDiagnoseHandler(svr *server.Server, rd *render.Render) *diagnoseHandler {
	return &diagnoseHandler{
		svr: svr,
		rd:  rd,
	}
}

func diagnosePD(key diagnoseType, descAdd, instAdd string) *Recommendation {
	d, ok := diagnoseMap[key]
	if !ok {
		return &Recommendation{
			Module:      modDefault,
			Description: descAdd,
			Instruction: instAdd,
		}
	}
	if descAdd != "" {
		d.Description = fmt.Sprintf("%s %s", d.Description, descAdd)
	}
	if instAdd != "" {
		d.Instruction = fmt.Sprintf("%s %s", d.Instruction, instAdd)
	}
	return &d
}

func (d *diagnoseHandler) membersDiagnose(rdd *[]*Recommendation) error {
	var lostMemberIDs, runningMemberIDs []uint64
	var newLeaderID uint64
	req := &pdpb.GetMembersRequest{Header: &pdpb.RequestHeader{ClusterId: d.svr.ClusterID()}}
	members, err := d.svr.GetMembers(context.Background(), req)
	if err != nil {
		return errors.WithStack(err)
	}
	lenMembers := len(members.Members)
	if lenMembers > 0 {
		for _, m := range members.Members {
			pm, err := getEtcdPeerStats(m.ClientUrls[0])
			if err != nil {
				// get peer etcd failed
				lostMemberIDs = append(lostMemberIDs, m.MemberId)
				continue
			}
			runningMemberIDs = append(runningMemberIDs, m.MemberId)
			if time.Since(pm.LeaderInfo.StartTime) < time.Minute {
				newLeaderID = m.MemberId
			}
		}
	} else {
		return errors.Errorf("get PD member error")
	}
	lenLostMembers := len(lostMemberIDs)
	if newLeaderID != 0 {
		*rdd = append(*rdd, diagnosePD(memberLeaderChanged, fmt.Sprintf("new leader %d", newLeaderID), ""))
	}
	if len(runningMemberIDs) == 1 {
		// only one pd peer running
		*rdd = append(*rdd, diagnosePD(memberOneInstance, fmt.Sprintf("running PD member ID %d", runningMemberIDs[0]), ""))
	}
	if lenLostMembers > 0 {
		// some pd's peers can not be connected
		stringID := "lost members ID "
		for _, m := range lostMemberIDs {
			stringID = fmt.Sprintf("%s %d,", stringID, m)
		}
		*rdd = append(*rdd, diagnosePD(memberLostPeers, stringID, ""))
	}
	if len(runningMemberIDs)%2 == 0 {
		// alived pd's numbers is even
		*rdd = append(*rdd, diagnosePD(memberEvenInstance, "", ""))
	}
	if float64(lenMembers)/2 < float64(lenLostMembers) {
		*rdd = append(*rdd, diagnosePD(memberLostPeersMoreThanHalf, "", ""))
	}
	return nil
}

func (d *diagnoseHandler) tikvDiagnose(rdd *[]*Recommendation) error {
	return nil
}

func (d *diagnoseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rdd := []*Recommendation{}
	if err := d.membersDiagnose(&rdd); err != nil {
		d.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	d.rd.JSON(w, http.StatusOK, rdd)
}
