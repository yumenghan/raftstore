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

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	if len(cluster.GetStores()) <= 1 {
		return nil
	}

	var availableStores []*core.StoreInfo
	stores := cluster.GetStores()
	for _, s := range stores {
		if s.DownTime() < cluster.GetMaxStoreDownTime() {
			availableStores = append(availableStores, s)
		}
	}

	if len(availableStores) <= 0 {
		return nil
	}
	// 排序
	sortStores := core.StoreSlice(availableStores)
	sort.Sort(sortStores)

	var region *core.RegionInfo
	var fromStore *core.StoreInfo
	var toStore *core.StoreInfo
	for i := len(sortStores) - 1; i >= 0; i-- {
		fromStore = sortStores[i]
		for j := 0; j < i; j++ {
			toStore = sortStores[j]
			if region = s.pickUpRegion(fromStore, toStore, cluster); region != nil {
				break
			}
		}
	}

	if region == nil {
		return nil
	}

	peer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		log.Error("cluster allocPeer err", zap.Error(err))
		return nil
	}

	op, err := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.GetId())
	if err != nil {
		log.Error("operator createMovePeerOperator err", zap.Error(err))
		return nil
	}

	log.Info("scheduler move peer success", zap.String("scheduler", s.GetName()), zap.Uint64("regionId", region.GetID()), zap.Uint64("fromStore", fromStore.GetID()), zap.Uint64("toStore", toStore.GetID()), zap.Uint64("peer", peer.GetId()))
	return op
}

func (s *balanceRegionScheduler) pickUpRegion(fromStore, toStore *core.StoreInfo, cluster opt.Cluster) *core.RegionInfo {
	var region *core.RegionInfo
	start := []byte("")
	end := []byte("")
	cluster.GetPendingRegionsWithLock(fromStore.GetID(), func(container core.RegionsContainer) {
		region = container.RandomRegion(start, end)
	})
	if validateRegion(region, fromStore, toStore, cluster) {
		return region
	}

	cluster.GetFollowersWithLock(fromStore.GetID(), func(container core.RegionsContainer) {
		region = container.RandomRegion(start, end)
	})
	if validateRegion(region, fromStore, toStore, cluster) {
		return region
	}

	cluster.GetLeadersWithLock(fromStore.GetID(), func(container core.RegionsContainer) {
		region = container.RandomRegion(start, end)
	})
	if validateRegion(region, fromStore, toStore, cluster) {
		return region
	}

	return nil
}

func validateRegion(region *core.RegionInfo, fromStore, toStore *core.StoreInfo, cluster opt.Cluster) bool {
	if region == nil {
		return false
	}
	if region.GetStorePeer(toStore.GetID()) != nil {
		return false
	}
	if len(region.GetPeers()) < cluster.GetMaxReplicas() {
		return false
	}
	if fromStore.GetRegionSize()-toStore.GetRegionSize() > 2*region.GetApproximateSize() {
		return true
	}
	return false
}
