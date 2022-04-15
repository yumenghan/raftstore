package raftstore

import (
	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"sort"
)

func (d *peerMsgHandler) process() {
	done := false
	for !done {
		task, ok := d.toApplyQ.Get()
		if !ok {
			done = true
		}
		for _, entry := range task.Entries {
			switch entry.GetEntryType() {
			case eraftpb.EntryType_EntryConfChange:
				d.processConfChange(entry)
			default:
				d.processNormal(entry)
			}
			d.processReadIndex(entry.GetIndex())
		}
	}
}

func (d *peerMsgHandler) processReadIndex(applyIndex uint64) {
	readIndexReq := d.pendingReadIndexes.getApply(applyIndex)
	if len(readIndexReq) <= 0 {
		return
	}

	resps := make([]*raft_cmdpb.RaftCmdResponse, len(readIndexReq))
	for j, reqIndex := range readIndexReq {
		if err := util.CheckRegionEpoch(reqIndex.request, d.Region(), true); err != nil {
			log.Infof("[%v] peer applyCmd epoch has changed, prev epoch:%v epoch:%v", d.Tag, reqIndex.request.GetHeader().GetRegionEpoch(), d.Region().GetRegionEpoch())
			resps[j] = ErrResp(err)
			continue
		}
		resp := make([]*raft_cmdpb.Response, len(reqIndex.request.GetRequests()))
		i := 0
		var err error
		for _, r := range reqIndex.request.GetRequests() {
			switch r.CmdType {
			case raft_cmdpb.CmdType_Get:
				getRequest := r.GetGet()
				if err = util.CheckKeyInRegion(getRequest.GetKey(), d.Region()); err != nil {
					log.Warnf("[%v] peer checkKeyInRegion key [%v] not in region", d.Tag, string(getRequest.GetKey()))
					break
				}
				value, err := engine_util.GetCF(d.ctx.engine.Kv, getRequest.GetCf(), getRequest.GetKey())
				if err != nil {
					log.Errorf("[%v] peer GetCF error:%s", d.Tag, err)
					break
				}
				resp[i].Get = &raft_cmdpb.GetResponse{Value: value}
				i++
			case raft_cmdpb.CmdType_Snap:
				reqIndex.requestState.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
				resp[i].Snap = &raft_cmdpb.SnapResponse{Region: d.Region()}
				i++
			}
		}
		if i < len(resp) {
			resps[j] = ErrResp(err)
			continue
		}
		cmdResp := newCmdResp()
		cmdResp.Header.CurrentTerm = reqIndex.request.GetHeader().GetTerm()
		cmdResp.Responses = append(cmdResp.Responses, resp...)
		resps[j] = cmdResp
	}
	d.pendingReadIndexes.applied(readIndexReq, resps)
}

func (d *peerMsgHandler) processConfChange(entry eraftpb.Entry) {
	var cc eraftpb.ConfChange
	if err := cc.Unmarshal(entry.GetData()); err != nil {
		log.Errorf("[%v] peer processConfChange unmarshal data err:%v", d.Tag, err)
		return
	}
	region := &metapb.Region{}
	util.CloneMsg(d.Region(), region)
	switch cc.GetChangeType() {
	case eraftpb.ConfChangeType_AddNode:
		if !d.applyAddNode(region, &cc) {
			log.Warnf("[%v] peer peerStorage ApplyConfChange fail", d.Tag)
			return
		}
		var peer metapb.Peer
		if err := peer.Unmarshal(cc.GetContext()); err != nil {
			log.Errorf("[%v] peer processConfChange unmarshal confChange err:%v", d.Tag, err)
			return
		}
		// 更新 storeMeta 的 region 结构体
		d.ctx.storeMeta.setRegion(region, d.peer)
		d.insertPeerCache(&peer)
	case eraftpb.ConfChangeType_RemoveNode:
		peer := d.peer.getPeerFromCache(cc.GetNodeId())
		if peer == nil {
			log.Warnf("[%v] peer processConfChange getPeerFromCache %d is nil ", d.Tag, cc.GetNodeId())
			return
		}
		if util.RemovePeer(region, peer.GetStoreId()) == nil {
			log.Infof("[%v] peer processConfChange peer %v has already delete in region", d.Tag)
			return
		}
		region.GetRegionEpoch().ConfVer++
		if d.Meta.GetId() == cc.GetNodeId() {
			d.ctx.storeMeta.setRegion(region, d.peer)
			d.destroyPeer()
		} else {
			var kvwb engine_util.WriteBatch
			meta.WriteRegionState(&kvwb, region, rspb.PeerState_Normal)
			if err := kvwb.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
				log.Warnf("[%v] peer peerStorage ApplyConfChange fail", d.Tag)
				return
			}
			d.ctx.storeMeta.setRegion(region, d.peer)
		}
		d.removePeerCache(peer.GetId())
	}

	d.RaftGroup.ApplyConfChange(cc)
}

func (d *peerMsgHandler) processNormal(entry eraftpb.Entry) {
	if len(entry.GetData()) <= 0 {
		return
	}
	req := &RaftCmdRequestWrapper{}
	if err := req.Unmarshal(entry.GetData()); err != nil {
		log.Errorf("[%v] peer processNormal unmarshal requestWrapper err:%v", d.Tag, err)
		return
	}
	log.Debugf("[%v] peer processNormal entry data len:%d index:%d msg:%s", d.Tag, len(entry.GetData()), req.key, req.GetMsg().String())
	if isAdminRequest(req.GetMsg()) {
		//todo handle adminRequest
		switch req.GetMsg().GetAdminRequest().GetCmdType() {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.handleCompactLog(req.GetMsg())
		case raft_cmdpb.AdminCmdType_Split:
			d.handleRegionSplit(req.GetMsg())
		}
		return
	}

	d.applyCmd(req)
}

func (d *peerMsgHandler) applyCmd(req *RaftCmdRequestWrapper) {
	//prop := d.popProposal(req.GetID(), req.GetMsg().GetHeader().GetTerm())
	var cb *message.Callback
	//if prop != nil {
	//	cb = prop.cb
	//}
	//
	err := util.CheckRegionEpoch(req.GetMsg(), d.Region(), true)
	if err != nil {
		log.Infof("[%v] peer applyCmd epoch has changed, prev epoch:%v epoch:%v", d.Tag, req.GetMsg().GetHeader().GetRegionEpoch(), d.Region().GetRegionEpoch())
		cb.Done(ErrResp(err))
		return
	}

	wb := engine_util.WriteBatch{}
	resp := make([]*raft_cmdpb.Response, len(req.GetMsg().GetRequests()))
	for i, r := range req.GetMsg().GetRequests() {
		resp[i] = &raft_cmdpb.Response{}
		resp[i].CmdType = r.GetCmdType()
		switch r.CmdType {
		case raft_cmdpb.CmdType_Put:
			putRequest := r.GetPut()
			if !d.checkKeyInRegion(putRequest.Key, cb) {
				return
			}
			wb.SetCF(putRequest.GetCf(), putRequest.GetKey(), putRequest.GetValue())
			d.addSizeDiffHint(putRequest.GetKey(), putRequest.GetValue())
			resp[i].Put = &raft_cmdpb.PutResponse{}
		case raft_cmdpb.CmdType_Delete:
			delRequest := r.GetDelete()
			if !d.checkKeyInRegion(delRequest.Key, cb) {
				return
			}
			wb.DeleteCF(delRequest.GetCf(), delRequest.GetKey())
			resp[i].Delete = &raft_cmdpb.DeleteResponse{}
		default:
			log.Warnf("[%v] peer apply cmd invalid type:%v", d.Tag, r.CmdType)
		}
	}

	if err := wb.WriteToDB(d.ctx.engine.Kv); err != nil {
		log.Errorf("[%v] peer applyCmd writeToDB err:%v", d.Tag, err)
		cb.Done(ErrResp(err))
		return
	}

	cmdResp := newCmdResp()
	cmdResp.Header.CurrentTerm = req.GetMsg().GetHeader().GetTerm()
	cmdResp.Responses = append(cmdResp.Responses, resp...)
	cb.Done(cmdResp)
}

func (d *peerMsgHandler) applyAddNode(region *metapb.Region, cc *eraftpb.ConfChange) bool {
	region.RegionEpoch.ConfVer++

	var peer metapb.Peer
	if err := peer.Unmarshal(cc.GetContext()); err != nil {
		log.Errorf("[%v] peerStorage applyAddNode unmarshal err:%v", d.Tag, err)
		return false
	}

	if util.FindPeer(region, peer.GetStoreId()) != nil {
		log.Warnf("[%v] peerStorage applyAddNode %v has already in region %v", d.Tag, peer, region)
		return false
	}
	// id、 storeId
	region.Peers = append(region.Peers, &peer)

	wb := &engine_util.WriteBatch{}
	meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
	if err := wb.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		log.Errorf("[%v] peerStorage applyAddNode writeToDB err:%v", d.Tag, err)
		return false
	}
	return true
}

func (d *peerMsgHandler) handleCompactLog(msg *raft_cmdpb.RaftCmdRequest) {
	compactTerm := msg.GetAdminRequest().GetCompactLog().GetCompactTerm()
	compactIndex := msg.GetAdminRequest().GetCompactLog().GetCompactIndex()

	if !d.validateCompactLog(compactTerm, compactIndex) {
		log.Infof("peer-[%s] handleCompactLog validate can compact fail [compactTerm:%d compactIndex:%d]", compactTerm, compactIndex)
		return
	}

	wb := &engine_util.WriteBatch{}
	d.peerStorage.applyState.TruncatedState.Index = compactIndex
	d.peerStorage.applyState.TruncatedState.Term = compactTerm
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

	wb.WriteToDB(d.ctx.engine.Kv)
	// 异步删除
	d.ScheduleCompactLog(compactIndex)
}

func (d *peerMsgHandler) validateCompactLog(compactTerm, compactIndex uint64) bool {
	lastIndex := d.RaftGroup.Raft.RaftLog.LastIndex()
	if compactIndex > lastIndex {
		return false
	}
	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIndex)
	if err != nil {
		log.Errorf("peer-%d validateCompactLog get %d term err:%v", d.Tag, compactIndex, err)
		return false
	}
	if term != compactTerm {
		return false
	}
	return true
}

func (d *peerMsgHandler) checkKeyInRegion(key []byte, cb *message.Callback) bool {
	if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
		log.Warnf("[%v] peer checkKeyInRegion key [%v] not in region", d.Tag, string(key))
		cb.Done(ErrResp(err))
		return false
	}
	return true
}

func (d *peerMsgHandler) handleRegionSplit(msg *raft_cmdpb.RaftCmdRequest) {
	splitRequest := msg.GetAdminRequest().GetSplit()

	if engine_util.ExceedEndKey(splitRequest.GetSplitKey(), d.Region().GetEndKey()) || engine_util.ExceedEndKey(d.Region().GetStartKey(), splitRequest.GetSplitKey()) {
		log.Warnf("[%v] peer handleRegionSplit split key invalid, not match startKey < splitKey < endKey", d.Tag)
		return
	}

	// 原 region
	region := &metapb.Region{}
	util.CloneMsg(d.Region(), region)

	splitRegion := d.createRegion(region, splitRequest)
	if splitRegion == nil {
		return
	}

	peer, err := createPeer(d.Meta.GetStoreId(), d.ctx.cfg, d.ctx.schedulerTaskSender, d.peerStorage.Engines, splitRegion)
	if err != nil {
		log.Errorf("[%v] peer handleRegionSplit createPeerr err:%v", d.Tag, err)
		return
	}

	// set splitRegion info
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: splitRegion})
	d.ctx.storeMeta.regions[splitRegion.GetId()] = splitRegion
	d.ctx.router.register(peer)

	// trigger start
	err = d.ctx.router.send(splitRegion.GetId(), message.Msg{RegionID: splitRegion.GetId(), Type: message.MsgTypeStart})
	if err != nil {
		log.Errorf("[%v] peer handleRegionSplit send msg to trigger start err :%v", d.Tag, err)
		return
	}

	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	d.ctx.storeMeta.regions[region.GetId()] = region
	d.ctx.storeMeta.setRegion(region, d.peer)

	log.Infof("[%v] peer handleRegionSplit (peer: %d -> store %d) (region: %+v) (splitRegion: %+v) ", d.Tag, peer.Meta.GetId(), peer.Meta.GetStoreId(), region, splitRegion)
}

func (d *peerMsgHandler) createRegion(region *metapb.Region, splitRequest *raft_cmdpb.SplitRequest) *metapb.Region {
	peerSlice := PeerSlice(region.GetPeers())
	sort.Sort(peerSlice)
	newPeers := splitRequest.GetNewPeerIds()
	sortkeys.Uint64s(newPeers)

	peers := make([]*metapb.Peer, len(peerSlice))
	for i, p := range peerSlice {
		peers[i] = &metapb.Peer{
			Id:      newPeers[i],
			StoreId: p.GetStoreId(),
		}
	}

	// newRegion
	splitRegion := &metapb.Region{
		Id:       splitRequest.GetNewRegionId(),
		StartKey: splitRequest.GetSplitKey(),
		EndKey:   region.GetEndKey(),
		Peers:    peers,
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
	}

	if util.FindPeer(splitRegion, d.Meta.GetStoreId()) == nil {
		log.Errorf("[%v] peer createRegion err not find store %d in splitRegion:%v", d.Tag, d.Meta.GetStoreId(), splitRegion)
		return nil
	}

	raftwb := &engine_util.WriteBatch{}
	kvwb := &engine_util.WriteBatch{}
	writeInitialApplyState(kvwb, splitRegion.GetId())
	writeInitialRaftState(raftwb, splitRegion.GetId())
	meta.WriteRegionState(kvwb, splitRegion, rspb.PeerState_Normal)

	raftwb.WriteToDB(d.ctx.engine.Raft)
	kvwb.WriteToDB(d.ctx.engine.Kv)

	kvwb.Reset()
	// set origin region
	region.EndKey = splitRequest.GetSplitKey()
	region.RegionEpoch.Version++
	meta.WriteRegionState(kvwb, region, rspb.PeerState_Normal)
	if err := kvwb.WriteToDB(d.ctx.engine.Kv); err != nil {
		log.Errorf("[%v] peer handleRegionSplit writeToDB err:%v", d.Tag, err)
		return nil
	}

	return splitRegion
}

func isAdminRequest(req *raft_cmdpb.RaftCmdRequest) bool {
	return req.GetAdminRequest() != nil
}
