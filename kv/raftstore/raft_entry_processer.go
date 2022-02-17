package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

func (d *peerMsgHandler) process(entries []eraftpb.Entry) {
	for _, entry := range entries {
		switch entry.GetEntryType() {
		case eraftpb.EntryType_EntryConfChange:
			d.processConfChange(entry)
		default:
			d.processNormal(entry)
		}
	}
}

func (d *peerMsgHandler) processConfChange(entry eraftpb.Entry) {
	var cc eraftpb.ConfChange
	if err := cc.Unmarshal(entry.GetData()); err != nil {
		log.Errorf("[%v] peer processConfChange unmarshal data err:%v", d.Tag, err)
		return
	}
	switch cc.GetChangeType() {
	case eraftpb.ConfChangeType_AddNode:
		if !d.peerStorage.ApplyConfChange(&cc) {
			log.Warnf("[%v] peer peerStorage ApplyConfChange fail", d.Tag)
			return
		}
		var peer metapb.Peer
		if err := peer.Unmarshal(cc.GetContext()); err != nil {
			log.Errorf("[%v] peer processConfChange unmarshal confChange err:%v", d.Tag, err)
			return
		}
		// 更新 storeMeta 的 region 结构体
		d.ctx.storeMeta.setRegion(d.Region(), d.peer)
		d.insertPeerCache(&peer)
	case eraftpb.ConfChangeType_RemoveNode:
		peer := d.peer.getPeerFromCache(cc.GetNodeId())
		if peer == nil {
			log.Warnf("[%v] peer processConfChange getPeerFromCache %d is nil ", d.Tag, cc.GetNodeId())
			return
		}
		if util.RemovePeer(d.Region(), peer.GetStoreId()) == nil {
			log.Infof("[%v] peer processConfChange peer %v has already delete in region", d.Tag)
			return
		}
		if d.Meta.GetId() == cc.GetNodeId() {
			d.destroyPeer()
		} else {
			if !d.peerStorage.ApplyConfChange(&cc) {
				log.Warnf("[%v] peer peerStorage ApplyConfChange fail", d.Tag)
				return
			}
		}
		d.ctx.storeMeta.setRegion(d.Region(), d.peer)
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
	log.Debugf("[%v] peer processNormal entry data len:%d index:%d msg:%s", d.Tag, len(entry.GetData()), req.GetID(), req.GetMsg().String())
	if isAdminRequest(req.GetMsg()) {
		//todo handle adminRequest
		switch req.GetMsg().GetAdminRequest().GetCmdType() {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.handleCompactLog(req.GetMsg())
		}
		return
	}

	d.applyCmd(req)
}

func (d *peerMsgHandler) applyCmd(req *RaftCmdRequestWrapper) {
	prop := d.popProposal(req.GetID(), req.GetMsg().GetHeader().GetTerm())
	var cb *message.Callback
	if prop != nil {
		cb = prop.cb
	}

	wb := engine_util.WriteBatch{}
	resp := make([]*raft_cmdpb.Response, len(req.GetMsg().GetRequests()))
	for i, r := range req.GetMsg().GetRequests() {
		resp[i] = &raft_cmdpb.Response{}
		resp[i].CmdType = r.GetCmdType()
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			getRequest := r.GetGet()
			if !d.checkKeyInRegion(getRequest.Key, cb) {
				return
			}
			value, err := engine_util.GetCF(d.ctx.engine.Kv, getRequest.GetCf(), getRequest.GetKey())
			if err != nil {
				log.Errorf("[%v] peer GetCF error:%s", d.Tag, err)
				cb.Done(ErrResp(err))
				return
			}
			resp[i].Get = &raft_cmdpb.GetResponse{Value: value}
		case raft_cmdpb.CmdType_Put:
			putRequest := r.GetPut()
			if !d.checkKeyInRegion(putRequest.Key, cb) {
				return
			}
			wb.SetCF(putRequest.GetCf(), putRequest.GetKey(), putRequest.GetValue())
			resp[i].Put = &raft_cmdpb.PutResponse{}
		case raft_cmdpb.CmdType_Delete:
			delRequest := r.GetDelete()
			if !d.checkKeyInRegion(delRequest.Key, cb) {
				return
			}
			wb.DeleteCF(delRequest.GetCf(), delRequest.GetKey())
			resp[i].Delete = &raft_cmdpb.DeleteResponse{}
		case raft_cmdpb.CmdType_Snap:
			if cb == nil {
				continue
			}
			cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			resp[i].Snap = &raft_cmdpb.SnapResponse{Region: d.Region()}
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

func isAdminRequest(req *raft_cmdpb.RaftCmdRequest) bool {
	return req.GetAdminRequest() != nil
}
