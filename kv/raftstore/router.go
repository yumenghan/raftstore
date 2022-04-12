package raftstore

import (
	"github.com/pingcap-incubator/tinykv/log"
	"sync"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// peerState contains the peer states that needs to run raft command and apply command.
type peerState struct {
	closed uint32
	peer   *peer
}

// router routes a message to a peer.
type router struct {
	peers       sync.Map // regionID -> peerState
	peerSender  chan message.Msg // 不再直接发送到 sender， dispatch 消息类型 发送到对应的 peer 队列
	storeSender chan<- message.Msg
}

func newRouter(storeSender chan<- message.Msg) *router {
	pm := &router{
		peerSender:  make(chan message.Msg, 40960),
		storeSender: storeSender,
	}
	return pm
}

func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

func (pr *router) register(peer *peer) {
	id := peer.regionId
	newPeer := &peerState{
		peer: peer,
	}
	pr.peers.Store(id, newPeer)
}

func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)
	}
}

func (pr *router) send(regionID uint64, msg message.Msg) error {
	msg.RegionID = regionID
	p := pr.get(regionID)
	if p == nil || atomic.LoadUint32(&p.closed) == 1 {
		return errPeerNotFound
	}
	peer := p.peer
	// todo 封装到 peer 内执行
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*raft_serverpb.RaftMessage)
		if ok, _ := peer.pendingRaftMsgQueue.Add(raftMsg); !ok {
			log.Warnf("")
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		if adminReq := raftCMD.Request.GetAdminRequest(); adminReq != nil {
			var err error
			switch adminReq.GetCmdType() {
			case raft_cmdpb.AdminCmdType_TransferLeader:
				err = peer.pendingLeaderTransfer.propose(raftCMD)
			case raft_cmdpb.AdminCmdType_ChangePeer:
				_, err = peer.pendingConfigChange.propose(raftCMD, 1000)
			}
			if err != nil {
				raftCMD.Callback.Done(ErrResp(err))
			}
			return nil
		}
		var err error
		for _, req := range raftCMD.Request.GetRequests() {
			switch req.GetCmdType() {
			case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
				_, err = peer.pendingReadIndexes.propose(raftCMD, 1000)
			case raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_Delete:
				_, err = peer.pendingProposal.propose(raftCMD, 1000)
			}
			if err != nil {
				raftCMD.Callback.Done(ErrResp(err))
			}
		}
		return nil
	case message.MsgTypeTick:
		//d.onTick()
	case message.MsgTypeSplitRegion:
		//split := msg.Data.(*message.MsgSplitRegion)
		//log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		//d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		//d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		//gcSnap := msg.Data.(*message.MsgGCSnap)
		//d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		//d.startTicker()
	}
	// normal 消息暂时走原流程
	pr.peerSender <- msg
	return nil
}

func (pr *router) sendStore(msg message.Msg) {
	pr.storeSender <- msg
}

var errPeerNotFound = errors.New("peer not found")

type RaftstoreRouter struct {
	router *router
}

func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

func (r *RaftstoreRouter) Send(regionID uint64, msg message.Msg) error {
	return r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	if r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftMessage, regionID, msg)) != nil {
		r.router.sendStore(message.NewPeerMsg(message.MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil

}

func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	cmd := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	regionID := req.Header.RegionId
	return r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}
