package raftstore

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sync"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

type signal interface {
	workerReady(uint64)
}
// peerState contains the peer states that needs to run raft command and apply command.
type peerState struct {
	closed uint32
	peer   *peer
}

// router routes a message to a peer.
type router struct {
	peers         sync.Map         // regionID -> peerState
	peerSender    chan message.Msg // 不再直接发送到 sender， dispatch 消息类型 发送到对应的 peer 队列
	storeSender   chan<- message.Msg
	partition     IPartitioner
	mu            sync.RWMutex        // used for protected peersWorker
	workerRegions map[uint64][]uint64 // worker -> regions
	signal        signal              // used to signal worker
}

func newRouter(storeSender chan<- message.Msg, cnt uint64) *router {
	pm := &router{
		peerSender:    make(chan message.Msg, 40960),
		storeSender:   storeSender,
		partition:     NewFixedPartitioner(cnt),
		workerRegions: make(map[uint64][]uint64),
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

func (pr *router) getAllRegions(workerID uint64) []uint64 {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return pr.workerRegions[workerID]
}

func (pr *router) register(peer *peer) {
	id := peer.regionId
	newPeer := &peerState{
		peer: peer,
	}
	pr.peers.Store(id, newPeer)
	workerId := pr.partition.GetPartitionID(peer.regionId)

	pr.mu.Lock()
	defer pr.mu.Unlock()
	peers, ok := pr.workerRegions[workerId]
	if !ok {
		var newRegions []uint64
		newRegions = append(newRegions, id)
		pr.workerRegions[workerId] = newRegions
		return
	}
	newPeers := make([]uint64, len(peers)+1)
	copy(newPeers, peers)
	newPeers[len(newPeers)-1] = id
	pr.workerRegions[workerId] = newPeers
}

func (pr *router) registerSignal(s signal) {
	pr.signal = s
}

func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)

		workerId := pr.partition.GetPartitionID(ps.peer.regionId)
		pr.mu.Lock()
		defer pr.mu.Unlock()
		regions, ok := pr.workerRegions[workerId]
		if !ok {
			return
		}
		if len(regions) == 1 {
			if regions[0] == regionID {
				pr.workerRegions[workerId] = nil
				return
			}
			return
		}
		newRegions := make([]uint64, 0, len(regions)-1)
		for _, region := range regions {
			if region != regionID {
				newRegions = append(newRegions, region)
			}
		}
		pr.workerRegions[workerId] = newRegions
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
			log.Warnf("peer[%s] raft msg queue add fail", peer.Tag)
			return nil
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		var err error
		if adminReq := raftCMD.Request.GetAdminRequest(); adminReq != nil {
			switch adminReq.GetCmdType() {
			case raft_cmdpb.AdminCmdType_TransferLeader:
				err = peer.pendingLeaderTransfer.propose(raftCMD)
			case raft_cmdpb.AdminCmdType_ChangePeer:
				_, err = peer.pendingConfigChange.propose(raftCMD, 1000)
			}
			if err != nil {
				log.Warnf("peer[%s] raft admin msg queue add fail %v", peer.Tag, raftCMD.Request.String())
				raftCMD.Callback.Done(ErrResp(err))
				return nil
			}
		}
		for _, req := range raftCMD.Request.GetRequests() {
			switch req.GetCmdType() {
			case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
				_, err = peer.pendingReadIndexes.propose(raftCMD, 1000)
			case raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_Delete:
				_, err = peer.pendingProposal.propose(raftCMD, 1000)
			}
			if err != nil {
				log.Warnf("peer[%s] raft normal msg queue add fail %v", peer.Tag, raftCMD.Request.String())
				raftCMD.Callback.Done(ErrResp(err))
			}
		}
	case message.MsgTypeTick:
		raftMsg := &raft_serverpb.RaftMessage{Message: &eraftpb.Message{MsgType: eraftpb.MessageType_MsgTick}}
		if ok, _ := peer.pendingRaftMsgQueue.Add(raftMsg); !ok {
			log.Warnf("peer[%s] raft msg queue add fail", peer.Tag)
			return nil
		}
	default:
		// normal 消息暂时走原流程
		pr.peerSender <- msg
		return nil
	}

	pr.signal.workerReady(regionID)
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
