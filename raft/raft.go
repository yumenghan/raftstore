// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		State:            StateFollower,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	// confState
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	r.RaftLog = newLog(c.Storage)
	// init hardState
	if !IsEmptyHardState(hardState) {
		r.RaftLog.committed = hardState.Commit
		r.Term = hardState.Term
		r.Vote = hardState.Vote
	}

	r.Prs = make(map[uint64]*Progress)

	var nodes []uint64
	if nodes = confState.GetNodes(); len(nodes) == 0 {
		nodes = c.peers
	}
	for _, p := range nodes {
		r.Prs[p] = &Progress{}
	}

	r.becomeFollower(r.Term, None)
	log.Debugf("newRaft [%d] peers:[%v] term [%d] commit [%d] applied [%d] lastIndex: [%d]", r.id, c.peers, r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex())
	return r
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	m := pb.Message{}
	m.To = to
	m.From = r.id
	m.MsgType = pb.MessageType_MsgAppend
	m.Term = r.Term

	pr := r.Prs[to]
	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.startAt(pr.Next)

	if errt != nil || erre != nil {
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		m.MsgType = pb.MessageType_MsgSnapshot
		m.Snapshot = &snapshot
		pr.Match = snapshot.GetMetadata().GetIndex()
		pr.Next = pr.Match + 1
		log.Debugf("raft-%d sendAppend snapshot index:%d term:%d to id %d ", r.id, snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetTerm(), to)

	} else {
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.RaftLog.committed
		m.Index = pr.Next - 1
	}

	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		Commit:  commit,
	}

	r.send(m)
}

func (r *Raft) send(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		//
		r.sendAppend(id)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}

	if r.Lead != r.id {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.reset(r.Term + 1)
	// 给自己投票
	r.Vote = r.id
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).

	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	for _, pr := range r.Prs {
		pr.Match = r.RaftLog.LastIndex()
		pr.Next = r.RaftLog.LastIndex() + 1 //初始化为领导人最后索引值加一
	}

	// NOTE: Leader should propose a noop entry on its term
	r.appendEntry([]*pb.Entry{
		&pb.Entry{Data: nil},
	})
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.PendingConfIndex = 0
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool, len(r.Prs))
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.GetTerm() == 0:
	case m.GetTerm() > r.Term:
		switch {
		default:
			// 如果 msg 的 term 较大 且当前是 app、 heartbeat、snap 消息， 不管当前 state 是啥， 直接转为 follower
			if m.GetMsgType() == pb.MessageType_MsgAppend || m.GetMsgType() == pb.MessageType_MsgHeartbeat || m.GetMsgType() == pb.MessageType_MsgSnapshot {
				r.becomeFollower(m.GetTerm(), m.GetFrom())
			} else {
				// 可能在选举
				r.becomeFollower(m.GetTerm(), None)
			}
		}
	case m.GetTerm() < r.Term:
		if m.GetMsgType() == pb.MessageType_MsgAppend {
			r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse})
		} else if m.GetMsgType() == pb.MessageType_MsgRequestVote {
			r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		if r.handleMsgRequestVote(m) {
			r.electionElapsed = 0
		}
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.electionElapsed = 0
			r.handleMsgHup(m)
		case pb.MessageType_MsgRequestVote:
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.GetFrom()
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.GetFrom()
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.electionElapsed = 0
			r.Lead = m.GetFrom()
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.electionElapsed = 0
			if _, ok := r.Prs[r.id]; !ok {
				log.Warnf("raft-%d was not int raft group drop msg", r.id)
				return nil
			}
			r.handleMsgHup(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgPropose})
		case pb.MessageType_MsgTransferLeader:
			m.To = r.Lead
			r.send(m)
		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			voteRes := r.pollQuorum(m)
			switch voteRes {
			case VoteWin:
				r.becomeLeader()
				r.broadcastAppend()
			case VoteLost:
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.GetTerm(), m.GetFrom())
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.GetTerm(), m.GetFrom())
			r.handleSnapshot(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.handleMsgBeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleMsgHeartbeatResponse(m)
		case pb.MessageType_MsgPropose:
			r.handleMsgPropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleMsgAppendResponse(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		}
	}
	return nil
}

func (r *Raft) handleMsgHup(m pb.Message) {
	r.becomeCandidate()
	// 给自己投票
	r.pollQuorum(m)
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		// todo
	}
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{From: r.id, Term: r.Term, To: id, MsgType: pb.MessageType_MsgRequestVote, LogTerm: logTerm, Index: r.RaftLog.LastIndex()})
	}
}

func (r *Raft) handleMsgRequestVote(m pb.Message) bool {
	// 1. r.Vote 还未投票
	// 2. m.Term > r.Term
	// 3. log 足够新
	// 4. 投票消息丢失， candidate 重新请求 vote
	// 5.
	canVote := r.Vote == m.GetFrom() || (r.Vote == None && r.Lead == None) || (r.Vote == None && m.GetTerm() > r.Term)
	if canVote && r.RaftLog.isUpToDate(m.GetIndex(), m.GetLogTerm()) {
		r.Vote = m.GetFrom()
		r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
		return true
	}
	r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	return false
}

type VoteType int

const (
	VoteWin     VoteType = 1
	VoteLost    VoteType = 2
	VotePending VoteType = 3
)

func (r *Raft) pollQuorum(m pb.Message) VoteType {
	// 1.计算是否满足 quorum ; 三种状态 1、win  2、lost  3、pending
	r.votes[m.GetFrom()] = !m.GetReject()
	voteInfo := [2]int{}
	missing := 0
	for id := range r.Prs {
		v, voted := r.votes[id]
		if !voted {
			missing++
			continue
		}
		if v {
			voteInfo[1]++
		} else {
			voteInfo[0]++
		}
	}

	q := len(r.Prs)/2 + 1
	if voteInfo[1] >= q {
		return VoteWin
	}
	if voteInfo[1]+missing >= q {
		return VotePending
	}
	return VoteLost
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.GetIndex() < r.RaftLog.committed {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	if lastNewIndex, ok := r.RaftLog.maybeAppend(m.GetIndex(), m.GetLogTerm(), m.GetCommit(), m.GetEntries()); ok {
		term, _ := r.RaftLog.Term(lastNewIndex)
		r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, LogTerm: term, Index: lastNewIndex})
		return
	}

	hintIndex := min(m.GetIndex(), r.RaftLog.LastIndex())
	hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.GetLogTerm())
	// 当前 index 对应的 logTerm 不 match
	term, err := r.RaftLog.Term(hintIndex)
	if err != nil {
		panic(fmt.Sprintf("raft-%d Index %d term valid err:%v", r.id, hintIndex, err))
	}
	r.send(pb.Message{To: m.GetFrom(), From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: hintIndex, LogTerm: term, Reject: true})
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendHeartbeat(id)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgHeartbeatResponse})
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.GetFrom()]
	if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.GetFrom())
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	if len(m.GetEntries()) == 0 {
		panic("msg propose empty")
	}

	for i, entry := range m.GetEntries() {
		if entry.GetEntryType() == pb.EntryType_EntryConfChange {
			var cc pb.ConfChange
			if err := cc.Unmarshal(entry.GetData()); err != nil {
				panic(err)
			}
			if r.PendingConfIndex <= r.RaftLog.applied {
				r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
			} else {
				log.Infof("raft-%d pendingConfIndex:%d applied:%d ignoring conf change conf %v", r.id, r.PendingConfIndex, r.RaftLog.applied, cc)
				m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
			}
		}
	}
	r.appendEntry(m.GetEntries())
	r.broadcastAppend()
}

func (r *Raft) handleMsgAppendResponse(m pb.Message) {
	//
	progress, ok := r.Prs[m.From]
	if !ok {
		return
	}

	if m.GetReject() {
		if m.GetLogTerm() > 0 {
			index := r.RaftLog.findConflictByTerm(m.GetIndex(), m.GetLogTerm())
			progress.Match = min(index, progress.Match)
		} else {
			progress.Match--
		}

		progress.Next = progress.Match + 1
		r.sendAppend(m.GetFrom())
		return
	}

	progress.Next = m.GetIndex() + 1
	progress.Match = m.GetIndex()

	if r.RaftLog.LastIndex() > progress.Match {
		r.sendAppend(m.GetFrom())
	}

	if r.leadTransferee == m.GetFrom() && progress.Match == r.RaftLog.LastIndex() {
		log.Infof("raft-%d log is up to date to send MsgTimeoutNow to %d", r.id, r.leadTransferee)
		r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgTimeoutNow, Term: r.Term})
		r.leadTransferee = None
	}

	if r.maybeCommit() {
		r.broadcastAppend()
	}

}

func (r *Raft) maybeCommit() bool {
	// 如何确定一个消息是否能被 commit？
	// 1.过半提交
	// 2.确保 leader 在当前 term 已经有一条被 commit 日志
	counter := make(map[uint64]int)
	for _, p := range r.Prs {
		cnt := counter[p.Match]
		counter[p.Match] = cnt + 1
	}

	indexSeq := make([]uint64, 0, len(counter))
	for index, _ := range counter {
		if index > r.RaftLog.committed {
			indexSeq = append(indexSeq, index)
		}
	}

	sortkeys.Uint64s(indexSeq)
	prevCnt := 0
	for i := len(indexSeq) - 1; i >= 0; i-- {
		index := indexSeq[i]
		if index < r.RaftLog.committed {
			// 落后太多
			break
		}
		cnt := counter[index] + prevCnt
		prevCnt = cnt
		// if majority
		if cnt >= len(r.Prs)/2+1 {
			term, err := r.RaftLog.Term(index)
			if err != nil {
				log.Errorf("update leader commit get index [%d] err:%v", index, err)
			}
			if term == r.Term {
				if index > r.RaftLog.committed {
					r.RaftLog.committed = index
					return true
				}
			}
		}
	}
	// term != r.Term 大多数 log 的 term 不是自己的，不能 commit
	return false
}

func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIndex + uint64(i) + 1
	}

	index := r.RaftLog.append(entries)
	pr := r.Prs[r.id]
	if pr.Match < index {
		pr.Match = index
	}

	pr.Next = max(pr.Next, index+1)
	r.maybeCommit()
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	snapshot := m.GetSnapshot()
	if snapshot == nil {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	if r.restore(snapshot) {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
	} else {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}

func (r *Raft) restore(s *pb.Snapshot) bool {
	metaInfo := s.GetMetadata()
	if metaInfo == nil {
		return false
	}
	if metaInfo.GetIndex() <= r.RaftLog.committed {
		return false
	}

	log.Infof("raft-%d handleSnapshot %+v", r.id, metaInfo)
	//
	if r.RaftLog.hasPendingSnapshot() && r.RaftLog.pendingSnapshot.GetMetadata().GetIndex() > metaInfo.GetIndex() {
		log.Warnf("raft-%d was installing snapshot index:%d term:%d", r.id, r.RaftLog.pendingSnapshot.GetMetadata().GetIndex(), r.RaftLog.pendingSnapshot.GetMetadata().GetTerm())
		return false
	}

	r.RaftLog.pendingSnapshot = s
	rlog := r.RaftLog
	rlog.entries = nil
	rlog.offset = metaInfo.GetIndex() + 1
	rlog.applied = metaInfo.GetIndex()
	rlog.committed = rlog.applied
	rlog.stabled = rlog.applied
	state := metaInfo.GetConfState()
	r.Prs = make(map[uint64]*Progress)
	for _, node := range state.GetNodes() {
		r.Prs[node] = &Progress{}
	}
	log.Infof("raft-%d handleSnapshot finish raftLog:%s", r.id, rlog.String())
	return true
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	transferee := m.GetFrom()
	progress, ok := r.Prs[transferee]
	if !ok {
		log.Errorf("raft-%v handleTransferLeader transfee %d not in progress ", r.id, transferee)
		return
	}
	// transfer 之后，应用没用及时更新，导致一直发送 tranfer 消息
	if r.id == transferee {
		return
	}
	if r.leadTransferee == transferee {
		// 已经在同步中
		return
	}
	log.Infof("raft-%d start to transfer leader to %d", r.id, transferee)
	if r.RaftLog.LastIndex() > progress.Match {
		// 需要暂停 propose 并且同步日志
		r.sendAppend(transferee)
		r.leadTransferee = transferee
		return
	}

	log.Infof("raft-%d send msgTimeOutNow immediately to %d", r.id, transferee)
	// 满足条件可以直接发送
	r.send(pb.Message{From: r.id, To: transferee, MsgType: pb.MessageType_MsgTimeoutNow, Term: r.Term})
	r.leadTransferee = None
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	prs := r.Prs[id]
	if prs != nil {
		log.Infof("raft-%d addNode %d already add", r.id, id)
		return
	}

	r.Prs[id] = &Progress{
		Match: 0,
		Next:  1,
	}

	r.sendHeartbeat(id)
	log.Infof("raft-%d addNode %d ok", r.id, id)
	r.PendingConfIndex = 0
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	prs := r.Prs[id]
	if prs == nil {
		log.Infof("raft-%d removeNode %d has already remove", r.id, id)
		return
	}

	delete(r.Prs, id)
	r.maybeCommit()
	log.Infof("raft-%d removeNode %d ok", r.id, id)
	r.PendingConfIndex = 0
}
