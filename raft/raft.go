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
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	// init hardState
	if !IsEmptyHardState(hardState) {
		r.RaftLog.committed = hardState.Commit
		r.Term = hardState.Term
		r.Vote = hardState.Vote
	}
	rLog := newLog(c.Storage)

	r.Prs = make(map[uint64]*Progress)
	for _, p := range c.peers {
		r.Prs[p] = &Progress{}
	}

	r.RaftLog = rLog
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

func (r *Raft) send(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{From: r.id, To: id, Term: r.Term, MsgType: pb.MessageType_MsgAppend})
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
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

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
			r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgAppendResponse})
		}
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.electionElapsed = 0
			r.handleMsgHup(m)
		case pb.MessageType_MsgRequestVote:
			r.electionElapsed = 0
			r.handleMsgRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.GetFrom()
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.GetFrom()
			r.handleHeartbeat(m)

		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			if r.pollQuorum(m) {
				r.becomeLeader()
				r.broadcastAppend()
			}
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.GetTerm(), m.GetFrom())
			r.handleAppendEntries(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.handleMsgBeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleMsgHeartbeatResponse(m)
		case pb.MessageType_MsgPropose:
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
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{From: r.id, Term: r.Term, To: id, MsgType: pb.MessageType_MsgRequestVote, Index: r.RaftLog.LastIndex()})
	}
}

func (r *Raft) handleMsgRequestVote(m pb.Message) {
	// 1. r.Vote 还未投票
	// 2. m.Term > r.Term
	// 3. log 足够新
	// 4. 投票消息丢失， candidate 重新请求 vote
	// 5.
	canVote := r.Vote == m.GetFrom() || (r.Vote == None && r.Lead == None) || (r.Vote == None && m.GetTerm() > r.Term)
	if canVote && r.RaftLog.isUpToDate(m.GetIndex(), m.GetLogTerm()) {
		r.Vote = m.GetFrom()
		r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
	} else {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	}
}

func (r *Raft) pollQuorum(m pb.Message) bool {
	// 1.计算是否满足 quorum ; 三种状态 1、win  2、lost  3、pending
	if !m.Reject {
		r.votes[m.GetFrom()] = true
	}

	voteInfo := [2]int{}
	for id := range r.Prs {
		v, voted := r.votes[id]
		if !voted {
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
		return true
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.GetIndex() < r.RaftLog.committed {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	if lastNewIndex, ok := r.RaftLog.maybeAppend(m.GetIndex(), m.GetLogTerm(), m.GetCommit(), m.GetEntries()); ok {
		r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgAppendResponse, Index: lastNewIndex})
	}
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{From: r.id, To: id, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.send(pb.Message{From: r.id, To: m.GetFrom(), MsgType: pb.MessageType_MsgHeartbeatResponse})
}

func (r *Raft) handleMsgHeartbeatResponse(m pb.Message) {

}

func (r *Raft) handleMsgPropose(m pb.Message) {

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
