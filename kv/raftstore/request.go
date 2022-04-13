package raftstore

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"math/rand"
	"sync"
	"sync/atomic"
)

type logicalClock struct {
	ltick      uint64
	lastGcTime uint64
	gcTick     uint64
}

func newLogicalClock() logicalClock {
	return logicalClock{gcTick: 2}
}

func (p *logicalClock) tick(tick uint64) {
	atomic.StoreUint64(&p.ltick, tick)
}

func (p *logicalClock) getTick() uint64 {
	return atomic.LoadUint64(&p.ltick)
}

type RequestState struct {
	key      uint64
	cb       *message.Callback
	deadline uint64
}

func (r *RequestState) timeout() {
	r.notify(ErrRequestTimeout())
}

func (r *RequestState) terminated() {
	r.notify(ErrRequestTerminated())
}

func (r *RequestState) dropped(err error) {
	r.notify(ErrResp(err))
}

func (r *RequestState) notify(result *raft_cmdpb.RaftCmdResponse) {
	if r.cb != nil {
		r.cb.Done(result)
	}
}

func (r *RequestState) Clear() {
	r.cb = nil
	r.deadline = 0
	r.key = 0
}

type keyGenerator struct {
	randMu sync.Mutex
	rand   *rand.Rand
}

func (k *keyGenerator) nextKey() uint64 {
	k.randMu.Lock()
	v := k.rand.Uint64()
	k.randMu.Unlock()
	return v
}

func newKeyGenerator() *keyGenerator {
	m := sha512.New()
	sum := m.Sum(nil)
	seed := binary.LittleEndian.Uint64(sum)
	return &keyGenerator{rand: rand.New(rand.NewSource(int64(seed)))}
}

type proposalShard struct {
	mu        sync.Mutex
	proposals *entryQueue
	pending   map[uint64]*RequestState
	pool      *sync.Pool
	stopped   bool
	storeId   uint64
	peerId    uint64
	logicalClock
}

func newPendingProposalShard(storeId uint64, peerId uint64, pool *sync.Pool, proposals *entryQueue) *proposalShard {
	return &proposalShard{
		storeId:      storeId,
		peerId:       peerId,
		pool:         pool,
		proposals:    proposals,
		pending:      make(map[uint64]*RequestState),
		logicalClock: newLogicalClock(),
	}
}

func (p *proposalShard) propose(key uint64, cmd *message.MsgRaftCmd, timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, fmt.Errorf("errTimeoutIsEmpty")
	}

	entry := RaftCmdRequestWrapper{
		msg: cmd.Request,
		key: key,
	}

	reqState := p.pool.Get().(*RequestState)
	reqState.key = entry.key
	reqState.deadline = timeoutTick
	reqState.cb = cmd.Callback

	p.mu.Lock()
	p.pending[entry.key] = reqState
	p.mu.Unlock()

	added, stopped := p.proposals.add(entry)
	if stopped {
		log.Warningf("store[%d] peer[%d] dropped proposal, cluster stopped",
			p.storeId, p.peerId)
		p.mu.Lock()
		delete(p.pending, entry.key)
		p.mu.Unlock()
		return nil, fmt.Errorf("regionIsClose")
	}
	if !added {
		p.mu.Lock()
		delete(p.pending, entry.key)
		p.mu.Unlock()
		log.Debugf("store[%d] peer[%d] dropped proposal, overloaded",
			p.storeId, p.peerId)
		return nil, fmt.Errorf("systemIsBusy")
	}
	return reqState, nil
}

func (p *proposalShard) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.proposals != nil {
		p.proposals.close()
	}
	for _, rec := range p.pending {
		rec.terminated()
	}
}

func (p *proposalShard) takeProposal(key uint64, now uint64, remove bool) *RequestState {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil
	}
	ps, ok := p.pending[key]
	if ok && ps.deadline >= now {
		if remove {
			delete(p.pending, key)
		}
		return ps
	}
	return nil
}

func (p *proposalShard) tick(tick uint64) {
	p.tick(tick)
}

func (p *proposalShard) dropped(key uint64, err error) {
	if ps := p.takeProposal(key, p.getTick(), true); ps != nil {
		ps.dropped(err)
	}
}

func (p *proposalShard) applied(key uint64, result *raft_cmdpb.RaftCmdResponse) {
	now := p.getTick()
	if ps := p.takeProposal(key, now, true); ps != nil {
		ps.notify(result)
	}
}

func (p *proposalShard) gc() {
	now := p.getTick()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if now-p.lastGcTime < p.gcTick {
		return
	}
	p.lastGcTime = now
	for key, rec := range p.pending {
		if rec.deadline < now {
			rec.timeout()
			delete(p.pending, key)
		}
	}
}

type pendingProposal struct {
	shards []*proposalShard
	keyg   []*keyGenerator
	// use index to choose generator
	keygIndex uint64
	ps        uint64
}

func newPendingProposal(storeId, peerId uint64, pendingProposalShards uint64, pool *sync.Pool, proposals *entryQueue) pendingProposal {
	ps := pendingProposalShards
	p := pendingProposal{
		shards: make([]*proposalShard, ps),
		keyg:   make([]*keyGenerator, ps),
		ps:     ps,
	}
	for i := uint64(0); i < ps; i++ {
		p.shards[i] = newPendingProposalShard(storeId, peerId, pool, proposals)
		p.keyg[i] = newKeyGenerator()
	}
	return p
}

func (p *pendingProposal) propose(cmd *message.MsgRaftCmd, timeoutTick uint64) (*RequestState, error) {
	key := p.nextKey()
	pp := p.shards[key%p.ps]
	return pp.propose(key, cmd, timeoutTick)
}

func (p *pendingProposal) close() {
	for _, pp := range p.shards {
		pp.close()
	}
}

func (p *pendingProposal) dropped(key uint64, err error) {
	pp := p.shards[key%p.ps]
	pp.dropped(key, err)
}

func (p *pendingProposal) applied(key uint64, result *raft_cmdpb.RaftCmdResponse) {
	pp := p.shards[key%p.ps]
	pp.applied(key, result)
}

func (p *pendingProposal) nextKey() uint64 {
	idx := p.keygIndex % p.ps
	p.keygIndex++
	return p.keyg[idx].nextKey()
}

func (p *pendingProposal) tick(tick uint64) {
	for i := uint64(0); i < p.ps; i++ {
		p.shards[i].tick(tick)
	}
}

func (p *pendingProposal) gc() {
	for i := uint64(0); i < p.ps; i++ {
		pp := p.shards[i]
		pp.gc()
	}
}

type configChangeRequest struct {
	data *raft_cmdpb.RaftCmdRequest
	key  uint64
}

type pendingConfigChange struct {
	keyG        uint64
	mu          sync.Mutex
	pending     *RequestState
	confChangeC chan<- configChangeRequest
	logicalClock
}

func newPendingConfigChange(confChangeC chan<- configChangeRequest) pendingConfigChange {
	return pendingConfigChange{
		confChangeC:  confChangeC,
		logicalClock: newLogicalClock(),
	}
}

func (p *pendingConfigChange) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.confChangeC != nil {
		if p.pending != nil {
			p.pending.terminated()
			p.pending = nil
		}
		close(p.confChangeC)
		p.confChangeC = nil
	}
}

func (p *pendingConfigChange) propose(cmd *message.MsgRaftCmd,
	timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, fmt.Errorf("timeOutEmpty")
	}
	if cmd.Request.GetAdminRequest() == nil {
		return nil, fmt.Errorf("adminRequestIsEmpty")
	}
	if cmd.Request.GetAdminRequest().GetCmdType() != raft_cmdpb.AdminCmdType_ChangePeer {
		return nil, fmt.Errorf("cmdTypeNotEqualChangePeer")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending != nil {
		return nil, fmt.Errorf("confchangePending")
	}
	if p.confChangeC == nil {
		return nil, fmt.Errorf("confChangeChannelClose")
	}
	ccreq := configChangeRequest{
		key:  p.keyG,
		data: cmd.Request,
	}
	p.keyG++
	req := &RequestState{
		key:      ccreq.key,
		deadline: p.getTick() + timeoutTick,
		cb:       cmd.Callback,
	}
	select {
	case p.confChangeC <- ccreq:
		p.pending = req
		return req, nil
	default:
	}
	return nil, fmt.Errorf("systemIsBusy")
}

func (p *pendingConfigChange) gc() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	now := p.getTick()
	if now-p.lastGcTime < p.gcTick {
		return
	}
	p.lastGcTime = now
	if p.pending.deadline < now {
		p.pending.timeout()
		p.pending = nil
	}
}

func (p *pendingConfigChange) dropped(key uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		p.pending.dropped(err)
		p.pending = nil
	}
}

func (p *pendingConfigChange) apply(key uint64, result *raft_cmdpb.RaftCmdResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return
	}
	if p.pending.key == key {
		p.pending.notify(result)
		p.pending = nil
	}
}

type pendingLeaderTransfer struct {
	mu              sync.Mutex
	pending         *RequestState
	leaderTransferC chan *raft_cmdpb.RaftCmdRequest
}

func newPendingLeaderTransfer() pendingLeaderTransfer {
	return pendingLeaderTransfer{
		leaderTransferC: make(chan *raft_cmdpb.RaftCmdRequest, 1),
	}
}

func (p *pendingLeaderTransfer) propose(cmd *message.MsgRaftCmd) error {
	if cmd.Request.AdminRequest == nil {
		return fmt.Errorf("adminRequestIsEmpty")
	}
	if cmd.Request.GetAdminRequest().GetCmdType() != raft_cmdpb.AdminCmdType_TransferLeader {
		return fmt.Errorf("cmdTypeNotEqualTransferLeader")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = &RequestState{
		key: 0,
		cb:  cmd.Callback,
	}
	select {
	case p.leaderTransferC <- cmd.Request:
	default:
		return fmt.Errorf("systemIsBusy")
	}
	return nil
}

func (p *pendingLeaderTransfer) get() (*raft_cmdpb.RaftCmdRequest, bool) {
	select {
	case v := <-p.leaderTransferC:
		return v, true
	default:
	}
	return nil, false
}
func (p *pendingLeaderTransfer) dropped(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending.dropped(err)
	p.pending = nil
}

func (p *pendingLeaderTransfer) apply(result *raft_cmdpb.RaftCmdResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending.notify(result)
	p.pending = nil
}

type ReadIndexRequest struct {
	request      *raft_cmdpb.RaftCmdRequest
	requestState *RequestState
}

type readBatch struct {
	index    uint64
	requests []*ReadIndexRequest
}

type ReadIndexCtx struct {
	id       uint64
	deadline uint64
}

type ReadyToRead struct {
	Index uint64
	ctx   ReadIndexCtx
}

type pendingReadIndex struct {
	mu sync.Mutex
	// used to apply
	batches  map[ReadIndexCtx]readBatch
	requests *readIndexQueue
	stopped  bool
	pool     *sync.Pool
	logicalClock
	keyG uint64
}

func newPendingReadIndex(pool *sync.Pool, r *readIndexQueue) pendingReadIndex {
	return pendingReadIndex{
		batches:      make(map[ReadIndexCtx]readBatch),
		requests:     r,
		logicalClock: newLogicalClock(),
		pool:         pool,
	}
}

func (p *pendingReadIndex) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.requests != nil {
		p.requests.close()
		reqs := p.requests.get()
		for _, rec := range reqs {
			rec.requestState.terminated()
		}
	}
	for _, rb := range p.batches {
		for _, req := range rb.requests {
			if req.requestState != nil {
				req.requestState.terminated()
			}
		}
	}
}

func (p *pendingReadIndex) propose(cmd *message.MsgRaftCmd, timeoutTick uint64) (*RequestState, error) {
	if timeoutTick == 0 {
		return nil, fmt.Errorf("timeOutIsEmpty")
	}

	req := p.pool.Get().(*RequestState)
	req.deadline = p.getTick() + timeoutTick
	req.cb = cmd.Callback

	entry := &ReadIndexRequest{
		request:      cmd.Request,
		requestState: req,
	}

	ok, closed := p.requests.add(entry)
	if closed {
		return nil, fmt.Errorf("peerIsClose")
	}
	if !ok {
		return nil, fmt.Errorf("systemIsBusy")
	}
	return req, nil
}

func (p *pendingReadIndex) genCtx() ReadIndexCtx {
	p.keyG++
	return ReadIndexCtx{
		id:       p.keyG,
		deadline: p.getTick() + 30,
	}
}

func (p *pendingReadIndex) nextCtx() ReadIndexCtx {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.genCtx()
}

func (p *pendingReadIndex) addReady(reads []ReadyToRead) {
	if len(reads) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range reads {
		if rb, ok := p.batches[v.ctx]; ok {
			rb.index = v.Index
			p.batches[v.ctx] = rb
		}
	}
}

func (p *pendingReadIndex) add(ctx ReadIndexCtx, reqs []*ReadIndexRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if _, ok := p.batches[ctx]; ok {
		log.Panicf("same system ctx added again %v", ctx)
	} else {
		rs := make([]*ReadIndexRequest, len(reqs))
		copy(rs, reqs)
		p.batches[ctx] = readBatch{
			requests: rs,
		}
	}
}

func (p *pendingReadIndex) dropped(system ReadIndexCtx, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if rb, ok := p.batches[system]; ok {
		for _, req := range rb.requests {
			if req != nil {
				req.requestState.dropped(err)
			}
		}
		delete(p.batches, system)
	}
}

func (p *pendingReadIndex) getApply(applied uint64) []*ReadIndexRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || len(p.batches) == 0 {
		return nil
	}
	now := p.getTick()
	var res []*ReadIndexRequest
	for sys, rb := range p.batches {
		if rb.index > 0 && rb.index <= applied {
			for _, req := range rb.requests {
				if req != nil {
					if req.requestState.deadline > now {
						res = append(res, req)
					} else {
						req.requestState.timeout()
					}
				}
			}
			delete(p.batches, sys)
		}
	}
	return res
}

func (p *pendingReadIndex) applied(reqs []*ReadIndexRequest, resps []*raft_cmdpb.RaftCmdResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || len(p.batches) == 0 {
		return
	}
	now := p.getTick()
	for i, req := range reqs {
		req.requestState.notify(resps[i])
	}
	if now-p.lastGcTime < p.gcTick {
		return
	}
	p.lastGcTime = now
	p.gc(now)
}

func (p *pendingReadIndex) gc(now uint64) {
	if len(p.batches) == 0 {
		return
	}
	for sys, rb := range p.batches {
		for idx, req := range rb.requests {
			if req != nil && req.requestState.deadline < now {
				req.requestState.timeout()
				rb.requests[idx] = nil
				p.batches[sys] = rb
			}
		}
	}
	for sys, rb := range p.batches {
		if sys.deadline < now {
			empty := true
			for _, req := range rb.requests {
				if req != nil {
					empty = false
					break
				}
			}
			if empty {
				delete(p.batches, sys)
			}
		}
	}
}
