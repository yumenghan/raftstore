package raftstore

import (
	"sync"
)

type entryQueue struct {
	size          uint64
	left          []RaftCmdRequestWrapper
	right         []RaftCmdRequestWrapper
	leftInWrite   bool
	stopped       bool
	paused        bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
}

func newEntryQueue(size uint64, lazyFreeCycle uint64) *entryQueue {
	return &entryQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]RaftCmdRequestWrapper, size),
		right:         make([]RaftCmdRequestWrapper, size),
	}
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *entryQueue) targetQueue() []RaftCmdRequestWrapper {
	if q.leftInWrite {
		return q.left
	}
	return q.right
}

func (q *entryQueue) add(ent RaftCmdRequestWrapper) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.paused || q.idx >= q.size {
		return false, q.stopped
	}
	if q.stopped {
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = ent
	q.idx++
	return true, false
}

func (q *entryQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].msg = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].msg = nil
			}
		}
	}
}

func (q *entryQueue) get(paused bool) []RaftCmdRequestWrapper {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.paused = paused
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	q.gc()
	q.oldIdx = sz
	return t[:sz]
}

type readIndexQueue struct {
	size        uint64
	left        []*ReadIndexRequest
	right       []*ReadIndexRequest
	leftInWrite bool
	stopped     bool
	idx         uint64
	mu          sync.Mutex
}

func newReadIndexQueue(size uint64) *readIndexQueue {
	return &readIndexQueue{
		size:  size,
		left:  make([]*ReadIndexRequest, size),
		right: make([]*ReadIndexRequest, size),
	}
}

func (q *readIndexQueue) pendingSize() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.idx
}

func (q *readIndexQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *readIndexQueue) targetQueue() []*ReadIndexRequest {
	if q.leftInWrite {
		return q.left
	}
	return q.right
}

func (q *readIndexQueue) add(rs *ReadIndexRequest) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.idx >= q.size {
		return false, q.stopped
	}
	if q.stopped {
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = rs
	q.idx++
	return true, false
}

func (q *readIndexQueue) get() []*ReadIndexRequest {
	q.mu.Lock()
	defer q.mu.Unlock()
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	return t[:sz]
}