package raftstore

import (
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
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

type MessageQueue struct {
	left          []*rspb.RaftMessage
	right         []*rspb.RaftMessage
	cycle         uint64
	size          uint64
	lazyFreeCycle uint64
	idx           uint64
	oldIdx        uint64
	mu            sync.Mutex
	stopped       bool
	leftInWrite   bool
}

// NewMessageQueue creates a new MessageQueue instance.
func NewMessageQueue(size uint64, lazyFreeCycle uint64) *MessageQueue {
	q := &MessageQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]*rspb.RaftMessage, size),
		right:         make([]*rspb.RaftMessage, size),
	}
	return q
}

// Close closes the queue so no further messages can be added.
func (q *MessageQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *MessageQueue) targetQueue() []*rspb.RaftMessage {
	var t []*rspb.RaftMessage
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

// Add adds the specified message to the queue.
func (q *MessageQueue) Add(msg *rspb.RaftMessage) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.idx >= q.size {
		return false, q.stopped
	}
	if q.stopped {
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = msg
	q.idx++
	return true, false
}

func (q *MessageQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i] = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i] = nil
			}
		}
	}
}

// Get returns everything current in the queue.
func (q *MessageQueue) Get() []*rspb.RaftMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	q.gc()
	q.oldIdx = sz
	return t[:sz]
}