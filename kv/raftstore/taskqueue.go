package raftstore

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sync"
)

var (
	initialTaskQueueCap = uint64(24)
	taskQueueBusyCap    = uint64(64)
	emptyTask           = Task{}
)

type Task struct {
	Entries      []pb.Entry
	//SSRequest    SSRequest
	ClusterID    uint64
	NodeID       uint64
	Index        uint64
	Save         bool
	Stream       bool
	PeriodicSync bool
	NewNode      bool
	Recover      bool
	Initial      bool
}

// TaskQueue is a queue of tasks to be processed by the state machine.
type TaskQueue struct {
	tasks []Task
	next  uint64
	mu    sync.Mutex
}

// NewTaskQueue creates and returns a new task queue.
func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		tasks: make([]Task, 0, initialTaskQueueCap),
	}
}

// MoreEntryToApply returns a boolean value indicating whether it is ok to
// queue more entries to apply.
func (tq *TaskQueue) MoreEntryToApply() bool {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	return tq.size() < taskQueueBusyCap
}

// Add adds a new task to the queue.
func (tq *TaskQueue) Add(task Task) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	tq.tasks = append(tq.tasks, task)
}

// GetAll returns all tasks currently in the queue.
func (tq *TaskQueue) GetAll() []Task {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	result := tq.tasks
	tq.tasks = make([]Task, 0, initialTaskQueueCap)
	tq.next = 0
	return result
}

// Get returns a task from the queue if there is any.
func (tq *TaskQueue) Get() (Task, bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	if tq.next < uint64(len(tq.tasks)) {
		task := tq.tasks[tq.next]
		tq.tasks[tq.next] = emptyTask
		tq.next++
		tq.resize()
		return task, true
	}
	tq.resize()
	return emptyTask, false
}

// Size returns the number of queued tasks.
func (tq *TaskQueue) Size() uint64 {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	return tq.size()
}

func (tq *TaskQueue) size() uint64 {
	return uint64(len(tq.tasks)) - tq.next
}

func (tq *TaskQueue) resize() {
	if uint64(cap(tq.tasks)) > initialTaskQueueCap*2 {
		if tq.size() < initialTaskQueueCap {
			tasks := make([]Task, tq.size())
			copy(tasks, tq.tasks[tq.next:])
			tq.tasks = tasks
			tq.next = 0
		}
	}
}