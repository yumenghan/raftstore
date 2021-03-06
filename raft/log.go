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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	rLog := &RaftLog{
		storage: storage,
	}
	// ??? storage ???????????? log
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	rLog.committed = firstIndex - 1
	rLog.applied = firstIndex - 1
	rLog.offset = lastIndex + 1
	rLog.stabled = lastIndex
	if lastIndex >= firstIndex {
		entries, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
		rLog.entries = make([]pb.Entry, len(entries))
		copy(rLog.entries, entries)
		rLog.offset = entries[0].GetIndex()
	}
	return rLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	if l.stabled+1 < l.offset {
		return nil
	}

	return l.entries[l.stabled+1-l.offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed <= l.applied {
		return nil
	}
	applied := l.applied + 1
	committed := l.committed + 1

	var entries []pb.Entry
	if applied < l.offset {
		// ??? storage ??? find
		storageEntries, err := l.storage.Entries(applied, l.offset)
		if err == ErrCompacted {
			return nil
		} else if err == ErrUnavailable {
			panic("storage entries err:" + err.Error())
		} else if err != nil {
			panic(err)
		}
		entries = storageEntries
	}
	if committed > l.offset {
		u := max(applied, l.offset)
		unstable := l.entries[u-l.offset : committed-l.offset]
		if len(unstable) > 0 {
			entries = append(entries, unstable...)
		}
	}

	return entries
}

func (l *RaftLog) FirstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// ??? entries
	length := len(l.entries)
	if length != 0 {
		return l.offset + uint64(length) - 1
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.GetMetadata().GetIndex()
	}
	// ??? storage
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 1.?????? unstable
	last := l.LastIndex()
	if i < l.offset {
		if l.pendingSnapshot != nil && l.pendingSnapshot.GetMetadata().GetIndex() == i {
			return l.pendingSnapshot.GetMetadata().GetTerm(), nil
		}
	}
	if i <= last && i >= l.offset && len(l.entries) > 0 {
		return l.entries[i-l.offset].Term, nil
	}
	// 2.??? storage
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}

	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	//
	return 0, nil
}

func (l *RaftLog) isUpToDate(lastIndex uint64, logTerm uint64) bool {
	nowTerm, err := l.Term(l.LastIndex())
	if err != nil {
		return true
	}
	if logTerm > nowTerm {
		return true
	}
	if logTerm == nowTerm && lastIndex >= l.LastIndex() {
		return true
	}
	return false
}

func (l *RaftLog) maybeAppend(index uint64, logTerm uint64, committed uint64, entries []*pb.Entry) (lastNewIndex uint64, ok bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	lastNewIndex = index + uint64(len(entries))
	var notMatchIndex uint64
	for _, entry := range entries {
		if !l.matchTerm(entry.Index, entry.Term) {
			notMatchIndex = entry.Index
			break
		}
	}
	// ????????????????????? entries
	if notMatchIndex > 0 {
		offset := index + 1
		appendEntries := entries[notMatchIndex-offset:]
		l.append(appendEntries)
	}

	if ci := min(lastNewIndex, committed); l.committed < ci {
		l.committed = ci
	}
	return lastNewIndex, true
}

func (l *RaftLog) append(entries []*pb.Entry) uint64 {
	if len(entries) == 0 {
		return l.LastIndex()
	}
	l.truncateAndAppend(entries)
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(entries []*pb.Entry) {
	// after index
	after := entries[0].Index
	switch {
	case after == l.offset+uint64(len(l.entries)):
		l.entries = append(l.entries, l.copyPointerEntries(entries)...)
	case after <= l.offset:
		//u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		l.offset = after
		l.stabled = after - 1
		l.entries = l.copyPointerEntries(entries)
	default:
		// truncate to after and copy to u.entries
		log.Infof("truncate the unstable entries before index %d", after)
		l.entries = append([]pb.Entry{}, l.entries[:after-l.offset]...)
		lastIndex := l.entries[len(l.entries)-1].GetIndex()
		if l.stabled > lastIndex {
			l.stabled = lastIndex
		}
		l.entries = append(l.entries, l.copyPointerEntries(entries)...)
	}
}

func (l *RaftLog) startAt(i uint64) ([]*pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	if i > uint64(len(l.entries))+l.offset {
		return nil, ErrUnavailable
	}

	if i < l.FirstIndex() {
		return nil, ErrCompacted
	}

	if i < l.offset {
		// ???????????? storage ?????? entries
		index, _ := l.storage.LastIndex()
		if l.offset > index+1 {
			return nil, ErrCompacted
		}
		entries, err := l.storage.Entries(i, l.offset)
		if err == ErrCompacted {
			return nil, err
		}
		ents := l.copyEntries(entries)
		ents = append(ents, l.copyEntries(l.entries[:])...)
		return ents, nil
	}

	return l.copyEntries(l.entries[i-l.offset:]), nil
}

func (l *RaftLog) copyPointerEntries(entries []*pb.Entry) []pb.Entry {
	if len(entries) == 0 {
		return []pb.Entry{}
	}
	res := make([]pb.Entry, 0, len(entries))
	for _, entry := range entries {
		res = append(res, *entry)
	}
	return res
}

func (l *RaftLog) copyEntries(entries []pb.Entry) []*pb.Entry {
	if len(entries) == 0 {
		return []*pb.Entry{}
	}
	res := make([]*pb.Entry, 0, len(entries))
	for _, entry := range entries {
		res = append(res, &pb.Entry{
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
			EntryType: entry.EntryType,
		})
	}
	return res
}

func (l *RaftLog) matchTerm(index, LogTerm uint64) bool {
	term, err := l.Term(index)
	if err != nil {
		return false
	}
	return term == LogTerm
}

func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("applied:[%d] committed:[%d] stable:[%d] offset:[%d] entries:[%d]", l.applied, l.committed, l.stabled, l.offset, len(l.entries))
}
