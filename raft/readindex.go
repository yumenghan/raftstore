package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type readStatus struct {
	confirmed map[uint64]struct{}
	ctx       pb.ReadIndexCtx
	index     uint64
	from      uint64
}

// readIndex is the struct that implements the ReadIndex protocol described in
// section 6.4 (with the idea in section 6.4.1 excluded) of Diego Ongaro's PhD
// thesis.
type readIndex struct {
	pending map[uint64]*readStatus
	queue   []pb.ReadIndexCtx
}

func newReadIndex() *readIndex {
	return &readIndex{
		pending: make(map[uint64]*readStatus),
		queue:   make([]pb.ReadIndexCtx, 0),
	}
}

func (r *readIndex) addRequest(index uint64,
	ctx pb.ReadIndexCtx, from uint64) {
	if _, ok := r.pending[ctx.Id]; ok {
		return
	}
	// index is the committed value of the cluster, it should never move
	// backward, check it here
	if len(r.queue) > 0 {
		p, ok := r.pending[r.peepCtx().Id]
		if !ok {
			panic("inconsistent pending and queue")
		}
		if index < p.index {
			log.Panicf("index moved backward in readIndex, %d:%d",
				index, p.index)
		}
	}
	r.queue = append(r.queue, ctx)
	r.pending[ctx.GetId()] = &readStatus{
		index:     index,
		from:      from,
		ctx:       ctx,
		confirmed: make(map[uint64]struct{}),
	}
}

func (r *readIndex) hasPendingRequest() bool {
	return len(r.queue) > 0
}

func (r *readIndex) peepCtx() pb.ReadIndexCtx {
	return r.queue[len(r.queue)-1]
}

func (r *readIndex) confirm(ctx pb.ReadIndexCtx,
	from uint64, quorum int) []*readStatus {
	p, ok := r.pending[ctx.GetId()]
	if !ok {
		return nil
	}
	p.confirmed[from] = struct{}{}
	if len(p.confirmed) < quorum {
		return nil
	}
	done := 0
	cs := []*readStatus{}
	for _, pctx := range r.queue {
		done++
		s, ok := r.pending[pctx.Id]
		if !ok {
			panic("inconsistent pending and queue content")
		}
		cs = append(cs, s)
		if pctx.Id == ctx.Id {
			for _, v := range cs {
				if v.index > s.index {
					panic("v.index > s.index is unexpected")
				}
				// re-write the index for extra safety.
				// we don't know what we don't know.
				v.index = s.index
			}
			r.queue = r.queue[done:]
			for _, v := range cs {
				delete(r.pending, v.ctx.Id)
			}
			if len(r.queue) != len(r.pending) {
				panic("inconsistent length")
			}
			return cs
		}
	}
	return nil
}
