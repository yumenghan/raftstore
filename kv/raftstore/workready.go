package raftstore

import "sync"

type IPartitioner interface {
	GetPartitionID(clusterID uint64) uint64
}

// FixedPartitioner is the IPartitioner with fixed capacity and naive
// partitioning strategy.
type FixedPartitioner struct {
	capacity uint64
}

// NewFixedPartitioner creates a new FixedPartitioner instance.
func NewFixedPartitioner(capacity uint64) *FixedPartitioner {
	return &FixedPartitioner{capacity: capacity}
}

// GetPartitionID returns the partition ID for the specified raft cluster.
func (p *FixedPartitioner) GetPartitionID(clusterID uint64) uint64 {
	return clusterID % p.capacity
}

type readyCluster struct {
	mu    sync.Mutex
	ready map[uint64]struct{}
	maps  [2]map[uint64]struct{}
	index uint8
}

func newReadyCluster() *readyCluster {
	r := &readyCluster{}
	r.maps[0] = make(map[uint64]struct{})
	r.maps[1] = make(map[uint64]struct{})
	r.ready = r.maps[0]
	return r
}

func (r *readyCluster) setClusterReady(clusterID uint64) {
	r.mu.Lock()
	r.ready[clusterID] = struct{}{}
	r.mu.Unlock()
}

func (r *readyCluster) getReadyClusters() map[uint64]struct{} {
	m := r.maps[(r.index+1)%2]
	for k := range m {
		delete(m, k)
	}
	r.mu.Lock()
	v := r.ready
	r.index++
	r.ready = r.maps[r.index%2]
	r.mu.Unlock()
	return v
}

type workReady struct {
	partitioner IPartitioner
	maps        []*readyCluster
	channels    []chan struct{}
	count       uint64
}

func newWorkReady(count uint64) *workReady {
	wr := &workReady{
		partitioner: NewFixedPartitioner(count),
		count:       count,
		maps:        make([]*readyCluster, count),
		channels:    make([]chan struct{}, count),
	}
	for i := uint64(0); i < count; i++ {
		wr.channels[i] = make(chan struct{}, 1)
		wr.maps[i] = newReadyCluster()
	}
	return wr
}

func (wr *workReady) getPartitioner() IPartitioner {
	return wr.partitioner
}

func (wr *workReady) notify(idx uint64) {
	select {
	case wr.channels[idx] <- struct{}{}:
	default:
	}
}

//
//func (wr *workReady) clusterReadyByUpdates(updates []pb.Update) {
//	var notified bitmap
//	for _, ud := range updates {
//		if len(ud.CommittedEntries) > 0 {
//			idx := wr.partitioner.GetPartitionID(ud.ClusterID)
//			readyMap := wr.maps[idx]
//			readyMap.setClusterReady(ud.ClusterID)
//		}
//	}
//	for _, ud := range updates {
//		if len(ud.CommittedEntries) > 0 {
//			idx := wr.partitioner.GetPartitionID(ud.ClusterID)
//			if !notified.contains(idx) {
//				notified.add(idx)
//				wr.notify(idx)
//			}
//		}
//	}
//}

//func (wr *workReady) clusterReadyByMessageBatch(mb pb.MessageBatch) {
//	var notified bitmap
//	for _, req := range mb.Requests {
//		idx := wr.partitioner.GetPartitionID(req.ClusterId)
//		readyMap := wr.maps[idx]
//		readyMap.setClusterReady(req.ClusterId)
//	}
//	for _, req := range mb.Requests {
//		idx := wr.partitioner.GetPartitionID(req.ClusterId)
//		if !notified.contains(idx) {
//			notified.add(idx)
//			wr.notify(idx)
//		}
//	}
//}
//
//func (wr *workReady) allClustersReady(nodes []*node) {
//	var notified bitmap
//	for _, n := range nodes {
//		idx := wr.partitioner.GetPartitionID(n.clusterID)
//		readyMap := wr.maps[idx]
//		readyMap.setClusterReady(n.clusterID)
//	}
//	for _, n := range nodes {
//		idx := wr.partitioner.GetPartitionID(n.clusterID)
//		if !notified.contains(idx) {
//			notified.add(idx)
//			wr.notify(idx)
//		}
//	}
//}

func (wr *workReady) clusterReady(clusterID uint64) {
	idx := wr.partitioner.GetPartitionID(clusterID)
	readyMap := wr.maps[idx]
	readyMap.setClusterReady(clusterID)
	wr.notify(idx)
}

func (wr *workReady) waitCh(workerID uint64) chan struct{} {
	return wr.channels[workerID-1]
}

func (wr *workReady) getReadyMap(workerID uint64) map[uint64]struct{} {
	readyMap := wr.maps[workerID-1]
	return readyMap.getReadyClusters()
}
