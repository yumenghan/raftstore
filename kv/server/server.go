package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	var resp kvrpcpb.GetResponse
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Errorf("KvGet err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		log.Errorf("KvGet err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	// check snapshot-read （
	if lock.IsLockedFor(req.GetKey(), txn.StartTS, &resp){
		// locked: client retry
		return &resp, nil
	}
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		log.Errorf("KvGet err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	if len(value) == 0 {
		resp.NotFound = true
		return &resp, nil
	}
	resp.Value = value
	return &resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	var resp kvrpcpb.PrewriteResponse
	if len(req.GetMutations()) == 0 {
		return &resp, nil
	}
	// 保证 read - write 原子性
	server.Latches.WaitForLatches(getKeys(req.GetMutations()))
	defer server.Latches.ReleaseLatches(getKeys(req.GetMutations()))

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvPrewrite err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	primaryKey := &mvcc.Lock{
		Primary: req.GetPrimaryLock(),
		Ts: req.GetStartVersion(),
		Ttl: req.GetLockTtl(),
	}
	for _, m := range req.GetMutations() {
		// 1.check lock
		lock, err := txn.GetLock(m.GetKey())
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return &resp, nil
		}
		var errorLock mvcc.KeyErrorLock
		if lock.IsLockedFor(m.GetKey(), req.GetStartVersion(), &errorLock) {
			resp.Errors = append(resp.Errors, errorLock.Error)
			continue
		}
		// 2.check write-write conflict
		write, commitTime, err := txn.MostRecentWrite(m.GetKey())
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return &resp, nil
		}
		if write != nil && write.StartTS <= txn.StartTS && txn.StartTS <= commitTime {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs: write.StartTS,
				ConflictTs: commitTime,
				Key: m.GetKey(),
				Primary: req.GetPrimaryLock(),
			}})
		}

		// 1.write lock
		// 2.write data
		primaryKey.Kind = mvcc.WriteKindFromProto(m.GetOp())
		txn.PutLock(m.GetKey(), primaryKey)
		switch m.GetOp() {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.GetKey(), m.GetValue())
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.GetKey())
		}
	}
	if len(resp.Errors) > 0 {
		return &resp, nil
	}
	// flush
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvPrewrite err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	return &resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {

	var resp kvrpcpb.CommitResponse

	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetCommitVersion())

	// check locked or locked by other transaction
	for _, k := range req.GetKeys() {
		if err := commit(txn, req.GetStartVersion(), req.GetCommitVersion(), k); err != nil {
			resp.Error = err
			return &resp, nil
		}
	}

	if err := server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		// roll back todo
		log.Errorf("KvCommit error:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}

	return &resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	var resp kvrpcpb.ScanResponse
	if req.GetLimit() == 0 {
		return &resp, nil
	}
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvScan err:%v", err)
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}

	txn := mvcc.NewMvccTxn(reader, mvcc.TsMax)
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	if scanner == nil {
		return &resp, nil
	}
	defer scanner.Close()
	for i := uint32(0); i < req.GetLimit(); i++ {
		k, v, err := scanner.Next()
		if err != nil {
			log.Warnf("Kvscan scanner next err:%v", err)
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key: k,
			Value: v,
		})
	}
	return &resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	var resp kvrpcpb.CheckTxnStatusResponse
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvCheckTxnStatus err:%v")
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	// lockTs ?
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	write, commitTs, err := txn.GetWrite(req.GetPrimaryKey())
	if err != nil {
		log.Errorf("KvCheckTxnStatus err:%v")
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	if write != nil {
		resp.Action = kvrpcpb.Action_NoAction
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTs
		}
		return &resp, nil
	}
	// check lock
	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		log.Errorf("KvCheckTxnStatus err:%v")
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	//
	var modify []storage.Modify
	if lock != nil {
		resp.LockTtl = lock.Ttl
		if lock.IsExpired(req.GetCurrentTs()) {
			// expired lock del roll back
			txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{Kind: mvcc.WriteKindRollback, StartTS: lock.Ts})
			txn.DeleteLock(req.GetPrimaryKey())
			modify = txn.Writes()

			mvccTxn := mvcc.NewMvccTxn(reader, lock.Ts)
			mvccTxn.DeleteValue(req.GetPrimaryKey())
			modify = append(modify, mvccTxn.Writes()...)

			resp.Action = kvrpcpb.Action_TTLExpireRollback
			resp.CommitVersion = req.GetCurrentTs()
		} else {
			resp.Action = kvrpcpb.Action_NoAction
		}
	} else {
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			req.GetLockTs(),
			mvcc.WriteKindRollback,
		})
		modify = txn.Writes()
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}
	err = server.storage.Write(req.GetContext(), modify)
	if err != nil {
		log.Errorf("KvCheckTxnStatus error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		//server.rollBack()
		return &resp, nil
	}
	return &resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	var resp kvrpcpb.BatchRollbackResponse
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvBatchRollback err:%v")
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	for _, k := range req.GetKeys() {
		if err := rollback(txn, req.GetStartVersion(), k); err != nil {
			resp.Error = err
			return &resp, nil
		}
	}

	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvBatchRollback error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		//server.rollBack()
		return &resp, nil
	}
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	var resp kvrpcpb.ResolveLockResponse
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvResolveLock err:%v")
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		log.Warnf("KvResolveLock.AllLocks %s", err.Error())
		resp.Error = &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
		return &resp, nil
	}

	if req.GetCommitVersion() == 0 {
		for _, lock := range locks {
			if err := rollback(txn, req.GetStartVersion(), lock.Key) ; err != nil {
				resp.Error = err
				return &resp, nil
			}
		}
	} else {
		for _, lock := range locks {
			if err := commit(txn, req.GetStartVersion(), req.GetCommitVersion(), lock.Key) ; err != nil {
				resp.Error = err
				return &resp, nil
			}
		}
	}


	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvResolveLock error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		//server.rollBack()
		return &resp, nil
	}
	return nil, nil
}

func commit(txn *mvcc.MvccTxn, startTs uint64, commitTs uint64, k []byte) *kvrpcpb.KeyError {

	write, commitTimeStamp, err := txn.MostRecentWrite(k)
	if err != nil {
		log.Errorf("KvCommit err:%v", err)
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
	if write != nil {
		if write.StartTS == startTs && commitTimeStamp == commitTs {
			// 重复的 commit
			return &kvrpcpb.KeyError{
				Abort: err.Error(),
			}
		}
		if commitTs < commitTimeStamp {
			log.Fatalf("KvCommit has find newest commit:%+v old:%d", write, commitTimeStamp)
		}
		// overwrite
	}
	lock, err := txn.GetLock(k)
	if err != nil {
		log.Errorf("kvCommit err:%v", err)
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
	if lock == nil {
		// lock maybe clear
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}

	if startTs != lock.Ts {
		return &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
			StartTs:    lock.Ts,
			ConflictTs: startTs,
			Key:        k,
			Primary:    lock.Primary,
		}}

	}

	// put write and del lock
	txn.PutWrite(k, commitTs, &mvcc.Write{StartTS: startTs, Kind: lock.Kind})
	txn.DeleteLock(k)
	return nil
}

func rollback(txn *mvcc.MvccTxn, startTs uint64, k []byte) *kvrpcpb.KeyError {
	write, _, err := txn.CurrentWrite(k)
	if err != nil {
		log.Errorf("KvBatchRollback err:%v")
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
	// 1.has commit
	if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			// has rollback
			return nil
		}
		// has commit
		return &kvrpcpb.KeyError{
			Abort: fmt.Sprintf("KvBatchRollback.has commit(%v)(0x%x)", write.Kind, k),
		}
	}
	// 2.check lock
	lock, err := txn.GetLock(k)
	if err != nil {
		log.Warnf("KvBatchRollback.GetLock(0x%x) err:%s", k, err.Error())
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
	if lock != nil {
		if lock.Ts == startTs {
			txn.DeleteLock(k)
		}
	}

	txn.DeleteValue(k)
	txn.PutWrite(k, startTs, &mvcc.Write{Kind: mvcc.WriteKindRollback, StartTS: startTs})
	return nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

func getKeys(keys []*kvrpcpb.Mutation) [][]byte {
	res := make([][]byte, len(keys))
	for _, k := range keys {
		res = append(res, k.GetKey())
	}
	return res
}
