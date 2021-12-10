package standalone_storage

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{}
	kvEngine := engine_util.CreateDB(conf.DBPath, false)
	s.engine = engine_util.NewEngines(kvEngine, nil, conf.DBPath, "")
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engine.Kv.Close(); err != nil {
		return fmt.Errorf("storage stop err:%v", err)
	}

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return standaloneReader{engine: s.engine}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := engine_util.WriteBatch{}
	for _, b := range batch {
		writeBatch.SetCF(b.Cf(), b.Key(), b.Value())
	}
	writeBatch.MustWriteToDB(s.engine.Kv)
	return nil
}
