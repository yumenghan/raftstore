package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type standaloneReader struct {
	engine *engine_util.Engines
	txn    *badger.Txn
}

func (s standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	s.txn = s.engine.Kv.NewTransaction(false)
	res, err := engine_util.GetCF(s.engine.Kv, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("engine GetCF cf:%s key:%s err:%v", cf, key, err)
	}
	return res, nil
}

func (s standaloneReader) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.engine.Kv.NewTransaction(false)
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s standaloneReader) Close() {
	//panic("implement me")
	s.txn.Discard()
}
