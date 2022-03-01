package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"math"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	put := storage.Put{
		Key: EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf: engine_util.CfWrite,
	}
	txn.writes = append(txn.writes, storage.Modify{put})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	v, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, &KeyError{}
	}
	return ParseLock(v)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	put := storage.Put{
		Key: key,
		Value: lock.ToBytes(),
		Cf: engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{put})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	del := storage.Delete{
		Key: key,
		Cf: engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{del})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	it := txn.Reader.IterCF(engine_util.CfWrite)
	defer it.Close()
	// find current write
	it.Seek(EncodeKey(key, txn.StartTS))
	if !it.Valid() {
		return nil, nil
	}
	item := it.Item()
	value, err := item.Value()
	if err != nil {
		log.Fatalf("GetValue err:%v", err)
	}
	write, err := ParseWrite(value)
	if err != nil {
		log.Errorf("parse err:%v", err)
		return nil, nil
	}
	if write.Kind != WriteKindPut {
		log.Debugf("GetValue %x was %v", key, write.Kind)
		return nil, nil
	}
	// find the value by write.StarTS
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	put := storage.Put{
		Key: EncodeKey(key, txn.StartTS),
		Value: value,
		Cf: engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{put})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	del := storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf: engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{del})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	it := txn.Reader.IterCF(engine_util.CfWrite)
	defer it.Close()
	it.Seek(EncodeKey(key, math.MaxUint64))
	encodeKey := EncodeKey(key, txn.StartTS)
	for it.Valid() {
		item := it.Item()
		// 排除 commit ts 比 start ts 小的 change
		if bytes.Compare(item.Key(), encodeKey) > 0 {
			return nil, 0, nil
		}
		value, err := item.Value()
		if err != nil {
			log.Warnf("CurrentWrite get value err:%v", err)
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			log.Warnf("CurrentWrite parse write err:%v", err)
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(item.Key()), nil
		}
		it.Next()
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// 查找当前 key 的最近一次写入
	it := txn.Reader.IterCF(engine_util.CfWrite)
	defer it.Close()
	// 降序
	it.Seek(key)
	if !it.Valid() {
		return nil, 0, nil
	}
	writeKey := DecodeUserKey(it.Item().Key())
	if !bytes.Equal(key, writeKey) {
		return nil, 0, nil
	}
	value, err := it.Item().Value()
	if err != nil {
		log.Warnf("MostRecentWrite get value err:%v", err)
		return nil, 0, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		log.Warnf("MostRecentWrite parse write err:%v", err)
		return nil, 0, err
	}
	return write, decodeTimestamp(it.Item().Key()), nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
