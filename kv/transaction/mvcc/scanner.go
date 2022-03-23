package mvcc

import "github.com/pingcap-incubator/tinykv/kv/util/engine_util"

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn *MvccTxn
	startKey []byte
	it engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	it := txn.Reader.IterCF(engine_util.CfWrite)
	it.Seek(EncodeKey(startKey, txn.StartTS))
	if !it.Valid() {
		it.Close()
		return nil
	}
	scan := &Scanner{
		txn: txn,
		startKey: startKey,
		it: it,
	}
	return scan
}

func (scan *Scanner) Close() {
	scan.it.Close()
}

func (scan *Scanner) Valid() bool {
	return scan.it.Valid()
}
// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for scan.it.Valid() {
		item := scan.it.Item()
		key := DecodeUserKey(item.Key())
		timestamp := decodeTimestamp(item.Key())
		if timestamp > scan.txn.StartTS {
			scan.it.Next()
			continue
		}
		value, err := item.Value()
		if err != nil {
			scan.it.Next()
			return nil,nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			scan.it.Next()
			return nil, nil, err
		}
		if write.Kind != WriteKindPut {
			scan.it.Next()
			continue
		}
		val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		if err != nil {
			scan.it.Next()
			return nil, nil, err
		}

		scan.it.Next()
		return key, val, nil
	}
	return nil, nil, nil
}
