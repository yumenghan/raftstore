package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{NotFound: true, Error: err.Error()}, nil
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	putData := storage.Put{
		req.GetKey(),
		req.GetValue(),
		req.GetCf(),
	}
	if err := server.storage.Write(req.Context, []storage.Modify{{putData}}); err != nil {
		return &kvrpcpb.RawPutResponse{Error: fmt.Sprintf("put :%+v write err %v", *req, err)}, nil
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delData := storage.Delete{
		req.GetKey(),
		req.GetCf(),
	}
	if err := server.storage.Write(req.Context, []storage.Modify{{delData}}); err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: fmt.Sprintf("delete %+v write modify err %v", *req, err)}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: fmt.Sprintf("scan %+v get reader err:%v", *req, err)}, nil
	}
	var kvPairs []*kvrpcpb.KvPair
	iterCF := reader.IterCF(req.GetCf())
	if req.GetStartKey() != nil {
		iterCF.Seek(req.GetStartKey())
	}
	var getNum uint32
	for iterCF.Valid() && getNum < req.GetLimit() {
		item := iterCF.Item()
		val, err := item.Value()
		if err != nil {
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{Abort: fmt.Sprintf("key %v abort err %v", item.Key(), err)},
			})
			getNum++
			iterCF.Next()
			continue
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		getNum++
		iterCF.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
