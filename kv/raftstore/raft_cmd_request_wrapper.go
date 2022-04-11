package raftstore

import (
	"encoding/binary"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type RaftCmdRequestWrapper struct {
	key  uint64
	msg *raft_cmdpb.RaftCmdRequest
}

func (r *RaftCmdRequestWrapper) Marshal() ([]byte, error) {
	buf := make([]byte, r.msg.Size()+8)
	binary.BigEndian.PutUint64(buf[:8], r.key)
	if _, err := r.msg.MarshalTo(buf[8:]); err != nil {
		return nil, fmt.Errorf("msg marshal fail:%v", err)
	}
	return buf, nil
}

func (r *RaftCmdRequestWrapper) Unmarshal(buf []byte) error {
	var msg raft_cmdpb.RaftCmdRequest
	if err := msg.Unmarshal(buf[8:]); err != nil {
		return fmt.Errorf("msg unmarshal err:%v", err)
	}
	r.msg = &msg
	r.key = binary.BigEndian.Uint64(buf[:8])
	return nil
}

func (r RaftCmdRequestWrapper) GetKey() uint64 {
	return r.key
}

func (r RaftCmdRequestWrapper) GetMsg() *raft_cmdpb.RaftCmdRequest {
	return r.msg
}

