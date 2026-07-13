package server

import (
	"fmt"
	"kvstore/raft"
	"sync"
	"time"
)

type Op struct {
	OpType   string
	Key      string
	Value    []byte
	Manifest []string
}

type OmniServer struct {
	mu            sync.RWMutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	kvStore       map[string][]byte
	manifestStore map[string][]string
	notifyChans   map[int]chan Op
}

type PutArgs struct {
	Key      string
	Value    []byte
	Manifest []string
	OpType   string
}
type PutReply struct {
	Success bool
	Err     string
}

type DeleteArgs struct{ Key string }
type DeleteReply struct {
	Success bool
	Err     string
}

type UpdateArgs struct {
	Key      string
	Manifest []string
}
type UpdateReply struct {
	Success bool
	Err     string
}

func MakeServer(peers []string, me int, persister *raft.Persister) *OmniServer {
	applyCh := make(chan raft.ApplyMsg)
	srv := &OmniServer{
		me:            me,
		rf:            raft.Make(peers, me, persister, applyCh),
		applyCh:       applyCh,
		kvStore:       make(map[string][]byte),
		manifestStore: make(map[string][]string),
		notifyChans:   make(map[int]chan Op),
	}
	go srv.applier()
	return srv
}

func (srv *OmniServer) getNotifyChan(index int) chan Op {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, ok := srv.notifyChans[index]; !ok {
		srv.notifyChans[index] = make(chan Op, 1)
	}
	return srv.notifyChans[index]
}

func (srv *OmniServer) handleOp(op Op) (bool, string) {
	index, _, isLeader := srv.rf.Start(op)
	if !isLeader {
		return false, "Not Leader"
	}

	notifyCh := srv.getNotifyChan(index)
	select {
	case committedOp := <-notifyCh:
		if committedOp.Key == op.Key {
			return true, ""
		}
		return false, "Leadership changed, command overwritten"
	case <-time.After(2 * time.Second):
		return false, "Raft Consensus Timeout"
	}
}

func (srv *OmniServer) Put(args *PutArgs, reply *PutReply) error {
	reply.Success, reply.Err = srv.handleOp(Op{OpType: args.OpType, Key: args.Key, Value: args.Value, Manifest: args.Manifest})
	return nil
}

func (srv *OmniServer) Delete(args *DeleteArgs, reply *DeleteReply) error {
	reply.Success, reply.Err = srv.handleOp(Op{OpType: "DeleteObject", Key: args.Key})
	return nil
}

func (srv *OmniServer) Update(args *UpdateArgs, reply *UpdateReply) error {
	reply.Success, reply.Err = srv.handleOp(Op{OpType: "UpdateManifest", Key: args.Key, Manifest: args.Manifest})
	return nil
}

func (srv *OmniServer) applier() {
	for msg := range srv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			srv.mu.Lock()
			switch op.OpType {
			case "PutChunk":
				srv.kvStore[op.Key] = op.Value
			case "PutManifest":
				srv.manifestStore[op.Key] = op.Manifest
			case "DeleteObject":
				if hashes, exists := srv.manifestStore[op.Key]; exists {
					for _, hash := range hashes {
						delete(srv.kvStore, hash)
					}
					delete(srv.manifestStore, op.Key)
					fmt.Printf("[Node %d] Deleted Object & GC Chunks: %s\n", srv.me, op.Key)
				}
			case "UpdateManifest":
				srv.manifestStore[op.Key] = op.Manifest
				fmt.Printf("[Node %d] Updated Object Manifest: %s\n", srv.me, op.Key)
			}
			srv.mu.Unlock()

			srv.mu.Lock()
			if ch, ok := srv.notifyChans[msg.CommandIndex]; ok {
				ch <- op
			}
			srv.mu.Unlock()
		}
	}
}
