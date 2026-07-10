package server

import (
	"fmt"
	"net/rpc"
	"omnistore/raft"
	"sync"
	"time"
)

// Op represents an operation submitted to the Raft log
type Op struct {
	OpType   string // "PutChunk" or "PutManifest"
	Key      string
	Value    []byte
	Manifest []string
}

type OmniServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// The actual underlying storage Engine
	kvStore       map[string][]byte
	manifestStore map[string][]string
}

// Client RPC Args
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

func MakeServer(peers []string, me int, persister *raft.Persister) *OmniServer {
	applyCh := make(chan raft.ApplyMsg)
	
	srv := &OmniServer{
		me:            me,
		rf:            raft.Make(peers, me, persister, applyCh),
		applyCh:       applyCh,
		kvStore:       make(map[string][]byte),
		manifestStore: make(map[string][]string),
	}

	go srv.applier()
	return srv
}

// Client API Endpoint
func (srv *OmniServer) Put(args *PutArgs, reply *PutReply) error {
	op := Op{
		OpType:   args.OpType,
		Key:      args.Key,
		Value:    args.Value,
		Manifest: args.Manifest,
	}

	// 1. Submit to Raft Leader
	_, _, isLeader := srv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		reply.Err = "Not Leader"
		return nil
	}

	// In a real system, you block here waiting for the command to appear on applyCh
	// via a channel map using the log index.
	time.Sleep(50 * time.Millisecond) // Simulated wait for consensus
	reply.Success = true
	return nil
}

// applier continuously reads committed logs from Raft and applies them to the KV store
func (srv *OmniServer) applier() {
	for msg := range srv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			srv.mu.Lock()
			if op.OpType == "PutChunk" {
				srv.kvStore[op.Key] = op.Value
				fmt.Printf("[Node %d] Applied Chunk: %s\n", srv.me, op.Key)
			} else if op.OpType == "PutManifest" {
				srv.manifestStore[op.Key] = op.Manifest
				fmt.Printf("[Node %d] Applied Manifest: %s\n", srv.me, op.Key)
			}
			srv.mu.Unlock()
		}
	}
}