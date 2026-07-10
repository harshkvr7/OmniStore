package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

// Persister simulates the WAL and state storage
type Persister struct {
	mu        sync.Mutex
	raftState []byte
	filename  string
}

func MakePersister(filename string) *Persister {
	return &Persister{filename: filename}
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftState = state
	// Simulate flushing WAL to disk
	_ = os.WriteFile(ps.filename, state, 0644)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	data, err := os.ReadFile(ps.filename)
	if err != nil {
		return nil
	}
	return data
}