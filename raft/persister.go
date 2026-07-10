package raft

import (
	"os"
	"sync"
	"time"
)

type Persister struct {
	mu        sync.RWMutex
	raftState []byte
	filename  string
	saveCh    chan []byte
}

func MakePersister(filename string) *Persister {
	ps := &Persister{
		filename: filename,
		saveCh:   make(chan []byte, 1000),
	}
	go ps.batchFlusher()
	return ps
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	ps.raftState = state
	ps.mu.Unlock()

	select {
	case ps.saveCh <- state:
	default:
	}
}

func (ps *Persister) batchFlusher() {
	for {
		state := <-ps.saveCh
		draining := true
		for draining {
			select {
			case state = <-ps.saveCh:
			default:
				draining = false
			}
		}

		f, err := os.OpenFile(ps.filename, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
		if err == nil {
			f.Write(state)
			f.Sync()
			f.Close()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	data, err := os.ReadFile(ps.filename)
	if err != nil {
		return nil
	}
	return data
}