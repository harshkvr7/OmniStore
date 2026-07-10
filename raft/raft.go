package raft

import (
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.RWMutex
	peers     []string
	me        int
	persister *Persister

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int
	state       string // "follower", "candidate", "leader"

	applyCh     chan ApplyMsg
	lastContact time.Time
}

func Make(peers []string, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		me:          me,
		persister:   persister,
		state:       "follower",
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // 1-indexed for Raft math
		applyCh:     applyCh,
		lastContact: time.Now(),
	}

	go rf.ticker()
	return rf
}

// Start is called by the Server to propose a new command (like PutChunk)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	// In a full implementation, persist WAL here, then trigger append to followers.
	
	// Simulate immediate local commit for this skeleton
	rf.commitIndex = index
	go func() {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index}
	}()

	return index, term, true
}

func (rf *Raft) ticker() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		timeSinceLast := time.Since(rf.lastContact)
		rf.mu.Unlock()

		if state == "leader" {
			rf.broadcastHeartbeats()
			time.Sleep(100 * time.Millisecond)
		} else {
			// Randomized election timeout (150ms - 300ms)
			timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
			if timeSinceLast > timeout {
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastContact = time.Now()
	rf.mu.Unlock()
	// RPC RequestVote logic goes here in a full implementation
}

func (rf *Raft) broadcastHeartbeats() {
	// Async AppendEntries to all peers via net/rpc
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(address string) {
			client, err := rpc.DialHTTP("tcp", address)
			if err != nil { return }
			defer client.Close()
			
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			client.Call("Raft.AppendEntries", &args, &reply)
		}(peer)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastContact = time.Now()
	reply.Success = true
	// Full log matching and replication logic goes here
	return nil
}