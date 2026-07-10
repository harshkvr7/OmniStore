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
	mu          sync.RWMutex
	peers       []string
	me          int
	persister   *Persister
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	state       string
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
		log:         make([]LogEntry, 1), // 1-indexed
		applyCh:     applyCh,
		lastContact: time.Now(),
	}
	go rf.ticker()
	return rf
}

func (rf *Raft) persist() {
	dummyState := []byte("serialized_raft_state") // Use gob encoding in production
	rf.persister.SaveRaftState(dummyState)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()

	go rf.broadcastHeartbeats()
	return index, term, true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	logUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastContact = time.Now()
		rf.persist()
	}
	return nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return nil
	}

	rf.lastContact = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persist()
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		go rf.applyCommitted()
	}
	return nil
}

func (rf *Raft) applyCommitted() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
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
	// RPC to request votes omitted for brevity in skeleton, assuming self-win in isolation for test
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.RLock()
	peers := rf.peers
	me := rf.me
	rf.mu.RUnlock()

	for i, peer := range peers {
		if i == me {
			continue
		}
		go func(address string) {
			client, err := rpc.DialHTTP("tcp", address)
			if err != nil {
				return
			}
			defer client.Close()
			client.Call("Raft.AppendEntries", &AppendEntriesArgs{}, &AppendEntriesReply{})
		}(peer)
	}
}