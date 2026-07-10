package raft

// LogEntry represents a single command in the Raft WAL
type LogEntry struct {
	Term    int
	Command interface{}
}

// RequestVoteArgs / Reply
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs / Reply (Used for both heartbeats and log replication)
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}