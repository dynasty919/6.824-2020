package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) resetElectionTimer() {
	t := 300 + rand.Intn(100)
	rf.electionTimer = time.Duration(t) * time.Millisecond
}

func (rf *Raft) applyLog(appliedLog []LogEntry, startIndex int) {
	go func() {
		for _, entry := range appliedLog {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Entry,
				CommandIndex: startIndex,
			}
			startIndex++
		}
	}()
}

func (rf *Raft) receivedEntriesAlreadyExist(args *AppendEntriesArgs) bool {
	index := args.PrevLogIndex + 1
	for _, v := range args.Entries {
		if index < len(rf.log) && rf.log[index].Term == v.Term {
			index++
			continue
		} else {
			return false
		}
	}
	return true
}
