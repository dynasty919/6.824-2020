package raft

import (
	"log"
	"math/rand"
	"sort"
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
	Term                    int
	Success                 bool
	LastLogTerm             int
	FirstIndexOfLastLogTerm int
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

type SendSnapshotArg struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type SendSnapshotReply struct {
	Term int
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) updateLastApplied() {
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(rf.lastApplied).Entry,
			CommandIndex: rf.lastApplied,
			Snapshot:     nil,
		}
		rf.mu.Unlock()
		rf.applyChan <- applyMsg
		rf.mu.Lock()
	}
}

func (rf *Raft) getEntries(peerId int) []LogEntry {
	nextIndex := rf.getPrevLogIndex(peerId) + 1 - rf.lastIncludedIndex
	entries := append([]LogEntry{}, rf.log[nextIndex:]...)
	return entries
}

func (rf *Raft) getPrevLogIndex(peerId int) int {
	return min(rf.nextIndex[peerId]-1, rf.getLogLen()-1)
}

func (rf *Raft) getLogLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogIndex() int {
	return rf.getLogLen() - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) getLogEntry(i int) LogEntry {
	return rf.log[i-rf.lastIncludedIndex]
}

func (rf *Raft) turnFollower(term int, voteFor int) {
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.state = follower
	rf.resetElectionTimer()
	rf.persist()
}

func (rf *Raft) turnLeader(me int, peersNum int) {
	if rf.state != candidate {
		return
	}
	rf.state = leader
	rf.resetElectionTimer()
	rf.nextIndex = make([]int, peersNum)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
	}
	rf.matchIndex = make([]int, peersNum)
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	curTerm := rf.currentTerm
	votedFor := rf.votedFor
	entry := rf.log
	DPrintln("new elected leader ", me, "have term of ", curTerm, " voted for ", votedFor, "has log ", entry)
}

func (rf *Raft) turnCandidate(me int) {
	rf.currentTerm++
	rf.votedFor = me
	rf.resetElectionTimer()
	rf.state = candidate
	rf.persist()
}

func (rf *Raft) chSender(ch chan struct{}) {
	go func() {
		for {
			select {
			case <-ch: //if already set, consume it then resent to avoid block
			default:
				ch <- struct{}{}
				return
			}
		}
	}()
}

func (rf *Raft) SearchFirstIndexOfTerm(term int) int {
	return sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	}) + rf.lastIncludedIndex
}
