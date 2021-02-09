package raft

import (
	"6.824/src/labrpc"
	"time"
)

func (rf *Raft) Follower(done chan struct{}, me int, peers []*labrpc.ClientEnd) {

	rf.mu.Lock()
	rf.resetElectionTimer()
	timeout := rf.electionTimer
	rf.state = follower

	curTerm := rf.currentTerm
	votedFor := rf.votedFor
	DPrintln("follower ", me, "have term of ", curTerm, " voted for ", votedFor, "election timeout", timeout)
	rf.mu.Unlock()

	for !rf.killed() {
		select {
		case <-done:
			//			DPrintln("follower ", me, " turned off")
			return
		case <-time.After(timeout):
			DPrintln("follower ", me, " election timeout with", timeout)
			go rf.Candidate(done, me, peers)
			return
		}
	}
}
