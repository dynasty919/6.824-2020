package raft

import (
	"6.824/src/labrpc"
	"time"
)

func (rf *Raft) Follower(done chan struct{}, me int, peers []*labrpc.ClientEnd) {

	rf.mu.Lock()
	rf.resetElectionTimer()
	t := rf.electionTimer
	rf.state = follower

	curTerm := rf.currentTerm
	votedFor := rf.votedFor
	DPrintln("follower ", me, "have term of ", curTerm, " voted for ", votedFor, "election timeout", t)
	rf.mu.Unlock()

	var gap time.Duration

	for !rf.killed() {
		select {
		case <-done:
			//			DPrintln("follower ", me, " turned off")
			return
		default:
			time.Sleep(time.Millisecond * 10)
			gap += time.Millisecond * 10
			if gap >= t {
				DPrintln("follower ", me, " election timeout with", t)
				go rf.Candidate(done, me, peers)
				return
			}
		}
	}
}
