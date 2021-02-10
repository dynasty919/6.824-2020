package raft

import (
	"6.824/src/labrpc"
	"time"
)

func (rf *Raft) Candidate(done chan struct{}, me int, peers []*labrpc.ClientEnd) {
	done2 := make(chan struct{})
	defer close(done2)

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = me
	rf.resetElectionTimer()
	rf.state = candidate

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	timeout := rf.electionTimer
	rf.mu.Unlock()

	DPrintln("candidate ", me, " start to hold election , has term ", term)

	cnt := 1
	grantedVote := make(chan struct{})

	for i, peer := range peers {
		if i != me {
			go rf.callRequestVote(args, peer, done2, grantedVote, i, me)
		}
	}

	for !rf.killed() {
		select {
		case <-done:
			DPrintln("candidate ", me, " turned off")
			return
		case <-grantedVote:
			cnt++
			if cnt >= len(peers)/2+1 {
				DPrintln("candidate ", me, " has won election")
				go rf.Leader(done, me, peers)
				return
			}
		case <-time.After(timeout):
			go rf.Candidate(done, me, peers)
			DPrintln("candidate ", me, " election timeout")
			return
		}
	}
}

func (rf *Raft) callRequestVote(args RequestVoteArgs, peer *labrpc.ClientEnd,
	done chan struct{}, grantedVote chan struct{}, peerId int, me int) {
	reply := RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}

	suc := peer.Call("Raft.RequestVote", &args, &reply)

	select {
	case <-done:
		return
	default:
		if !suc {
			return
		}

		rf.mu.Lock()
		state := rf.state
		term := rf.currentTerm
		rf.mu.Unlock()

		if term != args.Term {
			DPrintln("candidate ", me, "'s request vote sent to ", peerId,
				" had old term", args.Term, " but candidate now is ", state, " have term ", term)
			return
		}

		if state != candidate {
			DPrintln("candidate ", me, "'s request vote sent to ", peerId,
				" but candidate now is no longer candidate ,but is", state)
			return
		}

		if reply.Term > term {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = follower
			rf.mu.Unlock()
			rf.newLeaderIncoming <- struct{}{}
			return
		} else {
			if reply.VoteGranted == true {
				grantedVote <- struct{}{}
				return
			} else {
				return
			}
		}
	}
}
