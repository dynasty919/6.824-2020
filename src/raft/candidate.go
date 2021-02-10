package raft

import (
	"6.824/src/labrpc"
	"time"
)

func (rf *Raft) Candidate(me int, peers []*labrpc.ClientEnd) {
	done := make(chan struct{})
	defer close(done)

	rf.mu.Lock()

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	DPrintln("candidate ", me, " start to hold election , has term ", term)

	cnt := 1

	for peerId, peer := range peers {
		if peerId == me {
			continue
		}
		go func(peerId int, peer *labrpc.ClientEnd) {
			reply := &RequestVoteReply{}
			suc := peer.Call("Raft.RequestVote", &args, &reply)
			if !suc {
				time.Sleep(time.Millisecond * 10)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			currentState := rf.state
			currentTerm := rf.currentTerm

			if currentTerm != args.Term {
				DPrintln("candidate ", me, "'s request vote sent to ", peerId,
					" had old term", args.Term, " but candidate now is ", currentState, " have term ", currentTerm)
				return
			}

			if currentState != candidate {
				DPrintln("candidate ", me, "'s request vote sent to ", peerId,
					" but candidate now is no longer candidate ,but is", currentState)
				return
			}

			if reply.Term > currentTerm {
				rf.turnFollower(reply.Term, -1)
			} else {
				if reply.VoteGranted {
					cnt++
					if cnt == len(peers)/2+1 {
						rf.turnLeader(me, len(peers))
						rf.leaderKicker <- struct{}{}
					}
				}
			}
		}(peerId, peer)
	}
}
