package raft

import (
	"6.824/src/labrpc"
)

func (rf *Raft) Candidate(me int, peers []*labrpc.ClientEnd) {
	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

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
						rf.chSender(rf.voteCh)
					}
				}
			}
		}(peerId, peer)
	}
}
