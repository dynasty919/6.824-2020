package raft

import (
	"6.824/src/labrpc"
	"time"
)

func (rf *Raft) Candidate(done chan struct{}, me int, peers []*labrpc.ClientEnd) {
	done2 := make(chan struct{})
	defer close(done2)

	DPrintln("candidate ", me, " start to hold election")
	grantedVote := make(chan struct{})

	cnt := 1
	var timeout time.Duration
	var gap time.Duration

	for !rf.killed() {
		select {
		case <-done:
			DPrintln("candidate ", me, "turned off")
			return
		case <-grantedVote:
			cnt++
			if cnt >= len(peers)/2+1 {
				DPrintln("candidate ", me, "have won election")
				go rf.Leader(done, me, peers)
				return
			}
		default:
			if gap >= timeout {

				rf.mu.Lock()
				rf.votedFor = rf.me
				rf.currentTerm++
				rf.resetElectionTimer()
				rf.state = candidate
				timeout = rf.electionTimer

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}

				curTerm := rf.currentTerm
				DPrintln("candidate ", me, "have term of ", curTerm)
				rf.mu.Unlock()

				for i, peer := range peers {
					if i != me {
						go rf.callRequestVote(args, peer, done2, grantedVote, i, me)
					}
				}
				gap = 0
			}
			time.Sleep(time.Millisecond * 20)
			gap += time.Millisecond * 20
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
		if rf.currentTerm != args.Term {
			DPrintln("candidate ", me, "'s request vote sent to ", peerId,
				" had old term", args.Term, " but candidate now is ", state, " have term ", term)
			return
		}

		if rf.state != candidate {
			DPrintln("candidate ", me, "'s request vote sent to ", peerId,
				" but candidate now is no longer candidate ,but is", state)
			return
		}
		rf.mu.Unlock()

		if reply.Term > args.Term {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = follower
			rf.newLeaderIncoming <- struct{}{}
			rf.mu.Unlock()
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
