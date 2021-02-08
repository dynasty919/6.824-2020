package raft

import (
	"6.824/src/labrpc"
	"time"
)

func (rf *Raft) Leader(done chan struct{}, me int, peers []*labrpc.ClientEnd) {
	done2 := make(chan struct{})
	defer close(done2)

	rf.mu.Lock()
	rf.state = leader
	heartbeat := rf.heartBeatTimer
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	curTerm := rf.currentTerm
	votedFor := rf.votedFor
	DPrintln("leader ", me, "have term of ", curTerm, " voted for ", votedFor)

	rf.mu.Unlock()

	go rf.commitIndexChecker(done2, me, peers)

	DPrintln("leader ", me, "running, but only sending heartbeat")
	for !rf.killed() {
		select {
		case <-done:
			DPrintln("leader ", me, "stepping down")
			return
		default:
			for i, v := range peers {
				if i != me {
					go rf.sendHeartBeatToPeer(v, me, i, done2)
				}
			}
			DPrintln("leader ", me, " sending heartbeat finished")
			time.Sleep(heartbeat)
		}
	}
}

func (rf *Raft) commitIndexChecker(done chan struct{}, me int, peers []*labrpc.ClientEnd) {
	rf.mu.Lock()
	n := rf.commitIndex + 1
	heartbeat := rf.heartBeatTimer
	rf.mu.Unlock()

	for !rf.killed() {
		select {
		case <-done:
			return
		default:
			rf.mu.Lock()
			kickApplyFlag := false
			for n < len(rf.log) && rf.log[n].Term != rf.currentTerm {
				n++
			}
			for {
				cnt := 1
				for peerId, peerMatchIndex := range rf.matchIndex {
					if peerId != me {
						if peerMatchIndex >= n {
							cnt++
						}
					}
				}
				if cnt >= len(peers)/2+1 {
					rf.commitIndex = n
					kickApplyFlag = true
					n++
				} else {
					break
				}
			}
			if kickApplyFlag {
				rf.applyLogKicker <- struct{}{}
			}
			rf.mu.Unlock()

			time.Sleep(heartbeat)
		}
	}
}

func (rf *Raft) sendHeartBeatToPeer(peer *labrpc.ClientEnd, me int, peerId int,
	done chan struct{}) {

	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     me,
		PrevLogIndex: rf.nextIndex[peerId] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peerId]-1].Term,
		Entries:      rf.log[rf.nextIndex[peerId]:],
		LeaderCommit: rf.commitIndex,
	}

	lastSentIndex := len(rf.log) - 1
	rf.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	suc := peer.Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.state

	select {
	case <-done:
		DPrintln("leader ", me, "'s heart beat sender to ", peerId, " is turned off", "leader now is ", state)
		return
	default:
		if !suc {
			DPrintln("leader ", me, "'s heartbeat sender to ", peerId, " is unsuccessful", "leader now is ", state)
			return
		}
		DPrintln("leader ", me, "'s heart beat sender to ", peerId, " have finished sending", "leader now is ", state)
		if rf.state != leader {
			return
		}

		if reply.Term > args.Term {
			DPrintln("leader ", me, " receive bigger term from ", peerId, " of term ", reply.Term)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = follower
			rf.higherTermFromReply <- struct{}{}
			DPrintln("leader ", me, " have sent NewTermInfo kick")
		} else {
			if reply.Success {
				rf.nextIndex[peerId] = max(lastSentIndex+1, rf.nextIndex[peerId])
				rf.matchIndex[peerId] = max(lastSentIndex, rf.matchIndex[peerId])
			} else {
				rf.nextIndex[peerId]--
			}
		}
	}
}
