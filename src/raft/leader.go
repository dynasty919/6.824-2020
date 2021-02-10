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
	entry := rf.log
	DPrintln("leader ", me, "have term of ", curTerm, " voted for ", votedFor, "has log ", entry)

	rf.mu.Unlock()

	go rf.commitIndexChecker(done2, me, peers)

	//	DPrintln("leader ", me, "running, but only sending heartbeat")
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
			//DPrintln("leader ", me, " sending heartbeat finished heartbeat term ", )
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
				log := rf.log
				commitIndex := rf.commitIndex
				DPrintln("leader ", me, "commit to commitIndex", commitIndex, "now have log ", log)
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
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     me,
		PrevLogIndex: rf.nextIndex[peerId] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peerId]-1].Term,
		Entries:      rf.getEntries(peerId),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:    0,
		Success: false,
	}

	suc := peer.Call("Raft.AppendEntries", &args, &reply)
	DPrintln("leader ", me, " sending heartbeat to server ", peerId, " finished heartbeat term ", args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.state
	term := rf.currentTerm
	log := rf.log

	select {
	case <-done:
		DPrintln("leader ", me, "'s heart beat sender to ", peerId, " is turned off", "leader now is ", state)
		return
	default:
		if !suc {
			DPrintln("leader ", me, "'s heartbeat sender to ", peerId, " is unsuccessful",
				"leader now is ", state, " have term ", term, "have log ", log)
			return
		}

		if term != args.Term {
			DPrintln("leader ", me, "'s heart beat sender to ", peerId,
				" had old term", args.Term, " but leader now is ", state, " have term ", term)
			return
		}

		if rf.state != leader {
			DPrintln("leader ", me, "'s request vote sent to ", peerId,
				" but leader now is no longer leader ,but is", state)
			return
		}

		DPrintln("leader ", me, "'s heart beat sender to ", peerId,
			" have finished sending", "leader now is ", state, " have term ", term)

		if reply.Term > term {
			DPrintln("leader ", me, " receive bigger term from ", peerId, " of term ", reply.Term)
			rf.beFollower(reply.Term, -1)
			return
		} else {
			if reply.Success {
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
			} else {
				rf.nextIndex[peerId]--
			}
		}
	}
}
