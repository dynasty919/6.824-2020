package raft

import (
	"6.824/src/labrpc"
	"sort"
)

func (rf *Raft) Leader(me int, peers []*labrpc.ClientEnd, curTerm int) {
	for i, v := range peers {
		if i != me {
			go rf.sendHeartBeatToPeer(v, me, i, curTerm)
		}
	}
}

func (rf *Raft) sendHeartBeatToPeer(peer *labrpc.ClientEnd, me int, peerId int, curTerm int) {

	rf.mu.Lock()
	if rf.state != leader || rf.currentTerm != curTerm {
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

	reply := &AppendEntriesReply{}

	suc := peer.Call("Raft.AppendEntries", &args, &reply)

	if !suc {
		DPrintln("leader ", me, "'s heartbeat sender to ", peerId, " is unsuccessful")
		return
	}
	DPrintln("leader ", me, " sending heartbeat to server ", peerId, " succeed heartbeat term ", args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.state
	term := rf.currentTerm

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

	//DPrintln("leader ", me, "'s heart beat sender to ", peerId,
	//	" have finished sending", "leader now is ", state, " have term ", term)

	if reply.Term > term {
		DPrintln("leader ", me, " receive bigger term from ", peerId, " of term ", reply.Term)
		rf.turnFollower(reply.Term, -1)
	} else {
		if reply.Success {
			rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
			rf.updateCommitIndex(me)
		} else {
			if reply.LastLogTerm == -1 {
				//[4],[4,6,6,6]
				rf.nextIndex[peerId] = reply.FirstIndexOfLastLogTerm + 1
			} else {
				pos := rf.SearchFirstIndexOfTerm(reply.LastLogTerm)
				if pos >= len(rf.log) || rf.log[pos].Term != reply.LastLogTerm {
					//[4,5,5],[4,6,6,6]
					rf.nextIndex[peerId] = reply.FirstIndexOfLastLogTerm
				} else {
					//[4,4,4],[4,6,6,6]
					rf.nextIndex[peerId] = pos + 1
				}
			}
		}
	}
}

func (rf *Raft) updateCommitIndex(me int) {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Slice(copyMatchIndex, func(i int, j int) bool {
		return copyMatchIndex[i] < copyMatchIndex[j]
	})
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}
