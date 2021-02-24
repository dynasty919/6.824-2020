package raft

import (
	"fmt"

	"6.824/src/labrpc"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintln(fmt.Sprintf("leader %d receive snapshot request index %d from kv server!!!", rf.me, index))
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintln(fmt.Sprintf("leader %d receive snapshot request has log %v", rf.me, rf.log))

	if index <= rf.lastIncludedIndex {
		DPrintln("snapshot trim index <= lastIncludedIndex of server", rf.me)
		return
	}

	if index >= rf.getLogLen() {
		panic(fmt.Sprintf("snapshot trim index >= logLen of server %d", rf.me))
	}

	rf.log = append([]LogEntry{}, rf.log[index-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term

	rf.persistWithSnapshot(snapshot)
}

func (rf *Raft) sendSnapshotToPeer(peer *labrpc.ClientEnd, me int, peerId int, curTerm int) {
	args := SendSnapshotArg{
		Term:              curTerm,
		LeaderId:          me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := SendSnapshotReply{}
	rf.mu.Unlock()

	suc := peer.Call("Raft.InstallSnapShot", &args, &reply)

	rf.mu.Lock()
	if !suc || rf.state != leader || rf.currentTerm != args.Term {
		DPrintln(fmt.Sprintf("leader %d 's send snapshop RPC to %d failed or unexecuted", me, peerId))
		return
	}

	if reply.Term > rf.currentTerm {
		rf.turnFollower(reply.Term, -1)
		return
	}

	rf.nextIndex[peerId] = args.LastIncludedIndex + 1
	rf.matchIndex[peerId] = args.LastIncludedIndex
	rf.updateCommitIndex(me)
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, snapshot)
}
