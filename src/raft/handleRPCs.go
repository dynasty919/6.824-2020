package raft

import "fmt"

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	if args.Term > rf.currentTerm { //all server rule 1 If RPC request or response contains term T > currentTerm:
		rf.turnFollower(args.Term, -1) // set currentTerm = T, convert to follower (§5.1)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (args.Term < rf.currentTerm) || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		// Reply false if term < currentTerm (§5.1)  If votedFor is not null and not candidateId,
	} else if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()) {
		//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		// Reply false if candidate’s log is at least as up-to-date as receiver’s log
	} else {
		//grant vote
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = follower
		rf.persist()
		rf.chSender(rf.voteCh) //because If election timeout elapses without receiving granting vote to candidate, so wake up
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.me
	term := rf.currentTerm
	id := args.LeaderId
	command := args.Entries
	preLogIndex := args.PrevLogIndex
	preLogTerm := args.PrevLogTerm
	leaderCommit := args.LeaderCommit
	DPrintln("server ", me, " of term ", term, " receive heartbeat from server",
		args.LeaderId, " with term ", args.Term,
		"server ", me, "receive append attempt from server ", id, " of command ", command,
		"attempt has preLogIndex ", preLogIndex, " preLogTerm", preLogTerm, " leaderCommit", leaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.chSender(rf.appendLogCh)

	if args.Term > rf.currentTerm {
		rf.turnFollower(args.Term, -1)
	}

	lastOldEntryIndex := rf.getLastLogIndex()

	if args.PrevLogIndex > lastOldEntryIndex || args.PrevLogIndex < rf.lastIncludedIndex {
		//这种情况下类似于[4],[4,6,6,6],follower在prevLogIndex的位置上没有条目
		//我们就直接返回follower的后的条目index
		reply.FirstIndexOfLastLogTerm = lastOldEntryIndex
		reply.LastLogTerm = -1
		return
	}

	if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		//[4,5,5],[4,6,6,6]和[4,4,4],[4,6,6,6]的情况
		reply.LastLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
		reply.FirstIndexOfLastLogTerm = rf.SearchFirstIndexOfTerm(rf.getLogEntry(args.PrevLogIndex).Term)
		return
	}

	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index < rf.getLogLen() {
			if rf.getLogEntry(index).Term == args.Entries[i].Term {
				continue
			} else {
				rf.log = rf.log[:index-rf.lastIncludedIndex]
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())

		commitIndex := rf.commitIndex
		DPrintln("server ", me, " have updated commitIndex,log length", rf.getLogLen(),
			"now commitIndex is ", commitIndex)
		rf.updateLastApplied()
	}
	reply.Success = true
	return
}

func (rf *Raft) InstallSnapshot(args *SendSnapshotArg, reply *SendSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintln(fmt.Sprintf("server %d receive install snapshot attempt from leader %d", rf.me, args.LeaderId) +
		fmt.Sprintf("attemp has LastIncludedIndex %d, LastIncludedTerm %d",
			args.LastIncludedIndex, args.LastIncludedTerm))

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.chSender(rf.appendLogCh)

	if args.Term > rf.currentTerm {
		rf.turnFollower(args.Term, -1)
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	msg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		Snapshot:      args.Data,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	if args.LastIncludedIndex <= rf.getLastLogIndex() {
		rf.log = append([]LogEntry{}, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	} else {
		rf.log = []LogEntry{{
			Entry: nil,
			Term:  args.LastIncludedTerm,
		}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	DPrintln(fmt.Sprintf("server %d has shorten its log, now lastIncludedIndex %d, lastIncludedTerm %d, log is %v ",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log))
	rf.persistWithSnapshot(args.Data)

	if args.LastIncludedIndex < rf.lastApplied {
		return
	}
	rf.applyChan <- msg
}
