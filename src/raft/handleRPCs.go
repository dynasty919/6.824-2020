package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm { //all server rule 1 If RPC request or response contains term T > currentTerm:
		rf.turnFollower(args.Term, -1) // set currentTerm = T, convert to follower (§5.1)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (args.Term < rf.currentTerm) || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		// Reply false if term < currentTerm (§5.1)  If votedFor is not null and not candidateId,
	} else if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.chSender(rf.appendLogCh)

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

	lastOldEntryIndex := len(rf.log) - 1

	if args.PrevLogIndex > lastOldEntryIndex {
		//这种情况下类似于[4],[4,6,6,6],follower在prevLogIndex的位置上没有条目
		//我们就直接返回follower的后的条目index
		reply.FirstIndexOfLastLogTerm = lastOldEntryIndex
		reply.LastLogTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//[4,5,5],[4,6,6,6]和[4,4,4],[4,6,6,6]的情况
		reply.LastLogTerm = rf.log[args.PrevLogIndex].Term
		reply.FirstIndexOfLastLogTerm = rf.SearchFirstIndexOfTerm(rf.log[args.PrevLogIndex].Term)
		return
	}

	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[:index]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

		log := rf.log
		commitIndex := rf.commitIndex
		DPrintln("server ", me, " have updated commitIndex, now log is", log, " commitIndex is ", commitIndex)
		rf.updateLastApplied()
	}
	reply.Success = true
	return
}
