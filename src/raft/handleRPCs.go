package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
				(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
					args.LastLogIndex >= len(rf.log)-1)) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.beFollower(args.Term, args.CandidateId)
		} else {
			rf.beFollower(args.Term, -1)
		}
	} else {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
				(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
					args.LastLogIndex >= len(rf.log)-1)) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.beFollower(args.Term, args.CandidateId)
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
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
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.beFollower(args.Term, -1)
	} else {
		rf.beFollower(rf.currentTerm, rf.votedFor)
	}

	lastOldEntryIndex := len(rf.log) - 1
	if args.PrevLogIndex > lastOldEntryIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex < lastOldEntryIndex {
		if rf.receivedEntriesAlreadyExist(args) {
			reply.Term = rf.currentTerm
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

				log := rf.log
				commitIndex := rf.commitIndex
				DPrintln("server ", me, " already have attempted log, now los is", log, " commitIndex is ", commitIndex)
				rf.applyLogKicker <- struct{}{}
			}
			return
		} else {
			rf.log = rf.log[:args.PrevLogIndex+1]
			lastOldEntryIndex = len(rf.log) - 1
			entry := rf.log
			DPrintln("server ", me, " truncate its log, now log is", entry)
		}
	}

	if args.PrevLogTerm == rf.log[lastOldEntryIndex].Term {
		rf.log = append(rf.log, args.Entries...)
		reply.Term = rf.currentTerm
		reply.Success = true

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

			log := rf.log
			commitIndex := rf.commitIndex
			DPrintln("server ", me, " has appended log, now los is", log, " commitIndex is ", commitIndex)
			rf.applyLogKicker <- struct{}{}
		}
		return
	} else {
		rf.log = rf.log[:len(rf.log)-1]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
}
