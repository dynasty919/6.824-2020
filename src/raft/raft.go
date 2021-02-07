package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Entry string
	Term  int
}

type LeaderInfo struct {
	term     int
	leaderId int
}

type HigherTermFromReplyInfo struct {
	term int
}

type RaftState int

const (
	follower RaftState = iota
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leader int
	state  RaftState

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimer  time.Duration
	heartBeatTimer time.Duration

	newLeaderIncoming        chan LeaderInfo
	followerElectionTimeout  chan struct{}
	candidateElectionTimeout chan struct{}
	electionWon              chan struct{}
	leaderKicker             chan struct{}
	higherTermFromReply      chan HigherTermFromReplyInfo
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

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
		return
	} else if args.Term > rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = follower
		rf.newLeaderIncoming <- LeaderInfo{
			term:     args.Term,
			leaderId: args.CandidateId,
		}
		return
	} else {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
				(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
					args.LastLogIndex >= len(rf.log)-1)) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.state = follower
			rf.newLeaderIncoming <- LeaderInfo{
				term:     args.Term,
				leaderId: args.CandidateId,
			}
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	me := rf.me
	term := rf.currentTerm
	DPrintln("server ", me, " of term ", term, " receive heartbeat from server", args.LeaderId, " with term ", args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.state = follower
		rf.leader = args.LeaderId
		rf.newLeaderIncoming <- LeaderInfo{
			term:     args.Term,
			leaderId: args.LeaderId,
		}
	}

	lastNewEntry := len(rf.log) - 1
	if args.PrevLogIndex > lastNewEntry {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.PrevLogIndex < lastNewEntry {
		rf.log = rf.log[:args.PrevLogIndex+1]
		lastNewEntry = len(rf.log) - 1
	}

	if args.PrevLogTerm == rf.log[lastNewEntry].Term {
		rf.log = append(rf.log, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, lastNewEntry)
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	} else {
		rf.log = rf.log[:len(rf.log)-1]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	fmt.Println("test is killing server")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		leader:      -1,
		state:       follower,
		currentTerm: 0,
		votedFor:    -1,
		log: []LogEntry{{
			Entry: "",
			Term:  -1,
		}},
		commitIndex:              0,
		lastApplied:              0,
		heartBeatTimer:           time.Millisecond * 200,
		newLeaderIncoming:        make(chan LeaderInfo),
		followerElectionTimeout:  make(chan struct{}),
		candidateElectionTimeout: make(chan struct{}),
		electionWon:              make(chan struct{}),
		leaderKicker:             make(chan struct{}),
		higherTermFromReply:      make(chan HigherTermFromReplyInfo),
	}

	// Your initialization code here (2A, 2B, 2C).

	rand.Seed(time.Now().UnixNano())

	go rf.Run(applyCh, me, peers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//我定义了一大堆channel，然后select监听所有的channel，只要有相应的事件kick in就直接reap掉所有的现有goroutine（并且
//指望它们能快点结束），然后开启新的工作goroutine。我感觉我做的可以说是非常丑陋了，我看了网上一个人的做法，他在Run中的select
//之前先去检查rf的state，然后根据state来判定需要监听什么channel，感觉这样做优雅许多。
func (rf *Raft) Run(applyCh chan ApplyMsg, me int, peers []*labrpc.ClientEnd) {
	done := make(chan struct{})
	go rf.Follower(done, me)
	for !rf.killed() {
		select {
		case <-rf.newLeaderIncoming:
			done <- struct{}{}
			go rf.Follower(done, me)
		case <-rf.higherTermFromReply:
			done <- struct{}{}
			go rf.Follower(done, me)
		case <-rf.followerElectionTimeout:
			done <- struct{}{}
			go rf.Candidate(done, me, peers)
		case <-rf.candidateElectionTimeout:
			done <- struct{}{}
			go rf.Candidate(done, me, peers)
		case <-rf.electionWon:
			go rf.Leader(done, me, peers)
		}
	}
}

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

	go rf.heartbeatSender(peers, me, done2, heartbeat)

	DPrintln("leader ", me, "running, but only sending heartbeat")
	for !rf.killed() {
		select {
		case <-done:
			DPrintln("leader ", me, "stepping down")
			return
		}
	}
}

func (rf *Raft) heartbeatSender(peers []*labrpc.ClientEnd, me int, done chan struct{}, heartbeat time.Duration) {
	done3 := make(chan struct{})
	defer close(done3)

	for !rf.killed() {
		select {
		case <-done:
			DPrintln("leader ", me, "'s heartbeat sender is turned off")
			return
		default:
			for i, v := range peers {
				if i != me {
					go rf.sendHeartBeatToPeer(v, me, i, done3)
				}
			}
			DPrintln("leader ", me, " sending heartbeat finished")
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
			rf.higherTermFromReply <- HigherTermFromReplyInfo{term: reply.Term}
			DPrintln("leader ", me, " have sent NewTermInfo kick")
		} else {
			if reply.Success {
				rf.nextIndex[peerId] = len(rf.log)
				rf.matchIndex[peerId] = len(rf.log) - 1
			} else {
				rf.nextIndex[peerId]--
			}
		}
	}
}

func (rf *Raft) Candidate(done chan struct{}, me int, peers []*labrpc.ClientEnd) {
	done2 := make(chan struct{})
	defer close(done2)

	DPrintln("candidate ", me, " start to hold election")

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.state = candidate
	t := rf.electionTimer

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	curTerm := rf.currentTerm
	DPrintln("candidate ", me, "have term of ", curTerm)
	rf.mu.Unlock()

	go rf.candidateElectionTimeoutChecker(done2, t, me)

	grantedVote := make(chan struct{})
	for i, peer := range peers {
		if i != me {
			go rf.callRequestVote(args, peer, done2, grantedVote, i)
		}
	}
	DPrintln("candidate ", me, " is waiting for votes")
	cnt := 1
	for !rf.killed() {
		select {
		case <-done:
			DPrintln("candidate ", me, "turned off")
			return
		case <-grantedVote:
			cnt++
			if cnt >= len(peers)/2+1 {
				DPrintln("candidate ", me, "have won election")
				rf.electionWon <- struct{}{}
				//			DPrintln("candidate ", me, "have set election won kicker")
				return
			}
		}
	}

}

func (rf *Raft) callRequestVote(args RequestVoteArgs, peer *labrpc.ClientEnd,
	done chan struct{}, grantedVote chan struct{}, peerId int) {
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
		} else {
			if reply.Term > args.Term {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = follower
				rf.mu.Unlock()
				rf.newLeaderIncoming <- LeaderInfo{
					term:     reply.Term,
					leaderId: peerId, //这个其实不是真正的leader id！只是暂时这么写
				}
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

}

func (rf *Raft) candidateElectionTimeoutChecker(done chan struct{}, t time.Duration, me int) {
	var gap time.Duration
	for gap < t && !rf.killed() {
		select {
		case <-done:
			DPrintln("candidate ", me, "'s election timeout checker with ", t, " turned off ")
			return
		default:
			time.Sleep(time.Millisecond * 10)
			gap += time.Millisecond * 10
		}
	}
	DPrintln("candidate ", me, " election timeout with ", t)
	rf.candidateElectionTimeout <- struct{}{}
}

func (rf *Raft) Follower(done chan struct{}, me int) {
	done2 := make(chan struct{})

	rf.mu.Lock()
	rf.resetElectionTimer()
	t := rf.electionTimer
	DPrintln("follower ", me, " following, have a election timeout of", t)
	rf.state = follower

	curTerm := rf.currentTerm
	votedFor := rf.votedFor
	DPrintln("follower ", me, "have term of ", curTerm, " voted for ", votedFor)
	rf.mu.Unlock()

	go rf.followElectionTimeoutChecker(done2, t, me)
	for !rf.killed() {
		select {
		case <-done:
			DPrintln("follower ", me, " turned off")
			close(done2)
			return
		}
	}
}

func (rf *Raft) followElectionTimeoutChecker(done chan struct{}, t time.Duration, me int) {
	var gap time.Duration

	for gap < t && !rf.killed() {
		select {
		case <-done:
			DPrintln("follower ", me, "'s election timeout checker with ", t, " turned off ")
			return
		default:
			time.Sleep(time.Millisecond * 10)
			gap += time.Millisecond * 10
		}
	}
	DPrintln("follower ", me, " election timeout with", t)
	rf.followerElectionTimeout <- struct{}{}
}

func (rf *Raft) resetElectionTimer() {
	t := 1000 + rand.Intn(1000)
	rf.electionTimer = time.Duration(t) * time.Millisecond
}
