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
	"bytes"
	log2 "log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"

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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Entry interface{}
	Term  int
}

type State int

const (
	follower State = iota
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
	state State

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimeout time.Duration
	heartBeatTimer  time.Duration

	appendLogCh chan struct{}
	voteCh      chan struct{}
	applyChan   chan ApplyMsg
	killChan    chan struct{}

	lastIncludedIndex int
	lastIncludedTerm  int
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var term, voteFor, lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&log) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log2.Fatalf("labgob decode error")
	} else {
		rf.log = log
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//index := -1
	//term := rf.currentTerm
	//isLeader := (rf.state == leader)
	////If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	//if isLeader {
	//	index = rf.getLastLogIndex() + 1
	//	newLog := LogEntry{
	//		command,
	//		rf.currentTerm,
	//	}
	//	rf.log = append(rf.log, newLog)
	//	rf.persist()
	////	rf.Leader(rf.me, rf.peers, rf.currentTerm)
	//}
	//return index, term, isLeader

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == leader

	me := rf.me
	DPrintln("!!!!!!!")

	if rf.state != leader || rf.killed() {
		DPrintln("server ", me, "fail to receive the command", command, " because it is killed or not leader")
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{
		Entry: command,
		Term:  rf.currentTerm,
	})
	rf.persist()

	index = rf.getLastLogIndex()

	DPrintln("client send to leader ", me, "command", command, "commandIndex", index, "term", term)
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
	DPrintln("test is killing Raft server")
	rf.chSender(rf.killChan)
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
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       follower,
		currentTerm: 0,
		votedFor:    -1,
		log: []LogEntry{{
			Entry: 0,
			Term:  0,
		}},
		commitIndex:       0,
		lastApplied:       0,
		heartBeatTimer:    time.Millisecond * 100,
		appendLogCh:       make(chan struct{}),
		voteCh:            make(chan struct{}),
		killChan:          make(chan struct{}),
		applyChan:         applyCh,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	// Your initialization code here (2A, 2B, 2C).

	rand.Seed(time.Now().UnixNano())
	rf.resetElectionTimer()

	rf.readPersist(persister.ReadRaftState())

	// initialize from state persisted before a crash
	DPrintln("test is starting server ", me, "log length", rf.getLogLen(), "last log entry", rf.getLogEntry(rf.getLastLogIndex()),
		"term", rf.currentTerm, " commitIndex", rf.commitIndex,
		"last applied", rf.lastApplied)
	go rf.Run(me, peers)

	return rf
}

func (rf *Raft) Run(me int, peers []*labrpc.ClientEnd) {

	rf.mu.Lock()
	heartBeat := rf.heartBeatTimer
	rf.mu.Unlock()

	for !rf.killed() {

		select {
		case <-rf.killChan:
			return
		default:
		}

		rf.mu.Lock()
		state := rf.state
		electionTimeout := rf.electionTimeout
		curTerm := rf.currentTerm
		rf.mu.Unlock()

		switch state {
		case follower, candidate:
			select {
			case <-rf.appendLogCh:
				continue
			case <-rf.voteCh:
				continue
			case <-time.After(electionTimeout):
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					select {
					case <-rf.appendLogCh:
						return
					case <-rf.voteCh:
						return
					default:
						rf.turnCandidate(me)
						rf.Candidate(me, peers)
					}
				}()
			}
		case leader:
			rf.Leader(me, peers, curTerm)
			<-time.After(heartBeat)
		default:
			panic("unknown state" + strconv.Itoa(int(state)))
		}
	}
}
