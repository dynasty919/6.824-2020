package shardmaster

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opChan    chan *Op
	unApplied chan *Op
	killChan  chan struct{}

	configs []Config // indexed by config num
	applied map[int64]string
}

type Op struct {
	// Your data here.
	Operation        string
	Args             OpArgs
	Reply            OpReply
	Done             chan struct{}
	OriginServer     int
	RaftCommandIndex int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	done := make(chan struct{})

	sm.opChan <- &Op{
		Operation:    Write,
		Args:         args,
		Reply:        reply,
		Done:         done,
		OriginServer: sm.me,
	}
	<-done
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	done := make(chan struct{})
	sm.opChan <- &Op{
		Operation:    Write,
		Args:         args,
		Reply:        reply,
		Done:         done,
		OriginServer: sm.me,
	}
	<-done
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	done := make(chan struct{})
	sm.opChan <- &Op{
		Operation:    Write,
		Args:         args,
		Reply:        reply,
		Done:         done,
		OriginServer: sm.me,
	}
	<-done
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	done := make(chan struct{})
	sm.opChan <- &Op{
		Operation:    Read,
		Args:         args,
		Reply:        reply,
		Done:         done,
		OriginServer: sm.me,
	}
	<-done
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	DPrintf("test is killing kv server")
	close(sm.killChan)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.opChan = make(chan *Op)
	sm.unApplied = make(chan *Op)
	sm.killChan = make(chan struct{})
	sm.applied = make(map[int64]string)
	sm.configs = []Config{{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}}

	go sm.Run(me)
	go sm.StateMachine(me)
	// Your code here.
	return sm
}

func (sm *ShardMaster) Run(me int) {
	for {
		select {
		case <-sm.killChan:
			return
		default:
		}
		DPrintf("Run of server %d is ready to take op from opChan", me)
		select {
		case op := <-sm.opChan:
			op.OriginServer = me
			b := new(bytes.Buffer)
			e := labgob.NewEncoder(b)
			e.Encode(op.OriginServer)
			e.Encode(op.Operation)
			e.Encode(&(op.Args))
			commandIndex, _, isLeader := sm.rf.Start(b.Bytes())
			if !isLeader {
				op.Reply.WriteError("server " + strconv.Itoa(me) + " is not leader")
				op.Done <- struct{}{}
			} else {
				DPrintf("server %d believe it is leader, operation sent to queue, ClientOpId %s!!!",
					me, op.Args.GetClientOpId())
				op.RaftCommandIndex = commandIndex
				sm.unApplied <- op
			}
		}
	}
}

func (sm *ShardMaster) StateMachine(me int) {
	var unAppliedQueue []*Op

	for {

		select {
		case <-sm.killChan:
			return
		default:
		}

		DPrintf("state machine of server %d is ready to select", me)
		select {
		case op := <-sm.unApplied:
			DPrintf("state machine of server %d put op into unAppliedQueue", me)
			unAppliedQueue = append(unAppliedQueue, op)
		case msg := <-sm.applyCh:
			if msg.CommandValid {
				sm.ApplyCommand(me, &unAppliedQueue, &msg)
			} else {
				panic("shardmaster doesn't support snapshot yet")
			}

		case <-time.After(time.Second):
			if len(unAppliedQueue) == 0 {
				continue
			} else {
				unAppliedQueue[0].Reply.WriteError("server " + strconv.Itoa(me) +
					" timeout waiting for a reply from raft" +
					", ClientOpId" + unAppliedQueue[0].Args.GetClientOpId() + " abandoned")
				PopAndDone(&unAppliedQueue)
			}
		}
	}
}

func (sm *ShardMaster) ApplyCommand(me int, unAppliedQueuePtr *[]*Op, msg *raft.ApplyMsg) {
	//	commandIndex := msg.CommandIndex
	b := bytes.NewBuffer(msg.Command.([]byte))
	d := labgob.NewDecoder(b)
	var origin int
	var operation string
	var opArgs OpArgs

	err1 := d.Decode(&origin)
	err2 := d.Decode(&operation)
	err3 := d.Decode(&opArgs)
	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatalf("labgob decode error in server %d, %v, %v, %v", me, err1, err2, err3)
	} else {
		clientId := opArgs.GetClientId()
		clientOpId := opArgs.GetClientOpId()

		DPrintf("incoming msg from applyCh in state machine of server %d of commandIndex %d,"+
			"origin:%d, clientOpId:%s, opArgs:%v , queue:%v",
			me, msg.CommandIndex, origin, clientOpId, opArgs, (*unAppliedQueuePtr))

		cur, ok := sm.applied[clientId]

		var res Config
		if !ok || isClientOpIdLarger(clientOpId, cur) || operation == Read {
			sm.applied[clientId] = clientOpId
			res = opArgs.Apply(&sm.configs)
		} else {
			DPrintf("server %d has abandoned duplicated PutAppend operation clientOpId %s",
				me, clientOpId)
		}

		if len((*unAppliedQueuePtr)) > 0 && (*unAppliedQueuePtr)[0].RaftCommandIndex == msg.CommandIndex {
			if (*unAppliedQueuePtr)[0].Args.GetClientOpId() != clientOpId || origin != me {
				(*unAppliedQueuePtr)[0].Reply.WriteError("operation failed probably due to server " + strconv.Itoa(me) +
					" is no longer leader, clientOpId " + (*unAppliedQueuePtr)[0].Args.GetClientOpId() + " abandoned")
				PopAndDone(unAppliedQueuePtr)
			}
		}

		if origin == me {
			if len((*unAppliedQueuePtr)) == 0 || (*unAppliedQueuePtr)[0].Args.GetClientOpId() != clientOpId {
				DPrintf("incoming operation of clientOpId %s to server "+strconv.Itoa(me)+
					" may has been re-executed or is outdated and dumped, queue %v",
					clientOpId, (*unAppliedQueuePtr))
				return
			}
			(*unAppliedQueuePtr)[0].Reply.WriteConfig(res)
			PopAndDone(unAppliedQueuePtr)
		}
	}
}
