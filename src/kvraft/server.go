package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation        string
	Key              string
	Value            string
	NRand            int64
	Reply            OpReply
	Done             chan struct{}
	OriginServer     int
	IndexInServer    int
	RaftCommandIndex int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opChan    chan *Op
	unApplied chan *Op
	killChan  chan struct{}

	dict    sync.Map
	applied sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// Your code here.

	done := make(chan struct{})
	kv.opChan <- &Op{
		Operation: "Get",
		Key:       args.Key,
		NRand:     args.NRand,
		Reply:     reply,
		Done:      done,
	}
	DPrintf("server %d receive Get RPC from client with key %s NRand %d", kv.me, args.Key, args.NRand)
	<-done
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	done := make(chan struct{})
	kv.opChan <- &Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		NRand:     args.NRand,
		Reply:     reply,
		Done:      done,
	}
	DPrintf("server %d receive %s RPC from client with key %s , value %s ,NRand %d ",
		kv.me, args.Op, args.Key, args.Value, args.NRand)
	<-done
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("test is killing kv server!!!")
	close(kv.killChan)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.killChan = make(chan struct{})
	kv.opChan = make(chan *Op)
	kv.unApplied = make(chan *Op)
	// You may need initialization code here.
	go kv.Run(me, persister, maxraftstate)
	go kv.StateMachine(me, persister, maxraftstate)

	return kv
}

func (kv *KVServer) Run(me int, persister *raft.Persister, maxraftstate int) {
	i := 1
	for {
		select {
		case <-kv.killChan:
			return
		default:
		}
		DPrintf("Run of server %d is ready to take op from opChan", me)
		select {
		case op := <-kv.opChan:
			op.OriginServer = me
			op.IndexInServer = i
			i++
			b := new(bytes.Buffer)
			e := labgob.NewEncoder(b)
			e.Encode(op.OriginServer)
			e.Encode(op.IndexInServer)
			e.Encode(op.NRand)
			e.Encode(op.Operation)
			e.Encode(op.Key)
			e.Encode(op.Value)
			commandIndex, _, isLeader := kv.rf.Start(b.Bytes())
			if !isLeader {
				op.Reply.WriteError("server " + strconv.Itoa(me) + " is not leader")
				op.Done <- struct{}{}
			} else {
				DPrintf("server %d believe it is leader, operation sent to queue, have index %d!!!",
					me, op.IndexInServer)
				op.RaftCommandIndex = commandIndex
				kv.unApplied <- op
			}
		}
	}
}

func (kv *KVServer) StateMachine(me int, persister *raft.Persister, maxraftstate int) {

	//	dict := make(map[string]string)
	//	applied := make(map[int64]struct{})
	var unAppliedQueue []*Op

	for {
		select {
		case <-kv.killChan:
			return
		default:
		}
		DPrintf("state machine of server %d is ready to select", me)
		select {
		case op := <-kv.unApplied:
			DPrintf("state machine of server %d put op into unAppliedQueue", me)
			unAppliedQueue = append(unAppliedQueue, op)
		case msg := <-kv.applyCh:
			b := bytes.NewBuffer(msg.Command.([]byte))
			d := labgob.NewDecoder(b)
			var origin, index int
			var NRand int64
			var operation, key, value string
			if d.Decode(&origin) != nil || d.Decode(&index) != nil || d.Decode(&NRand) != nil ||
				d.Decode(&operation) != nil || d.Decode(&key) != nil || d.Decode(&value) != nil {
				log.Fatalf("labgob decode error in server %d", me)
				DPrintf("labgob decode error in server %d", me)
			} else {
				DPrintf("incoming msg from applyCh in state machine of server %d of commandIndex %d,"+
					"origin:%d, index:%d, NRand:%d, operation:%s, key:%s, value:%s, queue:%v",
					me, msg.CommandIndex, origin, index, NRand, operation, key, value, unAppliedQueue)

				_, ok := kv.applied.Load(NRand)

				if !ok {
					kv.applied.Store(NRand, struct{}{})
					if operation == "Put" {
						kv.dict.Store(key, value)
						DPrintf("server %d has %s key %s with value %s, queue %v",
							me, operation, key, value, unAppliedQueue)
					} else if operation == "Append" {
						if v, ok := kv.dict.Load(key); !ok {
							kv.dict.Store(key, value)
						} else {
							kv.dict.Store(key, v.(string)+value)
						}
						DPrintf("server %d has %s key %s with value %s, queue %v",
							me, operation, key, value, unAppliedQueue)
					}
				} else {
					DPrintf("server %d has abandoned duplicated PutAppend operation NRand %d", me, NRand)
				}

				res := ""
				if operation == "Get" {
					if v, ok := kv.dict.Load(key); ok {
						res = v.(string)
					}
					DPrintf("server %d has %s key %s with value %s, queue %v", me, operation, key, res, unAppliedQueue)
				}

				if len(unAppliedQueue) > 0 && unAppliedQueue[0].RaftCommandIndex == msg.CommandIndex {
					if unAppliedQueue[0].NRand != NRand {
						DPrintf("operation of NRand %d failed probably due to server "+strconv.Itoa(me)+
							" is no longer leader, index "+strconv.Itoa(unAppliedQueue[0].IndexInServer)+" abandoned",
							NRand)
						unAppliedQueue[0].Reply.WriteError("operation failed probably due to server " + strconv.Itoa(me) +
							" is no longer leader, index " + strconv.Itoa(unAppliedQueue[0].IndexInServer) + " abandoned")
						unAppliedQueue[0].Done <- struct{}{}
						unAppliedQueue = unAppliedQueue[1:]
					}
				}

				if origin == me {
					if len(unAppliedQueue) == 0 || unAppliedQueue[0].IndexInServer != index {
						//unAppliedQueue[0].Reply.WriteError("operation failed probably due to server " + strconv.Itoa(me) +
						//	" couldn't commit entry when it was leader, index " +
						//	strconv.Itoa(unAppliedQueue[0].IndexInServer) + " abandoned")
						//unAppliedQueue[0].Done <- struct{}{}
						//unAppliedQueue = unAppliedQueue[1:]
						DPrintf("incoming operation if NRand %d to server "+strconv.Itoa(me)+
							" is outdated and may has been dumped, queue %v", NRand, unAppliedQueue)
						continue
					}

					//if len(unAppliedQueue) == 0 || unAppliedQueue[0].IndexInServer != index {
					//	fmt.Println(unAppliedQueue, index)//unAppliedQueue[0].IndexInServer
					//	panic("something is seriously fucked in server " + strconv.Itoa(me))
					//} else {
					if operation == "Get" {
						unAppliedQueue[0].Reply.WriteVal(res)
					}
					unAppliedQueue[0].Done <- struct{}{}
					unAppliedQueue = unAppliedQueue[1:]
					//}
				}
			}
		case <-time.After(time.Second):
			if len(unAppliedQueue) == 0 {
				continue
			} else {
				DPrintf("server " + strconv.Itoa(me) + " timeout waiting for a reply from raft" +
					", index " + strconv.Itoa(unAppliedQueue[0].IndexInServer) +
					", NRand" + strconv.Itoa(int(unAppliedQueue[0].NRand)) + " abandoned")
				unAppliedQueue[0].Reply.WriteError("server " + strconv.Itoa(me) + " timeout waiting for a reply from raft" +
					", index " + strconv.Itoa(unAppliedQueue[0].IndexInServer) +
					", NRand" + strconv.Itoa(int(unAppliedQueue[0].NRand)) + " abandoned")
				unAppliedQueue[0].Done <- struct{}{}
				unAppliedQueue = unAppliedQueue[1:]
			}
		}
	}
}
