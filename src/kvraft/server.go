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
	Operation     string
	Key           string
	Value         string
	Num           int64
	Reply         OpReply
	Done          chan struct{}
	OriginServer  int
	IndexInServer int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opChan    chan Op
	unApplied chan Op
	killChan  chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	done := make(chan struct{})
	kv.opChan <- Op{
		Operation: "get",
		Key:       args.Key,
		Num:       args.Num,
		Reply:     reply,
		Done:      done,
	}
	<-done
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	done := make(chan struct{})
	kv.opChan <- Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Num:       args.Num,
		Reply:     reply,
		Done:      done,
	}
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.killChan = make(chan struct{})
	kv.opChan = make(chan Op)
	kv.unApplied = make(chan Op)
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

		select {
		case op := <-kv.opChan:
			op.OriginServer = me
			op.IndexInServer = i
			i++
			b := new(bytes.Buffer)
			e := labgob.NewEncoder(b)
			e.Encode(op.OriginServer)
			e.Encode(op.IndexInServer)
			e.Encode(op.Num)
			e.Encode(op.Reply)
			e.Encode(op.Operation)
			e.Encode(op.Key)
			e.Encode(op.Value)
			e.Encode(op.Done)
			_, _, isLeader := kv.rf.Start(b.Bytes())
			if !isLeader {
				op.Reply.WriteError("server " + strconv.Itoa(me) + " is not leader")
				op.Done <- struct{}{}
			} else {
				kv.unApplied <- op
			}
		}
	}
}

func (kv *KVServer) StateMachine(me int, persister *raft.Persister, maxraftstate int) {

	dict := make(map[string]string)
	applied := make(map[int64]struct{})
	var unAppliedQueue []Op

	for {
		select {
		case <-kv.killChan:
			return
		default:
		}

		select {
		case op := <-kv.unApplied:
			unAppliedQueue = append(unAppliedQueue, op)
		case msg := <-kv.applyCh:
			b := bytes.NewBuffer(msg.Command.([]byte))
			d := labgob.NewDecoder(b)
			var origin, index int
			var num int64
			var reply OpReply
			var operation, key, value string
			var done chan struct{}
			if d.Decode(&origin) != nil || d.Decode(&index) != nil || d.Decode(&num) != nil || d.Decode(&reply) != nil ||
				d.Decode(operation) != nil || d.Decode(key) != nil || d.Decode(&value) != nil || d.Decode(done) != nil {
				log.Fatalf("labgob decode error")
			} else {
				if _, ok := applied[num]; ok {
					continue
				} else {
					applied[num] = struct{}{}
				}

				if operation == "Put" {
					dict[key] = value
				} else if operation == "Append" {
					if _, ok := dict[key]; !ok {
						dict[key] = value
					} else {
						dict[key] += value
					}
				}

				if origin != me {
					continue
				}

				for len(unAppliedQueue) > 0 && unAppliedQueue[0].IndexInServer < index {
					unAppliedQueue[0].Reply.WriteError("operation failed probably due to server " + strconv.Itoa(me) +
						" is no longer leader")
					unAppliedQueue[0].Done <- struct{}{}
					unAppliedQueue = unAppliedQueue[1:]
				}

				if len(unAppliedQueue) == 0 || unAppliedQueue[0].IndexInServer != index {
					panic("something is seriously fucked")
				}

				if operation == "Get" {
					reply.WriteVal(dict[key])
				}
				unAppliedQueue[0].Done <- struct{}{}
				unAppliedQueue = unAppliedQueue[1:]
			}
		}
	}
}
