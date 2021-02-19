package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type OpReply interface {
	WriteError(string)
	WriteVal(string)
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Num   int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Num int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func (err Err) isNil() bool {
	if string(err) == "" {
		return true
	}
	return false
}

func (reply *PutAppendReply) WriteError(e string) {
	reply.Err = Err(e)
}

func (reply *PutAppendReply) WriteVal(val string) {

}

func (reply *GetReply) WriteError(e string) {
	reply.Err = Err(e)
}

func (reply *GetReply) WriteVal(val string) {
	reply.Value = val
}

func (kv *KVServer) sendChan(ch chan struct{}) {
	go func() {
		for {
			select {
			case <-ch:
			default:
				ch <- struct{}{}
				return
			}
		}
	}()
}
