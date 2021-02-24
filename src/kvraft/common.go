package kvraft

import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

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
	NRand int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err    Err
	Server int
}

type GetArgs struct {
	Key   string
	NRand int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err    Err
	Value  string
	Server int
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
