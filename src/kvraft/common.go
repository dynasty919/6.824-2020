package kvraft

import (
	"log"
	"strconv"
	"strings"
)

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
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	ClientId   int64
	ClientOpId string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err    Err
	Server int
}

type GetArgs struct {
	Key        string
	ClientId   int64
	ClientOpId string
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

func isClientOpIdLarger(id1 string, id2 string) bool {
	a := strings.Split(id1, ",")
	b := strings.Split(id2, ",")
	if a[0] != b[0] {
		panic("op from different clients compared")
	}
	c, err1 := strconv.Atoi(a[1])
	d, err2 := strconv.Atoi(b[1])
	if err1 != nil || err2 != nil {
		panic("atoi error")
	}
	return c > d
}

func PopAndDone(queue *[]*Op) {
	if len(*queue) == 0 {
		panic("trying to pop an empty queue")
	}
	(*queue)[0].Done <- struct{}{}
	(*queue) = (*queue)[1:]
}
