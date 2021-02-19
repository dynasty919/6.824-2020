package kvraft

import (
	"6.824/src/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		Num: nrand(),
	}
	var reply GetReply
	i := 0
	for {
		reply = GetReply{
			Err:   "",
			Value: "",
		}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		//	DPrintf("client call server %d , get value of key %s", i, key)
		if ok && reply.Err.isNil() {
			DPrintf("client call server %d , get value of key %s succeed!!!", i, key)
			break
		} else {
			if !reply.Err.isNil() {
				//			DPrintf("%v", reply.Err)
			}
			i = (i + 1) % len(ck.servers)
		}
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Num:   nrand(),
	}
	var reply PutAppendReply
	i := 0
	for {
		reply = PutAppendReply{Err: ""}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		//	DPrintf("client call server %d , %s key %s with value %s", i, op, key, value)
		if ok && reply.Err.isNil() {
			DPrintf("client call server %d , %s key %s with value %s succeed!!!", i, op, key, value)
			break
		} else {
			if !reply.Err.isNil() {
				//			DPrintf("%v", reply.Err)
			}
			i = (i + 1) % len(ck.servers)
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
