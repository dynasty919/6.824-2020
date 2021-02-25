package kvraft

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"sync"
	"time"

	"6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lock           sync.Mutex
	possibleLeader int

	clientId      int64
	clientOpIndex int
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
	ck.clientId = nrand()
	ck.clientOpIndex = 0
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
		Key:        key,
		ClientId:   ck.clientId,
		ClientOpId: ck.getClientOpId(),
	}
	var reply GetReply

	for {
		reply = GetReply{
			Err:    "",
			Value:  "",
			Server: 0,
		}

		//		DPrintf("client call server %d , try to get value of key %s", i, key)
		server := ck.possibleLeader
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err.isNil() {
			DPrintf("client call server %d , get value %s of key %s ClientOpId %s from origin server %d succeed!!!",
				server, reply.Value, key, args.ClientOpId, reply.Server)
			break
		} else {
			if !reply.Err.isNil() {
				//			DPrintf("%v", reply.Err)
			}
			ck.possibleLeader = (ck.possibleLeader + 1) % len(ck.servers)
			time.Sleep(time.Millisecond * 100)
			continue
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
		Key:        key,
		Value:      value,
		Op:         op,
		ClientId:   ck.clientId,
		ClientOpId: ck.getClientOpId(),
	}

	var reply PutAppendReply
	for {
		reply = PutAppendReply{
			Err:    "",
			Server: 0,
		}
		server := ck.possibleLeader
		DPrintf("client call server %d , %s key %s with value %s", server, op, key, value)
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err.isNil() {
			DPrintf("client call server %d , %s key %s with value %s ClientOpId %s from origin server %d succeed!!!",
				server, op, key, value, args.ClientOpId, reply.Server)
			break
		} else {
			if !reply.Err.isNil() {
				DPrintf("%v", reply.Err)
			}
			ck.possibleLeader = (ck.possibleLeader + 1) % len(ck.servers)
			time.Sleep(time.Millisecond * 100)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getClientOpId() string {
	res := strconv.Itoa(int(ck.clientId)) + "," + strconv.Itoa(ck.clientOpIndex)
	ck.clientOpIndex++
	return res
}
