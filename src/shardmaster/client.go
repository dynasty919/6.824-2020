package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"time"

	"6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	possibleLeader int
	clientId       int64
	clientOpIndex  int
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
	// Your code here.
	ck.clientId = nrand()
	ck.clientOpIndex = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.ClientOpId = ck.getClientOpId()
	var reply QueryReply
	serverId := ck.possibleLeader
	for {
		// try each known server.
		reply = QueryReply{}

		ok := ck.servers[serverId].Call("ShardMaster.Query", args, &reply)
		if ok && reply.Err.isNil() {
			ck.possibleLeader = serverId
			break
		} else {
			time.Sleep(100 * time.Millisecond)
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.ClientOpId = ck.getClientOpId()
	var reply JoinReply
	serverId := ck.possibleLeader
	for {
		// try each known server.
		reply = JoinReply{}

		ok := ck.servers[serverId].Call("ShardMaster.Join", args, &reply)
		if ok && reply.Err.isNil() {
			ck.possibleLeader = serverId
			break
		} else {
			time.Sleep(100 * time.Millisecond)
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.ClientOpId = ck.getClientOpId()
	var reply LeaveReply
	serverId := ck.possibleLeader
	for {
		// try each known server.
		reply = LeaveReply{}

		ok := ck.servers[serverId].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.Err.isNil() {
			ck.possibleLeader = serverId
			break
		} else {
			time.Sleep(100 * time.Millisecond)
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.ClientOpId = ck.getClientOpId()
	var reply MoveReply
	serverId := ck.possibleLeader
	for {
		// try each known server.
		reply = MoveReply{}

		ok := ck.servers[serverId].Call("ShardMaster.Move", args, &reply)
		if ok && reply.Err.isNil() {
			ck.possibleLeader = serverId
			break
		} else {
			time.Sleep(100 * time.Millisecond)
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) getClientOpId() string {
	res := strconv.Itoa(int(ck.clientId)) + "," + strconv.Itoa(ck.clientOpIndex)
	ck.clientOpIndex++
	return res
}
