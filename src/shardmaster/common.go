package shardmaster

import (
	"log"
	"strconv"
	"strings"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	Read  = "Read"
	Write = "Write"
)

type OpArgs interface {
	Apply(list *[]Config) Config
	GetClientId() int64
	GetClientOpId() string
}

type OpReply interface {
	WriteError(string)
	WriteConfig(Config)
}

type Err string

type JoinArgs struct {
	Servers    map[int][]string // new GID -> servers mappings
	ClientId   int64
	ClientOpId string
}

func (j JoinArgs) Apply(list *[]Config) Config {
	var res Config
	old := (*list)[len(*list)-1]

	dict := make(map[int][]string)
	var gids []int
	for k, v := range old.Groups {
		dict[k] = v
		gids = append(gids, k)
	}
	for k, v := range j.Servers {
		if _, ok := dict[k]; ok {
			panic("trying to add existed gid to config")
		}
		dict[k] = v
		gids = append(gids, k)
	}

	res.Num = old.Num + 1
	res.Groups = dict
	res.Shards = rebalance(old.Shards, gids)
	*list = append(*list, res)
	return Config{}
}

func (j JoinArgs) GetClientId() int64 {
	return j.ClientId
}

func (j JoinArgs) GetClientOpId() string {
	return j.ClientOpId
}

type JoinReply struct {
	//	WrongLeader bool
	Err Err
}

type LeaveArgs struct {
	GIDs       []int
	ClientId   int64
	ClientOpId string
}

func (l LeaveArgs) Apply(list *[]Config) Config {
	var res Config
	old := (*list)[len(*list)-1]

	out := make(map[int]struct{})
	for _, v := range l.GIDs {
		out[v] = struct{}{}
	}

	dict := make(map[int][]string)
	var gids []int
	for k, v := range old.Groups {
		if _, ok := out[k]; !ok {
			dict[k] = v
			gids = append(gids, k)
		}
	}

	res.Num = old.Num + 1
	res.Groups = dict
	res.Shards = rebalance(old.Shards, gids)
	*list = append(*list, res)
	return Config{}
}

func (l LeaveArgs) GetClientId() int64 {
	return l.ClientId
}

func (l LeaveArgs) GetClientOpId() string {
	return l.ClientOpId
}

type LeaveReply struct {
	//	WrongLeader bool
	Err Err
}

type MoveArgs struct {
	Shard      int
	GID        int
	ClientId   int64
	ClientOpId string
}

func (m MoveArgs) Apply(list *[]Config) Config {
	var res Config
	old := (*list)[len(*list)-1]

	if _, ok := old.Groups[m.GID]; !ok {
		panic("the trying to move shard to non-existed gid")
	}

	dict := make(map[int][]string)
	for k, v := range old.Groups {
		dict[k] = v
	}

	var shards [NShards]int
	for i := 0; i < NShards; i++ {
		shards[i] = old.Shards[i]
	}
	shards[m.Shard] = m.GID

	res.Num = old.Num + 1
	res.Groups = dict
	res.Shards = shards
	*list = append(*list, res)
	return Config{}
}

func (m MoveArgs) GetClientId() int64 {
	return m.ClientId
}

func (m MoveArgs) GetClientOpId() string {
	return m.ClientOpId
}

type MoveReply struct {
	//	WrongLeader bool
	Err Err
}

type QueryArgs struct {
	Num        int // desired config number
	ClientId   int64
	ClientOpId string
}

func (q QueryArgs) Apply(list *[]Config) Config {
	if q.Num == -1 || q.Num >= ((*list)[len(*list)-1]).Num {
		return (*list)[len(*list)-1]
	} else {
		return (*list)[q.Num]
	}
}

func (q QueryArgs) GetClientId() int64 {
	return q.ClientId
}

func (q QueryArgs) GetClientOpId() string {
	return q.ClientOpId
}

type QueryReply struct {
	//	WrongLeader bool
	Err    Err
	Config Config
}

func (err Err) isNil() bool {
	if string(err) == "" {
		return true
	}
	return false
}

func (reply *JoinReply) WriteError(e string) {
	reply.Err = Err(e)
}

func (reply *JoinReply) WriteConfig(config Config) {

}

func (reply *LeaveReply) WriteError(e string) {
	reply.Err = Err(e)
}

func (reply *LeaveReply) WriteConfig(config Config) {

}

func (reply *MoveReply) WriteError(e string) {
	reply.Err = Err(e)
}

func (reply *MoveReply) WriteConfig(config Config) {

}

func (reply *QueryReply) WriteError(e string) {
	reply.Err = Err(e)
}

func (reply *QueryReply) WriteConfig(config Config) {
	reply.Config = config
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
