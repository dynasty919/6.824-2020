package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Add your RPC definitions here.

type MyArgs struct {
}

type MyReply struct {
	JobType    string
	JobNum     int
	InputPath  string
	InterPaths []string
	NReduce    int
}

type MapDoneArgs struct {
	JobNum    int
	InterPath []string
}

type MapDoneReply struct {
}

type ReduceDoneArgs struct {
	JobNum int
}

type ReduceDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
