package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskState int

const (
	defaultState TaskState = iota
	idle
	inProgress
	completed
)

type MapTask struct {
	DataPath  string
	State     TaskState
	InterPath []string
}

type ReduceTask struct {
	DataPath []string
	State    TaskState
}

type WorkerInfo struct {
	JobType string
	JobNum  int
}

type Master struct {
	// Your definitions here.
	nMap        int
	nReduce     int
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	workerList  []WorkerInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (m *Master) Scheduler(args *MyArgs, reply *MyReply) error {
	fmt.Println("in scheduler")
	for i, task := range m.mapTasks {
		if task.State == idle {
			reply.JobType = "map"
			reply.InputPath = task.DataPath
			reply.JobNum = i
			reply.NReduce = m.nReduce
			task.State = inProgress
			m.workerList = append(m.workerList, WorkerInfo{
				JobType: "map",
				JobNum:  i,
			})
			return nil
		}
	}
	fmt.Println("about to schedule reduce")
	for i, task := range m.reduceTasks {
		if task.State == idle {
			reply.JobType = "reduce"
			reply.InterPaths = task.DataPath
			reply.JobNum = i
			reply.NReduce = m.nReduce
			task.State = inProgress
			m.workerList = append(m.workerList, WorkerInfo{
				JobType: "reduce",
				JobNum:  i,
			})
			return nil
		}
	}
	if m.Done() {
		return errors.New("out of jobs")
	}
	return nil
}

func (m *Master) MapTaskDone(args *MapDoneArgs, reply *MapDoneReply) error {
	m.mapTasks[args.JobNum].State = completed
	m.mapTasks[args.JobNum].InterPath = args.InterPath
	return nil
}

func (m *Master) ReduceTaskDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	m.reduceTasks[args.JobNum].State = completed
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := true
	if len(m.mapTasks) == 0 || len(m.reduceTasks) == 0 {
		return false
	}

	// Your code here.
	for _, v := range m.mapTasks {
		ret = ret && (v.State == completed)
	}

	for _, v := range m.reduceTasks {
		ret = ret && (v.State == completed)
	}

	return ret
}

func (m *Master) MapDone() bool {
	ret := true

	for _, v := range m.mapTasks {
		ret = ret && (v.State == completed)
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	go Run(&m, files, nReduce)

	return &m
}

func Run(m *Master, files []string, nReduce int) {
	fmt.Println(m.Done())
	m.nMap = len(files)
	m.mapTasks = make([]MapTask, m.nMap)
	m.nReduce = nReduce
	m.reduceTasks = make([]ReduceTask, m.nReduce)

	for i, filename := range files {
		m.mapTasks[i] = MapTask{
			DataPath:  filename,
			State:     idle,
			InterPath: []string{},
		}
	}

	m.server()
	fmt.Println(m)
	for !(m.MapDone()) {
		time.Sleep(time.Second)
	}
	fmt.Println("map Done")
	fmt.Println(m.mapTasks)
	var intermediate []string
	for _, task := range m.mapTasks {
		intermediate = append(intermediate, task.InterPath...)
	}
	for i, _ := range m.reduceTasks {
		m.reduceTasks[i].DataPath = intermediate
		m.reduceTasks[i].State = idle
	}
	fmt.Println(m.reduceTasks)
}
