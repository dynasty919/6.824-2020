package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
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
	JobType   string
	JobNum    int
	TimeStamp int64
}

type Master struct {
	// Your definitions here.
	lock        sync.Mutex
	mapWt       sync.WaitGroup
	reduceWt    sync.WaitGroup
	nMap        int
	nReduce     int
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	workerChan  chan WorkerInfo
	//	workerDoneChan chan WorkerInfo
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
	fmt.Println("worker" + strconv.Itoa(args.WorkerNum) + "entering scheduler")
	m.lock.Lock()

	fmt.Println("in scheduler")
	for i, task := range m.mapTasks {
		if task.State == idle {
			reply.JobType = "map"
			reply.InputPath = task.DataPath
			reply.JobNum = i
			reply.NReduce = m.nReduce
			m.mapTasks[i].State = inProgress
			m.workerChan <- WorkerInfo{
				JobType:   "map",
				JobNum:    i,
				TimeStamp: time.Now().Unix(),
			}
			m.lock.Unlock()
			fmt.Println("map job distributed")
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
			m.reduceTasks[i].State = inProgress
			m.workerChan <- WorkerInfo{
				JobType: "reduce",
				JobNum:  i,
			}
			m.lock.Unlock()
			fmt.Println("reduce job distributed")
			return nil
		}
	}
	m.lock.Unlock()
	fmt.Println("currently no idle jobs, calling later")
	return nil
}

func (m *Master) MapTaskDone(args *MapDoneArgs, reply *MapDoneReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	//m.workerDoneChan <- WorkerInfo{
	//	JobType:   "map",
	//	JobNum:    args.JobNum,
	//	TimeStamp: time.Now().Unix(),
	//}
	if m.mapTasks[args.JobNum].State == completed {
		return nil
	}
	m.mapTasks[args.JobNum].State = completed
	m.mapTasks[args.JobNum].InterPath = args.InterPath
	m.mapWt.Done()
	return nil
}

func (m *Master) ReduceTaskDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	//m.workerDoneChan <- WorkerInfo{
	//	JobType:   "reduce",
	//	JobNum:    args.JobNum,
	//	TimeStamp: time.Now().Unix(),
	//}
	if m.reduceTasks[args.JobNum].State == completed {
		return nil
	}
	m.reduceTasks[args.JobNum].State = completed
	m.reduceWt.Done()
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
	m.reduceWt.Wait()
	return true
	//ret := true
	//
	//m.lock.Lock()
	//defer m.lock.Unlock()
	//
	//if len(m.mapTasks) == 0 || len(m.reduceTasks) == 0 {
	//	ret = false
	//}
	//
	//// Your code here.
	//for _, v := range m.mapTasks {
	//	ret = ret && (v.State == completed)
	//}
	//
	//for _, v := range m.reduceTasks {
	//	ret = ret && (v.State == completed)
	//}
	//
	//return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]MapTask, len(files)),
		reduceTasks: make([]ReduceTask, nReduce),
		workerChan:  make(chan WorkerInfo),
		//		workerDoneChan: make(chan WorkerInfo),
	}

	// Your code here.

	m.mapWt.Add(m.nMap)
	m.reduceWt.Add(m.nReduce)
	for i, filename := range files {
		m.mapTasks[i] = MapTask{
			DataPath:  filename,
			State:     idle,
			InterPath: []string{},
		}
	}
	go Run(&m, files, nReduce)
	return &m
}

func Run(m *Master, files []string, nReduce int) {

	go WorkerWatcher(m)
	m.server()

	m.mapWt.Wait()
	fmt.Println("map Done")

	m.lock.Lock()
	defer m.lock.Unlock()

	var intermediate []string
	for _, task := range m.mapTasks {
		intermediate = append(intermediate, task.InterPath...)
	}
	for i, _ := range m.reduceTasks {
		m.reduceTasks[i].DataPath = intermediate
		m.reduceTasks[i].State = idle
	}
}

//WorkerWatcher
func WorkerWatcher(m *Master) {
	for {
		select {
		case worker := <-m.workerChan:
			go WatchDog(m, worker)
		default:
			continue
		}
	}
}

func WatchDog(m *Master, worker WorkerInfo) {
	time.Sleep(time.Second * 10)
	m.lock.Lock()
	if worker.JobType == "map" {
		if m.mapTasks[worker.JobNum].State != completed {
			m.mapTasks[worker.JobNum].State = idle
		}
	} else if worker.JobType == "reduce" {
		if m.reduceTasks[worker.JobNum].State != completed {
			m.reduceTasks[worker.JobNum].State = idle
		}
	} else {
		log.Fatalln("wrong job type", worker.JobType)
	}
	m.lock.Unlock()
}
