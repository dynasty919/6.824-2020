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

//负责分发任务的调度器。这个吊玩意能称得上是调度器实在是让人笑掉大牙，他做的实际上就是反复的遍历m里面的mapTasks和reduceTasks
//这两个数据结构，检查里面的任务的state，这种遍历的方法也就这种jobs数比较小所以没有太慢，正常来说应该用channel来做的，后面
//考虑重新用channel做一遍
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

//负责map工作的worker完成工作后，调这个rpc函数来通知master工作完成，注意如果这个worker负责的工作已经完成了，就直接
//返回，否则会重复执行m.mapWt.Done()
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

//负责reduce工作的worker完成工作后，调这个rpc函数来通知master工作完成，注意如果这个worker负责的工作已经完成了，就直接
//返回，否则会重复执行m.reduceWt.Done()
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
//用reduce的wait group变量来控制全部任务是否结束，这个函数只能给外层的main.go来调，如果随便调的话
//，因为我各种地方加了一堆锁，所以很有可能会死锁
func (m *Master) Done() bool {
	m.reduceWt.Wait()
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
//主程序，先创建一个master， 加进map任务，然后就可以把master的指针返回去了，然后通过Run这个goroutine来跑流程
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

//先跑负责fault tolerance的WorkerWatcher，然后用m.mapWt这个wait group来控制流程，保证所有map job结束后才开始
//分配reduce job，之后在m里加入reduce job
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

//负责容错性，不停的从workerChan中拉出注册进来的worker，然后给它分配一个单独的看门狗
func WorkerWatcher(m *Master) {
	for {
		select {
		case worker := <-m.workerChan:
			go WatchDog(m, worker)
		}
	}
}

//检查这个worker负责的job10秒钟之后是否完成，如果没完成就task状态改回idle
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
