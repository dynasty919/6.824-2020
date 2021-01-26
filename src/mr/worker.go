package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type InterFileInfo struct {
	name        string
	filePointer *os.File
	encoder     *json.Encoder
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
//worker里不涉及很多并发，所以比较简明，就是不停的call master找任务，根据任务类型选择执行do map还是do reduce
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	for {
		args := MyArgs{WorkerNum: rand.Intn(1000)}
		reply := MyReply{}
		fmt.Println("worker calling for a new job", args.WorkerNum)
		err := call("Master.Scheduler", &args, &reply)
		if err != nil {
			fmt.Println("master probably has exited", err)
			break
		}

		if reply.JobType == "map" {
			fmt.Println("doing map with", reply)
			doMap(mapf, reply.InputPath, reply.JobNum, reply.NReduce)
		} else if reply.JobType == "reduce" {
			fmt.Println("doing reduce with", reply)
			doReduce(reducef, reply.InterPaths, reply.JobNum)
		} else {
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
	}
	fmt.Println("worker out, searching for another job")
	return
}

//每个map job实际上要制造出nReduce个中间文件，中间文件的文件名是按照，mr-X-Y这种格式，X和Y分摩尔是map job和reduce job
// 的序号，Y由上面那个ihash函数决定
func doMap(mapf func(string, string) []KeyValue, filename string, jobNum int, nReduce int) {

	fileList := make([]InterFileInfo, nReduce)
	for i := 0; i < nReduce; i++ {
		pathName := "mr-" + strconv.Itoa(jobNum) + "-" + strconv.Itoa(i)
		interFile, err := os.Create(pathName)
		if err != nil {
			log.Fatal(err)
		}
		fileList[i].name = pathName
		fileList[i].filePointer = interFile
		encoder := json.NewEncoder(interFile)
		fileList[i].encoder = encoder
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal()
	}
	defer file.Close()
	fmt.Println("entering map function")
	kva := mapf(filename, string(contents))
	for _, kv := range kva {
		err := fileList[ihash(kv.Key)%nReduce].encoder.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < nReduce; i++ {
		fileList[i].filePointer.Close()
	}

	var interPath []string
	for _, v := range fileList {
		interPath = append(interPath, v.name)
	}

	args := MapDoneArgs{
		JobNum:    jobNum,
		InterPath: interPath,
	}
	reply := MapDoneReply{}
	fmt.Println("ready to call back to master")
	err = call("Master.MapTaskDone", &args, &reply)
	if err != nil {
		log.Fatal(err)
	}

}

//reduce job需要收到了全部的intermediate files的路径，但是只选出和自己负责的那部分files来读入，也就是nMaps个文件，然后
//执行reducef这个外部用户函数。这里面用了一个论文上说的技巧，就是先把输出文件做成一个temp file，最后全写完才改成最终合规的
//文件名，老实说我想了半天也没想出这么做的目的是啥
func doReduce(reducef func(string, []string) string, filenames []string, jobNum int) {

	out, err := ioutil.TempFile("", "tmpFile****")
	//	out, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	var kva []KeyValue
	for _, filename := range filenames {
		list := strings.Split(filename, "-")
		if len(list) != 3 {
			log.Fatalln("wrong inter file name", filename)
		}
		num, err := strconv.Atoi(list[2])
		if err != nil {
			log.Fatalln(err)
		}
		if num != jobNum {
			continue
		}

		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Slice(kva, func(i int, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(out, "%v %v\n", kva[i].Key, output)

		i = j
	}

	fmt.Println("changing name from" + out.Name() + "to" + strconv.Itoa(jobNum))
	os.Rename(out.Name(), "mr-out-"+strconv.Itoa(jobNum))

	arg := ReduceDoneArgs{JobNum: jobNum}
	reply := ReduceDoneReply{}
	err = call("Master.ReduceTaskDone", &arg, &reply)
	if err != nil {
		log.Fatal(err)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		fmt.Println("out of jobs")
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
