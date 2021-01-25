package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	args := MyArgs{}
	reply := MyReply{
		JobType:    "",
		JobNum:     0,
		InputPath:  "",
		InterPaths: nil,
		nReduce:    0,
	}
	for {
		err := call("master.Scheduler", &args, &reply)
		if err != nil {
			break
		}

		if reply.JobType == "map" {
			doMap(mapf, reply.InputPath, reply.JobNum, reply.nReduce)
		} else if reply.JobType == "reduce" {
			doReduce(reducef, reply.InterPaths, reply.JobNum)
		} else {
			time.Sleep(time.Second)
			continue
		}
	}
	return
}

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
	err = call("master.MapTaskDone", &args, &reply)
	if err != nil {
		log.Fatal(err)
	}

}

func doReduce(reducef func(string, []string) string, filenames []string, jobNum int) {
	name := "mr-out-" + strconv.Itoa(jobNum)
	out, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	for _, filename := range filenames {
		var kva []KeyValue

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

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(out, "%v %v\n", kva[i].Key, output)

			i = j
		}
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
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
