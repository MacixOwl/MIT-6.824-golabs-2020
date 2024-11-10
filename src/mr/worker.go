package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var nReduce int

const TaskInterval = 100

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	n, succ := getReduceCnt() // call RPC to get nReduce
	if !succ {
		println("[Failed] Cannot get reduce task cnt number, worker exit.")
		return
	}
	nReduce = n //set nReduce only if rpc succ

	for {
		reply, succ := requestTask()

		if !succ {
			println("[Failed] Cannot contact master, worker exit.")
			return
		}
		if reply.TaskType == ExitTask {
			println("[Done] All task finished, worker exit.")
			return
		}

		exit, succ := false, true
		if reply.TaskType == NoTask { // empty task
			// ?
		} else if reply.TaskType == MapTask {
			doMap(mapf, reply.TasksFile, reply.TaskId)
			exit, succ = reportTaskDone(MapTask, reply.TaskId)
		} else if reply.TaskType == ReduceTask {
			doReduce(reducef, reply.TaskId)
			exit, succ = reportTaskDone(ReduceTask, reply.TaskId)
		}

		if exit || !succ {
			println("[Exit] All task finished or master exit, worker exit.")
			return
		}

		time.Sleep(time.Millisecond * TaskInterval)

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// make an RPC call to the master (funcs defined in rpc.go)
// return ReduceCnt as nReduce
func getReduceCnt() (int, bool) {
	args := GetReduceCntArgs{}
	reply := GetReduceCntReply{}
	// send the RPC request, wait for the reply.
	succ := call("Master.GetReduceCnt", &args, &reply)

	return reply.ReduceCnt, succ
}
func requestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{os.Getpid()}
	reply := RequestTaskReply{}
	succ := call("Master.RequestTask", &args, &reply)

	return &reply, succ
}
func doMap(mapf func(string, string) []KeyValue, filename string, mapId int) {
	// steal from main/mrsequential.go - func main
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open file %v\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot open file %v\n", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	writeMapOutput(kva, mapId)
}

// TODO: this func needs to be reviewed
func writeMapOutput(kva []KeyValue, mapId int) {
	// use io buffers to reduce disk I/O, which greatly improves
	// performance when running in containers with mounted volumes
	prefix := fmt.Sprintf("%v/mr-%v", TmpDir, mapId) // TmpDir defined in master.go
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	// create tmp files, use <pid> to uniquely identify this worker
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Cannot open file %v\n", filename)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write map outputs to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot open file %v\n", kv)
		}
	}

	// flush file buffer to disk
	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			log.Fatalf("Cannot open file %v\n", files[i].Name())
		}
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("Cannot open file %v\n", file.Name())
		}
	}
}

// TODO: this func needs to be reviewed
func doReduce(reducef func(string, []string) string, reduceId int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TmpDir, "*", reduceId))
	if err != nil {
		log.Fatalf("Cannot open reduce files.") // .
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("[Failed] Cannot open file %v\n", filename)
		}
		// this decorder part writes in MIT's Lab1 Hints
		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatalf("[Failed] Cannot decode file %v\n", filename)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	writeReduceOutput(reducef, kvMap, reduceId)
}

// TODO: this func needs to be reviewed
func writeReduceOutput(reducef func(string, []string) string,
	kvMap map[string][]string, reduceId int) {

	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	filename := fmt.Sprintf("%v/mr-out-%v-%v", TmpDir, reduceId, os.Getpid())
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("[Failed] Cannot create file %v\n", filename)
	}

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		if err != nil {
			log.Fatalf("[Failed] Cannot write mr output (%v, %v) to file", k, v)
		}
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filename, newPath)
	if err != nil {
		log.Fatalf("[Failed] Cannot rename file %v\n", filename)
	}
}

func reportTaskDone(taskType TaskType, taskId int) (bool, bool) {
	args := ReportTaskArgs{os.Getpid(), taskType, taskId}
	reply := ReportTaskReply{}
	succ := call("Master.ReportTaskDone", &args, &reply)

	return reply.IfExit, succ
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
