package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

const (
	REQUEST = 0
	COMMIT  = 1
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// 请求参数
	for {
		args := Args{
			Argtype: REQUEST,
		}
		// 响应参数
		reply := Reply{}

		ok := call("Coordinator.Rpchandler", &args, &reply)
		if !ok {
			fmt.Printf("call failed\n")
		}
		// fmt.Printf("%d", reply.Tasktype)
		switch reply.Tasktype {
		case MAPTASK:
			// map
			maptask(mapf, reply.Task)
		case REDUCETASK:
			// task
			reducetask(reducef, reply.Task)
		case WAIT:
			time.Sleep(1 * time.Second)
		case FINISH:
			return
		}
	}
}

func maptask(mapf func(string, string) []KeyValue, task Task) {
	file, err := os.Open(task.Input[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.Input[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Input[0])
	}
	file.Close()
	kva := mapf(task.Input[0], string(content))
	// 将中间kv分类
	divied_kva := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		y := ihash(kv.Key) % task.NReduce
		divied_kva[y] = append(divied_kva[y], kv)
	}
	// 将分类kv写入中间临时文件
	for y, kvs := range divied_kva {
		oname := fmt.Sprintf("mr-%d-%d", task.Workerid, y)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", oname)
			}
		}
		ofile.Close()
	}
	commit(true, task.Taskid)
}
func reducetask(reducef func(string, []string) string, task Task) {
	intermediate := make(map[string][]string) // key:values
	for _, filename := range task.Input {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
		file.Close()
	}
	oname := fmt.Sprintf("mr-out-%d", task.Taskid)
	temp_file, err := ioutil.TempFile(".", "temp-mr-out-*")
	if err != nil {
		log.Fatalf("create temp_file failed")
		return
	}
	for k, v := range intermediate {
		output := reducef(k, v)
		fmt.Fprintf(temp_file, "%v %v\n", k, output)
	}
	// temp_file.Close()
	os.Rename(temp_file.Name(), oname)
	commit(false, task.Taskid)

}

func commit(ismap bool, taskid int) {
	commit_arg := Args{Ismap: ismap, Argtype: COMMIT, Taskid: taskid}
	commit_reply := Reply{}

	call("Coordinator.Rpchandler", &commit_arg, &commit_reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
