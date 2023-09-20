package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE = iota
	PROCESS
	COMPLETED
)
const (
	MAPPHASE = iota
	REDUCEPHASE
	DONE
)
const (
	MAPTASK = iota
	REDUCETASK
	WAIT
	FINISH
)

type Coordinator struct {
	files   []string
	nReduce int

	maptasks    []*Task
	reducetasks []*Task
	phase       int
	workseq     int
	taskmap     map[int]int // kv --> taskid, workerid

	lock sync.Mutex
}
type Task struct {
	Status    int
	Taskid    int
	Workerid  int
	startTime time.Time
	Input     []string
	NReduce   int
}

// RPC handlers for the worker to call.

func (c *Coordinator) Rpchandler(args *Args, reply *Reply) error {
	reply.Tasktype = WAIT
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.phase == DONE {
		reply.Tasktype = FINISH
		return nil
	}
	if args.Argtype == REQUEST {
		if c.phase == MAPPHASE {
			for _, task := range c.maptasks {
				// 判断任务是否超时
				if task.Status == PROCESS {
					if task.startTime.Add(10 * time.Second).Before(time.Now()) {
						task.Status = IDLE
					}
				}
				if task.Status == IDLE {
					task.Status = PROCESS
					task.startTime = time.Now()
					task.Workerid = c.workseq
					task.NReduce = c.nReduce
					//task.Input = append(task.Input, c.files[c.mapid])
					reply.Task = *task
					reply.Tasktype = MAPTASK
					c.taskmap[task.Taskid] = c.workseq
					c.workseq++
					return nil
				}
			}
		}
		if c.phase == REDUCEPHASE {
			for reduceid, task := range c.reducetasks {
				if task.Status == PROCESS {
					if task.startTime.Add(10 * time.Second).Before(time.Now()) {
						task.Status = IDLE
					}
				}
				if task.Status == IDLE {
					task.Status = PROCESS
					task.startTime = time.Now()
					task.NReduce = reduceid
					task.Input = make([]string, 0)
					for _, workid := range c.taskmap {
						task.Input = append(task.Input, fmt.Sprintf("mr-%d-%d", workid, reduceid))
					}
					reply.Task = *task
					reply.Tasktype = REDUCETASK
					return nil
				}
			}
		}
	}
	if args.Argtype == COMMIT {
		if args.Ismap {
			c.maptasks[args.Taskid].Status = COMPLETED
			for _, t := range c.maptasks {
				if t.Status != COMPLETED {
					return nil
				}
			}
			c.phase = REDUCEPHASE
		} else {
			c.reducetasks[args.Taskid].Status = COMPLETED
			for _, t := range c.reducetasks {
				if t.Status != COMPLETED {
					return nil
				}
			}
			c.phase = DONE
		}

	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// // Your code here.

	// return ret
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.phase == DONE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// c := Coordinator{}
	// mr_files, _ := filepath.Glob("mr-*")
	// for _, mr_file := range mr_files {
	// 	os.Remove(mr_file)
	// }
	c := Coordinator{files: files,
		nReduce: nReduce,
		phase:   MAPPHASE,
		workseq: 0,
		taskmap: make(map[int]int, len(files)),
	}
	for maptaskid, file := range files {
		c.maptasks = append(c.maptasks, &Task{Status: IDLE,
			Workerid: 0,
			NReduce:  c.nReduce,
			Input:    []string{file},
			Taskid:   maptaskid})
	}
	for i := 0; i < nReduce; i++ {
		c.reducetasks = append(c.reducetasks, &Task{Status: IDLE, Taskid: i})
	}
	c.server()
	return &c
}
