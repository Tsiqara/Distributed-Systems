package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	files             []string
	nReduce           int
	mapTasks          []Task
	reduceTasks       []Task
	mapDone           bool
	reduceDone        bool
	allDone           bool
	mapDoneIndexes    []int
	reduceDoneIndexes []int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GiveMeTask(args *GiveTaskReply, reply *GiveTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//fmt.Println(c.mapDoneNum, c.mapDone, c.reduceDoneNum)
	if !c.mapDone {
		for i, task := range c.mapTasks {
			if !task.Started || (!task.Done && time.Now().Sub(task.StartTime).Seconds() > 10) {
				c.mapTasks[i].StartTime = time.Now()
				c.mapTasks[i].Started = true
				task.StartTime = time.Now()
				task.TypeIsMap = true
				task.Started = true
				reply.Task = task
				reply.TaskNumber = i
				reply.NReduce = c.nReduce
				reply.NFiles = len(c.files)
				reply.Exit = false
				reply.Wait = false
				//fmt.Println(c.mapDoneNum, c.mapDone, c.reduceDoneNum, reply)
				return nil
			}
		}
	} else {
		if !c.reduceDone {
			for i, task := range c.reduceTasks {
				if !task.Started || (!task.Done && time.Now().Sub(task.StartTime).Seconds() > 10) {
					c.reduceTasks[i].StartTime = time.Now()
					task.StartTime = time.Now()
					task.TypeIsMap = false
					task.Started = true
					reply.Task = task
					reply.TaskNumber = i
					reply.NReduce = c.nReduce
					reply.NFiles = len(c.files)
					reply.Exit = false
					reply.Wait = false
					//fmt.Println(c.mapDoneNum, c.mapDone, c.reduceDoneNum, reply)
					return nil
				}
			}
		}
	}
	if c.allDone {
		reply.Exit = true
	}

	reply.Wait = true
	//fmt.Println(c.mapDoneNum, c.mapDone, c.reduceDoneNum, reply)
	return nil
}

func (c *Coordinator) FinishedTask(args *GiveTaskReply, reply *GiveTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Task.TypeIsMap {
		c.mapTasks[args.TaskNumber] = args.Task
		if !Contains(c.mapDoneIndexes, args.TaskNumber) {
			c.mapDoneIndexes = append(c.mapDoneIndexes, args.TaskNumber)
		}
		if len(c.mapDoneIndexes) == len(c.files) {
			c.mapDone = true
		}
	} else {
		c.reduceTasks[args.TaskNumber] = args.Task
		if !Contains(c.reduceDoneIndexes, args.TaskNumber) {
			c.reduceDoneIndexes = append(c.reduceDoneIndexes, args.TaskNumber)
		}
		if len(c.reduceDoneIndexes) == c.nReduce {
			c.reduceDone = true
			c.allDone = true
		}
	}
	return nil
}

func Contains(slice []int, num int) bool {
	for _, v := range slice {
		if v == num {
			return true
		}
	}
	return false
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		nReduce:           nReduce,
		mapDone:           false,
		reduceDone:        false,
		allDone:           false,
		mapTasks:          make([]Task, len(files)),
		reduceTasks:       make([]Task, nReduce),
		mapDoneIndexes:    []int{},
		reduceDoneIndexes: []int{},
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = Task{
			TypeIsMap:   true,
			Started:     false,
			Done:        false,
			StartTime:   time.Now(),
			InputFiles:  []string{c.files[i]},
			OutputFiles: nil,
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{
			TypeIsMap:   false,
			Started:     false,
			Done:        false,
			StartTime:   time.Now(),
			InputFiles:  nil,
			OutputFiles: []string{fmt.Sprintf("mr-out-%v", i)},
		}
	}

	c.server()
	return &c
}
