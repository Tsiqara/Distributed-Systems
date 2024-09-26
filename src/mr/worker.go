package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		args := GiveTaskReply{}
		reply := GiveTaskReply{}

		ok := call("Coordinator.GiveMeTask", &args, &reply)
		if ok {
			//fmt.Println(reply)
			if reply.Exit {
				os.Exit(0)
			}
			if reply.Wait {
				time.Sleep(time.Second)
				continue
			}
			switch reply.Task.TypeIsMap {
			case true:
				Map(&reply, mapf)
			case false:
				Reduce(&reply, reducef)
			default:
				time.Sleep(time.Second)
			}
		} else {
			fmt.Printf("call failed!\n")
			os.Exit(0)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func Reduce(reply *GiveTaskReply, reducef func(string, []string) string) {
	var inputFiles []string
	for i := 0; i < reply.NFiles; i++ {
		inputFiles = append(inputFiles, fmt.Sprintf("mr-%v-%v", i, reply.TaskNumber))
	}
	reply.Task.InputFiles = inputFiles

	intermediate := []KeyValue{}
	for _, filename := range reply.Task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reply.TaskNumber)
	ofile, _ := os.CreateTemp("", oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
	reply.Task.Done = true
	replyEmp := GiveTaskReply{}
	call("Coordinator.FinishedTask", &reply, &replyEmp)
}

func Map(reply *GiveTaskReply, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, reply.NReduce)

	filename := reply.Task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % reply.NReduce
		intermediate[reduceNum] = append(intermediate[reduceNum], kv)
	}

	for rn, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskNumber, rn)
		ofile, _ := os.CreateTemp("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	reply.Task.Done = true
	replyEmp := GiveTaskReply{}
	call("Coordinator.FinishedTask", &reply, &replyEmp)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
