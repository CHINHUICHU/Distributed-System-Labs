package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	args := RpcArgs{}
	// record map start time for each map task
	reply := RpcReply{}
	for {
		if call("Coordinator.DoTask", &args, &reply) {
			// check the task type
			if reply.Task == "map" {
				doMap(mapf, reply.FileName, reply.NReduce, reply.MapNumber)
				args.Task = "map"
			} else if reply.Task == "reduce" {
				doReduce(reducef, reply.NMap, reply.ReduceNumber)
				args.Task = "reduce"
			}
			// notify coordinator that the task is finished
			call("Coordinator.TaskFinished", &args, &reply)
		} else {
			// sleep for 3 seconds and retry
			time.Sleep(3 * time.Second)
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, fileName string, NReduce int, mapNumber int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}

	file.Close()

	kva := mapf(fileName, string(content))

	intermediate := make([]*os.File, NReduce)
	encoders := make([]*json.Encoder, NReduce)
	splitKv := make([][]KeyValue, NReduce)

	// 4. create intermediate files
	for i := 0; i < NReduce; i += 1 {
		tempFilename := fmt.Sprintf("temp-mr-%d-%d", mapNumber, i)
		tempFile, err := ioutil.TempFile("", tempFilename)
		if err != nil {
			log.Fatalf("cannot create temp file %v", tempFilename)
		}
		intermediate[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	// split key value pairs into buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % NReduce
		splitKv[bucket] = append(splitKv[bucket], kv)
	}

	// 5. write kv to intermediate files
	for i, bucket := range splitKv {
		for _, kv := range bucket {
			err := encoders[i].Encode(&kv)
			if err != nil {
				fmt.Println("encode failed")
			}
		}
		intermediate[i].Close()
	}

	// 6. rename temp files to final files
	for i := 0; i < NReduce; i += 1 {
		finalFilename := fmt.Sprintf("mr-%d-%d", mapNumber, i)
		err := os.Rename(intermediate[i].Name(), finalFilename)
		if err != nil {
			log.Fatalf("cannot rename %v", intermediate[i].Name())
		}
	}

}

func doReduce(reducef func(string, []string) string, NMap int, reduceNumber int) {
	kva := []KeyValue{}
	for i := 0; i < NMap; i += 1 {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v\n", filename)
			panic(err)
		}
		// decode error handling
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// EOF
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("temp-mr-out-%d", reduceNumber)
	ofile, _ := ioutil.TempFile("", oname)

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	for i := 0; i < NMap; i += 1 {
		// remove intermediate files
		filename := fmt.Sprintf("mr-%d-%d", i, reduceNumber)
		if err := os.Remove(filename); err != nil {
			log.Fatalf("cannot remove %v", filename)
		}
	}
	err := os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduceNumber))
	if err != nil {
		log.Fatalf("cannot rename %v", ofile.Name())
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
