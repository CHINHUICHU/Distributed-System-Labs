package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1

	// for _, filename := range os.Args[2:] {
	// 	file, err := os.Open(filename)
	// 	if err != nil {
	// 		log.Fatalf("cannot open %v", filename)
	// 	}
	// 	content, err := ioutil.ReadAll(file)
	// 	if err != nil {
	// 		log.Fatalf("cannot read %v", filename)
	// 	}
	// 	file.Close()
	// 	kva := mapf(filename, string(content))
	// 	intermediate = append(intermediate, kva...)
	// }

	reply.FileName = os.Args[2]
	return nil
}

func (c *Coordinator) GetFileName(args *ExampleArgs, reply *ExampleReply) error {
	reply.FileName = os.Args[2]
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
