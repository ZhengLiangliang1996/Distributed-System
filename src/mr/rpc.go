package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	TaskType string           // Map or Reduce 

	Map_task_id int            // TaskNumber  
	Reduce_task_id int 

	N_reduce   int            // number of reduce 
	N_map      int            // number of mapper 

	InputFiles string       // InputFile name of Mapper 
	IntermediateFile []string // IntermediateFile name of Reducer 

	AllTaskFinished bool
}

type TaskArgs struct {}

type InfoToCoordinatorArgs struct {
	Task_type string
	Status int 
	Task_id int
	IntermediateFile []string
}

type InfoToCoordinatorReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
