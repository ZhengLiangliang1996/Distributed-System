package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "strconv"


type Coordinator struct {
	// Your definitions here.
	n_reduce   int            // number of reduce 
	n_map      int            // number of mapper 

	map_tasks  []MapTask
	reduce_tasks  []ReduceTask

	mu_worker sync.Mutex

	IntermediateFile [][]string 
}

type MapTask struct {
	task_id int 
	task_status int // 0 not started, 1 started & running, 2 completed 
	filename string
}

type ReduceTask struct {
	task_id int 
	task_status int // 0 not started, 1 started, 2 completed 
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Distribute Task 
func (c *Coordinator) DistributeTask(args *TaskArgs, reply *Task) error {
	c.mu_worker.Lock() 

	defer c.mu_worker.Unlock()

	if !c.GetIfMapperFinished() {
		c.assignMapTask(reply)
		return nil
	} else if !c.GetIfReduceFinished() {
		c.assignReduceTask(reply)
		return nil 
	} else {
		// task all done 
		reply.AllTaskFinished = true 
		return nil
	}
	return nil 
}

// Get finished Task 
func (c *Coordinator) GetIfMapperFinished() bool {
	for _, m := range c.map_tasks {
		if m.task_status != 2 {
			return false 
		}
	}
	// fmt.Println("All mapper finished!")
	return true 
}

func (c *Coordinator) GetIfReduceFinished() bool {
	for _, m := range c.reduce_tasks {
		if m.task_status != 2 {
			return false 
		}
	}
	return true 
}

// assignMapTask assigns a Map task to a worker.
func (c *Coordinator) assignMapTask(reply *Task) error {
    // Check if the worker is already assigned a task.
    for i, m := range c.map_tasks {
		if m.task_status == 0 {
			// update task information and assign tasks 
			reply.TaskType = "MapTask"
			reply.Map_task_id = m.task_id 
			reply.InputFiles = m.filename
			reply.N_reduce = c.n_reduce
			reply.AllTaskFinished = false 
			//change status 
			m.task_status = 1 
			c.map_tasks[i] = m
			//heartbeat 
			go c.HeatbeatMap(i)
			return nil
		} 
	}
	return nil
}

// assignReduceTask assigns a Reduce task to a worker.
func (c *Coordinator) assignReduceTask(reply *Task) error {

	for i, r := range c.reduce_tasks {
		if r.task_status == 0 {
			// update reduce task information and assign tasks 
			reply.TaskType = "ReduceTask"
			reply.Reduce_task_id = r.task_id 
			reply.N_reduce = c.n_reduce 
			reply.AllTaskFinished = false 

			IntermediateFile := make([]string, 0)

			for _, arr := range c.IntermediateFile {
				IntermediateFile = append(IntermediateFile, arr[r.task_id])
			}
			reply.IntermediateFile = IntermediateFile

			r.task_status = 1
			c.reduce_tasks[i] = r 
			
			//heartbeat 
			go c.HeatbeatReduce(i)
			return nil 
		}
	}

	return nil
}

// heartbeatMap 
func (c * Coordinator) HeatbeatMap(task_id int) {
	time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
	c.mu_worker.Lock() 

	defer c.mu_worker.Unlock()
	// after 10 seconds, if intermediatefiles are created, means mapper[task_id] is finished
	if len(c.IntermediateFile[task_id]) == c.n_reduce {
		c.map_tasks[task_id].task_status = 2
	} else {
		// assuming it is dead (even its not :))
		c.map_tasks[task_id].task_status = 0
	}
	return 
}

// heartbeatReducer 
func (c * Coordinator) HeatbeatReduce(task_id int) {
	time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
	c.mu_worker.Lock() 

	defer c.mu_worker.Unlock()
	// after 10 seconds, if output files from reducer are created, means reducer[task_id] is finished
	output_file_name := "mr-out-" + strconv.Itoa(task_id)
	if _, err := os.Stat(output_file_name); err == nil {
		c.reduce_tasks[task_id].task_status = 2
	} else {
		// assuming it is dead (even its not :))
		c.reduce_tasks[task_id].task_status = 0
	}
	return 
}

// Mapper and Reducer report back to coordinator, handle report 
func (c * Coordinator) ReportHandler(args *InfoToCoordinatorArgs, reply *InfoToCoordinatorReply) error {
	c.mu_worker.Lock()

	defer c.mu_worker.Unlock()

	if args.Task_type == "Map" {
		c.IntermediateFile[args.Task_id] = args.IntermediateFile
		if args.Status == 2 {
			c.map_tasks[args.Task_id].task_status = 2
		} else {
			c.map_tasks[args.Task_id].task_status = 0
		}
	} else {
		if args.Status == 2 {
			c.reduce_tasks[args.Task_id].task_status = 2
		} else {
			c.reduce_tasks[args.Task_id].task_status = 0
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
	ret := false

	// Your code here.
	c.mu_worker.Lock()
	defer c.mu_worker.Unlock()
	if c.GetIfMapperFinished() && c.GetIfReduceFinished() {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.n_reduce = nReduce 

	//1. Task initialization 
	c.map_tasks = make([]MapTask, len(files))
	c.reduce_tasks = make([]ReduceTask, nReduce)
	//1.1 Map initizalition 
	for i, f := range files {
		c.map_tasks[i] = MapTask{task_id:i, task_status:0, filename:f}
	}

	//1.2 Reducer initilization 
	for i :=0; i < nReduce; i++ {
		c.reduce_tasks[i] = ReduceTask{task_id:i, task_status:0}
	}
	c.IntermediateFile = make([][]string, len(files))
	c.server()
	return &c
}
