package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "strconv"
import "io/ioutil"
import "os"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		// 1. request task from coordinator
		args := TaskArgs{}
		task := Task{}

		ok := call("Coordinator.DistributeTask", &args, &task)
		if !ok {
			continue 
		}
		
		// 2. perform assigned tasks 
		switch task.TaskType {
			case "MapTask":
				// fmt.Println("Start Map phase")
				err := perform_map(mapf, &task)
				
				report_args := InfoToCoordinatorArgs{}
				report_reply := InfoToCoordinatorReply{}
				
				if err == nil {
					report_args.Status = 2
				} else {
					report_args.Status = 0
				}
				report_args.Task_id = task.Map_task_id
				report_args.Task_type = "Map"
				report_args.IntermediateFile = task.IntermediateFile

				// 2.1 Reporting task completion to coordinator via rpc 
				ok := call("Coordinator.ReportHandler", &report_args, &report_reply)

				if !ok {
					continue 
				}

			case "ReduceTask":
				// fmt.Println("Start Reduce phase")
				err := perform_reduce(reducef, &task)

				report_args := InfoToCoordinatorArgs{}
				report_reply := InfoToCoordinatorReply{}
				report_args.Task_id = task.Reduce_task_id 
				report_args.Task_type = "Reduce"
				if err == nil {
					report_args.Status = 2 
				} else {
					report_args.Status = 0
				}
				// 2.2 Reporting reduce task completion to coordinator via rpc 
				ok := call("Coordinator.ReportHandler", &report_args, &report_reply)

				if !ok {
					continue 
				}
		} 
		
		if task.AllTaskFinished {
			break
		}
	}

}

func perform_map(mapf func(string, string)[]KeyValue, task *Task) error {

	// Open and read File and call map function 
	inputFileName := task.InputFiles

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", inputFileName)
	}

	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", inputFileName)
	}

	inputFile.Close()
	
	kva := mapf(inputFileName, string(content))

	// create intermediate files based on number of reduce 
	intermediate_files := make([][]KeyValue, task.N_reduce)

	for _,kv := range kva {
		intermediate_files[ihash(kv.Key)%task.N_reduce] = append(intermediate_files[ihash(kv.Key)%task.N_reduce], kv)
	}

	var intermediate_file_names []string
	// Write ntermediate_files into json
	for i := range intermediate_files {
		intermediate_file_name := "mr-" + strconv.Itoa(task.Map_task_id) + "-" + strconv.Itoa(i)

		temp_file, _ := ioutil.TempFile(".", "")
		enc := json.NewEncoder(temp_file)
		
		for _, kv := range intermediate_files[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write into %v", kv.Key)
			}
		}
		os.Rename(temp_file.Name(), intermediate_file_name)
		intermediate_file_names = append(intermediate_file_names, intermediate_file_name)
		temp_file.Close()
	}
	task.IntermediateFile = intermediate_file_names
	return nil
}

func perform_reduce(reducef func(string, []string)string, task *Task) error {
	// Step 1: Read Intermediate file from mr-map_task_all_reduce_task Key-Value Pairsa
	intermediate := []KeyValue{}
	for _, intermediate_file_name := range(task.IntermediateFile){
		file, err := os.Open(intermediate_file_name)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate_file_name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
    // Step 2: Group Values by Key
	sort.Sort(ByKey(intermediate))
	
	// Define output file 
	output_file_name := "mr-out-" + strconv.Itoa(task.Reduce_task_id)
	// create a temporary file in case its not working 
	temp_file, _ := ioutil.TempFile(".", "")

    // Step 3: Call Reduce Function
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

		fmt.Fprintf(temp_file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
    // Step 4: Write Reduce Output
	temp_file.Close()
	err := os.Rename(temp_file.Name(), output_file_name)
	if err != nil {
		return err
	}
	
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
	
	// fmt.Println(err)
	return false
}
