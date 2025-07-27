package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "io/ioutil"
import "os"
import "sort"


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

// doMapTask 执行 Map 任务，读取输入文件内容，调用 mapf，
// 将结果分桶写入多个中间文件 (mr-X-Y)，X是map任务号，Y是reduce任务号
func doMapTask(reply TaskReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("doMapTask: task %d start\n", reply.TaskNumber)

	if len(reply.FileNames) == 0 {
		log.Printf("doMapTask: no input files for map task %d", reply.TaskNumber)
		return
	}

	filename := reply.FileNames[0] // Map任务只对应一个输入文件
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("doMapTask: cannot read %v", filename)
	}

	kva := mapf(filename, string(content))

	// 创建 nReduce 个文件，对应 nReduce 个 reduce 任务
	nReduce := reply.NReduce

	// 为每个 reduce 分区创建一个编码器，写入对应中间文件
	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediateFile := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, i)
		f, err := os.Create(intermediateFile)
		if err != nil {
			log.Fatalf("doMapTask: cannot create file %v", intermediateFile)
		}
		files[i] = f
		encoders[i] = json.NewEncoder(f)
	}

	// 按 key hash 分桶写入对应文件
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("doMapTask: encode error: %v", err)
		}
	}

	// 关闭所有文件
	for _, f := range files {
		f.Close()
	}

	// TODO: 任务完成后，向 Coordinator 汇报（需要你实现 RPC 调用）
	fmt.Printf("doMapTask: task %d done\n", reply.TaskNumber)
	doneArgs := TaskCompleteArgs{
		TaskType:   "Map",
		TaskNumber: reply.TaskNumber,
	}
	doneReply := TaskCompleteReply{}
	call("Coordinator.ReportTaskDone", &doneArgs, &doneReply)
}

// doReduceTask 执行 Reduce 任务，读取所有对应的中间文件，
// 解码合并 KeyValue，排序，调用 reducef，输出最终结果文件 mr-out-X
func doReduceTask(reply TaskReply, reducef func(string, []string) string) {
	fmt.Printf("doReduceTask: task %d start\n", reply.TaskNumber)

	nReduce := reply.NReduce
	taskNum := reply.TaskNumber

	// 读取所有 map 任务生成的对应 reduce 分区的中间文件
	var kva []KeyValue
	for mapTaskNum := 0; mapTaskNum < nReduce; mapTaskNum++ {
		intermediateFile := fmt.Sprintf("mr-%d-%d", mapTaskNum, taskNum)
		file, err := os.Open(intermediateFile)
		if err != nil {
			// 如果文件不存在，跳过（可能某些任务不产生该文件）
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 按 Key 排序
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 输出文件
	outFileName := fmt.Sprintf("mr-out-%d", taskNum)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("doReduceTask: cannot create output file %v", outFileName)
	}
	defer outFile.Close()

	// 按 Key 分组，调用 reducef 写结果
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	fmt.Printf("doReduceTask: task %d done\n", taskNum)

	// TODO: 任务完成后，向 Coordinator 汇报（需要你实现 RPC 调用）
	doneArgs := TaskCompleteArgs{
		TaskType:   "Reduce",
		TaskNumber: reply.TaskNumber,
	}
	doneReply := TaskCompleteReply{}
	call("Coordinator.ReportTaskDone", &doneArgs, &doneReply)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// 1. 请求任务
		args := TaskRequestArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			// RPC调用失败，可能Coordinator已退出，Worker也退出
			log.Println("Coordinator not responding, exiting")
			return
		}

		switch reply.TaskType {
		case "Map":
			doMapTask(reply, mapf)
		case "Reduce":
			doReduceTask(reply, reducef)
		case "Wait":
			time.Sleep(time.Second) // 等待1秒再请求
		case "Exit":
			log.Println("All tasks done, worker exiting")
			return
		default:
			log.Printf("Unknown task type: %v", reply.TaskType)
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
