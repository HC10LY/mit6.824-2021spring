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

// MapReduce 任务请求参数
type TaskRequestArgs struct {
	WorkerId int // 暂时不需要，可以扩展
}

// MapReduce 任务分配返回
type TaskReply struct {
	TaskType   string   // "Map", "Reduce", "Wait", "Exit"
	FileNames  []string // Map任务输入文件名列表，Reduce任务中间文件名列表
	TaskNumber int      // 任务编号（map或reduce任务号）
	NReduce    int      // Reduce任务总数
}

// 上报完成任务的参数
type TaskCompleteArgs struct {
    TaskType   string // "Map" or "Reduce"
    TaskNumber int    // 完成的任务号
}

// 上报完成任务的回复（可以不需要特别内容）
type TaskCompleteReply struct{}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
