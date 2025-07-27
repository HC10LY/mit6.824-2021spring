package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskStatus int

const (
    TaskIdle TaskStatus = iota
    TaskInProgress
    TaskCompleted
)


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

    files []string
    nReduce int

    // 任务状态和分配时间，key 是任务号
    mapStatus    []TaskStatus
    reduceStatus []TaskStatus

    // 任务开始时间（用于超时重试）
    mapStartTime    []time.Time
    reduceStartTime []time.Time

    // 记录当前阶段：map阶段还是reduce阶段
    phase string // "map" 或 "reduce"
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    timeout := 10 * time.Second
    now := time.Now()

    // 超时重试：如果任务分配超过10秒还没完成，重置状态为Idle，方便重新分配
    if c.phase == "map" {
        for i := range c.mapStatus {
            if c.mapStatus[i] == TaskInProgress && now.Sub(c.mapStartTime[i]) > timeout {
                c.mapStatus[i] = TaskIdle
            }
        }
    } else if c.phase == "reduce" {
        for i := range c.reduceStatus {
            if c.reduceStatus[i] == TaskInProgress && now.Sub(c.reduceStartTime[i]) > timeout {
                c.reduceStatus[i] = TaskIdle
            }
        }
    }

    // 分配 map 任务
    if c.phase == "map" {
        for i, status := range c.mapStatus {
            if status == TaskIdle {
                reply.TaskType = "Map"
                reply.FileNames = []string{c.files[i]}
                reply.TaskNumber = i
                reply.NReduce = c.nReduce
                c.mapStatus[i] = TaskInProgress
                c.mapStartTime[i] = time.Now()
                return nil
            }
        }
        // 判断所有map任务是否完成，如果完成，切换阶段
        allDone := true
        for _, status := range c.mapStatus {
            if status != TaskCompleted {
                allDone = false
                break
            }
        }
        if allDone {
            c.phase = "reduce"
        } else {
            reply.TaskType = "Wait" // 等待已有任务完成
            return nil
        }
    }

    // 分配 reduce 任务
    if c.phase == "reduce" {
        for i, status := range c.reduceStatus {
            if status == TaskIdle {
                reply.TaskType = "Reduce"
                reply.TaskNumber = i
                reply.NReduce = c.nReduce
                reply.FileNames = nil
                c.reduceStatus[i] = TaskInProgress
                c.reduceStartTime[i] = time.Now()
                return nil
            }
        }

        // 判断 reduce 任务是否全部完成
        allDone := true
        for _, status := range c.reduceStatus {
            if status != TaskCompleted {
                allDone = false
                break
            }
        }
        if allDone {
            reply.TaskType = "Exit"  // 所有任务完成，worker退出
            return nil
        } else {
            reply.TaskType = "Wait"  // 等待已有任务完成
            return nil
        }
    }

    reply.TaskType = "Wait"
    return nil
}

func (c *Coordinator) ReportTaskDone(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if args.TaskType == "Map" {
        if c.mapStatus[args.TaskNumber] == TaskInProgress {
            c.mapStatus[args.TaskNumber] = TaskCompleted
        }
    } else if args.TaskType == "Reduce" {
        if c.reduceStatus[args.TaskNumber] == TaskInProgress {
            c.reduceStatus[args.TaskNumber] = TaskCompleted
        }
    }

    return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
    defer c.mu.Unlock()

    if c.phase == "reduce" {
        for _, status := range c.reduceStatus {
            if status != TaskCompleted {
                return false
            }
        }
        return true
    }
    return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
        files: files,
        nReduce: nReduce,
        phase: "map",
        mapStatus: make([]TaskStatus, len(files)),
        mapStartTime: make([]time.Time, len(files)),
        reduceStatus: make([]TaskStatus, nReduce),
        reduceStartTime: make([]time.Time, nReduce),
    }

    c.server()
    return &c
}
