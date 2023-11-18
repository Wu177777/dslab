package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	ReduceNum         int
	TaskId            int
	DistPhase         Phase
	TaskChannelReduce chan *Task
	TaskChannelMap    chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state     State
	TaskAdr   *Task
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

var mu sync.Mutex

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
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Println("All done!")
		return true
	} else {
		return false
	}
	// ret := false

	// // Your code here.

	// return ret
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Now().Sub(v.StartTime) > time.Second*10 {
				fmt.Printf("task id: %d is crashed\n, teake %d s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				v.state = Waiting
				if v.TaskAdr.TaskType == MapTask {
					c.TaskChannelMap <- v.TaskAdr
				} else {
					c.TaskChannelReduce <- v.TaskAdr
				}
			}

		}
		mu.Unlock()

	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReduceNum:         nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.makeMapTask(files)

	// Your code here.

	c.server()

	go c.CrashDetector()
	return &c
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]

	if meta != nil {
		fmt.Println("task id: ", taskId, " already exist")
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func (c *Coordinator) makeMapTask(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReduceNum,
			FileSlice: []string{v},
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("make a map task:", &task)
		c.TaskChannelMap <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp-") && strings.HasSuffix(fi.Name(), "-"+strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			FileSlice: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("make a reduce task:", &task)
		c.TaskChannelReduce <- &task
	}
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task %d done!\n", args.TaskId)
		} else {
			fmt.Printf("Map task %d is already finished!\n", args.TaskId)
		}
		break
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		if ok && meta.state == Working {
			meta.state = Done
		} else {
			fmt.Printf("Reduce task %d is already finished!\n", args.TaskId)
		}
		break
	default:
		panic("The task type is not supported")
	}
	return nil
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("task id: %d running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					fmt.Println("1245")
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("task id: %d running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil

}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}
