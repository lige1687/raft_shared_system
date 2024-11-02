package mr

import (
	"fmt"

	"log"
	"strconv"
	"strings"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段( 如map阶段, reduce阶段等
	TaskChannelReduce chan *Task     // 使用chan保证并发安全 ( 尤其在分配任务的时候
	TaskChannelMap    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // stash tasks
	files             []string       // 传入的文件数组

}

// TaskMetaHolder 保存全部任务的元数据 ! 元数据即数据的数据
// meta 即元的意思是吧
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
	// int应该是每个任务对应的 独特的 序号
}

// TaskMetaInfo 保存任务的元数据 结构体
type TaskMetaInfo struct {
	state   State // 任务的状态 , 完成还是?
	TaskAdr *Task // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成 ( 因为指针能保证 修改到task任务本身
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
// 对于一个协作者 类型, 启动一个server , 监听 所有的rpc请求

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
// Done 主函数mr调用，如果所有tas k完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)), // 对应map任务的个数, 即文件的长度,
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
			// files对应了map worker 的个数? 即map task 的个数
		},
	}
	c.makeMapTasks(files) // 即产生一系列的maptask  ,分配给 worker

	c.server() // 启动这个协作者的server
	return &c  // 返回产生的协作者的 地址 !
}

// 将任务标记为完成的rpc调用, 给一个任务列表, 将其标记为完成
// 如何标记? 通过协作者中的 taskholder 的状态的修改
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:

		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
		break

	default:
		panic("The task type undefined ! ! !")
	}
	return nil //返回错误值
}
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 分发任务应该上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()

	// 判断任务类型存任务
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 { //表示任务还没发送完, 还有其余的任务
				*reply = *<-c.TaskChannelMap // 那么将任务放回到 reply的 参数中 ,此时 应该是worker通过 这个函数调用, reply 通过指针的形式
				//返回 task给 调用worker
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					// 判断任务的状态是正在做还是 ?   如果是正在做, 则返回真
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else { // 任务都发完了
				reply.TaskType = WaittingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting, 表示让你这个worker等着
				// 有的时候分完task不代表你的任务结束了 , 因为还有redone /fail的任务重新的发回 coordinator 的channel中
				if c.taskMetaHolder.checkTaskDone() { // 如果所有的任务都完成了, 就进入下一个阶段
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
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else { // 任务发完了
				reply.TaskType = WaittingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.taskMetaHolder.checkTaskDone() {
					// 并且都做完了
					c.toNextPhase()
				}
				return nil
			}
		}

	case AllDone: // 只有明确的所有的都做完了

		{
			reply.TaskType = ExitTask
		}
	default:
		panic("The phase undefined ! ! !")

	}

	return nil
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	return true
}

// 将当前的coordinator进入到下一个阶段
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase { // 如果处于 map阶段,
		//c.makeReduceTasks() , 可以产生一些新的 map任务

		// todo
		c.DistPhase = AllDone //
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// 检查多少个任务done 了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAdr.TaskType == MapTask {
			// 判断任务是否完成,下同
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
	//fmt.Printf("map tasks  are finished %d/%d, reduce task are finished %d/%d \n",
	//	mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true

	//
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

// 对map任务进行处理,初始化map任务 , 如将任务放到对应的 协作者的管道里, 准备发给worker
func (c *Coordinator) makeMapTasks(files []string) {

	for _, v := range files { // 遍历灭一个文件, 分配任务
		fileslice := []string

		fileslice[0] = v
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  fileslice,
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		//放入master用来存放所有 任务的holder中去

		fmt.Println("make a map task :", &task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i), // 选出文件名以 "mr-tmp" 开头，并以指定的 reduceNum 结尾的文件
			// 对应分区, 即要交给的reducer 是谁
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo) // 将其交给 协作者的holder, 总的记录一下

		//fmt.Println("make a reduce task :", &task)
		c.TaskChannelReduce <- &task // 将任务放到 协作者的 channel中去, 这是交给reducer 的通道
	}
}

// 因为一个reduce会有多个map文件输入, 所以通过这个方法 对应reducenum 的temp文件选出来
// reducenum对应了reducer , 类似于分区的任务
// 选出文件名以 "mr-tmp" 开头，并以指定的 reduceNum 结尾的文件
// 返回当前reudceNum的 文件名列表
func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()        //得到 当前工作路径, _ 表示忽略错误, 因为返回的第二个为error
	files, _ := os.ReadDir(path) // 使用 os.ReadDir , 读取路径下的所有文件
	for _, fi := range files {
		// 匹配对应的 reduce 文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

// 通过结构体的TaskId 自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {

	res := c.TaskId
	c.TaskId++
	return res
}

// 将接受taskMetaInfo储存进MetaHolder里
// holder是一个map, value对应了taskmetaInfo
// 传入一个 info , 将其放入holder
// 根据唯一的id, 将其
// 注意taskinfo 的包含内容为: 1. 任务状态, 2. task指针
// 因为要对holder进行修改, 所以传入 holder指针
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}
