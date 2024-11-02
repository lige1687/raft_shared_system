package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
// 根据设计, map 做的任务 即 产生一个 keyvalue 的中间结果 数组!
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	keepFlag := true
	for keepFlag {
		task := GetTask() // 请求任务
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task) // 如果maptask即
				// task传指针哦, !! 指针,因为task不是引用, 要改变他的值
				callDone()
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone()
			}
		case WaittingTask: // 即idle
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask: // 不工作了
			{
				fmt.Println("Task about :[", task.TaskId, "] is terminated...")
				keepFlag = false
			}

		}
	}
}

// 对之前得到的map中间文件进行洗牌shuffle ,得到一组 排序好的kv ,

// DoReduceTask 为什么是response ?因为这里是 通过rpc发来的reduce任务, 必须经过网络的步骤
// 并且此时网络一般是 json格式的, 所以洗牌的时候通过json解码
// 解码得到的还是 文件路径们 !只不过这个时候 所需要的文件已经拉取到本地了?直接open即可

// 之前的domaptask 的分区操作,  只能保证 所有的相同的key发送到一个 reduce 上, 而不能保证他们是有序的, 所以需要汇总一下进行洗牌
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice) // 汇总的结果 , 接下来当然是依次排序取出来, 然后 输出到文件里边
	dir, err := os.Getwd()                      // 工作路径
	if err != nil {
		return
	}
	// 设置输出文件的初始化
	tempfile, err := os.CreateTemp(dir, "mr-tmp-*")
	defer func(tempfile *os.File) {
		err := tempfile.Close()
		if err != nil {

		}
	}(tempfile)
	if err != nil {
		return
	}
	i := 0
	for i < len(intermediate) { // 对于每一个 key都产生一个文件吗?当然不是, 对于汇总好的洗牌好的key 和对应的值
		//reduce负责产生一个总的文件, 就像paper中说的, 最后经过reducef, 只能产生一个输出文件, 对于一个key 只有一个reducef后的结果!
		j := i + 1 // 找到同一个key下的所有 kv
		for j < len(intermediate) && intermediate[j] == intermediate[i] {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value) //将结果加入到 同一个结果集中
			// 注意一个append 的用法, 将第二个value  append到 第一个参数中!
		}
		output := reducef(intermediate[i].Key, values) // 将 kv交给reduce 函数,  即这个key下的所有结果
		//reducef会对结果 进行处理的, 如求sum或者怎么样, 不知道, 看reducef 的定义
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output) // 将结果答应道tempfile中去
		i = j                                                         // 更新i 为j从下一个k开始
	}
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum) // 产生一个新的文件名
	os.Rename(tempfile.Name(), fn)                //将tempfile重命名为临时文件

}

// 按 Key 排序的自定义排序类型
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 虽然partition保证了相同的key能发往一个reduce 但是还没排序呢
// 先从本地读取到对应的文件们
func shuffle(files []string) []KeyValue {

	var kva []KeyValue

	for _, filepath := range files { // 也是
		file, err := os.Open(filepath)
		defer file.Close()
		if err != nil {
			return nil
		}
		dec := json.NewDecoder(file) //文件的解码器, json格式的而已, 一个访问文件的方式
		//
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// 逐行的解码, 每一行对应一个key 一个value  , 将其放入kv中
				// 并且err 如果成立表示读到结尾了 , 需要返回
				// 如果没有读到就是 在文件里边有一个指针, 一直每行的读每次返回一个结果到kv中
				// 其实就是一个用法的问题
				break
			}
			kva = append(kva, kv) // 这里的思路很简单
			//就是直接全都汇总到一个大的slice里边, 直接整体排序即可! 这样就能得到 总的结果了

		}

	}
	sort.Sort(ByKey(kva))
	return kva
}

// mapf 的定义! 应该是 访问一个文件, 返回kv , 而第二个参数string 其实是文件的字节数组, 我们要通过读取文件, 返回每一行和其value 作为一个kv
// paper中也讲过了, 用line number 为k , content为每一行的内容, 作为 value 来划分结构体
// 实际 如何划分kv ? 是mapf关注的事情, 我们知道他返回的是一组kv 即可, 我们要处理的是kv
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue       // 定义中间的kv 数组
	filename := response.FileSlice[0] // 因为map任务只有一个文件, 所以取第一个参数即可

	file, err := os.Open(filename) // 打开文件
	if err != nil {
		// 日常判断一下是不是 错误
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取content,作为mapf的参数 ( 因为是本地读取map所需要的data文件
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map返回一组KV结构体数组, 是读取文件后的结果
	intermediate = mapf(filename, string(content))

	rn := response.ReducerNum // 根据论文, nreduce是 中间文件的个数, 发给几个reducer的意思吗?
	// 创建一个长度为nReduce的二维切片 , 这个切片的作用应该是 根据 每一个intermediate 的key 和value, 讲对应的 key 的value 聚集起来
	// 每一个元素是一个kv键值对, 即一个二维的map
	// 存放的每一行即这个分区 对应的所有的kv门
	// 保证了 相同的key 会在一个reduce 的任务中 , 即保证了一个key会被传递给 了一个reducer
	//实际是一个分区partition操作 !
	HashedKV := make([][]KeyValue, rn)

	// 通过一个hash函数, 来将所有值分类, 分到几个文件上, 保证一个key在一个reduce任务里( 因为reduce阶段一个key必须在一个reduce上完成
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ { //
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile) // 使用json 输出文件
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

// callDone Call RPC to mark the task as completed
// 此时我们的目的不是 调用rpc进行什么信息传递, 只是告诉coordinator当前这个work的任务完成了!
// 所以创建一个空的结构体task的, 主要是为了返回一个rpcname , 即 taskFinished 的信息!
// call函数是 master提供的 rpc函数!!, 可以交互一些信息和worker
func callDone() Task {

	args := Task{}
	reply := Task{}
	//
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

// uncomment to send the Example RPC to the coordinator.
// CallExample()
// GetTask 获取任务（需要知道是Map任务，还是Reduce）
func GetTask() Task {

	args := TaskArgs{} //
	reply := Task{}
	// 表示reply是一个task 的结构体的实例, 用于存储master返回的任务
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply // 返回task !

}

// CallExample example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// 总之 这个例子代码告诉你了, 创建一个空的reply 进去, 等待call结束, 即可接收到这个reply!

	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator. rpcname
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response. 发送请求给cordiantor
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname) // 通过rpc调用链接到了一个客户端 , c 即客户端

	// c即client
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // 通过客户端调用
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
