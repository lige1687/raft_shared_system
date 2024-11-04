package kvsrv

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// 先来一个kvmap 存储所有数据
	data   map[string]string
	status map[int64]string // 记录不同的请求 id 的之前的result 结果
	// status是一个缓存, 对于请求的缓存, 而data是一个持久化的数据, 如此设计的
	// lastSeenRequest map[int64]bool // 记录重复请求, key是 request 的id , 如果是重复的请求, 就只能执行一个哦
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Confirm {
		if _, ok := kv.status[args.RequestID]; ok {
			delete(kv.status, args.RequestID)
		}
		return
	}
	if _, ok := kv.status[args.RequestID]; ok {
		reply.Value = kv.status[args.RequestID]
	} else {
		val := kv.data[key]
		reply.Value = val
		kv.status[args.RequestID] = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	fmt.Println("request " + strconv.FormatInt(args.RequestID, 10) + "get lock")

	defer kv.mu.Unlock() // get也需要 锁是因为 需要访问到 并发方便的一个map里边去

	// 检查是否是重复请求
	//	fmt.Println("request duplicated?")
	fmt.Println(kv.status[args.RequestID]) // 测试到确实重复了, 已经出现了
	if args.Confirm {
		// 表示这是一个重复的确认请求, 此时server可以删除缓存啦
		if _, ok := kv.status[args.RequestID]; ok {
			delete(kv.status, args.RequestID)
		}
		return
	}
	if _, ok := kv.status[args.RequestID]; ok {
		// 即没有confirm, 这个请求是第一次来
		// 并且已经存在 旧值 缓存的情况 , 就直接返回旧值即可
		// 这个相当于是一个缓存! 如果put缓存到了, 就直接返回
		reply.Value = kv.status[args.RequestID]

	} else {
		// 不存在, 就需要更新 同时的两个map 了
		val := kv.data[args.Key] // 将数据取给put 请求
		reply.Value = val
		kv.status[args.RequestID] = val // 更新旧值 缓存
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	fmt.Println("request " + strconv.FormatInt(args.RequestID, 10) + "get lock")
	defer kv.mu.Unlock()

	if args.Confirm {
		// 是一个重复请求, 让你取删除缓存
		if _, ok := kv.status[args.RequestID]; ok {
			delete(kv.status, args.RequestID)
		}
		return // 别忘记返回, 有的时候你就是忘记返回, 并且逻辑结果写的不严谨导致 出的bug

	}
	if _, ok := kv.status[args.RequestID]; ok { // 存在这个请求的旧值了
		// 直接对这个请求返回上一次请求的结果即可, 因为归根到底是一个请求 , 必须保证只能执行一次!
		reply.Value = kv.status[args.RequestID]

	} else {
		val := kv.data[args.Key]
		kv.data[args.Key] = val + args.Value // 新的结果是 拼接的
		reply.Value = val
		kv.status[args.RequestID] = val // 同时更新缓存结果
	}
}
func StartKVServer() *KVServer {
	kv := new(KVServer)

	// 初始化服务器的数据存储（例如一个 map）
	kv.data = make(map[string]string)

	// 初始化锁，确保数据访问时的线程安全
	kv.mu = sync.Mutex{}

	// 如果需要支持重复请求检测，可以初始化一个请求ID的记录
	kv.status = make(map[int64]string)
	// 记录已处理的请求ID , 以及这个请求 上一次来的时候 的结果 是?

	// 其他初始化代码，比如设置服务器的配置或日志
	fmt.Println("KVServer started")

	return kv
}
