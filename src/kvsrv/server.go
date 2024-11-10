package kvsrv

import (
	"log"
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
	mu sync.RWMutex

	// Your definitions here.
	kvMap  map[string]string
	status map[int64]string //key: arg id; value: previous result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key

	// 只在必要时加读锁，避免阻塞其他读取操作
	kv.mu.RLock()
	if args.Confirm {
		kv.mu.RUnlock()
		kv.mu.Lock()
		// 确认模式下，删除已完成的请求，避免长期缓存
		if _, ok := kv.status[args.Id]; ok {
			delete(kv.status, args.Id)
		}
		kv.mu.Unlock()
		return
	}

	// 检查请求缓存，返回之前的结果
	if val, ok := kv.status[args.Id]; ok {
		reply.Value = val
		kv.mu.RUnlock()
		return
	}

	// 未命中缓存时从kvMap读取，并更新status缓存
	val := kv.kvMap[key]
	kv.mu.RUnlock()

	// 更新状态缓存，避免长时间持有写锁
	kv.mu.Lock()
	reply.Value = val
	kv.status[args.Id] = val
	kv.mu.Unlock()
}
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key

	// 确认模式下删除状态记录，避免长时间占用主流程
	if args.Confirm {
		kv.mu.Lock()
		if _, ok := kv.status[args.Id]; ok {
			delete(kv.status, args.Id)
		}
		kv.mu.Unlock()
		return
	}

	// 检查是否已有该请求的缓存结果，减少重复执行
	kv.mu.RLock()
	if val, ok := kv.status[args.Id]; ok {
		reply.Value = val
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	// 获取当前值并执行更新操作
	kv.mu.Lock()
	val := kv.kvMap[key]
	kv.kvMap[key] = args.Value
	reply.Value = val
	kv.status[args.Id] = val
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key

	// 处理确认请求，避免干扰主流程
	if args.Confirm {
		kv.mu.Lock()
		if _, ok := kv.status[args.Id]; ok {
			delete(kv.status, args.Id)
		}
		kv.mu.Unlock()
		return
	}

	// 检查缓存的结果，避免重复执行相同请求
	kv.mu.RLock()
	if val, ok := kv.status[args.Id]; ok {
		reply.Value = val
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	// 没有缓存结果时，读取并更新 `kvMap`
	kv.mu.Lock()
	val := kv.kvMap[key]
	kv.kvMap[key] = val + args.Value
	reply.Value = val
	kv.status[args.Id] = val
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.status = make(map[int64]string)

	return kv
}
