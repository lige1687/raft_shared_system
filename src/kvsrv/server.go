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
	mu sync.Mutex

	// Your definitions here.
	kvMap  map[string]string
	status map[int64]string //key: arg id; value: previous result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Confirm {
		if _, ok := kv.status[args.Id]; ok {
			delete(kv.status, args.Id)
		}
		return
	}
	if _, ok := kv.status[args.Id]; ok {
		reply.Value = kv.status[args.Id]
	} else {
		val := kv.kvMap[key]
		reply.Value = val
		kv.status[args.Id] = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Confirm {
		if _, ok := kv.status[args.Id]; ok {
			delete(kv.status, args.Id)
		}
		return
	}

	if _, ok := kv.status[args.Id]; ok {
		reply.Value = kv.status[args.Id]
	} else {
		val := kv.kvMap[key]
		kv.kvMap[key] = args.Value
		reply.Value = val
		kv.status[args.Id] = val
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Confirm {
		if _, ok := kv.status[args.Id]; ok {
			delete(kv.status, args.Id)
		}
		return
	}
	if _, ok := kv.status[args.Id]; ok {
		reply.Value = kv.status[args.Id]
	} else {
		val := kv.kvMap[key]
		kv.kvMap[key] = val + args.Value
		reply.Value = val
		kv.status[args.Id] = val
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.status = make(map[int64]string)

	return kv
}
