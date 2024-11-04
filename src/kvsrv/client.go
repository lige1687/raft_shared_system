package kvsrv

import (
	"6.5840/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

// Clerk 通过clerk 进行rpc 的调用,, 管理server和client之间的rpc交流
type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	// clerk 的设计应该还有一些管理 请求通道的东西?
	// mu        sync.Mutex
}

// 难道这个方法是为了给我们提供requestid 的吗
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server

	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		RequestID: nrand(),
		Confirm:   false,
	}
	var getReply GetReply
	ok := ck.server.Call("KVServer.Get", &args, &getReply)
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &getReply)
	}
	// 此时保证了 server收到了结果, confirm了
	args.Confirm = true
	confirmok := ck.server.Call("KVServer.Get", &args, &getReply)
	// 此时检查server 的confrm是否完成, 即 是否删除了缓存?
	for !confirmok {
		confirmok = ck.server.Call("KVServer.Get", &args, &getReply)
	}
	return getReply.Value

	/*

		for {
			ok := ck.server.Call("KVServer.Get", &args, &getReply)
			if ok && len(getReply.Value) > 0 {
				confirm := ck.server.Call("KVServer.", &args, &getReply)
				for !confirm { // 如果server没有确认 删除了缓存就一直发!
					confirm = ck.server.Call("KVServer.", &args, &getReply)
				}
				return getReply.Value
			}
			// else 处理错误情况? 比如重新的发送
			time.Sleep(100 * time.Millisecond)
		}
		return  getReply.Value

	*/
}

/*
func (ck *Clerk) nextRequestID() int64 {
	ck.mu.Lock()          // 上锁，确保线程安全
	ck.requestID++        // 增加 requestID
	newID := ck.requestID // 记录新生成的ID
	ck.mu.Unlock()        // 解锁

	fmt.Printf("Generated new RequestID: %d\n", newID)
	return newID
}


func (ck *Clerk) nextRequestID() int64 {
	newID := atomic.AddInt64(&ck.requestID, 1)
	fmt.Printf("Generated new RequestID: %d\n", newID)
	return newID // 原子递增
}
*/

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's (必须和rpc 的handler 函数匹配
// arguments. and reply must be passed as a pointer. ( reply必须返回一个指针 !
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestID: nrand(), // 使用老师定义的 随机request 的方法
		// ck.nextRequestID(), // 因为这里的递增 是需要一个原子的, 所以使用 一个单独的方法实现
		Confirm: false,
	}

	var putAppendReply PutAppendReply

	ok := ck.server.Call("KVServer"+op, &args, &putAppendReply)
	fmt.Println(ok)
	for !ok {
		ok = ck.server.Call("KVServer"+op, &args, &putAppendReply)
	}
	// 此时保证了 server收到了结果, confirm了
	args.Confirm = true
	confirmok := ck.server.Call("KVServer"+op, &args, &putAppendReply)
	// 此时检查server 的confrm是否完成, 即 是否删除了缓存?
	for !confirmok {
		confirmok = ck.server.Call("KVServer"+op, &args, &putAppendReply)
	}
	return putAppendReply.Value

	/*
		for {
			ok := ck.server.Call("KVServer."+op, &args, &reply)

			//	fmt.Println(args.Key) // 而key一直是 0
			//fmt.Println(args.Value)  // 调试, value 的结果是空的
			// 得到的调试结果就是一直 重复的调用这个, 为啥呢
			//fmt.Println("CALL KVServer." + op + strconv.FormatInt(args.RequestID, 10))
			// fmt.Println(ok)
			// fmt.Println(reply) // 这里调试的结果是reply始终是一个空的 , 而且是一个put操作, 返回一个空?

			if ok && len(reply.Value) > 0 {
				// 如果ok 记得再发一遍给server让他确认, 从而能删除 在server 中的缓存
				confirm := ck.server.Call("KVServer."+op, &args, &reply)
				for !confirm { // 如果server没有确认 删除了缓存就一直发!
					confirm = ck.server.Call("KVServer."+op, &args, &reply)
					fmt.Println(confirm)
				}
				return reply.Value
			}
			// else 处理错误
		}

	*/
	return "failed"
}

func (ck *Clerk) Put(key string, value string) {
	fmt.Println(key)   // 事实证明脚本第一个给的key就是0
	fmt.Println(value) // 脚本第一个给的value就是空,然后就阻塞了
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
