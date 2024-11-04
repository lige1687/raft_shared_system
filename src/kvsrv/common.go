package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // 操作类型："Put" 或 "Append" 因为这个方法是通用的
	RequestID int64
	Confirm   bool
	// todo 使用一个confirm 的bool参数对append是否完成进行记录, 在server检查的时候, 如果confirm是假, 说明没有完成 , 如果confirm为真则删除server中对应的缓存即可

	// You'll have to add definitions here.
	// Field names must start with capital letters, 首写字母
	// otherwise RPC will break
	//还需要一些什么定义? 比如 client id? 或者request ID
	// clert作为 记账员, 一定会 记录基本所有的 request和clinet 的id ? 所有的server门
}

type PutAppendReply struct {
	Value string
} // reply是否需要额外的定义标记是否 完成这个reply?

type GetArgs struct {
	Key       string
	RequestID int64 // 可选: 用于跟踪请求的唯一ID
	Confirm   bool
	//	ClientID  int64 // 可选: 用于标识客户端
	//	Timestamp int64 // 可选: 请求发送的时间戳

	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
