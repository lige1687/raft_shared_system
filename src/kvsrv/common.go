package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Id      int64
	Confirm bool //是否是确认消息
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key     string
	Id      int64
	Confirm bool //是否是确认消息
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
