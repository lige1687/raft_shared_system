package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server. 创建新的raft
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader 获取日志状态, 当前的term 并且是否是leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)




// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry. 包含了一个新提交的 日志项目
//
// in part 3D you'll want to send other kinds of messages (e.g.,  发送其他通过这个 applymsg 如快照
// 如果 valid为false 表示 是用于其他用途的 , ! 一个flag 而已
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
// 即每个节点 需要能通知上层的服务自己的节点提交到哪里了? applymas就是这样的作用 commmand 就是实际的日志的命令, 具体类型取决于你的 实现

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有的 其他集群存储 地方 , 通过rpc交流 ,所以存放的是一群rpc 的client
	persister *Persister          // Object to hold this peer's persisted state  持久者, 存放当前节点的持久化数据
	me        int                 // this peer's index into peers[] 表示当前 节点的索引
	dead      int32               // set by Kill() 使用kill

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	// 持久化的属性
	currentTerm int         // 当前任期
	votedFor    int         // 当前任期投票给了谁
	lm          *LogManager // 管理日志存储
	snapshot    []byte      // 上一次保存的快照

	//volatile 属性 , 易失属性

	// 易失属性
	state         string        // 当前角色状态

	commitIndex   int           // 已提交的最大下标
	lastApplied   int           // 以应用到状态机的最大下标( 提交和应用没是两回事
	applyCh       chan ApplyMsg // apply通道 , 用于和peer交流
	applyCond     *sync.Cond    // apply协程唤醒条件
	installSnapCh chan int      // install snapshot的信号通道，传入trim index
	backupApplied bool          // 从磁盘恢复的snapshot已经apply
	notTicking    chan bool     // 没有进行选举计时
	electionTimer *time.Timer   // 选举计时器

	// Leader特有字段
	nextIndexes    []int       // 对于每个follower，leader要发送的下一个复制日志下标
	matchIndexes   []int       // 已知每个follower和自己一致的最大日志下标
	heartbeatTimer *time.Timer // 心跳计时器


}

// GetState return currentTerm and whether this server believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)


	// 注意, 应该是需要锁的
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w) // 创建一个新的写入 的encoder
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lm) // 持久化日志
	e.Encode(rf.snapshot)

	raftstate := w.Bytes() //将缓冲区的东西写到 机器
	// 返回结果
	// 将raft 结果 保存
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// 超时选举
func ( rf *Raft) kickOffElection ( ){
	rf.currentTerm ++
	rf.state = CANDIDATE
	rf.votedFor= rf.me // candidate id
	rf.persist() // 将当前的持久化状态持久化 ! 防止宕机导致的不一致
	votes:=1
	total:= len(rf.peers)
	args:= rf.getRequestVoteArgs () // 获取 请求的content  , 即创建请求的方法
	mu :=sync.Mutex{}
	for i:=0 ;i < total ;i++ {
		//
		if i== rf.me {
			continue
		}
		// 使用goroutine去 投票, 并发的,
		// 将 server传入, 表示是发给 谁的 ? 对应参数i,  即每个 server都会给你发一个则更好
		go func ( server int ) {
			reply:=RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply)
			if args.Term == rf.currentTerm && rf.state== CANDIDATE {
				// 判断防止自己身份已经改变, 并且判断term 是否和自己是一个
				if reply.Term < rf.currentTerm {
					// 投票者日期小于自己, 丢弃
					return
				}
				if reply.VoteGranted {
					//如果确认给自己投票
					mu.Lock()
					votes++
					if rf.state != LEADER && votes > total/2{
						// 超过半数, 并且当前不是leader  !
						rf.initLeader() // 初始化一些leader 才有的信息
						rf.state= LEADER
						// 立刻开始心跳, 告诉所有的 节点我是leader了
						go rf.heartbeat()
					}

					mu.Unlock()
				}
			}
		}(i )


	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters !
type RequestVoteArgs struct {
	// 详细内容参考 论文中的设计
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	// Your data here (3A, 3B).
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
// 大写字母开始! 必须
type RequestVoteReply struct {

	// Your data here (3A).
	Term int // 投票者的任期
	VoteGranted bool // 是否决定给你投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) getRequestVoteArgs() interface{} {
	 args := new(RequestVoteArgs)
args.Term = rf.currentTerm
args.CandidateId = rf.me // 让投票者知道我是谁

// args.LastLogTerm = rf. 通过 logmanager获取吧
//  todo args.LastLogIndex = rf.me
return args
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
