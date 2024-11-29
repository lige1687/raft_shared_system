package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"os"
	"sync"
)

// 存放了raft 的持久化状态
type Persister struct {
	mu        sync.Mutex
	raftstate []byte //  ? 保留了当前节点的一些状态, 因为是持久化
	// 即raft中需要持久化的信息, 如votedFor , 如 currentterm
	// 所以需要通过字节的形式写入,
	snapshot []byte // 快照 ?  为啥是字节数组, 我猜是 快照是持久化存储的
	// 而不是一条命令的存储,需要一些 encoder将 log变为 字节
}

func MakePersister() *Persister {
	return &Persister{}
}

// 复制所有的日志? 返回一个字节数组
func clone(orig []byte) []byte {

	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

// todo实现可能涉及的文件或者磁盘存储
func (p *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 更新内存中的状态和快照
	p.raftstate = clone(state)
	p.snapshot = clone(snapshot)

	// 将状态和快照写入磁盘（假设文件存储路径为 raft_state 和 snapshot）
	err := writeToFile("raft_state", p.raftstate)
	if err != nil {
		log.Fatalf("Failed to save raft state: %v", err)
	}

	err = writeToFile("snapshot", p.snapshot)
	if err != nil {
		log.Fatalf("Failed to save snapshot: %v", err)
	}
}

func writeToFile(filename string, data []byte) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// 复制一个persister

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

// 给的是克隆的部分
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

// 当前所有日志的长度?
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

// restore previously persisted state.
// 恢复之前的持久化状态（解码）
// 传入的是一个byte数组，而ReadRaftState()返回的就是[]byte
// 因此恢复持久化状态应该：
// 1. data := rf.persister.ReadRaftState()
// 2. rf.readPersist(data)
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Raft server %d readPersist ERROR!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lm.logs = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

	}
}
