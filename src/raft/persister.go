package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

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
