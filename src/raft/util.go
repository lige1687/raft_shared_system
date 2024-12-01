package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) resetElectionTimer() {
	timeout := RandomElectionTimeout()
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeout)
	} else {
		rf.electionTimer.Reset(timeout)
	}
}

// 重置 心跳
func (rf *Raft) resetHeartbeatTimer() {
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
	} else {
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

const ElectionTimeout = 1000
const HeartbeatTimeout = 125

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var GlobalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

// 生成一个随机的选举超时时间（例如 150 到 300 毫秒之间）
func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond
}
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}
