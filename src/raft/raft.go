package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Status int
const(
	Follower Status = iota + 1
	Candidate
	Leader
)
type Log struct{
	comm interface{}
	termId int
}

func printTime(){
	fmt.Printf("%v\n",time.Now())
}
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor int
	logs []Log
	status Status
	lastHeartReceive time.Time
	timeOut int64   //产生的那个随机数

}

func (rf *Raft)upTimeout(){
	rf.timeOut = 50 + (rand.Int63() % 300)
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status==Leader
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	//LastLogIndex
	//LastLogTerm
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type HeartRequest struct{
	Term int
	CandidateId int
}
type HeartReply struct{
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("我是%d 我现在开始为%d投任期为%d的票\n",rf.me,args.CandidateId,args.Term)
	if args.Term > rf.currentTerm{
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.mu.Unlock()
	}
	if args.Term < rf.currentTerm || rf.votedFor != -1{
		reply.VoteGranted = false
		if args.Term < rf.currentTerm{
		}else{
			rf.mu.Lock()
			rf.lastHeartReceive = time.Now()
			rf.mu.Unlock()
		}
		return
	}
	//todo 还要实现Log相关的逻辑
	rf.mu.Lock()
	rf.votedFor = args.CandidateId
	rf.lastHeartReceive = time.Now()
	rf.mu.Unlock()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

//Leader还要调用这个函数
func (rf *Raft) AppendEntries(args *HeartRequest,reply *HeartReply){
	//收到心跳 要先判断一下传过来的term和现在term的关系
	if args.Term < rf.currentTerm{
		return
	}
	if args.Term > rf.currentTerm || rf.status != Follower{
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.mu.Unlock()
	}
	rf.lastHeartReceive = time.Now()
}


func (rf *Raft) sendAppendEntries(server int, args *HeartRequest, reply *HeartReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (2B).


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


func (rf *Raft) elect() {
	rf.mu.Lock()
	rf.votedFor = -1
	rf.currentTerm++
	rf.status = Candidate
	rf.lastHeartReceive = time.Now()
	rf.upTimeout()
	rf.mu.Unlock()
	tot,agree := 0,0

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i:=0;i<3;i++{
		if i==rf.me{   //是自己
			if rf.votedFor!=-1{
				fmt.Printf("%d的任期为%d的票已投给了%d,不能给自己投票\n",rf.me,rf.currentTerm,rf.votedFor)
				continue
			}
			agree++
			tot++
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.mu.Unlock()
			fmt.Printf("%d把任期为%d的票投给了自己\n",rf.me,rf.currentTerm)
			continue
		}
		go func(x int){
			request := RequestVoteArgs{
				Term : rf.currentTerm,
				CandidateId : rf.me,
				//todo 这里没实现
				//LastLogIndex : 0,
				//LastLogTerm : 0,
			}
			reply := RequestVoteReply{
				Term : 0,
				VoteGranted : false,
			}
			fmt.Printf("%d的任期为%d的发给%d的票已发出\n",rf.me,rf.currentTerm,x)
			ok := rf.sendRequestVote(x,&request,&reply)
			if ok{
				if reply.VoteGranted{
					agree++
					fmt.Printf("%d赢得了%d的任期为%d的一票\n",rf.me,x,reply.Term)
				}else{
					fmt.Printf("%d没有赢得%d的任期为%d的一票\n",rf.me,x,reply.Term)
				}
			}else{
				fmt.Printf("%d的消息没有回应 我是%d...\n",x,rf.me)
			}
			tot++
		}(i)

	}
	go func(){
		for{
			if tot==3 && agree<2{
				rf.mu.Lock()
				rf.status = Follower
				rf.mu.Unlock()
				cond.Broadcast()
				fmt.Printf("%d在任期为%d的时间内被否决\n",rf.me,rf.currentTerm)
				return
			}
			if agree>=2{
				if rf.status != Candidate{
					return
				}
				fmt.Printf("%d成为了任期为%d的领导\n",rf.me,rf.currentTerm)
				rf.mu.Lock()
				if rf.status == Candidate{
					rf.status = Leader
				}
				rf.mu.Unlock()
				cond.Broadcast()
				return
			}
		}
	}()
	go func(){
		for{
			now := time.Now().UnixMilli()
			if now - rf.lastHeartReceive.UnixMilli() > rf.timeOut{
				fmt.Printf("%d的任期为%d的投票超时，只收到了%d票\n",rf.me,rf.currentTerm,tot)
				//这里还以Candidate身份退出 外面检测如果还是Candidate就再次选举
				cond.Broadcast()
				return
			}
		}
	}()
	go func(){
		for{
			if rf.status !=Candidate {
				fmt.Printf("%d在任期为%d的投票中已经不是Candidate 变成了%v  投票终止\n",rf.me,rf.currentTerm,rf.status)
				cond.Broadcast()
				return
			}
		}
	}()
	mu.Lock()
	cond.Wait()
}

func (rf *Raft)leaderDo(){
	//for{
		if rf.status != Leader{
			return
		}
		for i:= 0;i<3;i++{
			if i==rf.me{
				continue
			}
			request := HeartRequest{
				Term : rf.currentTerm,
				CandidateId : rf.me,
			}
			reply := HeartReply{}
			rf.sendAppendEntries(i,&request,&reply)
		}
	//}

}
func (rf *Raft) ticker() {
	//var ms int64
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		now := time.Now().UnixMilli()
		if rf.status == Follower && now - rf.lastHeartReceive.UnixMilli() > rf.timeOut{
			fmt.Printf("我是%d 因为超时我要变成Candidate 我申请开始任期为%d的选举,因为我的超时时间是%v,但是已经经过了%v的时间\n",rf.me,rf.currentTerm+1,rf.timeOut,now-rf.lastHeartReceive.UnixMilli())
			rf.elect()
			fmt.Printf("sure :: %d退出时候的状态是%d\n",rf.me,rf.status)
		}
		for{
			if rf.status !=Candidate{
				break
			}
			fmt.Printf("%d 还是Candidte 继续选举\n",rf.me)
			rf.elect()
		}
		if rf.status == Leader{
			fmt.Printf("%d成为任期为%d的了领导 开始发送心跳\n",rf.me,rf.currentTerm)
			for rf.status == Leader{
				rf.leaderDo()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//time.Sleep(time.Duration(rf.timeOut) * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.status = Follower
	rf.upTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
