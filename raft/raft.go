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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "log"

import "bytes"
import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []Log

	commitIndex int
	lastApplied int

	// for leaders
	nextIndex []int
	matchIndex []int
	numVotes int
	isLeader int // 0: follower; 1: leader; 2: candidate

	// for candidates
	numReply int

	// for snapshot
	LastIncludedIndex int
	LastIncludedTerm int

	//channels
	receiveHeartBeatsch chan bool
	grantedVotech chan bool
	allReplyReceivedch chan bool
	newEntrayStartch chan bool
	applych chan ApplyMsg
	newcommitch chan bool
	//otherClaimLeaderch chan bool


}

type Log struct{
	Command interface{}
	Term int
	Index int
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.isLeader==1 {
		isleader=true
	}else{
		isleader=false
	}
	return term, isleader
}
func (rf *Raft) IsLeader()(bool){
	return rf.isLeader==1
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.log)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.lastApplied)
	 e.Encode(rf.LastIncludedIndex)
	 e.Encode(rf.LastIncludedTerm)
	 //e.Encode()
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.log)
	 d.Decode(&rf.votedFor)
	 d.Decode(&rf.lastApplied)
	 d.Decode(&rf.LastIncludedIndex)
	 d.Decode(&rf.LastIncludedTerm)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

func (rf *Raft) readSnapshot(data []byte){
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.LastIncludedIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.LastIncludedTerm = LastIncludedTerm

	var newLog []Log
	newLog = append(newLog, Log{Term: 0, Index:0})
	for i:=len(rf.log)-1; i>=0; i--{
		if rf.log[i].Index==LastIncludedIndex && rf.log[i].Term == LastIncludedTerm{
			if i!= len(rf.log)-1{
				newLog = append(newLog, rf.log[(i+1):]...)
			}
			break
		}
	}
	rf.log = newLog
	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	go func() {
		rf.applych <- msg
	}()
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGranted  bool
}

type AppendEntriesArgs struct{
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct{
	Term         int
	Success      bool
	IndexExpect	 int
}

type InstallSnapshotArgs struct{
	Term 				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	//Offset				int
	Data 				[]byte
	//Done 				bool
}


type InstallSnapshotReply struct {
	Term 				int
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//reply false if term < currentTerm
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// we are out of date.
	if rf.currentTerm < args.Term{
		reply.Term = args.Term
		rf.currentTerm = args.Term
		//back to follower state
		rf.isLeader = 0
		rf.votedFor = -1

	}
	reply.Term = rf.currentTerm
	// now decide if we should vote for him.
	// quote paper: If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	// if (votedFor==-1 || votedFor==args.CandidateId) &&
	// determine if up to date:
	//If the logs have last entries with different terms, 
	//then the log with the later term is more up-to-date. 
	//If the logs end with the same term, 
	//then whichever log is longer is more up-to-date.
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	myLastLogIndex := rf.log[len(rf.log)-1].Index
	//myLastLogTerm := 0
	//myLastLogIndex := 0
	if (rf.votedFor==-1 || rf.votedFor==args.CandidateId) && (myLastLogTerm < args.LastLogTerm || (myLastLogTerm==args.LastLogTerm && myLastLogIndex<=args.LastLogIndex)){
		reply.VoteGranted = true
		rf.isLeader = 0
		rf.votedFor = args.CandidateId
		rf.grantedVotech <- true
	}




}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok{
	//rf.numReply++
	/*
	if args.Term != rf.currentTerm {
			return ok
	}
	*/

	/* NOTE HERE! IF CURRENT TERM HAS ALREADY BEEN CHANGED, WE IGNORE THIS VOTE*/
	if rf.isLeader==2 && args.Term==rf.currentTerm{
		if reply.VoteGranted {
			rf.numVotes++
			if rf.numVotes > len(rf.peers)/2{
				rf.isLeader=1
				//log.Printf("%d is leader, term %d",rf.me,rf.currentTerm)
				rf.allReplyReceivedch <- true
			}
		}else{
			rf.currentTerm = reply.Term
			rf.isLeader=0
			rf.persist()
		}
	}
	//if rf.numReply==len(rf.peers){
			//received all votes
			//rf.allReplyReceivedch <- true
		//}
	}
	//rf.mu.Unlock()
	return ok
}
//helper function for correcting commitindex.
func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

func Max(x, y int) int {
    if x > y {
        return x
    }
    return y
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	firstlogindex := rf.LastIncludedIndex
	//log.Printf("%d receive entry from %d, term him %d, term me %d",rf.me,args.Entries,args.Term,rf.currentTerm)
	//1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm{
		//log.Printf("case 2 server term: %d my term: %d",args.Term,rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.IndexExpect = Max(rf.log[len(rf.log)-1].Index + 1,rf.LastIncludedIndex+1)
	}else{
		/* 
			normal, sort of.
		*/
		// args.Term >= rf.currentTerm
		rf.receiveHeartBeatsch <- true
		reply.Term = args.Term
		//rf.currentTerm = args.Term
		



		//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if rf.currentTerm < args.Term{
			reply.Success = false
			rf.currentTerm = args.Term
			rf.isLeader = 0
			rf.votedFor = -1
		}
		if args.PrevLogIndex > rf.log[len(rf.log)-1].Index{
			//this is for 2B fail agreement - with follower failure.
			//log.Printf("case 1")
			reply.Success = false
			reply.IndexExpect = Max(rf.log[len(rf.log)-1].Index + 1,rf.LastIncludedIndex+1)
		}else{
			//absolutely normal...
			if rf.currentTerm == args.Term{
				//log.Printf("%d term normal",rf.me)
				//log.Printf("here!!!!!!")
				// if log is inconsistent Find the lastest log that has the same Term as leader's
				term := rf.log[args.PrevLogIndex-firstlogindex].Term
				//term:=args.Term
				if args.PrevLogTerm != term {
					index:=args.PrevLogIndex-1
					for i :=index ; i >= 0; i-- {
						if rf.log[i-firstlogindex].Term == args.Term {
							reply.IndexExpect =i+1
							break
						}
						
					}
					
					reply.Success = false
					return
				}
				/*
				If an existing entry conflicts with a new one (same index
				but different terms), delete the existing entry and all that
				follow it
				*/
				if args.PrevLogIndex < rf.log[len(rf.log)-1].Index{
					//log.Printf("truncated")
					rf.log = rf.log[:args.PrevLogIndex-firstlogindex+1]
				}
				// this is for 2B basic agreement
				if args.PrevLogIndex == rf.log[len(rf.log)-1].Index{

					// if this is a heartbeat
					if args.Entries == nil{
						// this is just a heartbeat.
						reply.IndexExpect=Max(rf.log[len(rf.log)-1].Index + 1,rf.LastIncludedIndex+1)
						reply.Success=true
						//log.Printf("%d received heartbeat from %d, term changed to %d",rf.me, args.LeaderId,args.Term)
						rf.currentTerm=args.Term

						rf.isLeader=0
						rf.persist()
						/* 
						 this is for updating last commitIndex.
						 Since the server would not send commitindex after last time,
						 It informs the follower the last commit by this heartbeat.
						*/
						if args.PrevLogTerm ==rf.log[args.PrevLogIndex-firstlogindex].Term {
							rf.commitIndex=Min(args.LeaderCommit,rf.log[len(rf.log)-1].Index)
							rf.newcommitch <- true
						}
						return
					}

					// if this is a normal append log request
					rf.log = append(rf.log, args.Entries...)
					//log.Printf("I'm %d, len is %d, log is %v",rf.me,len(rf.log), rf.log)
					reply.Success = true
					reply.IndexExpect = Max(rf.log[len(rf.log)-1].Index + 1,rf.LastIncludedIndex+1)
					if args.LeaderCommit>rf.commitIndex{
						rf.commitIndex=Min(args.LeaderCommit,rf.log[len(rf.log)-1].Index)
						rf.newcommitch <- true
					}
					//log.Printf("I'm %d, IndexExpect is %d", rf.me, reply.IndexExpect)
				}
			}
		}
		// replay.Term = args.Term
		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		//if rf.currentTerm == args.Term && args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
			//reply.IndexExpect = rf.log[len(rf.log)-1].Index + 1
			//return
		//}

	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok{
		//log.Printf("reply from server %d to %d term: %d",server,rf.me, reply.Term)
		if rf.currentTerm!=args.Term || rf.isLeader!=1{
			return ok
		}else{
			if reply.Success{
				//log.Printf("length: %d",len(rf.matchIndex))
				//rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
				rf.matchIndex[server] = reply.IndexExpect-1
				rf.nextIndex[server] = reply.IndexExpect
			}else{
				if reply.Term > rf.currentTerm{
					//log.Printf("server %d should not be server now, reply term %d",rf.me,reply.Term)
					//log.Printf("%d is no longer leader. term %d to term %d",rf.me, rf.currentTerm,reply.Term)
					rf.isLeader = 0
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()
				}else{
					//rf.matchIndex[server] = reply.IndexExpect-1
					rf.nextIndex[server] = reply.IndexExpect
				}
			}
		}

	}
	return ok
}

func (rf *Raft) checkAndSendAppendEntries() {
	//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check for commit
	firstlogindex:=rf.LastIncludedIndex
	lastlogindex:=rf.log[len(rf.log)-1].Index
	newcommit:=false
	for i:=rf.commitIndex+1; i<=lastlogindex; i++{
		totalcommits := 1
		for j:= range rf.peers{
			if j!=rf.me {
				if rf.matchIndex[j]>=i{
					totalcommits++
				}
			}
		}
		if totalcommits>len(rf.peers)/2{
			rf.commitIndex=i
			newcommit=true
		}
	}
	if newcommit{
		rf.newcommitch<-true
		//log.Printf("YOOOOOOOOOOOOO")
	}

	
	//log.Printf("I'm leader %d, len is %d, log is %v, Term is %d",rf.me,len(rf.log), rf.log, rf.currentTerm)
	/*
	for i:=range rf.peers{
		log.Printf("server %d, nextIndex is %d",i,rf.nextIndex[i])
	}*/
	//var wg sync.WaitGroup
	for i:= range rf.peers{
		//rf.mu.Lock()
		if i!= rf.me && rf.isLeader==1{
			// now adding snapshot: the leader would send a snapshot to a follower only if that follower is lag behind.
			// which means the leader has already discarded the next log entry that it needs to send to a follower
			// which means nextIndex[i] < leader's first log index.
			log.Printf("nextIndex for server %v is %v, baseindex is %v, lastindex is %v",i,rf.nextIndex[i],firstlogindex,lastlogindex)
			if rf.nextIndex[i] >= firstlogindex{
				if lastlogindex >= rf.nextIndex[i]{
					var args AppendEntriesArgs
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					// this is in case we have a race condition. And that would render PrevLogIndex 0.
					args.PrevLogIndex = Max(0,rf.nextIndex[i]-1)
					//log.Printf("nextIndex[%d] is %d", i, rf.nextIndex[i])
					//log.Printf("PrevLogIndex for %d is %d my lenth is %d",i, args.PrevLogIndex,len(rf.log))
					args.PrevLogTerm = rf.log[args.PrevLogIndex-firstlogindex].Term
					args.Entries = make([]Log,lastlogindex-args.PrevLogIndex)
					//log.Printf("lastlogindex: %d, prevLogIndex, %d", lastlogindex, args.PrevLogIndex)
					copy(args.Entries, rf.log[args.PrevLogIndex+1-firstlogindex:])
					//log.Printf("sending entries: %v, term %d", args.Entries,args.Term)
					args.LeaderCommit = rf.commitIndex
					//rf.mu.Unlock()
					//wg.Add(1)
					go func(i int, args AppendEntriesArgs){
						//defer wg.Done()
						var reply AppendEntriesReply
						rf.sendAppendEntries(i, &args, &reply)
					}(i,args)
				}else{
					// send heartbeat
					//var wg sync.WaitGroup

					//args:= AppendEntriesArgs{Term:rf.currentTerm,LeaderCommit:rf.commitIndex,LeaderId:rf.me}
					var args AppendEntriesArgs
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					// this is in case we have a race condition. And that would render PrevLogIndex 0.
					args.PrevLogIndex = Max(0,rf.nextIndex[i]-1)
					//log.Printf("nextIndex[%d] is %d", i, rf.nextIndex[i])
					//log.Printf("PrevLogIndex for %d is %d my lenth is %d",i, args.PrevLogIndex,len(rf.log))
					args.PrevLogTerm = rf.log[args.PrevLogIndex-firstlogindex].Term
					reply:=AppendEntriesReply{}
					for j:=0; j<len(rf.peers); j++{
						if j!= rf.me{
							//wg.Add(1)
								go func(j int){
									//defer wg.Done()
									rf.sendAppendEntries(j,&args,&reply)				
								}(j)
						}
					}
					
				}
			}else{
				// need to send a snapshot.
				args := InstallSnapshotArgs{Term:rf.currentTerm, LeaderId:rf.me, LastIncludedIndex:rf.LastIncludedIndex, LastIncludedTerm:rf.LastIncludedTerm, Data:rf.persister.ReadSnapshot()}
				go func(i int, args InstallSnapshotArgs){
					var reply InstallSnapshotReply
					rf.sendInstallSnapshot(i,args, &reply)
				}(i,args)
			}
		}
	}
	//wg.Wait()
	//log.Printf("after wait group")
	
}

func (rf *Raft) MakeSnapshot(serverdata []byte, index int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstlogindex := rf.LastIncludedIndex
	if index > firstlogindex && index <= rf.log[len(rf.log)-1].Index {

		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = rf.log[index-firstlogindex].Term
		var newLog []Log
		// note that we need to add a new log entry here since if we simply truncate the log to make it empty,
		// we'll get an index out of range error.
		newLog = append(newLog, Log{Term: 0, Index:0})
		if index != rf.log[len(rf.log)-1].Index{
			newLog = append(newLog, rf.log[(index - firstlogindex+1):]...)
		}
		rf.log = newLog
		rf.persist()
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(index)
		e.Encode(rf.LastIncludedTerm)
		data := w.Bytes()
		data = append(data, serverdata...)
		rf.persister.SaveSnapshot(data)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	log.Printf("%v",ok)
	if ok{
		if reply.Term > rf.currentTerm{
			rf.isLeader = 0
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}else{
			rf.nextIndex[server] = args.LastIncludedIndex+1
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	}
	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("InstallSnapshot")
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	rf.receiveHeartBeatsch <- true
	rf.isLeader = 0
	rf.currentTerm = args.Term

	// truncate the log.
	var newLog []Log
	newLog = append(newLog, Log{Term: 0, Index:0})
	for i:=len(rf.log)-1; i>=0; i--{
		if rf.log[i].Index==args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm{
			if i!= len(rf.log)-1{
				newLog = append(newLog, rf.log[(i+1):]...)
			}
			break
		}
	}
	log.Printf("checked here")
	rf.log = newLog
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persister.SaveSnapshot(args.Data)
	rf.persist()
	msg:=ApplyMsg{UseSnapshot:true, Snapshot:args.Data}
	log.Printf("sending back")
	rf.applych <- msg
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.isLeader==1

	// Your code here (2B).
	if isLeader{
		//log.Printf("I'm leader %d, start called on %v",rf.me, command)
		index = rf.log[len(rf.log)-1].Index + 1
		rf.log=append(rf.log,Log{Term:term,Index:index,Command:command})
		rf.persist()
		//log.Printf("server %d get new entry %v, log %v",rf.me,command,rf.log)
		//rf.newEntrayStartch <- true

	}


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me


	// Your initialization code here (2A, 2B, 2C).
	rf.isLeader = 0
	rf.log = append(rf.log, Log{Term: 0, Index:0})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.numVotes = 0
	rf.numReply = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.applych = applyCh

	rf.receiveHeartBeatsch = make(chan bool,200)
	rf.grantedVotech = make(chan bool,200)
	rf.allReplyReceivedch = make(chan bool,200)
	rf.newEntrayStartch = make(chan bool, 200)
	rf.newcommitch = make(chan bool, 50)
	//rf.otherClaimLeaderch = make(chan bool,50)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	go func(){
		for{
			switch rf.isLeader{
				// follower
				case 0:
					select{
						case <-rf.receiveHeartBeatsch:
							rf.mu.Lock()
							rf.isLeader=0
							rf.mu.Unlock()
						case <-rf.grantedVotech:
						// send heartbeat 8 times per second. so 125 milliseconds per time. Thus my timeout is set to be 300-400
						case <-time.After(time.Millisecond*time.Duration(rand.Intn(300)+500)):
							// switch to candidate state.
							rf.mu.Lock()
							rf.isLeader=2
							rf.mu.Unlock()

					}
				//leader
				case 1:
					
						// if after this time still no new entry comes, heartbeat.
						//heartbeat Timeout
					rf.checkAndSendAppendEntries()
					time.Sleep(time.Millisecond*time.Duration(125))

						
							
					
					
					
					
				//candidate
				case 2:
					rf.mu.Lock()
					//log.Printf("%d enter candidate, currentTerm %d",rf.me,rf.currentTerm)
					rf.currentTerm ++
					//log.Printf("%d enter candidate, currentTerm added to %d",rf.me,rf.currentTerm)
					rf.votedFor = me
					rf.numVotes = 1
					rf.persist()

					args:= RequestVoteArgs{
						Term:rf.currentTerm,
						CandidateId:me,
						LastLogIndex:rf.log[len(rf.log)-1].Index,
						//LastLogIndex:0,
						LastLogTerm:rf.log[len(rf.log)-1].Term,
						//LastLogTerm:0,
					}
					rf.mu.Unlock()
					for i:=0; i<len(rf.peers); i++{
						if i!=me{
							go func(i int){
								var reply RequestVoteReply
								rf.sendRequestVote(i,&args,&reply)
							}(i)
						}
					}
					// either received all votes or eletion timeout.
					select{
						case <-rf.allReplyReceivedch:
							rf.mu.Lock()
							//if rf.numVotes > len(rf.peers)/2{
								//I am elected leader
								//log.Printf("I'm %d, granted vote for term %d",rf.me, rf.currentTerm)
								//rf.isLeader=1
								//initialize
								rf.nextIndex = make([]int, len(rf.peers))
								rf.matchIndex = make([]int, len(rf.peers))
								for i := range rf.peers {
									rf.nextIndex[i] = Max(rf.log[len(rf.log)-1].Index + 1,rf.LastIncludedIndex+1)
									//if i!= rf.me{
									//log.Printf("initialize, nextIndex %d is %d", i, rf.nextIndex[i])
								//}
									rf.matchIndex[i] = 0
								}

							//}else{
							//	rf.isLeader=0
							//}
							rf.numVotes=0
							rf.numReply=0
							rf.mu.Unlock()
						// if during this time someone else already is a leader
						case <-rf.receiveHeartBeatsch:
							rf.mu.Lock()
							rf.isLeader = 0
							rf.mu.Unlock()
						// if during this time I granted someone else's vote
						case <- rf.grantedVotech:
							rf.mu.Lock()
							rf.isLeader=0
							rf.mu.Unlock()
						// election timeout:
						case <-time.After(time.Millisecond*time.Duration(rand.Intn(300)+500)):
					}

			}
		}
	}()

	// new commit
	go func(){
		for{
			<- rf.newcommitch
			rf.mu.Lock()
			firstlogindex := rf.LastIncludedIndex
			for i:=rf.lastApplied+1; i<=rf.commitIndex; i++{
				msg := ApplyMsg{Index:i, Command:rf.log[i-firstlogindex].Command}
				//log.Printf("commit msg %v", msg)
				//for _:=range rf.peers{
					applyCh <- msg
					//log.Printf("server %d commit %v, log is %v", rf.me,msg,rf.log)
				//}
				rf.lastApplied = i
			}
			rf.mu.Unlock()

		}
	}()
	return rf
}
