package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	Clientid  int64
	Requestid int 
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdatabase map[string] string
	finished map[int] chan Op
	// for unrelieble case, I need to check if I received same requestid multiple times.
	duplicate map[int64] int
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// check if i am the leader.
	// comment
	if !kv.rf.IsLeader(){
		reply.WrongLeader = true
	} else{
		reply.WrongLeader = false
		operation := Op{Operation:"Get", Key:args.Key,Clientid:args.Clientid,Requestid:args.Requestid}
		index, _, _ := kv.rf.Start(operation)
		kv.mu.Lock()
		//log.Printf("%v received get request",kv.me)
		
		ch, ok := kv.finished[index]
		if !ok{
			ch = make(chan Op,1)
			kv.finished[index] = ch
		} 
		kv.mu.Unlock()

		// now wait for all logs consistent.
		select{
		case op := <-ch:
			// finished loging

			if op!=operation{
				reply.WrongLeader = true
				return
			}

			reply.Err = OK
			kv.mu.Lock()
			//log.Printf("here2")
			val, ok := kv.kvdatabase[args.Key]
			if !ok{
				reply.Err = ErrNoKey
			}else{
				reply.Err = OK
				//log.Printf("val is %v",val)
				reply.Value = val
				kv.duplicate[args.Clientid] = args.Requestid
			}
			kv.mu.Unlock()
			return
		case <-time.After(time.Millisecond*time.Duration(1000)):
			reply.WrongLeader = true
			return
		}
	}
	
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// comment
	if !kv.rf.IsLeader(){
		reply.WrongLeader = true
	} else{
		reply.WrongLeader = false
		operation := Op{Operation:args.Op, Key:args.Key,Value:args.Value,Clientid:args.Clientid,Requestid:args.Requestid}
		index, _, _ := kv.rf.Start(operation)
		kv.mu.Lock()
		ch, ok := kv.finished[index]
		if !ok{
			ch = make(chan Op,1)
			kv.finished[index] = ch
		} 
		kv.mu.Unlock()

		select{
		case op := <-ch:
			// finished loging

			if op != operation{
				reply.WrongLeader = true
				return
			}
			reply.Err = OK

			return
		case <-time.After(time.Millisecond*time.Duration(1500)):
			reply.WrongLeader = true
			return
	}
}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *RaftKV) NeedSnapshot(currindex int){
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kvdatabase)
	e.Encode(kv.duplicate)
	data := w.Bytes()
	go kv.rf.MakeSnapshot(data,currindex)
	kv.mu.Unlock()
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	kv.kvdatabase = make(map[string]string)
	kv.finished = make(map[int]chan Op)
	kv.duplicate = make(map[int64]int)


	go func(){
		for{

			msg := <- kv.applyCh
			//log.Printf("here")
			// if this is a snapshot msg.
			if msg.UseSnapshot==true{

				log.Printf("install snapshot")
				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)
				kv.mu.Lock()
				
				kv.kvdatabase = make(map[string]string)
				kv.duplicate = make(map[int64]int)
				d.Decode(&kv.kvdatabase)
				d.Decode(&kv.duplicate)
				//d.Decode(&kv.rf.LastIncludedIndex)
				//d.Decode(&kv.rf.LastIncludedTerm)
				kv.mu.Unlock()
			}else{
				kv.mu.Lock()
				index := msg.Index
				command := msg.Command.(Op)
				//log.Printf("operation get back: %v", command)
				// check unreliable before applying.
				request, ok := kv.duplicate[command.Clientid]
				// if this is not a new request, ignore it.
				if ok && request >= command.Requestid{
					//do nothing...
				}else{
					// actually apply it.
					if command.Operation=="Put"{
						//log.Printf("puting, key: %v, value: %v",command.Key,command.Value)
						kv.kvdatabase[command.Key] = command.Value
					}else if command.Operation=="Append"{
						//log.Printf("appending")
						kv.kvdatabase[command.Key] += command.Value
					}
					kv.duplicate[command.Clientid] = command.Requestid
				}
				// now reply to the client that we have done it.
				ch,ok := kv.finished[index]
				if !ok {
					ch = make(chan Op, 1)
					kv.finished[index] = ch
				}else{
					select {
					case <-ch:
					default:
					}
					ch <- command
				}

				kv.mu.Unlock()
				// check if we need a snapshot.
				if maxraftstate!= -1 && persister.RaftStateSize()>maxraftstate+50{
					go kv.NeedSnapshot(msg.Index)
				}
			}

			

		}
	}()




	return kv
}
