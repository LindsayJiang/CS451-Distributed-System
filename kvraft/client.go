package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
//import "log"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientid int64
	requestid int
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientid = nrand()
	ck.requestid = 0
	
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	// build args
	ck.mu.Lock()
	args:=GetArgs{Key:key, Clientid:ck.clientid,Requestid:ck.requestid}
	ck.requestid++
	ck.mu.Unlock()

	// send RPC
	for{
		for i := range ck.servers{
			var reply GetReply
			//log.Printf("client side send getid %v to %v key %v",ck.requestid,i,key)
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
			if ok{
				// if reached right server:
				if !reply.WrongLeader{
					//log.Printf("client get back from server value is  %v",reply.Value)
					return reply.Value
					//log.Printf("hereee????")
				}
			}
		}

	}
	
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args:=PutAppendArgs{Key:key, Value:value, Op:op, Clientid:ck.clientid,Requestid:ck.requestid}
	ck.requestid++
	ck.mu.Unlock()
	// send RPC
	for{
		for _,i := range ck.servers{
			var reply GetReply
			ok := i.Call("RaftKV.PutAppend", &args, &reply)
			if ok{
				// if reached right server:
				if !reply.WrongLeader{
					return
				}
			}
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	//log.Printf("client put key %v value %v",key,value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	//log.Printf("client append key %v value %v",key,value)
	ck.PutAppend(key, value, "Append")
}
