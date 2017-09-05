package dns


import (
"net/http"
"net/rpc"
"fmt"
//"time"
"encoding/gob"
"bytes"
)



var leaderID int
var ip []string
var count int 
var servers []*rpc.Client

type DnsArgs struct{
	Op string
	R *http.Request 
	//W http.ResponseWriter `gob:"-"`
}

type DnsReply struct{
	IsLeader bool
}

func StartDNS(){
	gob.Register(DnsArgs{})
	gob.Register(DnsReply{})
	leaderID=0
	ip = make([]string, 3)
	ip[0] = "10.0.0.163"
	ip[1] = "10.0.0.168"
	//ip[1] = "155.41.43.134"
	//ip[2] = "155.41.30.113"

	count=2

	servers = make([]*rpc.Client,2)


	for i := 0; i < count; i++ {
		
			//var client *rpc.Client
			fmt.Println("Hi")
			client, err := rpc.DialHTTP("tcp", ip[i] + ":8080")
			//err := 'a'
			for err != nil {
				//log.Fatal("dialing:", err)
				fmt.Println("connecting")
				client, err = rpc.DialHTTP("tcp", ip[i] + ":8080")
			}
			servers[i] = client
			fmt.Println("connected %d", i)
	}
	fmt.Printf("connected success\n")



	go func(){
	for{
		listenToPort()
		}
	}()

}

/*
func findLeader(servers []*rpc.Client ) {

	for i:=0; i< count;i++{
		//if i!=me{
			var reply DnsReply
			args:= DnsArgs{Op:"GetLeader"}
			err := servers[i].Call("RaftKV.DNSrequest",&args,&reply)
			if err != nil{
				//log.Fatal("arith error:",err)
			}
			leaderID=i
			fmt.Println("Arith:",reply)
			time.Sleep(10*time.Second)
		//}
	}

}*/


func listenToPort() {
	//isLeader := kv.rf.IsLeader()
	//DPrintf("%v isLeader:%v", kv.me, isLeader)

		
		http.DefaultServeMux = new(http.ServeMux)
		mux := http.NewServeMux()
		//mux.Handle("/html/", http.StripPrefix("/html/", http.FileServer(http.Dir("../html/"))))
		mux.HandleFunc("/homePage",homePage)
		//mux.HandleFunc("/login", kv.login)
		//mux.HandleFunc("/view/", kv.makeHandler(kv.viewHandler))

		//http.HandleFunc("/edit/", makeHandler(editHandler))
		http.ListenAndServe(":8000", mux)

	
}


func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("receive request")
	args:= DnsArgs{Op:"GetLeader", R:r}
	/*
	q := new(bytes.Buffer)
	e := gob.NewEncoder(q)
	err:= e.Encode(args)
	    if err != nil {
        fmt.Println("there is encoding error",err)
    }
	data := q.Bytes()

*/
	
	var reply DnsReply
	fmt.Println("%v",servers)

	for i:=0; i< count;i++{
		//if i!=me{
			//args:= DnsArgs{Op:"GetLeader"}
			var reply DnsReply
			var network bytes.Buffer
			encoder:= gob.NewEncoder(&network)
			p:= &DnsArgs{Op:"GetLeader", R:r}
			encoder.Encode(p)
			err := servers[i].Call("RaftKV.DNSrequest",p,&reply)
			if err != nil{
				//log.Fatal("arith error:",err)
				fmt.Println("there is error1",err)
			}

			if reply.IsLeader{
			fmt.Println("leader",reply.IsLeader)
			leaderID=i
		}
			//fmt.Println("Arith:",reply)
			//time.Sleep(10*time.Second)
		//}
	}
		//args = DnsArgs{Op:"homePage", R:r, W:w}
		args = DnsArgs{Op:"homePage"}
		err := servers[leaderID].Call("RaftKV.DNSrequest",&args,&reply)
			if err != nil{
				//log.Fatal("arith error:",err)
				fmt.Println("there is error",err)
			}
}