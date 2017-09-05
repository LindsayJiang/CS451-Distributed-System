package main

import (
	"net/rpc"
	//"net/http"
	//"net"
	"log"
	//"fmt"
	//"time"
	//"reflect"
	//"errors"
	"errors"
	//"reflect"
	"net"
	"net/http"
	//"time"
	"time"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}
func main(){
	/*

	client, err := rpc.DialHTTP("tcp", "155.41.43.134" + ":8080")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println(reflect.TypeOf(client))
	var reply int

	//fmt.Printf("%v\n",client)
	err = client.Call("Arith.Multiply",&Args{2,3},&reply)
	if err != nil{
		log.Fatal("arith error:",err)
	}
	fmt.Printf("Arith: %d",reply)

*/

	//listen

	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func(){
		for{
			http.Serve(l, nil)
		}
	}()
	time.Sleep(10*time.Second)


/*
	me := 2
	count:= 3
	ip:=make([]string,3)
	//ip[0] = "155.41.30.113"
	ip[0] = "155.41.102.147"
	ip[1] = "155.41.43.134"
	ip[2] = "155.41.30.113"
	var servers []*rpc.Client
	for i := 0; i < count; i++ {
		if i!=me{
			var client *rpc.Client
			client, err := rpc.DialHTTP("tcp", ip[i] + ":8080")
			//err := 'a'
			for err != nil {
				//log.Fatal("dialing:", err)
				client, err = rpc.DialHTTP("tcp", ip[i] + ":8080")
			}
			servers = append(servers,client)
			fmt.Println("connected %d",i)
		}
	}

	for i:=0; i< count-1;i++{
		//if i!=me{
			var reply int
			
			err := servers[i].Call("Arith.Multiply",&Args{2,3},&reply)
			if err != nil{
				//log.Fatal("arith error:",err)
				fmt.Println(err)
			}
			fmt.Println("Arith: %d",reply)
			time.Sleep(10*time.Second)
		//}
	}
	*/
}
