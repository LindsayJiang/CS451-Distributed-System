package main

import (
	"errors"
	//"net/rpc"
	//"net"
	//"log"
	//"net/http"
	//"time"
	//"time"
	//"fmt"
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
/*

func main(){

	arith := new(Arith)
	fmt.Println(arith)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//for {
	go	http.Serve(l, nil)
	//}
	//go http.Serve(l, nil)
	//time.Sleep(10*time.Second)
}*/
