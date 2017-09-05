package mapreduce

import "fmt"


// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//


	// this is for waiting these many tasks done. Like a semaphore.
	c := make(chan int, ntasks)

	for i:=0; i<ntasks; i++{
		// note taskIndex is the one should be used later.
		//taskIndex:=i
		// start channel for each task.
		go func(i int){
			// in case of worker failure
			for{
				//fmt.Printf("waiting for a worker")
				wk:= <-mr.registerChannel
				
				// doTaskArgs in common_rpc.go
				args:=DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
				
				ok:=call(wk, "Worker.DoTask",args,new(struct{}))
				// release workers
				go func() {
					mr.registerChannel <- wk
				}()
				if ok{
					c <- 1
					//fmt.Printf("sent done %d", i)
					break
				}
			}
				


		}(i) 
	}

	//Drain the channel.
	for i:=0; i<ntasks;i++{
		<-c
		//fmt.Printf("one worker done")
	}

	

	fmt.Printf("Schedule: %v phase done\n", phase)
}


