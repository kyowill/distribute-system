package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	failureTime int
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	name1, name2 := <- mr.registerChannel, <- mr.registerChannel
	mr.Workers[name1] = &WorkerInfo{name1, 0}
	mr.Workers[name2] = &WorkerInfo{name2, 0}

	log.Printf("RunMaster")
	var loop int
	for i := 0; i < mr.nMap; {
		args := new(DoJobArgs)
		args.File = mr.file
		args.JobNumber = i
		args.Operation = "Map"
		args.NumOtherPhase = mr.nReduce
		var reply DoJobReply
		var w *WorkerInfo
		if loop % 2 == 0 {
			w = mr.Workers[name1]
			if w.failureTime > 10 && mr.Workers[name2].failureTime > 10{
				// fmt.Printf("DoWork: RPC %s DoMap all error\n", w.address)
				// time.Sleep(1 * time.Second)
				delete(mr.Workers, name1)
				delete(mr.Workers, name2)
				name1, name2 = <- mr.registerChannel, <- mr.registerChannel
				mr.Workers[name1] = &WorkerInfo{name1, 0}
				mr.Workers[name2] = &WorkerInfo{name2, 0}
				continue
			} else if w.failureTime > 10 && mr.Workers[name2].failureTime <= 10{
				w = mr.Workers[name2]
				// mr.Workers[name1].failureTime -= 1
			}
		} else {
			w = mr.Workers[name2]
			if w.failureTime > 10 && mr.Workers[name1].failureTime > 10{
				// fmt.Printf("DoWork: RPC %s DoMap all error\n", w.address)
				// time.Sleep(1 * time.Second)
				delete(mr.Workers, name1)
				delete(mr.Workers, name2)
				name1, name2 = <- mr.registerChannel, <- mr.registerChannel
				mr.Workers[name1] = &WorkerInfo{name1, 0}
				mr.Workers[name2] = &WorkerInfo{name2, 0}
				continue
			} else if w.failureTime > 10 && mr.Workers[name1].failureTime <= 10{
				w = mr.Workers[name1]
				// mr.Workers[name2].failureTime -= 1
			}
		}

		ok := call(w.address, "Worker.DoJob", args, &reply)
		if ok == false {
			w.failureTime += 1
		} else {
			w.failureTime = 0
			i += 1
		}
		loop += 1
	}

	loop = 0
	for i := 0; i < mr.nReduce;  {
		args := new(DoJobArgs)
		args.File = mr.file
		args.JobNumber = i
		args.Operation = "Reduce"
		args.NumOtherPhase = mr.nMap
		var reply DoJobReply
		var w *WorkerInfo
		if loop % 2 == 0 {
			w = mr.Workers[name1]
			if w.failureTime > 10 && mr.Workers[name2].failureTime > 10{
				delete(mr.Workers, name1)
				delete(mr.Workers, name2)
				name1, name2 = <- mr.registerChannel, <- mr.registerChannel
				mr.Workers[name1] = &WorkerInfo{name1, 0}
				mr.Workers[name2] = &WorkerInfo{name2, 0}								
				continue
			} else if w.failureTime > 10 && mr.Workers[name2].failureTime <= 10{
				w = mr.Workers[name2]
				// mr.Workers[name1].failureTime -= 1
			}
		} else {
			w = mr.Workers[name2]
			if w.failureTime > 10 && mr.Workers[name1].failureTime > 10{
				delete(mr.Workers, name1)
				delete(mr.Workers, name2)
				name1, name2 = <- mr.registerChannel, <- mr.registerChannel
				mr.Workers[name1] = &WorkerInfo{name1, 0}
				mr.Workers[name2] = &WorkerInfo{name2, 0}
				continue
			} else if w.failureTime > 10 && mr.Workers[name1].failureTime <= 10{
				w = mr.Workers[name1]
				// mr.Workers[name2].failureTime -= 1
			}
		}

		ok := call(w.address, "Worker.DoJob", args, &reply)
		if ok == false {
			w.failureTime += 1
		} else {
			w.failureTime = 0
			i += 1
		}
		loop += 1
	}
	return mr.KillWorkers()
}
