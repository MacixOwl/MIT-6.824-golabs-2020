package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
type GetReduceCntArgs struct {
}
type GetReduceCntReply struct {
	ReduceCnt int
}

type RequestTaskArgs struct {
	WorkerId int
}
type RequestTaskReply struct {
	TaskType  TaskType
	TaskId    int
	TasksFile string
}

// worker report
type ReportTaskArgs struct {
	WorkerId int
	TaskType TaskType
	TaskId   int
}
type ReportTaskReply struct {
	IfExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
