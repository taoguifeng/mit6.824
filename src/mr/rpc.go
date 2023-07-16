package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type File_Status int
type Reply_Type int
const (
	NOT_STARTED File_Status = iota +1
	ASSIGNED
	FINISHED
)

const (
	MAP Reply_Type = iota + 1
	REDUCE
	SLEEP
	CLOSE
)
type Request struct{
}

type Reply struct{
	Type Reply_Type    //
	FileName string //map
	FileId int  //map,reduce
	WorkerId int //map
	NReduce int //map
	WorkerIds []int //reduce
	MFileNameSuffix string //reduce
}

type ConfirmRequest struct{
	FileId int
	WorkerId int
}

type ConfirmResponse struct{
	Suffix int
}
// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
