package mr
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "errors"
import "strconv"
import "time"
type Status struct{
	name string
	status File_Status
	_time int64
}

type Master struct {
	// Your definitions here.
	files []Status    //all files
	p int             //the file which will be talked
	finishNum int        //the assigement which had been finished
	totNum int
	workerIds map[int]int  //the relation between fileId and actual workerId

	//for reducer
	mFiles []Status
	mp int
	mFinishNum int
	nReduce int
	mWorkerIds []int  //all actual workerIds
}

func assignFileToWorker(m *Master,fileId int){
	file := &m.files[fileId]
	file.status = ASSIGNED
	file._time = time.Now().Unix()
}

func judgeFileCanAssign(m *Master,fileId int)bool{
	file := &m.files[fileId]
	if file.status == NOT_STARTED{
		return true
	}else if file.status == ASSIGNED{
		return time.Now().Unix()-file._time>10
	}else{
		return false
	}
}

func packageMapReply(m *Master,fileId int,reply *Reply,workerId int,nReduce int){
	file := m.files[fileId]
	reply.FileName = file.name
	reply.FileId = fileId
	reply.WorkerId = workerId
	reply.NReduce = m.nReduce
	reply.Type = MAP
}
func packageSleepReply(reply *Reply){
	reply.Type = SLEEP
}
func packageReduceReply(m *Master,mFileNameSuffix string,mFileId int,workerId int,reply *Reply){
	reply.MFileNameSuffix = mFileNameSuffix
	reply.WorkerIds = m.mWorkerIds
	//fmt.Print("master的mWorkerIds是")
	//fmt.Println(m.mWorkerIds)
	reply.Type = REDUCE
	reply.FileId = mFileId
	reply.WorkerId = workerId

}
// Your code here -- RPC handlers for the worker to call.

func assignMediateFileSuffix(m *Master)(string,int,error){
	if(m.mFinishNum >= m.nReduce){
		return "",-1,errors.New("所有输出文件已全部处理")
	}
	//todo lock
	for true{
		m.mp = (m.mp+1)%m.nReduce
		if m.mFiles[m.mp].status != FINISHED{
			return m.mFiles[m.mp].name,m.mp,nil
		}
	}
	return "",-1,nil
}

var workerNum int

func (m *Master) GetFile(request *Request, reply *Reply) error {
	fmt.Printf("收到请求")
	//workerNum++
	//workerId := workerNum
	if m.finishNum != m.totNum{  //分配map任务
		nowp := m.p
		for (m.p+1)%m.totNum!=nowp {
			fmt.Printf("nowp:%d m.p:%d\n",nowp,m.p)
			if judgeFileCanAssign(m,m.p){
				fmt.Printf("%d 可以被分配\n",m.p)
				workerNum ++
				workerId := workerNum
				assignFileToWorker(m,m.p)
				packageMapReply(m,m.p,reply,workerId,m.nReduce)
				return nil
			}else{
				fmt.Printf("%d不能被分配\n",m.p)
				m.p = (m.p+1)%m.totNum
			}
		}
		//没有分配出去说明目前没有可以分配的
		packageSleepReply(reply)
		return nil
        }else{    //分配reduce任务
		workerNum++
		workerId := workerNum
		mFileNameSuffix,mFileId,merr := assignMediateFileSuffix(m)
		if merr != nil{
			reply.Type = CLOSE
		}else{
			packageReduceReply(m,mFileNameSuffix,mFileId,workerId,reply)
		}
	}
	return nil
}

func (m *Master) ConfirmMapper(request *ConfirmRequest,response *ConfirmResponse) error {
	fmt.Println("收到来自mapper的确认请求")
	fileId := request.FileId
	workerId := request.WorkerId
	if m.workerIds[fileId]==0 {   //if the task hadn't been finish up to now
		m.files[fileId].status = FINISHED
		m.finishNum++
		m.workerIds[fileId] = workerId
		m.mWorkerIds = append(m.mWorkerIds,workerId)
		fmt.Printf("已完成%d/%d的mapper项,由%d完成了%d的任务\n",m.finishNum,m.totNum,workerId,fileId)
		return nil
	}
	fmt.Println("抱歉，这个任务之前已经被完")
	return errors.New("抱歉，这个任务之前已经被完成")
}


func (m *Master) ConfirmReducer(request *ConfirmRequest,response *ConfirmResponse) error {
        //fmt.Println("收到来自reducer的确认")
        fileId := request.FileId
        workerId := request.WorkerId
        if m.mFiles[fileId].status != FINISHED {   //if the task hadn't been finish up to now
                m.mFiles[fileId].status = FINISHED
                m.mFinishNum++
                fmt.Printf("已完成%d/%d的reducer项,由%d完成了%d的任务\n",m.mFinishNum,m.nReduce,workerId,fileId)
		response.Suffix = fileId
                return nil
        }
        return errors.New("抱歉，这个任务之前已经被完成")
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret :=  m.mFinishNum >= m.nReduce

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	println("hahahahaha")
	// Your code here.
	len := len(files)
	m.files = make([]Status,len)
	m.workerIds = make(map[int]int)
	m.totNum = len
	m.nReduce = nReduce
	m.mWorkerIds = make([]int,0)
	for i := range m.files {
		m.files[i].name = files[i]
		m.files[i].status = NOT_STARTED
	}
	m.mFiles = make([]Status,nReduce)
	for i:=0;i<nReduce;i++ {
		m.mFiles[i].name = strconv.Itoa(i)   //start from 0
	}
	m.server()
	return &m
}

