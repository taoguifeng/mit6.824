package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"
//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func mapper(reply *Reply,mapf func(string,string)[]KeyValue,reducef func(string,[]string)string) {
	println("map执行开始 你为什么不打印")
	fmt.Printf("%d执行的任务id为%d,name为%s，开始任务\n",reply.WorkerId,reply.FileId,reply.FileName)
	fileName := reply.FileName
	fileId := reply.FileId
	workerId := reply.WorkerId
	nReduce := reply.NReduce
println("1")
	intermediate := []KeyValue{}
	file,err := os.Open(fileName)
	if err!=nil{
		fmt.Println("cannot open %v", fileName)
	}
	content,err := ioutil.ReadAll(file)
	println("2a")

	if err!=nil{
		fmt.Println("cannot read %v", fileName)
	}
	println("2b")
	kva := mapf(fileName,string(content))
	println("2c")
	intermediate = append(intermediate,kva...)
	//fmt.Printf("intermediate长度为%d\n",len(intermediate))
println("2")

        //
        // for each distinct key in intermediate[],
        // 1 get the correct Y-location through func ihash
        // 2 get the intermediate fileName by the Y-location
	// 3 write the key-value to the file named fileName
	//

	files := make([]*os.File,nReduce)
	for i:=0;i<nReduce;i++{
		fileName := "inter"+"-"+strconv.Itoa(workerId)+"-"+strconv.Itoa(i)
		files[i],_ = os.Create(fileName)
	}
	for _,kv := range intermediate{
		yloc := ihash(kv.Key) % nReduce
		ofile := files[yloc]
		enc := json.NewEncoder(ofile)
		enc.Encode(&kv)
	}

	for i:=0;i<nReduce;i++{
		files[i].Close()
	}
println("3")
	// uncomment to send the Example RPC to the master.
	// CallExample()

	//intermediate file had been generated
	//feedback to the master 
	confirmRequest := ConfirmRequest{}
	confirmResponse := ConfirmResponse{}
	confirmRequest.FileId = fileId
	confirmRequest.WorkerId = workerId
	println("4")
fmt.Printf("%d执行的任务id为%d,name为%s，任务结束\n",reply.WorkerId,reply.FileId,reply.FileName)

	fmt.Println("请求确认开始\n")
	suc  := call("Master.ConfirmMapper",&confirmRequest,&confirmResponse)
	if !suc{
		fmt.Println("发生错误")
		return
	}
	fmt.Println("收到确认回复")
}

func reducer(reply *Reply,mapf func(string,string)[]KeyValue,reducef func(string,[]string)string){
	fileIdSuffix := reply.MFileNameSuffix
	workerId := reply.WorkerId
	workerIds := reply.WorkerIds
	fmt.Println(workerIds)
	//fmt.Printf("后缀为%s,workerId为%d扫描的文件有:\n",fileIdSuffix,workerId)
	var intermediate []KeyValue
	for _,mapperName := range workerIds{
		fileName := "inter"+"-"+strconv.Itoa(mapperName)+"-"+fileIdSuffix
		//fmt.Printf("%s  ",fileName)
		ifile,_ := os.Open(fileName)
		dec := json.NewDecoder(ifile)
		var kva []KeyValue
		for{
			var kv KeyValue
			if err:= dec.Decode(&kv);err!=nil{
				break
			}
			kva = append(kva,kv)
		}
		intermediate = append(intermediate,kva...)

	}
	//fmt.Println("扫描完毕")
	//fmt.Println(len(intermediate))
	sort.Sort(ByKey(intermediate))
	//oname := "mr-out-" + fileIdSuffix
	tmpFileName := "tmp-"+strconv.Itoa(workerId)  //named by solely workerId
	tmpFile,_ := os.Create(tmpFileName)


	i := 0
	for i < len(intermediate) {
                j := i + 1
                for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
                        j++
                }
                values := []string{}
                for k := i; k < j; k++ {
                        values = append(values, intermediate[k].Value)
                }
                output := reducef(intermediate[i].Key, values)

                // this is the correct format for each line of Reduce output.
                fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

                i = j
        }

        tmpFile.Close()


	confirmRequest := ConfirmRequest{}
        confirmResponse := ConfirmResponse{}
        confirmRequest.FileId,_ = strconv.Atoi(fileIdSuffix)
        confirmRequest.WorkerId = workerId
	suc := call("Master.ConfirmReducer",&confirmRequest,&confirmResponse)
	if suc{
		//rename the final file
		suffix := confirmResponse.Suffix
		newFileName := "mr-out-"+strconv.Itoa(suffix)
		err := os.Rename(tmpFileName,newFileName)
		if err!=nil{
			fmt.Print("改名失败")
		}else{
			fmt.Printf("改名成功，将%s改成了%s\n",tmpFileName,newFileName)
		}
		return
	}
	//fmt.Println("收到确认回复")

	//fmt.Printf("任务%d合并完成后缀名为%s的文件\n",workerId,fileIdSuffix)
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for true{

		request := Request{}
		reply := Reply{}
		fmt.Println("远程调用开始")
		call("Master.GetFile",&request,&reply)
		fmt.Println("远程调用结束")
		_type := reply.Type
		if _type == MAP{
			fmt.Println("MAP")
			mapper(&reply,mapf,reducef)
		}else if _type == REDUCE {
			fmt.Println("REDUCE")
			reducer(&reply,mapf,reducef)
		}else if _type == SLEEP {
			fmt.Println("SLEEP")
			time.Sleep(time.Second*5)
		}else{
			fmt.Println("BREAK")
			break
		}
	}
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	//fmt.Println(err)
	return false
}


