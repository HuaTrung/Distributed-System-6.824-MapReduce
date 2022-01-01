package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"../models"

	uti "../utilities"
	log "github.com/sirupsen/logrus"
)

type WorkerState int

const (
	Free WorkerState = iota
	Busy
	Failed
)

type JobType int

const (
	Map JobType = iota
	Reduce
	None
)

type MetaWorker struct {
	Id   string
	Host string
	Port string
}
type Worker struct {
	// Your definitions here.
	MetaWorker
	State      WorkerState
	CurTypeJob JobType
	maf        func(string, string) []models.KeyValue
	reducef    func(string, []string) string
	Client     *rpc.Client
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each models.KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func (w *Worker) Map(mapf func(string, string) []models.KeyValue, task models.MapTask) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	if task.Key == "" || task.Value == "" {
		log.Println("No available Map task left.")
	} else {
		path := uti.GetPath()
		file, err := os.Open(path + "data\\primary\\" + task.Value)
		if err != nil {
			log.Fatalf("cannot open %v", task.Value)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.Value)
		}
		kva := mapf(task.Key, string(content))
		sort.Sort(models.ByKey(kva))
		oname := "intermediate_"
		pre := kva[0].Key
		last_ind := 0
		f, err := os.OpenFile(uti.GetPath()+"data\\tmp\\"+w.Id+"\\"+oname+pre+".txt",
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		res := make([]*models.KeyValue, 0)
		for ind, v := range kva {
			if v.Key != pre {
				for i := last_ind; i < ind; i++ {
					err = enc.Encode(&kva[i])
					// fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, kva[i].Value)
				}
				f.Close()
				res = append(res, &models.KeyValue{Key: pre, Value: f.Name()})
				last_ind = ind
				pre = v.Key
				f, err = os.OpenFile(uti.GetPath()+"data\\tmp\\"+w.Id+"\\"+oname+pre+".txt",
					os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				enc = json.NewEncoder(f)
			}
		}
		for i := last_ind; i < len(kva); i++ {
			err = enc.Encode(&kva[i])
		}
		f.Close()
		res = append(res, &models.KeyValue{Key: pre, Value: f.Name()})
		tskReply := models.MapTaskResponse{
			Key:      task.Key,
			Value:    res,
			WorkerID: w.Id,
		}
		for !w.DoneMapTask(tskReply) {
			log.Info("Map Task has something wrong ", oname+pre+".txt")
			time.Sleep(5 * time.Second)
		}
		log.Info(" Finishing Map task at ", oname+pre+".txt")
	}
}

func (w *Worker) Reduce(reducef func(string, []string) string, task models.ReduceTaskWorker) error {

	res := reducef(task.Key, task.Value)
	f, err := os.OpenFile(uti.GetPath()+"data\\final\\mr-out-"+w.Id+".txt",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(task.Key + " " + res + "\n"); err != nil {
		log.Println(err)
	}
	if task.Key == "A" {
		log.Info(task.Key + " : " + res)
	}
	tskReply := models.ReduceTaskResponse{
		Key:      task.Key,
		Value:    res,
		WorkerID: w.Id,
	}
	for !w.DoneReduceTask(tskReply) {
		log.Info("Reduce Task has something wrong ", task.Key)
		time.Sleep(5 * time.Second)
	}
	return nil
}

func (w *Worker) DispatchMapTask(tsk models.MapTask, reply *models.KeyValue) error {
	go w.Map(w.maf, tsk)
	return nil
}
func (w *Worker) DispatchReduceTask(tsk models.ReduceTaskWorker, reply *models.KeyValue) error {
	go w.Reduce(w.reducef, tsk)
	return nil
}

func (w *Worker) HeartBeat(arg bool, reply *models.KeyValue) error {
	return nil
}

func (w *Worker) DoneMapTask(tsk models.MapTaskResponse) bool {
	reply := models.KeyValue{}
	w.call("Master.DoneMapTask", tsk, &reply)
	return true
}

func (w *Worker) DoneReduceTask(tsk models.ReduceTaskResponse) bool {
	reply := models.KeyValue{}
	w.call("Master.DoneReduceTask", tsk, &reply)
	return true
}

func (w *Worker) Register() bool {
	reply := models.KeyValue{}
	w.call("Master.Register",
		&MetaWorker{
			Id:   w.Id,
			Host: w.Host,
			Port: w.Port,
		}, &reply)
	return true
}

func MakeWorker(maf func(string, string) []models.KeyValue, ref func(string, []string) string) *Worker {
	i := 1235
	for ; IsOpened("127.0.0.1", strconv.Itoa(i)); i++ {
	}

	c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	w := Worker{
		MetaWorker: MetaWorker{
			Id:   strconv.Itoa(os.Getpid()),
			Host: "127.0.0.1",
			Port: strconv.Itoa(i),
		},
		State:      Free,
		CurTypeJob: None,
		maf:        maf,
		reducef:    ref,
		Client:     c,
	}

	// generating intermediate folder
	err = os.Mkdir(uti.GetPath()+"data\\tmp\\"+w.Id, 0755)
	if err != nil {
		log.Fatal(err)
	}
	w.server()
	return &w
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (w *Worker) call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// }
	// defer c.Close()
	err := w.Client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Error(err)
	return false
}

func IsOpened(host string, port string) bool {

	timeout := 5 * time.Second
	target := fmt.Sprintf("%s:%s", host, port)

	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		log.Info("IsOpen", err)
		return false
	}

	if conn != nil {
		conn.Close()
		return true
	}

	return false
}

func (w *Worker) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 5

	l, e := net.Listen("tcp", "127.0.0.1:"+w.Port)
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Info("Server's starting at ", w.Port)
	go http.Serve(l, nil)
}
