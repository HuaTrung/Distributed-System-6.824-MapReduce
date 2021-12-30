package mr

import (
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"regexp"
	"sync"

	"../models"

	log "github.com/sirupsen/logrus"
)

type Master struct {
	// Your definitions here.
	MapTasks    map[string]*models.MapTask
	ReduceTasks map[string]*models.ReduceTaskMaster
	Workers     map[string]*Worker
	WorkerMutex sync.Mutex
	WorkerQueue []string
}

func (m *Master) NumberOfIdle() int {
	count := 0
	for _, v := range m.Workers {
		if v.State == Free {
			count += 1
		}
	}
	return count
}

func (m *Master) HeartBeat(w *Worker) bool {
	reply := models.KeyValue{}
	// if !IsOpened(w.Host, w.Port) {
	// 	return false
	// }
	return m.call(w.Id, "Worker.HeartBeat", true, &reply)
}

func (m *Master) DispatchTask() int {
	if len(m.WorkerQueue) > 0 && len(m.Workers) > 0 {
		cur := m.WorkerQueue[0]
		m.WorkerQueue = m.WorkerQueue[1:]
		for k, v := range m.MapTasks {
			if v.State == models.Idle {
				tsk := &models.MapTask{
					TaskInfo: models.TaskInfo{
						Key:      k,
						WorkerID: cur,
					},
					Value: m.MapTasks[k].Value,
				}
				m.MapTasks[k].State = models.Inprogress
				m.Workers[cur].State = Busy
				reply := models.KeyValue{}
				m.call(m.Workers[cur].Id, "Worker.DispatchMapTask", tsk, &reply)
				log.Info("Deliveried ", tsk.Value, " map task to worker ", cur)
				return 1
			}
		}
		for k, v := range m.ReduceTasks {
			if v.State == models.Idle {
				values := make([]string, 0, len(m.ReduceTasks[k].Value))
				for k := range m.ReduceTasks[k].Value {
					values = append(values, k)
				}
				tsk := &models.ReduceTaskWorker{
					TaskInfo: models.TaskInfo{
						Key:      k,
						WorkerID: cur,
					},
					Value: values,
				}
				m.ReduceTasks[k].State = models.Inprogress
				m.Workers[cur].State = Busy
				reply := models.KeyValue{}
				m.call(m.Workers[cur].Id, "Worker.DispatchReduceTask", tsk, &reply)
				log.Info("Deliveried ", tsk.Value, " reduce task to worker ", cur)
				return 1
			}
		}
		return 0
	} else {
		// No idle worker
		return -1
	}
}
func (m *Master) GetMapTask(args int, reply *models.KeyValue) error {
	for k, v := range m.MapTasks {
		if v.State == models.Idle {
			reply.Key = k
			reply.Value = m.MapTasks[k].Value
			m.MapTasks[k].State = models.Inprogress
			log.Info("Deliveried ", reply.Value, " to worker")
			return nil
		}
	}
	log.Info("No more map taks left")
	return nil
}

func (m *Master) DoneReduceTask(tsk models.ReduceTaskResponse, reply *models.KeyValue) error {
	// marking completed
	m.ReduceTasks[tsk.Key].State = models.Completed
	m.WorkerMutex.Lock()
	// free worker
	if m.Workers[tsk.WorkerID].State == Busy {
		m.Workers[tsk.WorkerID].State = Free
		m.WorkerQueue = append(m.WorkerQueue, tsk.WorkerID)
	} else {
		log.Info(" Submit task response from failed worker ", tsk.WorkerID)
	}
	m.WorkerMutex.Unlock()
	log.Info("Reduce Task ", tsk.Key, " has been done")
	return nil
}

func (m *Master) DoneMapTask(tsk models.MapTaskResponse, reply *models.KeyValue) error {
	// marking completed
	m.MapTasks[tsk.Key].State = models.Completed

	// initing new reduce task relatively
	for _, v := range tsk.Value {
		if _, ok := m.ReduceTasks[v.Key]; !ok {
			m.ReduceTasks[v.Key] = &models.ReduceTaskMaster{
				TaskInfo: models.TaskInfo{
					WorkerID: tsk.WorkerID,
					Key:      tsk.Key,
					State:    models.Idle,
				},
				Value: make(map[string]bool),
			}
		}
		tmp := m.ReduceTasks[v.Key].Value
		tmp[v.Value] = true
		m.ReduceTasks[v.Key].Value = tmp
	}

	// free worker
	m.WorkerMutex.Lock()
	if m.Workers[tsk.WorkerID].State == Busy {
		m.Workers[tsk.WorkerID].State = Free
		m.WorkerQueue = append(m.WorkerQueue, tsk.WorkerID)
	} else {
		log.Info(" Submit task response from failed worker ", tsk.WorkerID)
	}
	m.WorkerMutex.Unlock()
	log.Info("Map Task ", tsk.Key, " has been done")
	return nil
}

func (m *Master) Register(worker *MetaWorker, reply *models.KeyValue) error {

	c, err := rpc.DialHTTP("tcp", "127.0.0.1:"+worker.Port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	m.WorkerMutex.Lock()
	m.Workers[worker.Id] = &Worker{
		MetaWorker: MetaWorker{
			Id:   worker.Id,
			Port: worker.Port,
			Host: worker.Host,
		},
		State:  Free,
		Client: c,
	}

	m.WorkerQueue = append(m.WorkerQueue, worker.Id)
	m.WorkerMutex.Unlock()
	log.Info("New worker with id = ", worker.Id, " & port = ", worker.Port, " has just came")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 5
	l, e := net.Listen("tcp", ":1234")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Info("Server's starting at ", 1234)
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (m *Master) GetListFiles(fileRegex []string, path string) {
	files, err := ioutil.ReadDir(path + "data\\primary")
	if err != nil {
		log.Fatal(err)
	}
	for _, regex := range fileRegex {
		r, _ := regexp.Compile(regex)
		for _, file := range files {
			if r.MatchString(file.Name()) {
				m.MapTasks[file.Name()] = &models.MapTask{
					TaskInfo: models.TaskInfo{
						Key:   file.Name(),
						State: models.Idle,
					},
					Value: file.Name(),
				}
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int, path string) *Master {
	m := Master{
		MapTasks:    make(map[string]*models.MapTask),
		ReduceTasks: make(map[string]*models.ReduceTaskMaster),
		Workers:     make(map[string]*Worker),
		WorkerQueue: make([]string, 0),
	}
	m.GetListFiles(files, path)
	// Your code here.
	m.server()
	return &m
}

func (m *Master) call(id string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1:"+w.Workers[id].Port)
	// // sockname := masterSock()
	// // c, err := rpc.DialHTTP("unix", sockname)
	// if err != nil {
	// 	log.Error("dialing:", err)
	// }
	// defer c.Close()

	err := m.Workers[id].Client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Info(err)
	return false
}
