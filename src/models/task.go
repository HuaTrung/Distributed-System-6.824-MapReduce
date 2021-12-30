package models

type TaskState int

const (
	Idle TaskState = iota
	Inprogress
	Completed
)

type TaskInfo struct {
	// Your definitions here.
	WorkerID string
	Key      string
	State    TaskState
}
type MapTask struct {
	TaskInfo
	// Your definitions here.
	Value string
}
type ReduceTaskMaster struct {
	// Your definitions here.
	TaskInfo
	Value map[string]bool
}

type ReduceTaskWorker struct {
	// Your definitions here.
	TaskInfo
	Value []string
}

type MapTaskResponse struct {
	// Your definitions here.
	WorkerID string
	Key      string
	Value    []*KeyValue
}
type ReduceTaskResponse struct {
	// Your definitions here.
	WorkerID string
	Key      string
	Value    string
}
