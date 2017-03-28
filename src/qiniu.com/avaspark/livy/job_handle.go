package livy

import (
	"net/http"

	"time"
)

const (
	stateStarting = "starting"
	stateRunning  = "running"
	stateSuccess  = "success"
	stateDead     = "dead"
)

//JobListener to respond to Job Event
type JobListener interface {
	StateChanged(state JobStateResult)
}

//JobHandle is returned after a job request is submitted, to track job status
type JobHandle struct {
	batchID  string
	baseURL  string
	duration time.Duration

	listeners []JobListener
	state     string
	done      chan struct{}
}

//JobStateResult is the state of submitted job
type JobStateResult struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

//NewJobHandle returns a new JobHandle
func NewJobHandle(baseURL string, batchID string) *JobHandle {
	return &JobHandle{
		baseURL:   baseURL,
		batchID:   batchID,
		duration:  5 * time.Second,
		listeners: make([]JobListener, 0),
		state:     "new",
	}
}

//GetBatchID returns job batchID
func (jobHandle *JobHandle) GetBatchID() string {
	return jobHandle.batchID
}

//AddListener add a state lister
func (jobHandle *JobHandle) AddListener(listener JobListener) {
	jobHandle.listeners = append(jobHandle.listeners, listener)
	println("listener added")
}

//Start polling result
func (jobHandle *JobHandle) Start() {
	println("start polling")
	jobHandle.done = make(chan struct{}, 1)
	go func() {
		jobHandle.pollResult()
	}()
}

//Stop polling result
func (jobHandle *JobHandle) Stop() {
	close(jobHandle.done)
}

func (jobHandle *JobHandle) callback(result JobStateResult) {
	print(result.State)
	for _, listener := range jobHandle.listeners {
		if listener != nil {
			listener.StateChanged(result)
		}
	}
}

func (jobHandle *JobHandle) pollResult() {
	for {
		select {
		case <-jobHandle.done:
			return
		default:
			println("send request")
			jobHandle.sendRequest()
			time.Sleep(jobHandle.duration)
		}
	}
}

func (jobHandle *JobHandle) sendRequest() {
	url1 := jobHandle.baseURL + "/batches/" + jobHandle.batchID + "/state"
	println(url1)
	rpcClient := DefaultClient
	result := JobStateResult{}
	err := rpcClient.Call(nil, &result, http.MethodGet, url1)
	if err != nil {
		println("job state request error")
	}
	if result.State != jobHandle.state {
		jobHandle.state = result.State
		jobHandle.callback(result)
	}
}
