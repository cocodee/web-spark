package livy

import (
	"net/http"
	"time"
)

const (
	StateNew      = "new"
	StateStarting = "starting"
	StateRunning  = "running"
	StateSuccess  = "success"
	StateDead     = "dead"
	StateTimeout  = "timeout"
)

//JobHandle is returned after a job request is submitted, to track job status
type JobHandle struct {
	UID       string
	batchID   string
	baseURL   string
	duration  time.Duration
	timeout   time.Duration
	listeners []func(state JobState)
	state     string
	done      chan struct{}
}

//JobStateResult is the state of submitted job, unmarshal from state http request response
type JobStateResult struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

//JobState is the state of the current job
type JobState struct {
	UID     string
	BatchID string
	State   string
}

//NewJobHandle returns a new JobHandle
func NewJobHandle(uid string, baseURL string, batchID string) *JobHandle {
	return &JobHandle{
		UID:       uid,
		baseURL:   baseURL,
		batchID:   batchID,
		duration:  5 * time.Second,
		timeout:   10 * time.Minute,
		listeners: make([]func(state JobState), 0),
		state:     "new",
	}
}

//GetBatchID returns job batchID
func (jobHandle *JobHandle) GetBatchID() string {
	return jobHandle.batchID
}

//AddListener add a state lister
func (jobHandle *JobHandle) AddListener(listener func(state JobState)) {
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

func (jobHandle *JobHandle) callback(result JobState) {
	println(result.State)
	for _, listener := range jobHandle.listeners {
		if listener != nil {
			listener(result)
		}
	}
	if (result.State == StateDead) || (result.State == StateSuccess) || (result.State == StateTimeout) {
		jobHandle.Stop()
	}
}

func (jobHandle *JobHandle) pollResult() {
	timeoutTicker := time.NewTicker(jobHandle.timeout)
	for {
		select {
		case <-jobHandle.done:
			return
		case <-timeoutTicker.C:
			jobHandle.callback(JobState{
				UID:     jobHandle.UID,
				BatchID: jobHandle.batchID,
				State:   StateTimeout,
			})
			return
		default:
			println("---------->")
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
		jobState := JobState{
			UID:     jobHandle.UID,
			BatchID: jobHandle.batchID,
			State:   result.State,
		}
		jobHandle.callback(jobState)
	}
}
