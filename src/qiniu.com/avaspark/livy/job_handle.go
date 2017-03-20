package livy

import (
	"context"
	"net/http"

	"time"

	"qiniupkg.com/x/rpc.v7"
)

//JobListener to respond to Job Event
type JobListener interface {
	StatusChanged(status string)
}

//JobHandle is returned after a job request is submitted, to track job status
type JobHandle struct {
	batchID  string
	baseURL  string
	duration time.Duration

	listeners []JobListener
	state     string
	ticker    *time.Ticker
	done      chan struct{}
}

//JobStateResult is the state of submitted job
type JobStateResult struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

//NewJobHandle returns a new JobHandle
func NewJobHandle(baseURL string, batchID string) *JobHandle {
	return &JobHandle{
		baseURL:   baseURL,
		batchID:   batchID,
		duration:  5 * time.Second,
		listeners: make([]JobListener, 10),
	}
}

//AddListener add a state lister
func (jobHandle *JobHandle) AddListener(listener JobListener) {
	jobHandle.listeners = append(jobHandle.listeners, listener)
}

//Start polling result
func (jobHandle *JobHandle) Start() {
	jobHandle.ticker = time.NewTicker(jobHandle.duration)
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
}

func (jobHandle *JobHandle) pollResult() {
	for {
		select {
		case <-jobHandle.ticker.C:
			jobHandle.sendRequest()
		case <-jobHandle.done:
			jobHandle.ticker.Stop()
			return
		}
	}
}

func (jobHandle *JobHandle) sendRequest() {
	url1 := jobHandle.baseURL + "/batches/" + jobHandle.batchID + "/state"
	rpcClient := rpc.DefaultClient
	result := JobStateResult{}
	err := rpcClient.Call(context.TODO(), &result, http.MethodGet, url1)
	if err != nil {
		jobHandle.callback(result)
	}
}
