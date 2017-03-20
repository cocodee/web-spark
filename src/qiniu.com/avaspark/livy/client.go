package livy

import (
	"context"
	"net/http"

	"qiniupkg.com/x/rpc.v7"
)

//Client to send request to livy server
type Client struct {
	baseURL string
}

//JobResult is the livy server api result for create batch request
type JobResult struct {
	ID string `json:"id"`
}

//Job is a livy job config struct
type Job struct {
	File           string                 `json:"file"`
	Args           []string               `json:"args"`
	PyFiles        string                 `json:"pyFiles"`
	DriverMemory   string                 `json:"driverMemory"`
	DriverCores    int                    `json:"driverCores"`
	ExecutorMemory string                 `json:"executorMemory"`
	ExecutorCores  int                    `json:"executorCores"`
	Conf           map[string]interface{} `json:"conf"`
}

//SubmitJob submits a job request to livy server and returns a JobHandle
func (client *Client) SubmitJob(job Job) (jobHandle *JobHandle, err error) {
	return client.sendRequest(job)
}

func (client *Client) sendRequest(job Job) (jobHandle *JobHandle, err error) {
	url1 := client.baseURL + "/batches"
	rpcClient := rpc.DefaultClient
	result := &JobResult{}
	err = rpcClient.CallWithJson(context.TODO(), result, http.MethodPost, url1, job)
	if err != nil {
		jobHandle := NewJobHandle(client.baseURL, result.ID)
		return jobHandle, nil
	}
	return nil, err
}
