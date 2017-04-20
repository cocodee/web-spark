package livy

import (
	"net/http"
	"strconv"
)

//Client to send request to livy server
type LivyClient struct {
	BaseURL string
}

//JobResult is the livy server api result for create batch request
type JobResult struct {
	ID int `json:"id"`
}

//Job is a livy job config struct
type LivyJob struct {
	File           string                 `json:"file"`
	Args           []string               `json:"args,omitempty"`
	PyFiles        []string               `json:"pyFiles,omitempty"`
	DriverMemory   string                 `json:"driverMemory,omitempty"`
	DriverCores    int                    `json:"driverCores,omitempty"`
	ExecutorMemory string                 `json:"executorMemory,omitempty"`
	ExecutorCores  int                    `json:"executorCores,omitempty"`
	Conf           map[string]interface{} `json:"conf,omitempty"`
}

type Job struct {
	UID     string  `json:"uid,omitempty"`
	BatchID string  `json:"batchid, omitempty"`
	LivyJob LivyJob `json:"livyjob"`
}

//SubmitJob submits a job request to livy server and returns a JobHandle
func (client *LivyClient) SubmitJob(job Job) (jobHandle *JobHandle, err error) {
	return client.sendRequest(job)
}

func (client *LivyClient) sendRequest(job Job) (jobHandle *JobHandle, err error) {
	url1 := client.BaseURL + "/batches"
	rpcClient := DefaultClient
	result := JobResult{}
	println(url1)
	err = rpcClient.CallWithJson(nil, &result, http.MethodPost, url1, job.LivyJob)
	if err != nil {
		return nil, err
	}
	jobHandle = NewJobHandle(job.UID, client.BaseURL, strconv.Itoa(result.ID))
	return jobHandle, nil
}
