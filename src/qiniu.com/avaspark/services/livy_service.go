package services

import (
	"net/http"

	"github.com/teapots/params"

	"container/list"

	"encoding/json"

	"github.com/qiniu/log.v1"
	"qiniu.com/avaspark/livy"
	"qiniu.com/avaspark/net"
)

type PulpServiceConf struct {
	File           string   `json:"file"`
	PyFiles        []string `json:"pyFiles,omitempty"`
	DriverMemory   string   `json:"driverMemory,omitempty"`
	DriverCores    int      `json:"driverCores,omitempty"`
	ExecutorMemory string   `json:"executorMemory,omitempty"`
	ExecutorCores  int      `json:"executorCores,omitempty"`
}
type PulpService struct {
	Req                 *http.Request        `inject`
	Rw                  http.ResponseWriter  `inject`
	Params              *params.Params       `inject`
	PulpServiceProvider *PulpServiceProvider `inject`
}

func (l *PulpService) SubmitJob() (err error) {
	url := l.Params.Get("image")
	if url == "" {
		net.ErrWriteResp(l.Rw, 401, "request should include file url", nil)
		return
	}
	err = l.PulpServiceProvider.SubmitJob(url)
	if err != nil {
		net.ErrWriteResp(l.Rw, 401, err.Error(), nil)
		return
	}
	net.WriteResp(l.Rw, "job submited", nil)
	//jobHandle, err := l.client.SubmitJob(job)
	//l.jobHandles.PushBack(jobHandle)
	return err
}

func NewPulpServiceProvider(host string, conf PulpServiceConf) *PulpServiceProvider {
	pulpServiceProvider := &PulpServiceProvider{
		Host: host,
		Conf: conf,
		client: &livy.LivyClient{
			BaseURL: host,
		},
		jobHandles: list.New(),
	}
	return pulpServiceProvider
}

type PulpServiceProvider struct {
	Host       string
	Conf       PulpServiceConf
	client     *livy.LivyClient
	jobHandles *list.List
}

func (l *PulpServiceProvider) SubmitJob(url string) (err error) {
	log.Debugf("pulpConf:%v", l.Conf)
	job := livy.Job{
		File: l.Conf.File,
		Args: []string{url},

		PyFiles: l.Conf.PyFiles,
	}
	job_json, err := json.Marshal(job)
	log.Debugf("job:%v", string(job_json))
	jobHandle, err := l.client.SubmitJob(job)
	if err != nil {
		return
	}
	jobHandle.Start()
	l.jobHandles.PushBack(jobHandle)
	return err
}
