package services

import (
	"net/http"

	"github.com/teapots/params"

	"container/list"

	"sync"

	"github.com/qiniu/log.v1"
	"github.com/qiniu/uuid"
	"qiniu.com/avaspark/db"
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
	DB                  *db.MongoDB          `inject`
}

func (l *PulpService) SubmitJob() (err error) {
	url := l.Params.Get("image")
	if url == "" {
		net.ErrWriteResp(l.Rw, 401, "request should include file url", nil)
		return
	}
	job, err := l.PulpServiceProvider.SubmitPulpJob(url, l.DB)
	//job, err := l.PulpServiceProvider.QueuePulpJob(url)
	if err != nil {
		net.ErrWriteResp(l.Rw, 401, err.Error(), nil)
		return
	}
	net.WriteResp(l.Rw, job, nil)
	//jobHandle, err := l.client.SubmitJob(job)
	//l.jobHandles.PushBack(jobHandle)
	return err
}

func NewPulpServiceProvider(host string, conf PulpServiceConf) *PulpServiceProvider {
	pulpServiceProvider := &PulpServiceProvider{

		LivyServiceProvider: LivyServiceProvider{
			Host: host,
			client: &livy.LivyClient{
				BaseURL: host,
			},
			jobHandles: list.New(),
			lock:       &sync.RWMutex{},
		},
		Conf: conf,
	}
	return pulpServiceProvider
}

type PulpServiceProvider struct {
	LivyServiceProvider
	Conf PulpServiceConf
}

func (l *PulpServiceProvider) SubmitPulpJob(url string, mdb *db.MongoDB) (job livy.Job, err error) {
	log.Debugf("pulpConf:%v", l.Conf)
	uid, _ := uuid.Gen(16)
	job = livy.Job{
		UID: uid,
		LivyJob: livy.LivyJob{
			File:    l.Conf.File,
			Args:    []string{url},
			PyFiles: l.Conf.PyFiles,
		},
	}
	err = l.SubmitJob(mdb, job)
	return job, err
}

func (l *PulpServiceProvider) QueuePulpJob(url string) (job livy.Job, err error) {
	log.Debugf("pulpConf:%v", l.Conf)
	uid, _ := uuid.Gen(16)
	job = livy.Job{
		UID: uid,
		LivyJob: livy.LivyJob{
			File: l.Conf.File,
			Args: []string{url},

			PyFiles: l.Conf.PyFiles,
		},
	}
	err = l.QueueJob(job)
	return job, err
}
