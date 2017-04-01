package services

import (
	"net/http"

	"github.com/teapots/params"

	"container/list"

	"encoding/json"

	"sync"

	"github.com/nsqio/go-nsq"
	"github.com/qiniu/log.v1"
	"github.com/qiniu/uuid"
	"qiniu.com/avaspark/db"
	"qiniu.com/avaspark/livy"
	"qiniu.com/avaspark/net"
	"qiniu.com/avaspark/queue"
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
	//err = l.PulpServiceProvider.SubmitJob(url, l.DB)
	_, err = l.PulpServiceProvider.QueueJob(url)
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
		lock:       &sync.RWMutex{},
	}
	return pulpServiceProvider
}

type PulpServiceProvider struct {
	Host       string
	Conf       PulpServiceConf
	client     *livy.LivyClient
	jobHandles *list.List
	lock       *sync.RWMutex
}

func (l *PulpServiceProvider) SubmitJob(url string, mdb *db.MongoDB) (err error) {
	log.Debugf("pulpConf:%v", l.Conf)
	uid, _ := uuid.Gen(16)
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
	job.UID = uid
	err = db.InsertJob(mdb, job, jobHandle.GetBatchID())
	if err != nil {
		return
	}
	jobHandle.AddListener(func(state livy.JobState) {
		db.UpdateJobState(mdb, job.UID, state.State)
		if state.State == livy.StateDead || state.State == livy.StateSuccess || state.State == livy.StateTimeout {
			log.Debugf("job finished with state:%v", state.State)
			l.lock.Lock()
			var next *list.Element
			for e := l.jobHandles.Front(); e != nil; {
				if e.Value.(*livy.JobHandle) == jobHandle {
					next = e.Next()
					l.jobHandles.Remove(e)
					e = next

				} else {
					e = e.Next()
				}
			}
			log.Debugf("list size:%v", l.jobHandles.Len())
			l.lock.Unlock()
		}
	})
	jobHandle.Start()
	l.lock.Lock()
	l.jobHandles.PushBack(jobHandle)
	l.lock.Unlock()
	return err
}

func (l *PulpServiceProvider) QueueJob(url string) (uid string, err error) {
	log.Debugf("pulpConf:%v", l.Conf)
	uid, _ = uuid.Gen(16)
	job := livy.Job{
		File: l.Conf.File,
		Args: []string{url},

		PyFiles: l.Conf.PyFiles,
		UID:     uid,
	}
	job_json, err := json.Marshal(job)
	log.Debugf("job:%v", string(job_json))

	producerHelper := queue.ProducerHelper{
		Topic: "avaspark",
		Conf:  nsq.NewConfig(),
		NSQD:  "127.0.0.1:4150",
	}
	producer, err := producerHelper.New()
	if err != nil {
		return
	}
	err = producerHelper.Publish(producer, job)
	if err != nil {
		return
	}
	return uid, nil
}
