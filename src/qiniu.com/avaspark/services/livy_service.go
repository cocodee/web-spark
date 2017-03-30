package services

import (
	"net/http"

	"github.com/teapots/params"

	"container/list"

	"encoding/json"

	"sync"

	"time"

	"github.com/qiniu/log.v1"
	"github.com/qiniu/uuid"
	"gopkg.in/mgo.v2/bson"
	"qiniu.com/avaspark/db"
	"qiniu.com/avaspark/livy"
	"qiniu.com/avaspark/models"
	"qiniu.com/avaspark/net"
	"qiniu.com/avaspark/utils"
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

const (
	CollectionBatchJob = "batchjob"
)

func (l *PulpService) SubmitJob() (err error) {
	url := l.Params.Get("image")
	if url == "" {
		net.ErrWriteResp(l.Rw, 401, "request should include file url", nil)
		return
	}
	err = l.PulpServiceProvider.SubmitJob(url, l.DB)
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

func (l *PulpServiceProvider) SubmitJob(url string, db *db.MongoDB) (err error) {
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
	err = l.InsertJob(db, job, jobHandle.GetBatchID())
	if err != nil {
		return
	}
	jobHandle.AddListener(func(state livy.JobState) {
		l.UpdateJobState(db, job.UID, state.State)
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

func (l *PulpServiceProvider) InsertJob(db *db.MongoDB, job livy.Job, batchID string) (err error) {
	conn := db.NewConn()
	//defer conn.Close()

	c := conn.C(CollectionBatchJob)
	job_json, err := json.Marshal(job)
	log.Debugf("job:%v", string(job_json))
	err = c.Insert(models.BatchJob{
		BatchJobID:  job.UID,
		JobConf:     utils.ToJson(job),
		LivyBatchID: batchID,
		State:       livy.StateNew,
		CreateAt:    time.Now(),
		UpdateAt:    time.Now(),
	})
	if err != nil {
		log.Error("insert into mongodb failed:%v", job)
	}
	return
}

func (l *PulpServiceProvider) UpdateJobState(db *db.MongoDB, batchJobID string, state string) (err error) {
	conn := db.NewConn()
	defer conn.Close()

	c := conn.C(CollectionBatchJob)
	err = c.Update(bson.M{"batch_job_id": batchJobID}, bson.M{"$set": bson.M{
		"state":     state,
		"update_at": time.Now()},
	})
	if err != nil {
		log.Errorf("update mongodb failed:%v", batchJobID)
	}
	return
}
