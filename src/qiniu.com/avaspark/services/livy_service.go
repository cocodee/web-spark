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

type LivyServiceConf struct {
}
type LivyService struct {
	Req                 *http.Request        `inject`
	Rw                  http.ResponseWriter  `inject`
	Params              *params.Params       `inject`
	LivyServiceProvider *LivyServiceProvider `inject`
	DB                  *db.MongoDB          `inject`
}

func (l *LivyService) SubmitJob() (err error) {
	livyJob := &livy.LivyJob{}
	err = l.Params.BindJsonBody(livyJob, false)
	if err != nil {
		net.ErrWriteResp(l.Rw, 401, "request should include file url", nil)
		return
	}
	uid, _ := uuid.Gen(16)
	job := livy.Job{
		UID:     uid,
		LivyJob: *livyJob,
	}
	batchID, err := l.LivyServiceProvider.SubmitJob(l.DB, job)
	//err = l.LivyServiceProvider.QueueJob(job)
	if err != nil {
		net.ErrWriteResp(l.Rw, 401, err.Error(), nil)
		return
	}
	log.Debug(batchID)
	job.BatchID = batchID
	net.WriteResp(l.Rw, job, nil)
	//jobHandle, err := l.client.SubmitJob(job)
	//l.jobHandles.PushBack(jobHandle)
	return err
}

func NewLivyServiceProvider(host string, conf LivyServiceConf) *LivyServiceProvider {
	livyServiceProvider := &LivyServiceProvider{
		Host: host,
		client: &livy.LivyClient{
			BaseURL: host,
		},
		jobHandles: list.New(),
		lock:       &sync.RWMutex{},
	}
	return livyServiceProvider
}

type LivyServiceProvider struct {
	Host       string
	client     *livy.LivyClient
	jobHandles *list.List
	lock       *sync.RWMutex
}

func (l *LivyServiceProvider) SubmitJob(mdb *db.MongoDB, job livy.Job) (batchID string, err error) {
	//uid, _ := uuid.Gen(16)
	job_json, err := json.Marshal(job)
	log.Debugf("job:%v", string(job_json))
	jobHandle, err := l.client.SubmitJob(job)
	if err != nil {
		return
	}
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
	/*
		jobHandle.Start()
		l.lock.Lock()
		l.jobHandles.PushBack(jobHandle)
		l.lock.Unlock()
	*/
	return jobHandle.GetBatchID(), err
}

func (l *LivyServiceProvider) QueueJob(job livy.Job) (err error) {
	//uid, _ = uuid.Gen(16)
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
	return nil
}
