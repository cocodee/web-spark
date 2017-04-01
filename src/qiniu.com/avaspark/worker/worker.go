package worker

import (
	"encoding/json"

	nsq "github.com/nsqio/go-nsq"
	"github.com/qiniu/log.v1"
	"qiniu.com/avaspark/configs"
	"qiniu.com/avaspark/db"
	"qiniu.com/avaspark/livy"
	"qiniu.com/avaspark/queue"
)

type handler struct {
	client *livy.LivyClient
	mdb    *db.MongoDB

	StopChan chan int
	exitChan chan int

	jobHandle *livy.JobHandle
}

func (h *handler) HandleMessage(msg *nsq.Message) (err error) {
	log.Debug("enter HandleMessage")
	data := msg.Body
	job := livy.Job{}
	err = json.Unmarshal(data, &job)
	if err != nil {
		return
	}

	uid := job.UID
	job.UID = ""
	h.jobHandle, err = h.client.SubmitJob(job)
	if err != nil {
		log.Errorf("submit job error:%v", err)
		return err
	}
	job.UID = uid
	err = db.InsertJob(h.mdb, job, h.jobHandle.GetBatchID())
	if err != nil {
		return
	}
	h.jobHandle.AddListener(func(state livy.JobState) {
		log.Debug("job state changed")
		db.UpdateJobState(h.mdb, uid, state.State)
		if state.State == livy.StateDead || state.State == livy.StateSuccess || state.State == livy.StateTimeout {
			log.Debugf("job finished with state:%v", state.State)
			h.exitChan <- 1
		}
	})
	h.jobHandle.Start()
	for {
		select {
		case <-h.StopChan:
			h.jobHandle.Stop()
			return
		case <-h.exitChan:
			return
		}
	}
	log.Debug("exit HandleMEssage")
	return
}

func NewConsumer(cfg *configs.AvaSparkConf, consumerHelper *queue.ConsumerHelper) (consumer *nsq.Consumer, err error) {
	mongodb := &db.MongoDB{
		Address:  cfg.DB.Address,
		Database: cfg.DB.Database,
	}
	client := &livy.LivyClient{
		BaseURL: cfg.SparkHost,
	}
	h := &handler{
		client:   client,
		mdb:      mongodb,
		StopChan: make(chan int, 0),
		exitChan: make(chan int, 0),
	}
	for i := 0; i < 10; i++ {
		consumer, err = consumerHelper.New()
		if err != nil {
			return
		}
		consumer.AddHandler(h)
		consumerHelper.Start(consumer)
	}

	return
}
