package db

import (
	"encoding/json"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/qiniu/log.v1"

	"qiniu.com/avaspark/livy"
	"qiniu.com/avaspark/models"
	"qiniu.com/avaspark/utils"
)

const (
	CollectionBatchJob = "batchjob"
)

func InsertJob(db *MongoDB, job livy.Job, batchID string) (err error) {
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

func UpdateJobState(db *MongoDB, batchJobID string, state string) (err error) {
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
