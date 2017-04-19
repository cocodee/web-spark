package models

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

type BatchJob struct {
	ObjectID    bson.ObjectId `bson:"_id,omitempty"`
	BatchJobID  string        `bson:"batch_job_id,omitempty"`
	JobConf     string        `bson:"job_conf,omitempty"`
	LivyBatchID string        `bson:"livy_batch_id,omitempty"`
	State       string        `bson:"state,omitempty"`
	SparkAppID  string        `bson:"spark_app_id"`
	CreateAt    time.Time     `bson:"create_at,omitempty"`
	UpdateAt    time.Time     `bson:"update_at,omitempty"`
}
