package services

import (
	"testing"

	"github.com/qiniu/uuid"
	"qiniu.com/avaspark/db"
	"qiniu.com/avaspark/livy"
)

func TestDB(t *testing.T) {
	mdb := db.MongoDB{
		Address:  "mongodb://115.238.147.153:27017,115.238.147.141:27017,115.238.147.148:27017",
		Database: "avaspark",
	}
	job := livy.Job{
		File: "test",
	}
	livyService := PulpServiceProvider{Host: "test"}
	t.Run("insert", func(t *testing.T) {
		uuid0, _ := uuid.Gen(16)
		job.UID = uuid0
		err := livyService.InsertJob(&mdb, job, "111")
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("update", func(t *testing.T) {
		uuid0, _ := uuid.Gen(16)
		job.UID = uuid0
		err := livyService.InsertJob(&mdb, job, "222")
		if err != nil {
			t.Error(err)
		}
		err = livyService.UpdateJobState(&mdb, uuid0, "dead")
		if err != nil {
			t.Error(err)
		}
	})
	//c := mdb.NewConn()
	//c.C(CollectionBatchJob).DropCollection()
}
