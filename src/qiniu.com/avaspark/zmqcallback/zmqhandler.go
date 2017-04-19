package zmqcallback

import (
	"encoding/json"

	"strconv"

	"github.com/qiniu/log.v1"
	"qiniu.com/avaspark/db"
	"qiniu.com/avaspark/livy"
)

type StateClass struct {
	EnumClass string `json:"enumClass"`
	Value     string `json:"value"`
}
type StateChangeMessage struct {
	Event    string     `json:"event"`
	ID       int        `json:"id,omitempty"`
	AppTag   string     `json:"appTag,omitempty"`
	AppID    string     `json:"appId,omitempty"`
	NewState StateClass `json:"newState,omitempty"`
	OldState StateClass `json:"oldState,omitempty"`
}
type IDKnownMessage struct {
	Event  string `json:"event"`
	ID     int    `json:"id,omitempty"`
	AppTag string `json:"appTag,omitempty"`
	AppID  string `json:"appId,omitempty"`
}

const (
	EventIdKnown     = "IdKnown"
	EventStateChange = "StateChange"
)
const (
	StateStarting = "STARTING"
	StateRunning  = "RUNNING"
	StateFinished = "FINISHED"
	StateFailed   = "FAILED"
)

func MessageHandler(mdb *db.MongoDB, msgs []string) {
	for _, msg := range msgs {
		v := make(map[string]interface{})
		err := json.Unmarshal([]byte(msg), &v)
		if err != nil {
			log.Errorf("unmarshal failed:%v", err)
		}
		event := v["event"]
		if event == nil || event == "" {
			log.Errorf("unrecognized message:%v", msg)
		} else if event == EventIdKnown {
			idKnown := IDKnownMessage{}
			err = json.Unmarshal([]byte(msg), &idKnown)
			if err != nil {
				log.Errorf("unmarshal failed:%v", err)
				continue
			}
			db.UPdateJobAppIDByBatchID(mdb, strconv.Itoa(idKnown.ID), idKnown.AppID)
		} else if event == EventStateChange {
			stateChange := StateChangeMessage{}
			err = json.Unmarshal([]byte(msg), &stateChange)
			if err != nil {
				log.Errorf("unmarshal failed:%v", err)
				continue
			}
			newState := "UNKNOWN"
			switch stateChange.NewState.Value {
			case StateStarting:
				newState = livy.StateStarting
			case StateRunning:
				newState = livy.StateRunning
			case StateFinished:
				newState = livy.StateSuccess
			case StateFailed:
				newState = livy.StateDead
			}
			if newState != "UNKNOWN" {
				db.UpdateJobStateByAppID(mdb, stateChange.AppID, newState)
			}
		}
	}
}
