package main

import (
	"time"

	"encoding/json"

	"github.com/qiniu/log.v1"
	"qiniu.com/avaspark/zmqcallback"
)

func main() {
	log.Std.SetOutputLevel(log.Ldebug)
	stop_chan, err := zmqcallback.Connect("tcp://61.153.154.157:5561", handler)
	if err != nil {
		log.Error("error happened:%v", err)
		return
	}
	time.Sleep(30 * time.Minute)
	stop_chan <- 0
}

func handler(msgs []string) {
	log.Debug("message received\n")
	log.Debug(msgs)
	for _, msg := range msgs {
		v := make(map[string]interface{})
		err := json.Unmarshal([]byte(msg), &v)
		if err != nil {
			log.Errorf("unmarshal failed:%v", err)
		}
		event := v["event"]
		if event == nil || event == "" {
			log.Errorf("unrecognized message:%v", msg)
		} else if event == zmqcallback.EventIdKnown {
			idKnown := zmqcallback.IDKnownMessage{}
			err = json.Unmarshal([]byte(msg), &idKnown)
			if err != nil {
				log.Errorf("unmarshal failed:%v", err)
				continue
			}
			log.Debugf("idknown message:%v", idKnown)
		} else if event == zmqcallback.EventStateChange {
			stateChange := zmqcallback.StateChangeMessage{}
			err = json.Unmarshal([]byte(msg), &stateChange)
			if err != nil {
				log.Errorf("unmarshal failed:%v", err)
				continue
			}
			log.Debugf("statechange message:%v", stateChange)
		}
	}
}
