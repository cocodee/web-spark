package livy

import (
	"testing"
	"time"
)

func testClient(t *testing.T) {
	client := &LivyClient{
		BaseURL: "http://61.153.154.154:8998",
	}
	handle, err := client.SubmitJob(Job{
		UID: "1",
		LivyJob: LivyJob{
			File: "/pi.py",
			Args: []string{"5"},
		},
	})

	callback := func(state JobState) {
		t.Logf("Livy listener:%v", state)
	}
	if err != nil {
		t.Errorf("send request error:%v", err)

	}
	handle.AddListener(callback)
	handle.Start()
	time.Sleep(5 * time.Minute)
}
