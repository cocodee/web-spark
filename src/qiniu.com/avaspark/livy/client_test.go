package livy

import (
	"testing"
	"time"
)

type LivyListener struct {
	t *testing.T
}

func (ll *LivyListener) StateChanged(state JobStateResult) {
	ll.t.Logf("Livy listener:%s", state)
}
func TestClient(t *testing.T) {
	client := &Client{
		BaseURL: "http://61.153.154.154:8998",
	}
	handle, err := client.SubmitJob(Job{
		File: "/pi.py",
		Args: []string{"5"},
	})
	if err != nil {
		t.Errorf("send request error:%v", err)

	}
	handle.AddListener(&LivyListener{})
	handle.Start()
	time.Sleep(5 * time.Minute)
}
