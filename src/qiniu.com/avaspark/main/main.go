package main

import (
	"time"

	"qiniu.com/avaspark/livy"
)

type LivyListener struct {
}

func (ll *LivyListener) StateChanged(state livy.JobStateResult) {
	println("Livy listener")
	println(state.State)
}
func main() {
	client := &livy.LivyClient{
		BaseURL: "http://61.153.154.154:8998",
	}
	handle, err := client.SubmitJob(livy.Job{
		File: "/pi.py",
		Args: []string{"5"},
	})
	if err != nil {
		println(err.Error())
		return
	}
	handle.AddListener(&LivyListener{})
	handle.Start()
	time.Sleep(5 * time.Minute)
}
