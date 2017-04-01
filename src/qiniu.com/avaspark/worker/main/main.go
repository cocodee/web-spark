package main

import (
	"os"
	"os/signal"
	"syscall"

	nsq "github.com/nsqio/go-nsq"
	"github.com/qiniu/log.v1"
	"qiniu.com/avaspark/configs"
	"qiniu.com/avaspark/queue"
	"qiniu.com/avaspark/worker"
)

func main() {
	log.Std.SetOutputLevel(log.Ldebug)
	if err := configs.LoadConfig(); err != nil {
		log.Fatalf("load configuration error: %v", err)
		return
	}
	cfg := configs.GlobalConfig
	log.Debugf("config:%v", cfg)
	consumerHelper := &queue.ConsumerHelper{
		Topic:   "avaspark",
		Channel: "worker",
		Conf:    nsq.NewConfig(),
		NSQDs:   []string{"127.0.0.1:4150"},
	}
	consumer, err := worker.NewConsumer(&cfg, consumerHelper)
	if err != nil {
		log.Errorf("new worker failed:%v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigChan:
			consumer.Stop()
			return
		}
	}
}
