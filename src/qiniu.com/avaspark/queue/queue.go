package queue

import (
	"errors"

	"encoding/json"

	"github.com/nsqio/go-nsq"
)

type ConsumerHelper struct {
	Topic       string
	Channel     string
	Conf        *nsq.Config
	NSQDs       []string
	NSQLookupds []string
}

func (r *ConsumerHelper) New() (consumer *nsq.Consumer, err error) {
	consumer, err = nsq.NewConsumer(r.Topic, r.Channel, r.Conf)
	if err != nil {
		return
	}
	return
}

func (r *ConsumerHelper) Start(consumer *nsq.Consumer) (err error) {
	if len(r.NSQDs) > 0 {
		return consumer.ConnectToNSQDs(r.NSQDs)
	}
	if len(r.NSQLookupds) > 0 {
		return consumer.ConnectToNSQLookupds(r.NSQLookupds)
	}
	return errors.New("Both NSQDs and NSQLookupds are not configured.")
}

type ProducerHelper struct {
	Topic string
	Conf  *nsq.Config
	NSQD  string
}

func (p *ProducerHelper) New() (producer *nsq.Producer, err error) {
	producer, err = nsq.NewProducer(p.NSQD, p.Conf)
	return
}

func (p *ProducerHelper) Publish(producer *nsq.Producer, v interface{}) (err error) {
	v_str, err := json.Marshal(v)
	err = producer.Publish(p.Topic, []byte(v_str))
	return
}
