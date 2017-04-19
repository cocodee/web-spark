package zmqcallback

import (
	zmq4 "github.com/pebbe/zmq4"
	"github.com/qiniu/log.v1"
)

func Connect(endpoint string, message_handler func(msg []string)) (stop_chan chan int, err error) {
	context, err := zmq4.NewContext()
	if err != nil {
		log.Errorf("new context failed:%v", err)
		return
	}
	socket, err := context.NewSocket(zmq4.SUB)
	if err != nil {
		log.Errorf("new sub socket failed:%v", err)
		return
	}
	socket.SetSubscribe("")
	log.Debug("connecting...")
	err = socket.Connect(endpoint)
	if err != nil {
		log.Errorf("connect failed:%v", err)
		return
	}
	log.Debug("connected")
	stop_chan = make(chan int)
	go func() {
		for {
			select {
			case <-stop_chan:
				return
			default:
				log.Debug("recving message...")
				msg, err := socket.RecvMessage(0)
				if err != nil {
					log.Errorf("recv message failed:%v", err)
				} else {
					message_handler(msg)
				}
			}
		}
	}()
	return
}
