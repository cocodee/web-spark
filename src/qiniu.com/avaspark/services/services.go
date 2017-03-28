package services

import (
	"net/http"

	"qiniu.com/avaspark/net"
)

type Resp struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func Default(Req *http.Request, Rw http.ResponseWriter) {
	resp := Resp{
		Code:    100,
		Message: "welcome to ava spark",
	}
	net.WriteResp(Rw, resp, nil)
}
