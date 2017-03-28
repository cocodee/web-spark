package net

import (
	"encoding/json"
	"net/http"

	"github.com/qiniu/log.v1"
)

func ErrWriteResp(w http.ResponseWriter, statuscode int, obj interface{}, HeaderInfo map[string][]string) {

	ma, err := json.Marshal(obj)
	if err != nil {
		log.Errorf("ErrWriteResp json.marshal  error: %v ", err)
		panic(err)
		return
	}

	for key, values := range HeaderInfo {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(statuscode)

	_, err = w.Write(ma)
	if err != nil {
		log.Errorf("ErrWriteResp write response body error: %v ", err)
		return
	}
}

func WriteResp(w http.ResponseWriter, obj interface{}, HeaderInfo map[string][]string) {

	ma, err := json.Marshal(obj)
	if err != nil {
		log.Errorf("WriteResp json.marshal  error: %v ", err)
		panic(err)
		return
	}

	for key, values := range HeaderInfo {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	_, err = w.Write(ma)
	if err != nil {
		return
	}

}
