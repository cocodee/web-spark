package utils

import (
	"encoding/json"

	"github.com/qiniu/log.v1"
)

func ToJson(v interface{}) string {
	json_bytes, err := json.Marshal(v)
	json_str := "{}"
	if err != nil {
		log.Errorf("json marshal failed:%v", v)

	}
	json_str = string(json_bytes)
	return json_str
}
