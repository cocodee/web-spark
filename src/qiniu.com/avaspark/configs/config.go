package configs

import "qiniupkg.com/x/config.v7"
import "qiniu.com/avaspark/services"

type AvaSparkConf struct {
	HttpPort string `json:"http_port"`
	HttpAddr string `json:"http_addr"`

	SparkHost string                   `json:"sparkHost`
	PulpConf  services.PulpServiceConf `json:"pulpConf"`
}

var GlobalConfig AvaSparkConf = AvaSparkConf{}

func LoadConfig() error {
	config.Init("f", "avaspark", "avaspark.conf")
	return config.Load(&GlobalConfig)
}
