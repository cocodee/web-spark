package main

import (
	"github.com/qiniu/log.v1"
	"github.com/teapots/params"
	"github.com/teapots/teapot"
	"qiniu.com/avaspark/configs"
	"qiniu.com/avaspark/db"
	"qiniu.com/avaspark/services"
	"qiniu.com/avaspark/zmqcallback"
)

type teapotConf map[string]string

func (t teapotConf) Find(name string) string {
	return t[name]
}
func main() {
	log.Std.SetOutputLevel(log.Ldebug)
	if err := configs.LoadConfig(); err != nil {
		log.Fatalf("load configuration error: %v", err)
		return
	}
	cfg := configs.GlobalConfig
	log.Debugf("config:%v", cfg)
	teaConf := teapotConf{
		"http_port": cfg.HttpPort,
		"http_addr": cfg.HttpAddr,
	}
	tea := teapot.New()
	tea.ImportConfig(teaConf)
	tea.Provide(params.ParamsParser())
	mongodb := &db.MongoDB{
		Address:  cfg.DB.Address,
		Database: cfg.DB.Database,
	}
	tea.Provide(mongodb)
	pulpServiceProvider := services.NewPulpServiceProvider(cfg.SparkHost, cfg.PulpConf)
	tea.Provide(pulpServiceProvider)
	livyServiceProvider := services.NewLivyServiceProvider(cfg.SparkHost, services.LivyServiceConf{})
	tea.Provide(livyServiceProvider)
	tea.Routers(
		teapot.Any(services.Default),
		teapot.Router("/fetch", teapot.Post(&services.PulpService{}).Action("SubmitJob")),
		teapot.Router("/submit", teapot.Post(&services.LivyService{}).Action("SubmitJob")),
	)
	zmqcallback.Connect("tcp://61.153.154.157:5561", func(msgs []string) {
		zmqcallback.MessageHandler(mongodb, msgs)
	})
	if err := tea.Run(); err != nil {
		log.Fatalf("service start error: %v", err)
	}
}
