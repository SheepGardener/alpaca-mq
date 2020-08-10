package alpaca

import (
	"time"

	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
)

type PushConfig struct {
	Servers []string
	Sconf   *sarama.Config
}

func NewPusherConfig(cf *GPusherConfig) *PushConfig {
	conf := &PushConfig{}
	conf.Sconf = sarama.NewConfig()

	conf.Sconf.Producer.Return.Successes = true
	conf.Sconf.Producer.Return.Errors = true

	conf.Sconf.Producer.RequiredAcks = sarama.WaitForAll

	conf.Sconf.Version = sarama.V2_5_0_0

	conf.Sconf.Producer.Retry.Max = 10

	conf.Servers = cf.Kafka
	return conf
}

type PullConfig struct {
	servers []string
	sarama_cluster.Config
}

func NewPullerConfig(gcf *GPullerConfig) *PullConfig {

	conf := &PullConfig{}
	conf.Config = *sarama_cluster.NewConfig()

	conf.Config.Config.Consumer.Return.Errors = true
	conf.Config.Group.Return.Notifications = true

	conf.Config.Config.Consumer.Offsets.CommitInterval = time.Duration(gcf.OffsetCtTime) * time.Second

	if gcf.Cmode == 1 {
		conf.Config.Config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		conf.Config.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	conf.servers = gcf.Kafka

	return conf
}

func InitPuller(logpath string, cfgfile string, apdir string) *Puller {

	lg := NewLogger(logpath)

	lg.Init(time.Hour)

	chd := NewCfgHandle(lg)

	Agpullcfg := chd.InitGPullerCfg(cfgfile)

	return NewPuller(lg, Agpullcfg, chd.InitAppCfg(apdir, Agpullcfg.Alist))
}

func InitPusher(logpath string, cfgfile string) *Pusher {

	lg := NewLogger(logpath)

	lg.Init(time.Hour)

	chd := NewCfgHandle(lg)

	return NewPusher(lg, chd.InitGPusherCfg(cfgfile))
}
