package alpaca

import (
	"io/ioutil"
	"os"

	yaml "gopkg.in/yaml.v2"
)

type App struct {
	Protocol   string   `yaml:"protocol"`
	Path       string   `yaml:path`
	Name       string   `yaml:"name"`
	ServerType string   `yaml:"server_type"`
	Cmd        string   `yaml:"cmd"`
	Servers    []string `yaml:"servers"`
}

type GPusherCmd struct {
	Cmd    string `yaml:"cmd"`
	Status bool   `yaml:"st"`
}

type GPusherConfig struct {
	Kafka     []string     `yaml:"kafka"`
	Topic     string       `yaml:"topic"`
	RateLimit int64        `yaml:"rate_limit"`
	Cmds      []GPusherCmd `yaml:"cmds"`
}

type GPullerConfig struct {
	Kafka         []string `yaml:"kafka"`
	Zookeeper     []string `yaml:"zookeeper"`
	Redis         string   `yaml:"redis"`
	ZkRetryTimes  int32    `yaml:"zk_retry_times"`
	MsgCmtMode    int8     `yaml:"msg_commit_mode"`
	MsgDelayAble  int8     `yaml:"msg_delay_able"`
	OffsetCtTime  int32    `yaml:"offset_ct_time"`
	TimeWheelSize int32    `yaml:"time_wheel_size"`
	Wnd           int32    `yaml:"wnd"`
	GroupName     string   `yaml:"group_name"`
	Topic         string   `yaml:"topic"`
	Cmode         int8     `yaml:"msg_consum_mode"`
	Gpath         string   `yaml:"gpath"`
	Alist         []string `yaml:"services"`
	LoadBMode     int8     `yaml:"load_balance_mode"`
}

type CfgHandle struct {
	logger *Logger
}

func NewCfgHandle(lg *Logger) *CfgHandle {
	return &CfgHandle{
		logger: lg,
	}
}

func (c *CfgHandle) InitGPusherCfg(cfgFile string) *GPusherConfig {

	gcfg := &GPusherConfig{}

	c.checkConfigFile(cfgFile)

	fileContent, err := ioutil.ReadFile(cfgFile)

	if err != nil {
		c.logger.Fatalf("Read Config File Failed Error:%s", err)
	}

	var errs error

	errs = yaml.Unmarshal(fileContent, gcfg)

	if errs != nil {
		c.logger.Fatalf("Parse Config File Failed Error:%s", errs)
	}

	return gcfg
}
func (c *CfgHandle) InitGPullerCfg(cfgFile string) *GPullerConfig {

	gcfg := &GPullerConfig{}

	c.checkConfigFile(cfgFile)

	fc, err := ioutil.ReadFile(cfgFile)

	if err != nil {
		c.logger.Fatalf("Read Config File Failed Error:%s", err)
	}

	var errs error

	errs = yaml.Unmarshal(fc, gcfg)

	if errs != nil {
		c.logger.Fatalf("Parse Config File Failed Error:%s", errs)
	}
	return gcfg
}
func (c *CfgHandle) InitAppCfg(apdir string, list []string) map[string]App {

	if len(list) == 0 {
		c.logger.Fatal("No App Configuration Available")
	}

	var errs error

	aps := make(map[string]App)

	for _, apfile := range list {

		apfPath := apdir + apfile

		s := App{}

		c.checkConfigFile(apfPath)

		fc, err := ioutil.ReadFile(apfPath)

		if err != nil {
			c.logger.WithFields(Fields{"app_file": apfile}).Fatalf("Init App Failed Err:", err)
		}

		errs = yaml.Unmarshal(fc, &s)

		if errs != nil {
			c.logger.WithFields(Fields{"app_file": apfile}).Fatalf("Init App Failed Err:", errs)
		}

		aps[s.Cmd] = s
	}

	return aps
}

func (c *CfgHandle) checkConfigFile(configPath string) {
	if configPath == "" {
		c.logger.Fatalf("Path:%s Err:The path is not set ", configPath)
	}
	_, err := os.Stat(configPath)

	if err != nil {
		c.logger.Fatalf("Path:%s Err:Conf load failed", configPath)
	}
	if os.IsNotExist(err) {
		c.logger.Fatalf("Path:%s Err:Path not exist", configPath)
	}
}
