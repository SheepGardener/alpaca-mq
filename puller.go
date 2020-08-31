package alpaca

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Puller struct {
	logger *Logger

	//zk customer service, failure and abnormal messages will be delivered to zk
	zk *Zk

	apps map[string]App

	//The consumer of the message will also submit the message offset, and sometimes it will rebalacne
	pom *Consumer

	topic        string
	gname        string
	lock         sync.Mutex
	msgCmtMode   int8
	zkRetryTimes int32
	wnd          chan bool
	mdelay       int8

	//The realization of the time wheel, delayed messages will be processed accordingly,
	//he provides polling of time slices,
	//and stores a large number of messages at the same time
	tw *TimeWheel

	//Kafka client, it will provide the initial offset so that the offset submission can be guaranteed
	kac        *KaClient
	ofts       map[int32]int64
	zksavePath map[int32]string
	reFlag     bool

	selector ServerSelector

	hdle MessageHandle
}

func NewPuller(lg *Logger, cg *GPullerConfig, aplist map[string]App) *Puller {

	zk, err := NewZk(cg.Zookeeper, 5*time.Second)

	if err != nil {
		lg.Fatalf("Init Zookeeper Server failed, err:%s", err)
	}

	cfg := NewPullerConfig(cg)

	pom, err := InitConsumer(cg.Topic, cg.GroupName, cfg)

	if err != nil {
		lg.Fatalf("Init Consumer Failed Err:%s", err)
	}

	client, err := NewKaClient(cg.Topic, cg.GroupName, cg.Kafka, cfg)

	if err != nil {
		lg.Fatalf("Init KaClient Failed Err:%s", err)
	}

	tw, err := NewTimeWheel(cg.TimeWheelSize)

	if err != nil {
		lg.Fatalf("Init TimeWheel Failed Err:%s", err)
	}

	var rs ServerSelector

	if cg.LoadBMode == 1 {
		rs = NewRoundRobin(aplist)
	} else {
		rs = NewRandomSelect()
	}

	return &Puller{
		topic:        cg.Topic,
		gname:        cg.GroupName,
		logger:       lg,
		zk:           zk,
		apps:         aplist,
		pom:          pom,
		msgCmtMode:   cg.MsgCmtMode,
		zkRetryTimes: cg.ZkRetryTimes,
		wnd:          make(chan bool, cg.Wnd),
		mdelay:       cg.MsgDelayAble,
		kac:          client,
		ofts:         make(map[int32]int64),
		zksavePath:   make(map[int32]string),
		tw:           tw,
		reFlag:       false,
		selector:     rs,
		hdle:         NewMsgHandle(lg),
	}
}

func (p *Puller) handleError() {

	for err := range p.pom.Errors() {
		p.logger.Warnf("Consumer Error:%s", err)
	}
}

//After receiving rebanlance, clean up the old data and initialize the new data
func (p *Puller) handleRebalanceNotify() {

	for ntf := range p.pom.Notifications() {

		p.logger.Infof("Rebalance %+v", ntf)

		tp := p.pom.Subscriptions()

		if len(tp) > 0 {

			//Check whether the rebalance initialization operation is required
			if len(p.ofts) > 0 {
				p.rebalance()
			}

			pls, ok := tp[p.topic]

			if !ok {
				p.logger.Fatal("Unable to get topic partition")
			}

			if len(pls) == 0 {
				p.logger.Fatal("No partition available")
			}

			p.init(pls)

			p.logger.Info("The consumption partition is instantiated successfully")
		}
	}
}

// When consumer-managed partitions are reallocated,
// resources that no longer exist will be recycled
func (p *Puller) rebalance() {

	p.reFlag = true

	p.ofts = make(map[int32]int64)

	p.zksavePath = make(map[int32]string)

	if p.tw.state() {
		p.tw.stop()
	}

	//Clear the old message data in the original time wheel to prevent it from affecting other consumers
	bks := p.tw.getBuckets()

	for _, bk := range bks {

		for bk.len() > 0 {
			bk.del(bk.first())
		}
	}

	go p.twLoop()

	p.reFlag = false
}

func (p *Puller) init(pls []int32) {

	for _, v := range pls {

		oft, err := p.kac.getNextOffset(p.topic, p.gname, v)

		if err != nil {
			p.logger.Fatal("Init Offsert Failed")
		}

		p.ofts[v] = oft - 1

		alcs := p.zk.WorldACL()

		filemsgPath := "/failedmsg"

		ferr := p.zk.Create(filemsgPath, []byte{}, 0, alcs)

		if ferr != nil && ferr.Error() != ZkErrExists {
			p.logger.Fatalf("Create filemsgPath ZkSavePath Failed Err:%s", ferr)
		}

		topicPath := "/failedmsg/" + p.topic

		terr := p.zk.Create(topicPath, []byte{}, 0, alcs)

		if terr != nil && terr.Error() != ZkErrExists {
			p.logger.Fatalf("Create topicPath ZkSavePath Failed Err:%s", terr)
		}

		partMsgPath := topicPath + "/" + strconv.Itoa(int(v))

		perr := p.zk.Create(partMsgPath, []byte{}, 0, alcs)

		if perr != nil && perr.Error() != ZkErrExists {
			p.logger.Fatalf("Create partMsgPath ZkSavePath Failed Err:%s", perr)
		}

		p.zksavePath[v] = partMsgPath
	}
}

//Delay queue, as time progresses, delayed messages will be processed gradually
//Just like a clock, walking in circles
func (p *Puller) twLoop() {

	for {
		select {
		case <-p.tw.start():

			tk := p.tw.gettk()

			var ele *list.Element

			for e := tk.first(); e != nil; e = ele {

				ele = e.Next()

				tg := e.Value.(*Tmsg)

				if tg.ntu > 0 {
					tg.ntu--
					continue
				}

				go p.callap(tg.amg)

				tk.del(e)
			}

			p.tw.inrcPos()

		case <-p.tw.stopChan:
			return
		}
	}
}

// If the default service selector and message processing do not meet your needs,
// you can implement your own way to meet your needs
func (p *Puller) GetLogger() *Logger {
	return p.logger
}

func (p *Puller) SetMessageHandle(mhdle MessageHandle) {
	p.hdle = mhdle
}

func (p *Puller) SetServerSelect(ss ServerSelector) {
	p.selector = ss
}

func (p *Puller) Pull() {

	go p.twLoop()

	go p.handleError()

	go p.handleRebalanceNotify()

	for {

		msg := <-p.pom.Recv()

		if p.reFlag {
			continue
		}

		p.wnd <- true

		p.logger.WithFields(Fields{"message": byteToString(msg.Value), "parition": msg.Partition, "offset": msg.Offset}).Info("Receive Message")

		kmsg := &Kmessage{}

		err := json.Unmarshal(msg.Value, kmsg)

		if err != nil {

			p.logger.WithFields(Fields{"message": byteToString(msg.Value), "parition": msg.Partition, "offset": msg.Offset}).Warnf("Json Unmarchar Error:%s", err)

			go p.smsg(msg)

			continue
		}

		almsg := &AlpaceMsg{
			oft:  msg.Offset,
			part: msg.Partition,
			kmsg: kmsg,
		}

		if kmsg.Delay > 0 && p.mdelay > 0 {

			go p.sDly(almsg)
			continue
		}

		go p.proc(almsg)
	}
}

func (p *Puller) cmtOft(part int32, offset int64) error {

	p.lock.Lock()

	defer p.lock.Unlock()

	oft, ok := p.ofts[part]

	if !ok {
		return nil
	}

	//At startup, if there is no recently submitted offset, it will return -1 or -2
	//Will cause the offset of the message to be submitted ahead of time
	//Need to pay attention to the processing of subsequent offsets
	if oft < 0 {
		p.ofts[part] = offset
		p.pom.MarkOffset(p.topic, part, offset, p.gname)
		return nil
	}

	if p.msgCmtMode > 0 {

		if oft < offset {
			p.ofts[part] = offset
			p.pom.MarkOffset(p.topic, part, offset, p.gname)
		}

	} else {

		if offset < oft {
			return nil
		}

		//To ensure that the message is not lost, and accurate consumption
		if oft+1 != offset {
			return errors.New("Offset Not Equal NextOffset")
		}

		p.ofts[part] = offset

		p.pom.MarkOffset(p.topic, part, offset, p.gname)
	}

	return nil
}

func (p *Puller) proc(almsg *AlpaceMsg) {

	if p.reFlag {
		return
	}

	<-p.wnd

	go p.callap(almsg)

}

func (p *Puller) callap(almsg *AlpaceMsg) {

	isRes := false

	for {

		if p.reFlag {
			return
		}

		if !isRes {

			err := p.hmsg(almsg.kmsg)

			if err != nil {
				p.logger.WithFields(Fields{"logId": almsg.kmsg.LogId, "message": fmt.Sprintf("%+v", almsg)}).Warnf("HanleMessag Failed err:%s", err)
				continue
			}

			isRes = true
		}

		err := p.cmtOft(almsg.part, almsg.oft)

		if err == nil {
			p.logger.WithFields(Fields{"logId": almsg.kmsg.LogId, "message": fmt.Sprintf("%+v", almsg)}).Info("Message Consumer Success")
			return
		}

		p.logger.WithFields(Fields{"partition:": almsg.part, "offsert": almsg.oft}).Warnf("CommitOffset Failed Err:%s", err)

		time.Sleep(100 * time.Millisecond)
	}
}

func (p *Puller) sDly(msg *AlpaceMsg) {

	if p.reFlag {
		return
	}

	<-p.wnd

	p.tw.addAmsg(msg)

}

func (p *Puller) gAurl(cmd string) (string, error) {

	app, ok := p.apps[cmd]

	if !ok {
		return "", errors.New("Not Cmd App Exists")
	}

	return p.selector.GetAppUrl(app), nil
}

func (p *Puller) hmsg(Kmsg *Kmessage) error {

	url, err := p.gAurl(Kmsg.Cmd)

	if err != nil {
		return err
	}

	p.logger.WithFields(Fields{"logId": Kmsg.LogId, "url": url}).Info("Request info")

	return p.hdle.HandleMessage(url, Kmsg)
}

//Abnormal messages will be saved to zookeeper to help us quickly restore and replay data
func (p *Puller) smsg(message *sarama.ConsumerMessage) {

	pth, ok := p.zksavePath[message.Partition]

	if !ok {
		return
	}

	bte, err := json.Marshal(message)

	if err != nil {
		p.logger.WithFields(Fields{"message": message}).Warnf("Json Marshal Message Failed Error:%s", err)
	}

	acls := p.zk.WorldACL()

	topicNodePath := "/failedmsg/" + p.topic

	ndpath := pth + "/" + strconv.Itoa(int(message.Offset))

	for i := 0; i < int(p.zkRetryTimes); i++ {

		if err := p.zk.Create(ndpath, bte, 0, acls); err != nil {

			if err.Error() == ZkErrExists {
				return
			}
			p.logger.WithFields(Fields{"topic": p.topic, "retry": i, "topic_path": topicNodePath, "node_path": ndpath}).Warnf("[Create Topic Node Child]Zookeeper Server Error Err:%s", err)
			continue
		}

		return
	}
}
