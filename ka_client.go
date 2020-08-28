package alpaca

import (
	"time"

	"github.com/Shopify/sarama"
)

type KaClient struct {
	clt sarama.Client
}

func NewKaClient(topic string, gname string, servers []string, cfg *PullConfig) (*KaClient, error) {

	client, err := sarama.NewClient(servers, &cfg.Config.Config)

	if err != nil {
		return nil, err
	}

	return &KaClient{
		clt: client,
	}, nil
}
func (o *KaClient) topics() ([]string, error) {
	return o.clt.Topics()
}
func (o *KaClient) partitions(topic string) ([]int32, error) {
	return o.clt.Partitions(topic)
}

func (o *KaClient) getOffset(topic string, partition int32) (int64, error) {
	return o.clt.GetOffset(topic, partition, time.Now().Unix())
}

func (o *KaClient) getNextOffset(topic string, gname string, partition int32) (int64, error) {

	ofm, err := sarama.NewOffsetManagerFromClient(gname, o.clt)

	if err != nil {
		return 0, err
	}

	poftm, err := ofm.ManagePartition(topic, partition)

	if err != nil {
		return 0, err
	}

	defer poftm.AsyncClose()

	soft, _ := poftm.NextOffset()

	return soft, nil
}
