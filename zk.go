package alpaca

import (
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var ZkErrExists = "zk: node already exists"

type Zk struct {
	ZkConn *zk.Conn
}

func NewZk(Servers []string, timeout time.Duration) (*Zk, error) {
	conn, _, err := zk.Connect(Servers, timeout)
	if err != nil {
		return nil, err
	}
	return &Zk{
		ZkConn: conn,
	}, nil
}
func (z *Zk) Create(path string, data []byte, nodeType int32, acl []zk.ACL) error {
	_, err := z.ZkConn.Create(path, data, nodeType, acl)
	if err != nil {
		return err
	}
	return nil
}
func (z *Zk) GetChildren(path string) ([]string, error) {
	childlist, _, err := z.ZkConn.Children(path)
	if err != nil {
		return nil, err
	}
	return childlist, nil
}
func (z *Zk) Get(path string) ([]byte, *zk.Stat, error) {
	data, st, err := z.ZkConn.Get(path)
	if err != nil {
		return nil, nil, err
	}
	return data, st, nil
}
func (z *Zk) Set(path string, data []byte, version int32) error {
	_, err := z.ZkConn.Set(path, data, version)
	if err != nil {
		return err
	}
	return nil
}
func (z *Zk) Delete(path string, version int32) error {
	err := z.ZkConn.Delete(path, version)
	if err != nil {
		return err
	}
	return nil
}
func (z *Zk) Exists(path string) (bool, error) {
	es, _, err := z.ZkConn.Exists(path)
	if err != nil {
		return false, err
	}
	return es, nil
}
func (z *Zk) WorldACL() []zk.ACL {
	acls := zk.WorldACL(zk.PermAll)
	return acls
}
