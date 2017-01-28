package dlock_etcd

import (
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/jrivets/dist_lock"
)

type Config struct {
	Path    string
	EtcdCfg clientv3.Config
}

type etcd_storage struct {
	client *clientv3.Client
}

func NewEtcdStorage(cfg *Config) *etcd_storage {
	client, err := clientv3.New(cfg.EtcdCfg)
	if err != nil {
		panic(errors.New("Cannot create new etcd client: " + err.Error()))
	}

	path := cfg.Path
	if path == "" {
		path = "/dist_lock"
	}

	return &etcd_storage{client: client}
}

func reOpen(cfg *Config) *clientv3.Client, error {
	client, err := clientv3.New(cfg.EtcdCfg)
	if err != nil {
		return nil, err
	}

	path := cfg.Path
	if path == "" {
		path = "/dist_lock"
	}
	
	client
}

func (es *etcd_storage) Create(record *dlock.Record) (*dlock.Record, error) {
	return nil, nil
}

func (es *etcd_storage) Get(key string) (*dlock.Record, error) {
	return nil, nil
}

func (es *etcd_storage) CasByVersion(record *dlock.Record) (*dlock.Record, error) {
	return nil, nil
}

func (es *etcd_storage) Delete(record *dlock.Record) (*dlock.Record, error) {
	return nil, nil
}

func (es *etcd_storage) WaitVersionChange(key string, version int, timeout time.Duration) (*dlock.Record, error) {
	return nil, nil
}

func (es *etcd_storage) Close() {
	es.client.Close()
}
