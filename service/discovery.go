package service

import (
	"context"
	"fmt"
	"github.com/aluka-7/etcd"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
)

type (
	Discovery interface {
		WatchService(path []string) error
		SetServiceList(key, val string)
		DelServiceList(key string)
		GetServices() []string
		Close() error
	}

	// ServiceDiscovery 服务发现
	ServiceDiscovery struct {
		ctx        context.Context   // 上下文
		cli        *clientv3.Client  // etcd client
		serverList map[string]string // 服务列表
		lock       sync.Mutex
	}
)

// NewServiceDiscovery 新建发现服务
func NewServiceDiscovery(client *clientv3.Client) Discovery {
	fmt.Println("Loading Aluka Etcd Service Discovery")
	return &ServiceDiscovery{
		ctx:        context.Background(),
		cli:        client,
		serverList: make(map[string]string),
	}
}

// WatchService 初始化服务列表和监视
func (s *ServiceDiscovery) WatchService(path []string) error {
	//根据前缀获取现有的key
	prefix := strings.Join(append([]string{etcd.Namespace}, path...), "/")
	resp, err := s.cli.Get(s.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, ev := range resp.Kvs {
		s.SetServiceList(string(ev.Key), string(ev.Value))
	}
	//监视前缀，修改变更的server
	go s.watcher(prefix)
	return nil
}

// watcher 监听前缀
func (s *ServiceDiscovery) watcher(prefix string) {
	rch := s.cli.Watch(s.ctx, prefix, clientv3.WithPrefix())
	log.Info().Msgf("watching prefix:%s now...", prefix)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: // 修改或者新增
				s.SetServiceList(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: // 删除
				s.DelServiceList(string(ev.Kv.Key))
			}
		}
	}
}

// SetServiceList 新增服务地址
func (s *ServiceDiscovery) SetServiceList(key, val string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.serverList[key] = string(val)
	log.Info().Msgf("put key :", key, "val:", val)
}

// DelServiceList 删除服务地址
func (s *ServiceDiscovery) DelServiceList(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.serverList, key)
	log.Info().Msgf("del key:", key)
}

// GetServices 获取所有服务地址
func (s *ServiceDiscovery) GetServices() []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	addrs := make([]string, 0, len(s.serverList))
	for _, v := range s.serverList {
		addrs = append(addrs, v)
	}
	return addrs
}

// Close 关闭服务
func (s *ServiceDiscovery) Close() error {
	return s.cli.Close()
}
