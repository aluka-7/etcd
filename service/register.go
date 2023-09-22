package service

import (
	"context"
	"fmt"
	"github.com/aluka-7/etcd"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

type (
	Register interface {
		ListenLeaseRespChan()
		Close() error
	}

	// ServiceRegister 创建租约注册服务
	ServiceRegister struct {
		ctx           context.Context                         // 上下文
		cli           *clientv3.Client                        // etcd client
		leaseID       clientv3.LeaseID                        // 租约ID
		keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse // 租约keepalieve相应chan
		key           string                                  // key
		val           string                                  // value
	}
)

func NewServiceRegister(client *clientv3.Client, path []string, val string, ttl int64) (Register, error) {
	fmt.Println("Loading Aluka Etcd Service Register")
	key := strings.Join(append([]string{etcd.Namespace}, path...), "/")
	ser := &ServiceRegister{
		ctx: context.Background(),
		cli: client,
		key: key,
		val: val,
	}
	// 申请租约设置时间keepalive
	if err := ser.putKeyWithLease(ttl); err != nil {
		return nil, err
	}
	return ser, nil
}

// putKeyWithLease 设置租约
func (s *ServiceRegister) putKeyWithLease(ttl int64) error {
	//设置租约时间
	resp, err := s.cli.Grant(s.ctx, ttl)
	if err != nil {
		return err
	}
	// 注册服务并绑定租约
	_, err = s.cli.Put(s.ctx, s.key, s.val, clientv3.WithLease(resp.ID))
	if err != nil {
		return err
	}
	// 设置续租 定期发送需求请求
	leaseRespChan, err := s.cli.KeepAlive(s.ctx, resp.ID)
	if err != nil {
		return err
	}
	s.leaseID = resp.ID
	s.keepAliveChan = leaseRespChan
	return nil
}

// ListenLeaseRespChan 监听续租情况
func (s *ServiceRegister) ListenLeaseRespChan() {
	for leaseKeepResp := range s.keepAliveChan {
		log.Info().Msgf("续约成功, %+v", leaseKeepResp)
	}
	log.Info().Msg("关闭续租")
}

// Close 注销服务
func (s *ServiceRegister) Close() error {
	// 撤销租约
	if _, err := s.cli.Revoke(s.ctx, s.leaseID); err != nil {
		return err
	}
	log.Info().Msg("撤销租约")
	return s.cli.Close()
}
