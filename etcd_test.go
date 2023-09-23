package etcd_test

import (
	"context"
	"fmt"
	"github.com/aluka-7/etcd"
	"github.com/aluka-7/etcd/service"
	"sync"
	"testing"
	"time"
)

type test struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestClazz(t *testing.T) {
	conf := etcd.Engine(etcd.NewStoreConfig())
	ctx := context.Background()
	err := conf.Set(ctx, []string{"base", "app", "1000"}, "{\"key\":\"name\",\"value\":\"brandon\"}")
	if err != nil {
		t.Errorf("TestEtcd Error: %+v", err)
	}

	var ts test
	err = conf.Clazz(ctx, []string{"base", "app", "1000"}, &ts)
	if err != nil {
		t.Errorf("TestEtcd Error: %+v", err)
	}
}

func TestWatch(t *testing.T) {
	conf := etcd.Engine(etcd.NewStoreConfig())
	ctx := context.Background()
	var ts test
	err := conf.Clazz(ctx, []string{"base", "app", "1000"}, &ts)
	if err != nil {
		t.Errorf("TestEtcd Error: %+v", err)
	}
	go conf.Watch(ctx, []string{"base", "app", "1000"}, &ts)
	for i := 0; i < 10; i++ {
		fmt.Println(ts)
		time.Sleep(time.Second * 5)
	}
}

func TestLock(t *testing.T) {
	conf := etcd.Engine(etcd.NewStoreConfig())
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			m, s, err := conf.Lock(ctx, []string{"base", "app", "lock"}, 5)
			defer s.Close()
			if err != nil {
				t.Errorf("TestEtcd Error: %+v", err)
			}
			err = m.Lock(ctx)
			if err != nil {
				t.Errorf("TestEtcd Error: %+v", err)
			}
			t.Logf("%d 获取锁成功", i)
			time.Sleep(time.Second * 5)
			err = m.Unlock(ctx)
			if err != nil {
				t.Errorf("TestEtcd Error: %+v", err)
			}
			t.Logf("%d 释放锁成功", i)
		}(i)
	}
	wg.Wait()
}

func TestRegister(t *testing.T) {
	conf := etcd.Engine(etcd.NewStoreConfig())
	ser, err := service.NewServiceRegister(conf.Client(), []string{"service", "web"}, "127.0.0.1:9090", 5)
	if err != nil {
		t.Errorf("TestEtcd Error: %+v", err)
	}
	// 监听续租相应chan
	go ser.ListenLeaseRespChan()
	select {
	case <-time.After(20 * time.Second):
		ser.Close()
	}
}

func TestDiscovery(t *testing.T) {
	conf := etcd.Engine(etcd.NewStoreConfig())
	ser := service.NewServiceDiscovery(conf.Client())
	defer ser.Close()
	//监听续租相应chan
	_ = ser.WatchService([]string{"service"})
	for {
		select {
		case <-time.Tick(5 * time.Second):
			t.Log(ser.GetServices())
		}
	}
}
