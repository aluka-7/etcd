package etcd_test

import (
	"context"
	"fmt"
	"github.com/aluka-7/etcd"
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
	err := conf.Add(ctx, []string{"base", "app", "1000"}, "{\"key\":\"name\",\"value\":\"brandon\"}")
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
