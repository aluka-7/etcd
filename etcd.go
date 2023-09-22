package etcd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aluka-7/utils"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

const Namespace = "/system"
const DesKey = "aluka-7!"

func DefaultEngine() EtcdServer {
	return Engine(NewStoreConfig())
}

// NewStoreConfig 提供给所有业务系统使用的配置管理引擎，所有业务系统/中间件的可变配置通过集中配置中心进行统一
func NewStoreConfig() (conf ClientConfig) {
	uec := os.Getenv("UEC")
	if len(uec) == 0 {
		if b, err := ioutil.ReadFile("./configuration.uec"); err == nil {
			uec = string(b)
		}
	}
	if len(uec) > 0 {
		ds, _ := base64.URLEncoding.DecodeString(uec)
		data, er := utils.Decrypt(ds, []byte(DesKey))
		if er != nil {
			panic("解密configuration.uec出错")
		}
		if er = json.Unmarshal(data, &conf); er != nil {
			panic("不存在配置资源请使用本地配置信息")
		}
	} else {
		panic("请在环境变量中UEC或者./configuration.uec中配置内容")
	}
	return
}

// Engine 获取配置管理引擎的唯一实例。
func Engine(conf ClientConfig) EtcdServer {
	fmt.Println("Loading Aluka Etcd Engine")
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Endpoints,                                // etcd节点,因为使用的单节点,所以这里只有一个
		DialTimeout: time.Duration(conf.DialTimeout) * time.Second, //超时时间
	})
	if err != nil {
		panic(err)
	}
	return &etcdServer{client}
}

type EtcdServer interface {
	Client() *clientv3.Client
	Set(ctx context.Context, path []string, value string) error
	SetExpires(ctx context.Context, path []string, value string, ttl int64) error
	Delete(ctx context.Context, path []string) error
	Clazz(ctx context.Context, path []string, clazz interface{}) error
	Watch(ctx context.Context, path []string, clazz interface{}) error
	Lock(ctx context.Context, path []string, ttl int64) (m *concurrency.Mutex, s *concurrency.Session, err error)
}

type etcdServer struct {
	client *clientv3.Client
}

func (e etcdServer) Client() *clientv3.Client {
	return e.client
}

func (e etcdServer) Set(ctx context.Context, path []string, value string) (err error) {
	key := strings.Join(append([]string{Namespace}, path...), "/")
	s, err := e.client.Put(ctx, key, value)
	if err != nil {
		log.Err(err).Msgf("创建[%s]的配置信息出错:%+v", path, err)
	} else {
		log.Info().Msgf("创建配置项:%+v", s)
	}
	return
}

func (e etcdServer) SetExpires(ctx context.Context, path []string, value string, ttl int64) (err error) {
	key := strings.Join(append([]string{Namespace}, path...), "/")
	lease, err := e.client.Grant(ctx, ttl)
	if err != nil {
		log.Err(err).Msgf("创建租约出错:%+v", err)
		return
	}
	_, err = e.client.Put(context.TODO(), key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		log.Err(err).Msgf("创建[%s]的配置信息出错:%+v", path, err)
	}
	return
}

func (e etcdServer) Delete(ctx context.Context, path []string) (err error) {
	key := strings.Join(append([]string{Namespace}, path...), "/")
	_, err = e.client.Delete(ctx, key)
	if err != nil {
		log.Err(err).Msgf("删除[%s]的配置信息出错:%+v", path, err)
	} else {
		log.Info().Msgf("删除配置项:%+v", path)
	}
	return
}

// Clazz 获取指定配置项的配置信息，并且将配置信息（JSON格式的）转换为指定的Go结构体，如果获取失败或转换失败则抛出异常。
func (e etcdServer) Clazz(ctx context.Context, path []string, clazz interface{}) (err error) {
	key := strings.Join(append([]string{Namespace}, path...), "/")
	res, err := e.client.Get(ctx, key)
	if err != nil {
		log.Err(err).Msgf("获取多个配置项[%s]的配置信息出错:%+v", path, err)
		return
	} else {
		log.Info().Msgf("获取多个配置项为:%+v", res)
	}
	var value []byte
	for _, kv := range res.Kvs {
		value = kv.Value
	}
	err = json.Unmarshal(value, clazz)
	return
}

func (e etcdServer) Watch(ctx context.Context, path []string, clazz interface{}) error {
	key := strings.Join(append([]string{Namespace}, path...), "/")
	wch := e.client.Watch(ctx, key)
	for wresp := range wch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				err := json.Unmarshal(ev.Kv.Value, clazz)
				if err != nil {
					return err
				}
			case mvccpb.DELETE:
				clazz = nil
			}
		}
	}
	return nil
}

func (e etcdServer) Lock(ctx context.Context, path []string, ttl int64) (m *concurrency.Mutex, s *concurrency.Session, err error) {
	// 创建一个etcd租约
	lease, err := e.client.Grant(ctx, ttl)
	if err != nil {
		log.Err(err).Msgf("创建租约出错:%+v", err)
		return
	}
	prefix := strings.Join(append([]string{Namespace}, path...), "/")
	s, err = concurrency.NewSession(e.client, concurrency.WithContext(ctx), concurrency.WithLease(lease.ID))
	if err != nil {
		log.Err(err).Msgf("创建会话出错:%+v", err)
		return
	}
	m = concurrency.NewMutex(s, prefix)
	return
}
