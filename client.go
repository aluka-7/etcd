package etcd

type ClientConfig struct {
	Endpoints   []string `json:"endpoints"`   // etcd节点列表
	DialTimeout int64    `json:"dialTimeout"` // 超时时间
	Username    string   `json:"username"`    // 账号
	Password    string   `json:"password"`    // 密码
}
