package rediss

import (
	"net"
	"time"

	"github.com/pyihe/go-pkg/serialize"
	"github.com/pyihe/rediss/args"
	"github.com/pyihe/rediss/pool"
)

type Client struct {
	address      string               // redis地址
	username     string               // 用户名
	password     string               // 密码
	database     int32                // db索引
	writeTimeout time.Duration        // 每次发送请求的超时时间
	readTimeout  time.Duration        // 每次读取回复的超时时间
	serializer   serialize.Serializer // 序列化

	pool       *pool.Pool   // 连接池
	poolConfig *pool.Config // 连接池配置
}

func New(opts ...Option) *Client {
	c := &Client{
		address:    "127.0.0.1:6379", // 默认连接本机redis
		password:   "",               // 默认无密码
		database:   0,                // 默认选择索引为0的数据库
		poolConfig: &pool.Config{},
	}

	for _, opt := range opts {
		opt(c)
	}
	c.poolConfig.Dialer = func() (net.Conn, error) {
		return net.Dial("tcp", c.address)
	}
	c.pool = pool.New(c.poolConfig)

	assertDatabase(c.database)
	return c
}

func (c *Client) Close() {
	c.pool.Close()
}

func (c *Client) sendCommandWithoutTimeout(cmd []byte) (result *Reply, err error) {
	conn, err := c.pool.Get(c.checkConn)
	if err != nil {
		return nil, err
	}
	if err = writeConn(conn, cmd, 0); err != nil {
		return
	}
	result, err = readConn(conn, 0)
	c.pool.Put(conn)
	return
}

func (c *Client) sendCommand(cmd []byte) (result *Reply, err error) {
	conn, err := c.pool.Get(c.checkConn)
	if err != nil {
		return nil, err
	}
	if err = writeConn(conn, cmd, c.writeTimeout); err != nil {
		return
	}
	result, err = readConn(conn, c.readTimeout)
	c.pool.Put(conn)
	return
}

func (c *Client) checkConn(conn *pool.RedisConn) error {
	err := writeConn(conn, args.Command("PING"), 0)
	if err != nil {
		return err
	}
	if _, err = readConn(conn, 0); err != nil {
		return err
	}
	if len(c.password) > 0 {
		var cmd []byte
		if len(c.username) > 0 {
			cmd = args.Command("AUTH", c.username, c.password)
		} else {
			cmd = args.Command("AUTH", c.password)
		}
		if err = writeConn(conn, cmd, 0); err != nil {
			return err
		}
		if _, err = readConn(conn, 0); err != nil {
			return err
		}
	}
	if err = writeConn(conn, args.Command("SELECT", c.database), 0); err != nil {
		return err
	}
	_, err = readConn(conn, 0)
	return err
}
