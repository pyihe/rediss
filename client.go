package rediss

import (
	"net"
	"strconv"
	"time"

	"github.com/pyihe/go-pkg/serialize"
	"github.com/pyihe/rediss/args"
)

type Client struct {
	address      string               // redis地址
	username     string               // 用户名
	password     string               // 密码
	database     int                  // db索引
	poolSize     int                  // 连接池大小
	writeTimeout time.Duration        // 每次发送请求的超时时间
	readTimeout  time.Duration        // 每次读取回复的超时时间
	serializer   serialize.Serializer // 序列化
	connPool     chan *conn           // 用通道作为连接池
}

func New(opts ...Option) *Client {
	c := &Client{
		address:  "127.0.0.1:6379", // 默认连接本机redis
		password: "",               // 默认无密码
		database: 0,                // 默认选择索引为0的数据库
		poolSize: 8,                // 默认连接池连接数量为8
	}

	for _, opt := range opts {
		opt(c)
	}

	c.connPool = make(chan *conn, c.poolSize)
	for i := 0; i < c.poolSize; i++ {
		c.connPool <- nil
	}

	assertDatabase(c.database)

	return c
}

func (c *Client) sendCommand(cmd []byte) (result *Reply, err error) {
	conn, newConn, err := c.popConn()
	if err != nil {
		goto end
	}
	result, err = conn.writeCommand(cmd, c.writeTimeout, c.readTimeout)
	if err != nil {
		goto end
	}

end:
	if !newConn || (newConn && conn != nil) {
		c.pushConn(conn)
	}
	return
}

func (c *Client) readReply(conn *conn) (result *Reply, err error) {
	r, err := conn.reply(c.readTimeout)
	if err != nil {
		return nil, err
	}
	result = parse(r)
	return
}

func (c *Client) connect() (*conn, error) {
	rConn, err := net.Dial("tcp", c.address)
	if err != nil {
		return nil, err
	}
	connection := newConn(rConn)
	if len(c.password) > 0 {
		cmd := args.Get()
		defer args.Put(cmd)

		cmd.Append("AUTH", c.password)
		_, err = connection.writeCommand(cmd.Bytes(), c.writeTimeout, c.readTimeout)
		if err != nil {
			return nil, err
		}
	}
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("SELECT", strconv.FormatInt(int64(c.database), 10))
	_, err = connection.writeCommand(cmd.Bytes(), c.writeTimeout, c.readTimeout)
	return connection, err
}

func (c *Client) popConn() (conn *conn, isNew bool, err error) {
	if conn = <-c.connPool; conn != nil {
		cmd := args.Get()
		defer args.Put(cmd)

		cmd.Append("PING")
		_, err = conn.writeCommand(cmd.Bytes(), c.writeTimeout, c.readTimeout)
		if err != nil {
			return
		}
	}
	isNew = true
	conn, err = c.connect()
	return
}

func (c *Client) pushConn(conn *conn) {
	c.connPool <- conn
}
