package rediss

import (
	"net"

	"github.com/pyihe/go-pkg/serialize"
)

type Client struct {
	address    string               // redis地址
	password   string               // 密码
	database   int                  // db索引
	poolSize   int                  // 连接池大小
	serializer serialize.Serializer // 序列化
	connPool   chan *conn           // 用通道作为连接池
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

func (c *Client) sendCommand(args *Args) (result *Reply, err error) {
	conn, newConn, err := c.popConn()
	if err != nil {
		goto end
	}
	if _, err = conn.write(args.command()); err != nil {
		goto end
	}

	// 回收Args
	putArgs(args)
	result, err = c.reply(conn)

end:
	if !newConn || (newConn && conn != nil) {
		c.pushConn(conn)
	}
	return
}

func (c *Client) reply(conn *conn) (result *Reply, err error) {
	r, err := conn.reply()
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
		if err = connection.auth(c.password); err != nil {
			return nil, err
		}
	}
	if err = connection.selectDB(c.database); err != nil {
		return nil, err
	}
	return connection, err
}

func (c *Client) popConn() (conn *conn, isNew bool, err error) {
	if conn = <-c.connPool; conn != nil {
		if err = conn.ping(); err != nil {
			goto end
		}
		return
	}
end:
	isNew = true
	conn, err = c.connect()
	return
}

func (c *Client) pushConn(conn *conn) {
	c.connPool <- conn
}
