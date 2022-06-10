package rediss

import (
	"time"

	"github.com/pyihe/go-pkg/serialize"
)

type Option func(client *Client)

func WithAddress(addr string) Option {
	return func(client *Client) {
		client.address = addr
	}
}

func WithUsername(username string) Option {
	return func(client *Client) {
		client.username = username
	}
}

func WithPassword(password string) Option {
	return func(client *Client) {
		client.password = password
	}
}

func WithDatabase(db int) Option {
	return func(client *Client) {
		client.database = int32(db)
	}
}

func WithSerializer(serializer serialize.Serializer) Option {
	return func(client *Client) {
		client.serializer = serializer
	}
}

func WithWriteTimeout(timeout time.Duration) Option {
	return func(client *Client) {
		client.writeTimeout = timeout
	}
}

func WithReadTimeout(timeout time.Duration) Option {
	return func(client *Client) {
		client.readTimeout = timeout
	}
}

func WithMaxIdleTime(idleTime time.Duration) Option {
	return func(client *Client) {
		client.poolConfig.MaxIdleTime = idleTime
	}
}

func WithRetry(retry int) Option {
	return func(client *Client) {
		client.poolConfig.Retry = retry
	}
}

func WithPoolSize(size int) Option {
	return func(client *Client) {
		client.poolConfig.MaxConnSize = size
	}
}

func WithMinConnNum(num int) Option {
	return func(c *Client) {
		c.poolConfig.MinConnSize = num
	}
}
