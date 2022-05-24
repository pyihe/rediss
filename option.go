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
		client.database = db
	}
}

func WithPoolSize(size int) Option {
	return func(client *Client) {
		client.poolSize = size
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
