package rediss

import "github.com/pyihe/go-pkg/errors"

const (
	separator  = "\r\n"
	timeLayout = "2006-01-02 15:04:05.000000"
)

var (
	ErrKeyNotExists          = errors.New("key not exists")
	ErrInvalidReplyFormat    = errors.New("invalid reply format")
	ErrInvalidArgumentFormat = errors.New("invalid argument's format")
	ErrNotSupportArgument    = errors.New("not support argument")
)
