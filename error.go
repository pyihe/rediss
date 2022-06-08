package rediss

import "github.com/pyihe/go-pkg/errors"

var (
	NilReply                 = errors.New("nil reply")
	ErrInvalidReplyFormat    = errors.New("invalid reply format")
	ErrInvalidArgumentFormat = errors.New("invalid argument's format")
	ErrNotSupportArgument    = errors.New("not support argument")
	ErrEmptyOptionArgument   = errors.New("option argument cannot be empty")
)
