package rediss

import "github.com/pyihe/go-pkg/errors"

var (
	NilReply               = errors.New("nil reply")
	ErrNotSupportArgument  = errors.New("not support argument")
	ErrEmptyOptionArgument = errors.New("option argument cannot be empty")
)
