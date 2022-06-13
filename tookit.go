package rediss

import (
	"strconv"

	innerBytes "github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/rediss/args"
)

func assertDatabase(db int32) {
	if db < 0 || db > 15 {
		panic("invalid database")
	}
}

func appendArgs(args *args.Args, arg interface{}) (err error) {
	switch data := arg.(type) {
	case []string:
		args.Append(data...)
	case []interface{}:
		args.AppendArgs(data...)
	case map[string]interface{}:
		for k, v := range data {
			args.Append(k)
			args.AppendArgs(v)
		}
	case map[string]string:
		for k, v := range data {
			args.Append(k, v)
		}
	default:
		err = ErrNotSupportArgument
	}
	return
}

func isNilReply(reply []byte) bool {
	if len(reply) == 3 && (reply[0] == '$' || reply[0] == '*') && reply[1] == '-' && reply[2] == '1' {
		return true
	}
	return false
}

func dataLen(b []byte) (int, error) {
	if len(b) == 0 {
		return -1, errors.New("invalid length")
	}

	return strconv.Atoi(innerBytes.String(b))
}
