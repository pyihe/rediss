package rediss

import (
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
