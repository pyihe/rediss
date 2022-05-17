package rediss

import (
	"strconv"

	innerBytes "github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
)

func assertDatabase(db int) {
	if db < 0 || db > 15 {
		panic("invalid database")
	}
}

func isNilReply(b []byte) bool {
	n := len(b)
	if n == 3 && (b[0] == '$' || b[0] == '*') && b[1] == '-' && b[2] == '1' {
		return true
	}
	return false
}

func isEmptyReply(b []byte) bool {
	n := len(b)
	if n == 2 && (b[0] == '$' || b[0] == '*') && b[1] == '0' {
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

func parse(reply interface{}) (result *Reply) {
	switch data := reply.(type) {
	case *Reply: // 单个回复
		result = newReply(data.value)
		result.array = data.array
		result.err = data.err
	case []interface{}: // 嵌套数组
		result = newReply(nil)
		result.array = make([]*Reply, len(data))
		for i, e := range data {
			temp := parse(e)
			result.array[i] = temp
		}
	case nil:
		//fmt.Println(nil)
	}
	return
}