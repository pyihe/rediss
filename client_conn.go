package rediss

import (
	"time"

	innerBytes "github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/rediss/pool"
)

func writeConn(conn *pool.RedisConn, cmd []byte, timeout time.Duration) error {
	_, err := conn.WriteBytes(cmd, timeout)
	return err
}

func readConn(conn *pool.RedisConn, timeout time.Duration) (*Reply, error) {
	response, err := readResponse(conn, timeout)
	if err != nil {
		return nil, err
	}

	reply := parseReply(response)
	if reply == nil {
		return nil, NilReply
	}

	return reply, reply.Err
}

func readResponse(conn *pool.RedisConn, timeout time.Duration) (interface{}, error) {
	line, err := conn.ReadLine(timeout)
	if err != nil {
		return nil, err
	}
	if isNilReply(line) {
		return nil, nil
	}
	switch line[0] {
	case '+', ':':
		return newReply(line[1:]), nil
	case '-':
		return newReply(nil, innerBytes.String(line[1:])), nil
	case '$':
		b, err := readBulkString(conn, line, timeout)
		if err != nil {
			return nil, err
		}
		return newReply(b), nil
	case '*':
		array, err := readArray(conn, line, timeout)
		if err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, errors.New("rediss: invalid reply format")
	}
}

func readBulkString(conn *pool.RedisConn, head []byte, timeout time.Duration) ([]byte, error) {
	count, err := dataLen(head[1:])
	if err != nil {
		return nil, err
	}

	buf := make([]byte, count+2)
	if _, err = conn.Read(buf, timeout); err != nil {
		return nil, err
	}
	return buf[:count], nil
}

func readArray(conn *pool.RedisConn, head []byte, timeout time.Duration) (interface{}, error) {
	count, err := dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return &Reply{}, nil
	}
	array := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		data, err := readResponse(conn, timeout)
		if err != nil {
			return nil, err
		}
		array = append(array, data)
	}
	return array, nil
}

func parseReply(response interface{}) (result *Reply) {
	switch data := response.(type) {
	case *Reply: // 单个回复
		result = newReply(data.Value)
		result.Array = data.Array
		result.Err = data.Err
	case []interface{}: // 嵌套数组
		result = newReply(nil)
		result.Array = make([]*Reply, len(data))
		for i, e := range data {
			temp := parseReply(e)
			result.Array[i] = temp
		}
	case nil:
	}
	return
}
