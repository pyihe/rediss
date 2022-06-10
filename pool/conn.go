package pool

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"

	innerBytes "github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
)

type RedisConn struct {
	conn         net.Conn // 真实连接
	writer       *bufio.Writer
	reader       *bufio.Reader
	lastUsedTime time.Time // 最后一次使用时间
}

func newConnection(c net.Conn) *RedisConn {
	return &RedisConn{
		conn:         c,
		writer:       bufio.NewWriter(c),
		reader:       bufio.NewReader(c),
		lastUsedTime: time.Now(),
	}
}

func (rc *RedisConn) SetReadTimeout(timeout time.Duration) (err error) {
	if timeout > 0 {
		err = rc.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		err = rc.conn.SetReadDeadline(time.Time{})
	}
	return
}

func (rc *RedisConn) SetWriteTimeout(timeout time.Duration) (err error) {
	if timeout > 0 {
		err = rc.conn.SetWriteDeadline(time.Now().Add(timeout))
	} else {
		err = rc.conn.SetWriteDeadline(time.Time{})
	}
	return
}

func (rc *RedisConn) WriteBytes(b []byte, timeout time.Duration) (n int, err error) {
	if err = rc.SetWriteTimeout(timeout); err != nil {
		return
	}
	n, err = rc.writer.Write(b)
	if err != nil {
		return
	}
	err = rc.writer.Flush()
	return
}

func (rc *RedisConn) ReadLine(timeout time.Duration) (line []byte, err error) {
	if err = rc.SetReadTimeout(timeout); err != nil {
		return
	}
	line, err = rc.reader.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return
		}
		full := make([]byte, len(line))
		copy(full, line)

		line, err = rc.reader.ReadBytes('\n')
		if err != nil {
			return
		}
		full = append(full, line...)
		line = full
	}
	if len(line) <= 2 || line[len(line)-1] != '\n' || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("read invalid reply: %q", line)
	}
	return line[:len(line)-2], nil
}

func (rc *RedisConn) ReadReply(timeout time.Duration) (reply *Reply, err error) {
	response, err := rc.reply(timeout)
	if err != nil {
		return nil, err
	}
	reply = parse(response)
	if reply == nil {
		err = NilReply
		return
	}
	err = reply.err
	return
}

func (rc *RedisConn) reply(timeout time.Duration) (interface{}, error) {
	var line, err = rc.ReadLine(timeout)
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
		bulkString, err := rc.readBulkString(line, timeout)
		if err != nil {
			return nil, err
		}
		return newReply(bulkString), nil
	case '*':
		array, err := rc.readArray(line, timeout)
		if err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, errors.New("invalid reply format")
	}
}

func (rc *RedisConn) readBulkString(head []byte, timeout time.Duration) (bulk []byte, err error) {
	count, err := dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	buf := make([]byte, count+2)
	if err = rc.SetReadTimeout(timeout); err != nil {
		return nil, err
	}
	if _, err = rc.reader.Read(buf); err != nil {
		return nil, err
	}
	return buf[:count], nil
}

func (rc *RedisConn) readArray(head []byte, timeout time.Duration) (interface{}, error) {
	var count, err = dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return &Reply{}, nil
	}

	var array = make([]interface{}, count)
	for i := 0; i < count; i++ {
		array[i], err = rc.ReadReply(timeout)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
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
