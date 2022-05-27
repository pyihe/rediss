package rediss

import (
	"bufio"
	"fmt"
	"net"
	"time"

	innerBytes "github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
)

type conn struct {
	c      net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func newConn(c net.Conn) *conn {
	return &conn{
		c:      c,
		reader: bufio.NewReader(c),
		writer: bufio.NewWriter(c),
	}
}

func (c *conn) writeBytes(b []byte, timeout time.Duration) (n int, err error) {
	if timeout > 0 {
		if err = c.c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return
		}
	}
	n, err = c.writer.Write(b)
	if err != nil {
		return
	}
	err = c.writer.Flush()
	return
}

func (c *conn) request(b []byte, writeTimeout, readTimeout time.Duration) (reply *Reply, err error) {
	if _, err = c.writeBytes(b, writeTimeout); err != nil {
		return
	}
	reply, err = c.readReply(readTimeout)
	return
}

func (c *conn) readReply(timeout time.Duration) (reply *Reply, err error) {
	response, err := c.reply(timeout)
	if err != nil {
		return
	}
	reply = parse(response)
	return
}

func (c *conn) reply(timeout time.Duration) (interface{}, error) {
	var head, err = c.readLine(timeout)
	if err != nil {
		return nil, err
	}
	if isNilReply(head) {
		return nil, nil
	}
	if isEmptyReply(head) {
		return &Reply{}, nil
	}
	if len(head) == 0 {
		return nil, errors.New("got empty reply from redis")
	}
	switch head[0] {
	case '+', ':':
		return newReply(head[1:]), nil
	case '-':
		return newReply(nil, innerBytes.String(head[1:])), nil
	case '$':
		bulkString, err := c.readBulkString(head, timeout)
		if err != nil {
			return nil, err
		}
		return newReply(bulkString), nil
	case '*':
		array, err := c.readArray(head, timeout)
		if err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, ErrInvalidReplyFormat
	}
}

func (c *conn) readLine(timeout time.Duration) ([]byte, error) {
	if timeout > 0 {
		if err := c.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
	}
	line, err := c.reader.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}
		full := make([]byte, len(line))
		copy(full, line)

		line, err = c.reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		full = append(full, line...)
		line = full
	}
	if len(line) <= 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return nil, fmt.Errorf("read invalid reply: %q", line)
	}

	return line[:len(line)-2], nil // 去掉结尾的'\r\n'
}

func (c *conn) readBulkString(head []byte, timeout time.Duration) ([]byte, error) {
	count, err := dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}
	buf := make([]byte, count+2)
	if timeout > 0 {
		if err = c.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
	}
	if _, err = c.reader.Read(buf); err != nil {
		return nil, err
	}
	return buf[:count], nil
}

func (c *conn) readArray(head []byte, timeout time.Duration) (interface{}, error) {
	var count, err = dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}

	var array = make([]interface{}, count)
	for i := 0; i < count; i++ {
		array[i], err = c.reply(timeout)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}
