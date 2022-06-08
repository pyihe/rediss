package rediss

import (
	"bufio"
	"fmt"
	"net"
	"time"

	innerBytes "github.com/pyihe/go-pkg/bytes"
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

func (c *conn) setReadTimeout(timeout time.Duration) (err error) {
	if timeout > 0 {
		err = c.c.SetReadDeadline(time.Now().Add(timeout))
	} else {
		err = c.c.SetReadDeadline(time.Time{})
	}
	return
}

func (c *conn) setWriteTimeout(timeout time.Duration) (err error) {
	if timeout > 0 {
		err = c.c.SetWriteDeadline(time.Now().Add(timeout))
	} else {
		err = c.c.SetWriteDeadline(time.Time{})
	}
	return
}

func (c *conn) writeBytes(b []byte, timeout time.Duration) (n int, err error) {
	if err = c.setWriteTimeout(timeout); err != nil {
		return
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
	if reply == nil {
		err = NilReply
		return
	}
	err = reply.err
	return
}

func (c *conn) reply(timeout time.Duration) (interface{}, error) {
	var line, err = c.readLine(timeout)
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
		bulkString, err := c.readBulkString(line, timeout)
		if err != nil {
			return nil, err
		}
		return newReply(bulkString), nil
	case '*':
		array, err := c.readArray(line, timeout)
		if err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, ErrInvalidReplyFormat
	}
}

func (c *conn) readLine(timeout time.Duration) ([]byte, error) {
	if err := c.setReadTimeout(timeout); err != nil {
		return nil, err
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
	buf := make([]byte, count+2)
	if err = c.setReadTimeout(timeout); err != nil {
		return nil, err
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
		return &Reply{}, nil
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
