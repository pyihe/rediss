package rediss

import (
	"bufio"
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

func (c *conn) writeCommand(args *Args, writeTimeout, readTimeout time.Duration) (reply *Reply, err error) {
	var response interface{}
	if args == nil {
		goto end
	}
	if _, err = c.writeBytes(args.command(), writeTimeout); err != nil {
		goto end
	}

	response, err = c.reply(readTimeout)
	if err != nil {
		goto end
	}

	reply = parse(response)

end:
	putArgs(args)
	return
}

func (c *conn) reply(timeout time.Duration) (interface{}, error) {
	if timeout > 0 {
		if err := c.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
	}
	var head, err = c.readLine(timeout)
	if err != nil {
		return nil, err
	}
	if len(head) == 0 {
		return nil, errors.New("too short reply")
	}
	if isEmptyReply(head) {
		return &Reply{}, nil
	}
	if isNilReply(head) {
		return nil, nil
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

func (c *conn) readLine(timeout time.Duration) (line []byte, err error) {
	if timeout > 0 {
		if err = c.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return
		}
	}
	line, err = c.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		buf := append([]byte{}, line...)
		for err == bufio.ErrBufferFull {
			line, err = c.reader.ReadSlice('\n')
			buf = append(buf, line...)
		}
		line = buf
	}
	if err != nil {
		return
	}

	if n := len(line); n > 1 && line[n-2] == '\r' {
		line = line[:n-2] // 去掉结尾的'\r\n'
		return
	}
	err = ErrInvalidReplyFormat
	return
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
