package rediss

import (
	"bufio"
	"net"
	"strconv"

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

func (c *conn) ping() (err error) {
	args := getArgs()
	args.Append("PING")
	if _, err = c.write(args.command()); err != nil {
		goto end
	}

	if rsp, err1 := c.reply(); err != nil {
		err = err1
		goto end
	} else {
		if reply := parse(rsp); reply.GetString() != "PONG" {
			err = errors.New("failed ping")
			goto end
		}
	}
end:
	putArgs(args)
	return
}

func (c *conn) auth(pass string) (err error) {
	args := getArgs()
	args.Append("AUTH", pass)
	if _, err = c.write(args.command()); err != nil {
		goto end
	}
	if rsp, err1 := c.reply(); err != nil {
		err = err1
		goto end
	} else {
		if reply := parse(rsp); reply.GetString() != "OK" {
			err = errors.New("failed auth")
			goto end
		}
	}
end:
	putArgs(args)
	return
}

func (c *conn) selectDB(db int) (err error) {
	args := getArgs()
	args.Append("SELECT", strconv.FormatInt(int64(db), 10))
	if _, err = c.write(args.command()); err != nil {
		goto end
	}
	if rsp, err1 := c.reply(); err != nil {
		err = err1
		goto end
	} else {
		if reply := parse(rsp); reply.GetString() != "OK" {
			err = errors.New("failed select database")
			goto end
		}
	}
end:
	putArgs(args)
	return
}

func (c *conn) write(b []byte) (n int, err error) {
	n, err = c.writer.Write(b)
	if err != nil {
		return
	}
	err = c.writer.Flush()
	return
}

func (c *conn) reply() (interface{}, error) {
	var head, err = c.readLine()
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
		bulkString, err := c.readBulkString(head)
		if err != nil {
			return nil, err
		}
		return newReply(bulkString), nil
	case '*':
		array, err := c.readArray(head)
		if err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, ErrInvalidReplyFormat
	}
}

func (c *conn) readLine() (line []byte, err error) {
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

func (c *conn) readBulkString(head []byte) ([]byte, error) {
	count, err := dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}
	buf := make([]byte, count+2)
	if _, err = c.reader.Read(buf); err != nil {
		return nil, err
	}
	return buf[:count], nil
}

func (c *conn) readArray(head []byte) (interface{}, error) {
	var count, err = dataLen(head[1:])
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}

	var array = make([]interface{}, count)
	for i := 0; i < count; i++ {
		array[i], err = c.reply()
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}
