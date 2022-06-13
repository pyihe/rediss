package pool

import (
	"bufio"
	"fmt"
	"net"
	"time"
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

func (rc *RedisConn) setReadTimeout(timeout time.Duration) (err error) {
	if timeout > 0 {
		err = rc.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		err = rc.conn.SetReadDeadline(time.Time{})
	}
	return
}

func (rc *RedisConn) setWriteTimeout(timeout time.Duration) (err error) {
	if timeout > 0 {
		err = rc.conn.SetWriteDeadline(time.Now().Add(timeout))
	} else {
		err = rc.conn.SetWriteDeadline(time.Time{})
	}
	return
}

func (rc *RedisConn) WriteBytes(b []byte, timeout time.Duration) (n int, err error) {
	if err = rc.setWriteTimeout(timeout); err != nil {
		return
	}
	n, err = rc.writer.Write(b)
	if err != nil {
		return
	}
	err = rc.writer.Flush()
	return
}

func (rc *RedisConn) Read(p []byte, timeout time.Duration) (n int, err error) {
	if err = rc.setReadTimeout(timeout); err != nil {
		return
	}
	return rc.reader.Read(p)
}

func (rc *RedisConn) ReadLine(timeout time.Duration) (line []byte, err error) {
	if err = rc.setReadTimeout(timeout); err != nil {
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
