package pool

import (
	"fmt"
	"time"

	"github.com/pyihe/go-pkg/errors"
)

type queue struct {
	conns   []*RedisConn
	expired []*RedisConn
	p       *Pool
}

func newQueue(pool *Pool) *queue {
	return &queue{
		conns: make([]*RedisConn, 0, pool.config.MaxConnSize),
		p:     pool,
	}
}

func (q *queue) len() int {
	return len(q.conns)
}

func (q *queue) insert(conn *RedisConn) (err error) {
	if conn == nil {
		return
	}
	// 缓冲区是否有尚未使用的数据
	if conn.writer.Buffered() > 0 {
		return errors.New("buffer has unused data")
	}

	conn.lastUsedTime = time.Now()

	if length := q.len(); length == q.p.config.MaxConnSize { // 如果队列已经满了, 则用conn替换使用时间最早的连接
		oldConn := q.conns[0]
		n := copy(q.conns, q.conns[1:])
		q.conns[length-1] = nil
		q.conns = q.conns[:n]
		_ = oldConn.conn.Close()
		fmt.Printf("队列已满, 需要替换旧连接: %v->%v\n", oldConn.conn.LocalAddr(), conn.conn.LocalAddr())
	}

	q.conns = append(q.conns, conn)
	fmt.Printf("insert连接[%v]后: %v\n", conn.conn.LocalAddr(), q.len())
	return
}

func (q *queue) pop() *RedisConn {
	length := q.len()
	if length == 0 {
		return nil
	}
	c := q.conns[length-1]
	q.conns[length-1] = nil
	q.conns = q.conns[:length-1]
	fmt.Printf("获取连接: %v\n", c.conn.LocalAddr())
	return c
}

func (q *queue) searchExpiredConns(expire time.Duration) []*RedisConn {
	length := q.len()
	// 没有连接
	if length == 0 {
		return nil
	}

	// 连接数正好为最小值
	if length == q.p.config.MinConnSize {
		return nil
	}

	// 找到过期了的连接
	now, left, right, mid := time.Now(), 0, length-1, 0
	for left <= right {
		mid = (left + right) / 2
		usedTime := q.conns[mid].lastUsedTime
		if now.Sub(usedTime) > expire {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	// 没有过期的连接
	if right == -1 {
		return nil
	}

	q.expired = q.expired[0:0]
	q.expired = append(q.expired, q.conns[:right+1]...)
	n := copy(q.conns, q.conns[right+1:])
	for i := n; i < length; i++ {
		q.conns[i] = nil
	}
	q.conns = q.conns[:n]
	fmt.Printf("xxx: %v, %v, %v, %v\n", len(q.expired), q.expired[len(q.expired)-1].lastUsedTime.String(), now.String(), expire)
	return q.expired
}

func (q *queue) reset() {
	for i := range q.conns {
		_ = q.conns[i].conn.Close()
		q.conns[i] = nil
	}

	for i := range q.expired {
		_ = q.expired[i].conn.Close()
		q.expired[i] = nil
	}
	q.conns = q.conns[:0]
	q.expired = q.expired[:0]
	q.p = nil
}
