package pool

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/pyihe/go-pkg/backoff"
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/go-pkg/maths"
)

var (
	ErrUninitializedPool = errors.New("uninitialized pool")
	ErrAlreadyClosedPool = errors.New("pool closed")
)

var (
	defaultIdleDuration = 10 * time.Second
	defaultMaxConnSize  = 16
	defaultMinConnSize  = 4
)

type Config struct {
	Dialer      func() (net.Conn, error) // 拨号
	MaxIdleTime time.Duration            // 连接最大闲置时长
	Retry       int                      // 拨号失败后的重试次数
	MaxConnSize int                      // 最大连接数
	MinConnSize int                      // 最小连接数
}

type Pool struct {
	initialized bool               // 是否已经初始化
	closed      bool               // 是否已关闭
	stop        context.CancelFunc // 取消信号量
	config      *Config            // 连接池的配置

	mu    sync.Mutex // 读取连接队列的锁
	conns *queue     // 连接
}

func New(cfg *Config) *Pool {
	if cfg == nil {
		panic("nil config")
	}
	p := &Pool{
		config: cfg,
	}
	p.init()
	return p
}

func (p *Pool) init() {
	if p.config.Dialer == nil {
		panic("nil dialer")
	}
	if p.config.MaxIdleTime <= 0 {
		p.config.MaxIdleTime = defaultIdleDuration
	}
	if p.config.MaxConnSize <= 0 {
		p.config.MaxConnSize = defaultMaxConnSize
	}
	if p.config.MinConnSize <= 0 {
		p.config.MinConnSize = defaultMinConnSize
	}
	p.config.Retry = maths.MaxInt(p.config.Retry, 0)

	// 初始化连接池中的连接队列
	p.conns = newQueue(p)

	// 初始化连接
	for i := 0; i < p.config.MaxConnSize; i++ {
		c, err := p.dialConn()
		if err != nil {
			panic(err)
		}
		_ = p.addConn(c)
	}

	var ctx context.Context
	ctx, p.stop = context.WithCancel(context.Background())
	go p.periodicClean(ctx)
	p.initialized = true
}

func (p *Pool) addConn(c net.Conn) (err error) {
	p.mu.Lock()
	err = p.conns.insert(newConnection(c))
	p.mu.Unlock()
	return
}

func (p *Pool) dialConn() (c net.Conn, err error) {
	c, err = p.config.Dialer()
	if err != nil && p.config.Retry > 0 {
		retry := 0
		for {
			delay := backoff.Get(nil, retry)
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
				break
			}
			c, err = p.config.Dialer()
			if err == nil {
				timer.Stop()
				break
			}
			retry++
			if retry == p.config.Retry {
				timer.Stop()
				break
			}
		}
	}
	return
}

// 周期性的清理闲置的连接
// 清理周期为最大闲置时长 MaxIdleTime
// 凡是闲置时间超过 MaxIdleTime 的都进行清理
func (p *Pool) periodicClean(ctx context.Context) {
	var expiredConns []*RedisConn
	var ticker = time.NewTicker(p.config.MaxIdleTime)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		if p.closed {
			return
		}
		// 找到所有已经过期的连接
		p.mu.Lock()
		expiredConns = p.conns.searchExpiredConns(p.config.MaxIdleTime)
		p.checkMinConns(&expiredConns)
		p.mu.Unlock()

		// 关闭已经过期的连接
		for i := range expiredConns {
			//fmt.Printf("断开连接: %v, %v\n", expiredConns[i].conn.LocalAddr(), expiredConns[i].conn.RemoteAddr())
			expiredConns[i].conn.Close()
			expiredConns[i] = nil
		}
	}
}

func (p *Pool) checkMinConns(expiredConns *[]*RedisConn) {
	if p.conns.len() >= p.config.MinConnSize {
		return
	}
	for p.conns.len() < p.config.MinConnSize {
		if expireCount := len(*expiredConns); expireCount > 0 {
			_ = p.conns.insert((*expiredConns)[expireCount-1])
			(*expiredConns)[expireCount-1] = nil
			*expiredConns = (*expiredConns)[:expireCount-1]
		} else {
			c, err := p.dialConn()
			if err != nil {
				return
			}
			_ = p.addConn(c)
		}
	}
}

func (p *Pool) Get(check func(conn *RedisConn) error) (c *RedisConn, err error) {
	if !p.initialized {
		return nil, ErrUninitializedPool
	}
	if p.closed {
		return nil, ErrAlreadyClosedPool
	}
	p.mu.Lock()
	c = p.conns.pop()
	p.mu.Unlock()

	if c == nil {
		conn, err := p.dialConn()
		if err != nil {
			return nil, err
		}
		c = newConnection(conn)
	} else if check != nil {
		if err = check(c); err != nil {
			return
		}
	}
	return
}

func (p *Pool) Put(conn *RedisConn) error {
	if !p.initialized {
		return ErrUninitializedPool
	}
	if p.closed {
		return ErrAlreadyClosedPool
	}
	p.mu.Lock()
	err := p.conns.insert(conn)
	p.mu.Unlock()
	return err
}

func (p *Pool) Close() {
	p.closed = true
	p.stop()
	p.mu.Lock()
	p.conns.reset()
	p.mu.Unlock()
}
