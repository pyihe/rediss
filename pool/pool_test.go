package pool

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	cfg := &Config{
		Dialer: func() (net.Conn, error) {
			return net.Dial("tcp", ":8080")
		},
		MaxIdleTime: 10 * time.Second,
		Retry:       1,
		MaxConnSize: 64,
		MinConnSize: 16,
	}
	pool := New(cfg)
	defer pool.Close()

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int, w *sync.WaitGroup) {
			defer w.Done()
			c, err := pool.Get(nil)
			if err != nil {
				fmt.Printf("pool get err: %v\n", err)
				return
			}
			defer func() {
				if err = pool.Put(c); err != nil {
					fmt.Printf("put err: %v\n", err)
				}
			}()
			_, err = c.WriteBytes([]byte(fmt.Sprintf("我是%d\n", index)), 0)
			fmt.Printf("%d发送数据: %v\n", index, err)
		}(i, &wg)
	}
	wg.Wait()
}
