package args

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	innerBytes "github.com/pyihe/go-pkg/bytes"
)

const (
	separator  = "\r\n"
	timeLayout = "2006-01-02 15:04:05.000000"
)

var pool sync.Pool

func Get() *Args {
	data := pool.Get()
	if data == nil {
		args := make(Args, 0, 16)
		return &args
	}
	return data.(*Args)
}

func Put(args *Args) {
	if args == nil {
		return
	}
	args.Reset()
	pool.Put(args)
}

func Command(args ...interface{}) (b []byte) {
	cmd := Get()
	cmd.AppendArgs(args...)
	b = cmd.Bytes()
	Put(cmd)
	return
}

type Args []string

func (a *Args) String() string {
	if a == nil {
		return ""
	}
	return fmt.Sprintf("%v", *a)
}

func (a *Args) Append(arg ...string) {
	*a = append(*a, arg...)
}

func (a *Args) Reset() {
	*a = (*a)[:0]
}

func (a *Args) AppendArgs(args ...interface{}) {
	for _, arg := range args {
		switch v := arg.(type) {
		case bool:
			if v {
				a.Append("1")
			} else {
				a.Append("0")
			}
		case string:
			a.Append(v)
		case []byte:
			a.Append(innerBytes.String(v))
		case uint8:
			a.Append(strconv.FormatUint(uint64(v), 10))
		case uint16:
			a.Append(strconv.FormatUint(uint64(v), 10))
		case uint:
			a.Append(strconv.FormatUint(uint64(v), 10))
		case uint32:
			a.Append(strconv.FormatUint(uint64(v), 10))
		case uint64:
			a.Append(strconv.FormatUint(v, 10))
		case int8:
			a.Append(strconv.FormatInt(int64(v), 10))
		case int16:
			a.Append(strconv.FormatInt(int64(v), 10))
		case int32:
			a.Append(strconv.FormatInt(int64(v), 10))
		case int64:
			a.Append(strconv.FormatInt(v, 10))
		case float32:
			a.Append(strconv.FormatFloat(float64(v), 'f', -1, 64))
		case float64:
			a.Append(strconv.FormatFloat(v, 'f', -1, 64))
		case time.Time: // 时间转换为timeLayout格式
			a.Append(v.Format(timeLayout))
		default:
			a.Append(fmt.Sprint(v))
		}
	}
}

// Bytes
//拼接完整的命令行, redis通信协议格式为:
//*<参数数量> CR LF
//$<参数 1 的字节数量> CR LF
//<参数 1 的数据> CR LF
//...
//$<参数 N 的字节数量> CR LF
//<参数 N 的数据> CR LF
//
//命令示例:
//*3
//$3
//SET
//$5
//myKey
//$7
//myValue
//
//实际传输值为: *3\r\n$3\r\nSET\r\n$5\r\nmyKey\r\n$7\r\nmyValue\r\n
func (a *Args) Bytes() (b []byte) {
	var buf = innerBytes.Get()
	_, _ = buf.WriteString("*")
	_, _ = buf.WriteString(strconv.FormatInt(int64(len(*a)), 10))
	_, _ = buf.WriteString(separator)
	for _, v := range *a {
		_, _ = buf.WriteString("$")
		_, _ = buf.WriteString(strconv.FormatInt(int64(len(v)), 10))
		_, _ = buf.WriteString(separator)
		_, _ = buf.WriteString(v)
		_, _ = buf.WriteString(separator)
	}
	b = make([]byte, buf.Len())
	copy(b, buf.B)
	innerBytes.Put(buf)
	return
}
