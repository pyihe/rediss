package bitmap

import (
	"strings"

	"github.com/pyihe/go-pkg/errors"
)

// BitOption BITCOUNT命令选项
type BitOption struct {
	Start int64
	End   int64
	Unit  string
}

// FieldOption BITFIELD命令选项
type FieldOption struct {
	SubCommand     string // 子命令类型: GET, SET, INCRBY
	Overflow       string // 对于SET和INCRBY子命令, OVERFLOW选项: WARP, SAT, FAIL
	Offset         int64  // 偏移量
	Value          int64  // SET命令设置的值
	Increment      int64  // INCRBY命令的增量
	Encoding       int64  // 编码
	EncodingPrefix byte   // 编码前缀: u, i
	OffsetPrefix   byte   // 偏移量前缀: "#"
}

func (opt *FieldOption) Check(ro bool) error {
	// 校验子命令合法性
	command := strings.ToUpper(opt.SubCommand)
	switch command {
	case "GET": // 任何情况都可以执行GET子命令
	case "SET", "INCRBY": // 只有当执行可写属性的命令才能执行SET和INCRBY
		if ro {
			return errors.New("only support sub command: GET")
		}
	default:
		return errors.New("not support sub command")
	}

	// 校验编码前缀
	if opt.EncodingPrefix != 'i' && opt.EncodingPrefix != 'u' {
		return errors.New("encoding prefix option: 'i', 'u'")
	}
	// 校验偏移量前缀
	if opt.OffsetPrefix != 0 && opt.OffsetPrefix != '#' {
		return errors.New("offset prefix option: '#'")
	}
	// 校验OVERFLOW子命令
	switch strings.ToUpper(opt.Overflow) {
	case "":
		return nil
	case "WARP", "SAT", "FAIL":
		if command != "SET" {
			return errors.New("SET command only support OVERFLOW")
		}
		return nil
	default:
		return errors.New("overflow option: WARP, SAT, FAIL")
	}
}

// FieldRoOption BitField只读命令的选项
type FieldRoOption struct {
	Encoding       int64
	Offset         int64
	EncodingPrefix byte
	OffsetPrefix   byte
}

func (opt *FieldRoOption) Check() error {
	if opt.EncodingPrefix != 'i' && opt.EncodingPrefix != 'u' {
		return errors.New("encoding prefix option: 'i', 'u'")
	}
	if opt.OffsetPrefix != 0 && opt.OffsetPrefix != '#' {
		return errors.New("offset prefix option: '#'")
	}
	return nil
}
