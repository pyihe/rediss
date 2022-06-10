package rediss

import (
	"strconv"
	"strings"

	innerBytes "github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/rediss/args"
	"github.com/pyihe/rediss/model/bitmap"
	"github.com/pyihe/rediss/pool"
)

// BitCount v2.6.0后可用
// 命令格式: BITCOUNT key [ start end [ BYTE | BIT]]
// v7.0.0开始增加BYTE|BIT参数
// 时间复杂度: O(N)
// 计算字符串中设置的位数(人口计数)
// 默认情况下, 检查字符串中包含的所有字节; 可以仅在传递附加参数start和end的间隔中指定计数操作
// start和end可以为负数
// 不存在的key将被视为空字符串, 命令将会返回0
// 默认情况下, 附加参数start和end指定字节索引; 我们可以使用附加参数BIT来指定位索引
// 返回值类型: Integer, 被设置为1的位的数量
func (c *Client) BitCount(key string, option *bitmap.BitOption) (int64, error) {
	cmd := args.Get()
	cmd.Append("BITCOUNT", key)
	if option != nil {
		cmd.AppendArgs(option.Start, option.End)
		if option.Unit != "" {
			cmd.Append(option.Unit)
		}
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// BitFieldGet v3.2.0后可用
// 命令格式: BITFIELD key GET encoding offset [ GET encoding offset ...]
// 时间复杂度: 每个子命令的时间复杂度为: O(1)
// BITFIELD只读属性的命令, 类似于GEORADIUS_RO
// 返回值类型: Integer, 返回GET指定位的值
func (c *Client) BitFieldGet(key string, opts ...*bitmap.FieldOption) (*pool.Reply, error) {
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("BITFIELD", key)

	buf := innerBytes.Get()
	for _, op := range opts {
		if op == nil {
			continue
		}
		if err := op.Check(true); err != nil {
			return nil, err
		}
		// append 子命令
		cmd.Append("GET")

		// encoding
		_ = buf.WriteByte(op.EncodingPrefix)
		_, _ = buf.WriteString(strconv.FormatInt(op.Encoding, 10))
		cmd.Append(buf.String())
		buf.Reset()

		// 偏移量是否有前缀
		if op.OffsetPrefix != 0 {
			_ = buf.WriteByte(op.OffsetPrefix)
		}
		_, _ = buf.WriteString(strconv.FormatInt(op.Offset, 10))
		cmd.Append(buf.String())
		buf.Reset()
	}
	innerBytes.Put(buf)

	return c.sendCommand(cmd.Bytes())
}

// BitField v3.2.0后可用
// 命令格式: BITFIELD key GET encoding offset | [OVERFLOW WRAP | SAT | FAIL] SET encoding offset value | INCRBY encoding offset increment [ GET encoding offset | [OVERFLOW WRAP | SAT | FAIL] SET encoding offset value | INCRBY encoding offset increment ...]
// 时间复杂度: O(1)对于每个指定的子命令
// 返回值类型: 该命令返回一个数组，其中每个条目是在同一位置给出的子命令的相应结果。 OVERFLOW 子命令不计为生成回复
func (c *Client) BitField(key string, opts ...*bitmap.FieldOption) (*pool.Reply, error) {
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("BITFIELD", key)

	buf := innerBytes.Get()
	for _, op := range opts {
		if err := op.Check(false); err != nil {
			return nil, err
		}

		encoding, offset := "", ""
		// 拼接encoding
		_ = buf.WriteByte(op.EncodingPrefix)
		_, _ = buf.WriteString(strconv.FormatInt(op.Encoding, 10))
		encoding = buf.String()
		buf.Reset()

		// 拼接offset
		if op.OffsetPrefix != 0 {
			_ = buf.WriteByte(op.OffsetPrefix)
		}
		_, _ = buf.WriteString(strconv.FormatInt(op.Offset, 10))
		offset = buf.String()
		buf.Reset()

		switch strings.ToUpper(op.SubCommand) {
		case "GET":
			cmd.Append(op.SubCommand, encoding, offset)
		case "SET":
			if op.Overflow != "" {
				cmd.Append("OVERFLOW", op.Overflow)
			}
			cmd.Append(op.SubCommand, encoding, offset)
			cmd.AppendArgs(op.Value)
		case "INCRBY":
			cmd.Append("INCRBY", encoding, offset)
			cmd.AppendArgs(op.Increment)
		default:
			return nil, errors.New("BitField sub command option: GET, SET, INCRBY")
		}
		buf.Reset()
	}
	innerBytes.Put(buf)
	return c.sendCommand(cmd.Bytes())
}

// BitFieldRo v6.2.0后可用
// 命令格式: BITFIELD_RO key GET encoding offset [ encoding offset ...]
// 时间复杂度: 每个子命令O(1)
// BitField命令的只读版本
// 返回值类型: Array, 没回每个子命令回复组成的数组, 子命令和回复的位置一一对应
func (c *Client) BitFieldRo(key string, opts ...*bitmap.FieldRoOption) (*pool.Reply, error) {
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("BITFIELD_RO", key)

	buf := innerBytes.Get()
	for _, op := range opts {
		if err := op.Check(); err != nil {
			return nil, err
		}

		encoding, offset := "", ""
		_ = buf.WriteByte(op.EncodingPrefix)
		_, _ = buf.WriteString(strconv.FormatInt(op.Encoding, 10))
		encoding = buf.String()
		buf.Reset()

		if op.OffsetPrefix != 0 {
			_ = buf.WriteByte(op.OffsetPrefix)
		}
		_, _ = buf.WriteString(strconv.FormatInt(op.Offset, 10))
		offset = buf.String()
		buf.Reset()

		cmd.Append("GET", encoding, offset)
	}
	innerBytes.Put(buf)
	return c.sendCommand(cmd.Bytes())
}

// BitOp v2.6.0后可用
// 命令格式: BITOP operation destkey key [key ...]
// 时间复杂度: O(N)
// 在多个键(包含字符串值)之间执行按位运算并将结果存储在目标键中
// BITOP命令支持四种按位运算: AND, OR, XOR, NOT, 因此调用该命令的有效形式是:
// 1. BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// 2. BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
// 3. BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN
// 4. BITOP NOT destkey srckey
// 操作的结果总是存放在destkey
// 当在具有不同长度的字符串之间执行操作时，所有比集合中最长字符串短的字符串都被视为补零直到最长字符串的长度
// 返回值类型: Integer, 存储在目标键中的字符串的大小，即等于最长输入字符串的大小
func (c *Client) BitOp(op, dst string, keys ...string) (int64, error) {
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("BITOP")
	switch strings.ToUpper(op) {
	case "NOT":
		if len(keys) > 1 {
			return 0, errors.New("NOT operation only support one input key")
		}
		fallthrough
	case "AND", "OR", "XOR":
		cmd.Append(op, dst)
	default:
		return 0, ErrInvalidArgumentFormat
	}
	cmd.Append(keys...)

	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// BitPos v2.8.7后可用
// 命令格式: BITPOS key bit [ start [ end [ BYTE | BIT]]]
// v7.0.0开始增加BYTE|BIT选项
// 时间复杂度: O(N)
// 获取字符串中第一位设置为1或者0的位置
// 默认情况下, 检查字符串中包含的所有字节;
// 可以仅在传递附加参数start和end的指定间隔中查找位(可以只传递start, 操作将假定end是字符串的最后一个字节; 但是如解释的那样存在语义差异之后)
// 默认情况下, 范围被解释为字节范围而不是位范围, 因此start=0和end=2表示查看前三个字节
// 可以使用可选的BIT修饰符来指定应将范围解释为位范围; 所以start=0和end=2表示看前三位
// 请注意, 即使使用start和end指定范围, 位位置也始终作为从位零开始的绝对值返回
// 不存在的key被视为空字符串
// 返回值类型: Integer, 返回设置为1或0的第一位的位置
// 如果我们查找设置位(位参数为 1)并且字符串为空或仅由零字节组成, 则返回 -1
// 如果我们寻找清除位(位参数为 0)并且字符串仅包含设置为1的位, 则该函数返回第一个位而不是右侧字符串的一部分。因此如果字符串是三个字节设置为值 0xff, 则命令BITPOS键0将返回24, 因为直到第23位所有位都是1
// 基本上, 如果您查找清除位并且不指定范围或仅指定开始参数, 该函数会将字符串的右侧视为用零填充
// 但是, 如果您正在寻找清除位并指定包含开始和结束的范围, 则此行为会发生变化。如果在指定范围内没有找到清除位, 则函数返回-1, 因为用户指定了一个清除范围并且该范围内没有0位
func (c *Client) BitPos(key string, bit int64, option *bitmap.BitOption) (int64, error) {
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("BITPOS", key)
	cmd.AppendArgs(bit)
	if option != nil {
		cmd.AppendArgs(option.Start)
		if option.End != 0 {
			cmd.AppendArgs(option.End)
			if option.Unit != "" {
				cmd.Append(option.Unit)
			}
		}
	}
	reply, err := c.sendCommand(cmd.Bytes())
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// GetBit v2.2.0后可用
// 命令格式: GETBIT key offset
// 时间复杂度: O(1)
// 返回存储在 key 的字符串值中偏移处的位值
// 当offset超过字符串长度时, 字符串被假定为一个0位的连续空间; 当key不存在时, 它被假定为一个空字符串, 因此偏移量总是超出范围, 并且该值也被假定为一个0位的连续空间
// 返回值类型: Integer, 返回offset处的位值
func (c *Client) GetBit(key string, offset int64) (int64, error) {
	cmd := args.Get()
	cmd.Append("GETBIT", key)
	cmd.AppendArgs(offset)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// SetBit v2.2.0后可用
// 命令格式: SETBIT key offset value
// 时间复杂度: O(1)
// 设置或清除存储在 key 的字符串在偏移处的位
// 该位根据值设置或清除，值可以是 0 或 1
// 当key不存在时, 创建一个新的字符串: 字符串被增长以确保它可以在偏移量处设置值。偏移量参数必须大于或等于0, 并且小于2^32(这将位图限制为512MB); 当key处的字符串增长时, 添加的位设置为0
// 返回值类型: Integer, 返回存储在offset的原始bit值
func (c *Client) SetBit(key string, offset int64, value uint8) (int64, error) {
	cmd := args.Get()
	cmd.Append("SETBIT", key)
	cmd.AppendArgs(offset, value)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}
