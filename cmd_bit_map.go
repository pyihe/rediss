package rediss

import (
	"strings"

	"github.com/pyihe/rediss/args"
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
func (c *Client) BitCount(key string, start, end int64, unit string) (*Reply, error) {
	cmd := args.Get()
	defer args.Put(cmd)
	cmd.Append("BITCOUNT", key)
	if start != 0 && end != 0 {
		cmd.AppendArgs(start, end)
		switch strings.ToUpper(unit) {
		case "BYTE", "BIT":
			cmd.Append(unit)
		case "":
			break
		default:
			return nil, ErrInvalidArgumentFormat
		}
	}
	return c.sendCommand(cmd.Bytes())
}

// BitField v3.2.0后可用
// 命令格式: BITFIELD key GET encoding offset | [OVERFLOW WRAP | SAT | FAIL] SET encoding offset value | INCRBY encoding offset increment [ GET encoding offset | [OVERFLOW WRAP | SAT | FAIL] SET encoding offset value | INCRBY encoding offset increment ...]
// 时间复杂度: O(1)对于每个指定的子命令
// 返回值类型: 该命令返回一个数组，其中每个条目是在同一位置给出的子命令的相应结果。 OVERFLOW 子命令不计为生成回复
func (c *Client) BitField(key string, arguments ...interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("BITFIELD", key)
	cmd.AppendArgs(arguments...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
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
func (c *Client) BitOp(op, dst string, keys ...string) (*Reply, error) {
	cmd := args.Get()
	defer args.Put(cmd)
	cmd.Append("BITOP")
	switch strings.ToUpper(op) {
	case "AND", "OR", "XOR", "NOT":
		cmd.Append(op, dst)
	default:
		return nil, ErrInvalidArgumentFormat
	}
	cmd.Append(keys...)
	return c.sendCommand(cmd.Bytes())
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
func (c *Client) BitPos(key string, bit int64, start, end int64, unit string) (*Reply, error) {
	cmd := args.Get()
	defer args.Put(cmd)

	cmd.Append("BITPOS", key)
	cmd.AppendArgs(bit)
	if start != 0 {
		cmd.AppendArgs(start)
		if end != 0 {
			cmd.AppendArgs(end)
			switch strings.ToUpper(unit) {
			case "BYTE", "BIT":
				cmd.Append(unit)
			case "":
				break
			default:
				return nil, ErrInvalidArgumentFormat
			}
		}
	}
	return c.sendCommand(cmd.Bytes())
}

// GetBit v2.2.0后可用
// 命令格式: GETBIT key offset
// 时间复杂度: O(1)
// 返回存储在 key 的字符串值中偏移处的位值
// 当offset超过字符串长度时, 字符串被假定为一个0位的连续空间; 当key不存在时, 它被假定为一个空字符串, 因此偏移量总是超出范围, 并且该值也被假定为一个0位的连续空间
// 返回值类型: Integer, 返回offset处的位值
func (c *Client) GetBit(key string, offset int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("GETBIT", key)
	cmd.AppendArgs(offset)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SetBit v2.2.0后可用
// 命令格式: SETBIT key offset value
// 时间复杂度: O(1)
// 设置或清除存储在 key 的字符串在偏移处的位
// 该位根据值设置或清除，值可以是 0 或 1
// 当key不存在时, 创建一个新的字符串: 字符串被增长以确保它可以在偏移量处设置值。偏移量参数必须大于或等于0, 并且小于2^32(这将位图限制为512MB); 当key处的字符串增长时, 添加的位设置为0
// 返回值类型: Integer, 返回存储在offset的原始bit值
func (c *Client) SetBit(key string, offset int64, value int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("SETBIT", key)
	cmd.AppendArgs(offset, value)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}
