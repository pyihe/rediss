package rediss

import "github.com/pyihe/rediss/args"

// PFAdd v2.8.9后可用
// 命令格式: PFADD key [element [element ...]]
// 时间复杂度: O(1)添加每个元素
// 将所有元素添加到key所存储的HyperLogLog数据结构中
// 作为此命令的副作用, HyperLogLog 内部可能会更新, 以反映对迄今为止添加的唯一项目数量(集合的基数)的不同估计
// 如果HyperLogLog估计的近似基数在执行命令后发生变化, 则PFADD返回1, 否则返回0
// 如果指定的key不存在, 该命令会自动创建一个空的HyperLogLog结构(即指定长度和给定编码的Redis String)
// 调用不带元素但仅变量名有效的命令, 如果变量已存在则不执行任何操作, 或者如果键不存在则仅创建数据结构(在后一种情况下返回1)
// 返回值类型: Integer
func (c *Client) PFAdd(key string, elements ...interface{}) (int64, error) {
	cmd := args.Get()
	cmd.Append("PFADD", key)
	cmd.AppendArgs(elements...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// PFCount v2.8.9后可用
// 命令格式: PFCOUNT key [key ...]
// 时间复杂度: O(1)在使用单个键调用时具有非常小的平均常数时间; O(N)其中N是键的数量, 当使用多个键调用时, 常数时间要大得多
// 当使用单个键调用时, 返回由存储在指定变量中的HyperLogLog数据结构计算的近似基数, 如果key不存在, 则返回0
// 当使用多个键调用时, 通过内部将存储在提供的key处的HyperLogLogs合并到临时HyperLogLog中, 返回传递的HyperLogLogs联合的近似基数
// 可以使用HyperLogLog数据结构来计算集合中的唯一元素, 只需使用少量的恒定内存, 特别是每个HyperLogLog12k字节(加上键本身的几个字节)
// 返回值类型: Integer, 返回通过PFADD观察到的唯一元素的近似数量
func (c *Client) PFCount(keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("PFCOUNT")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// PFMerge v2.8.9后可用
// 命令格式: PFMERGE destkey sourcekey [sourcekey ...]
// 时间复杂度: O(N)合并N个HyperLogLogs, 但具有较高的常数时间
// 将多个HyperLogLog值合并为一个唯一值, 该值将近似于观察到的源HyperLogLog结构集的并集的基数
// 如果目标变量存在, 则将其视为源集之一, 其基数将包含在计算的HyperLogLog的基数中
// 返回值类型: Simple String
func (c *Client) PFMerge(dst string, srcs ...string) (string, error) {
	cmd := args.Get()
	cmd.Append("PFMERGE", dst)
	cmd.Append(srcs...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return "", err
	}
	return reply.ValueString(), nil
}
