package rediss

import (
	"github.com/pyihe/rediss/args"
	"github.com/pyihe/rediss/model/set"
	"github.com/pyihe/rediss/pool"
)

// SAdd v1.0.0后可用
// 命令格式: SADD key member [member ...]
// 时间复杂度: O(N), N为需要添加的元素数量
// 向key对应的set中添加指定成员, 已经存在的成员将会被忽略, 如果key不存在, add之前将会用key创建新的空set
// 如果key存在并且值类型不为set, 将会返回一个错误
// 返回值类型: Integer, 最终添加的成员数量, 不包含已经存在的成员
// v2.4.0后开始支持接收多个成员参数
func (c *Client) SAdd(key string, members ...interface{}) (int64, error) {
	cmd := args.Get()
	cmd.Append("SADD", key)
	cmd.AppendArgs(members...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}

	return reply.Integer()
}

// SCard v1.0.0后可用
// 命令格式: SCARD key
// 时间复杂度: O(1)
// 返回存储在key指定集合的成员数量
// 返回值类型: Integer, 返回成员数量, 如果key不存在返回0
func (c *Client) SCard(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("SCARD", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// SDiff v1.0.0后可用
// 命令格式: SDIFF key [key ...]
// 时间复杂度: O(N), N为所有set的元素总数
// 获取第一个key对应的set和其他key对应的set之间不同成员构成的set
// 返回值类型: Array, 返回差异集合的所有成员
func (c *Client) SDiff(keys ...string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("SDIFF")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SDiffStore v1.0.0后可用
// 命令格式: SDIFFSTORE destination key [key ...]
// 时间复杂度: O(N), N为所有set的成员总数
// 获取第一个key对应的set和其他key对应的set之间不同成员构成的set, 并将结果set的值存储进dst中, 如果dst已经存在, 命令将会覆盖其旧值
// 返回值类型: Integer, 返回差异集合的成员数量
func (c *Client) SDiffStore(dst string, keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("SDIFFSTORE", dst)
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// SInter v1.0.0后可用
// 命令格式: SINTER key [key ...]
// 时间复杂度: O(N*M), N为最小set的成员数量, M为集合数量
// 返回由所有给定set的交集产生的集合成员
// 如果key不存在将会被视为空set, 交集也将会是空
// 返回值类型: Array, 结果集合的成员
func (c *Client) SInter(keys ...string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("SINTER")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SInterCard v7.0.0后可用
// 命令格式: SINTERCARD numkeys key [key ...] [LIMIT limit]
// 时间复杂度: O(N*M), N为最小集合的成员数量, M为set的数量
// 与SINTER一样, 获取所有集合的交集, 不同的是SINTERCARD返回成员数量, 而不是返回成员列表
// 如果key不存在, set将会被视为空, 交集也将为空
// 如果提供了LIMIT(默认为0, 表示无限制)选项,
// 返回值类型: Integer, 返回结果set的元素数量
func (c *Client) SInterCard(keys []string, limit int64) (int64, error) {
	cmd := args.Get()
	cmd.Append("SINTERCARD")
	cmd.AppendArgs(len(keys))
	cmd.Append(keys...)
	if limit > 0 {
		cmd.AppendArgs("LIMIT", limit)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// SInterStore v1.0.0后可用
// 命令格式: SINTERSTORE destination key [key ...]
// 时间复杂度: O(N*M), N为最小set的元素数量, M为set数量
// 与SINTER命令一样, SINTERSTORE也是获取多个set的交集, 不同的是SINTERSTORE会将交集存储在指定的key中
// 如果dst已经存在, SINTERSTORE将会覆盖其旧值
// 返回值类型: 返回结果集合的元素数量
func (c *Client) SInterStore(dst string, keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("SINTERSTORE", dst)
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// SIsMember v1.0.0后可用
// 命令格式: SISMEMBER key member
// 时间复杂度: O(1)
// 判断member是否是key对应的集合的成员
// 返回值类型: Integer, 如果是返回1, 如果不是或者key不存在则返回0
func (c *Client) SIsMember(key string, member interface{}) (bool, error) {
	cmd := args.Get()
	cmd.Append("SISMEMBER", key)
	cmd.AppendArgs(member)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}

	return reply.Bool()
}

// SMembers v1.0.0后可用
// 命令格式: SMEMBERS key
// 时间复杂度: O(N), N为集合基数(即集合成员数量)
// 获取key对应的集合的所有成员
// 返回值类型: Array, 返回成员组成的数组
func (c *Client) SMembers(key string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("SMEMBERS", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SMIsMember v6.2.0后可用
// 命令格式: SMISMEMBER key member [member ...]
// 时间复杂度: O(N), N为被检查的元素数量
// 判断members中的每个值是否都是key对应set的成员
// 返回值类型: Array, 按照提供的成员顺序返回每个成员的校验结果, 如果是set的成员返回1, 不是或者key不存在时返回0
func (c *Client) SMIsMember(key string, members ...interface{}) (result []bool, err error) {
	cmd := args.Get()
	cmd.Append("SMISMEMBER", key)
	cmd.AppendArgs(members...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}

	array := reply.Array()
	result = make([]bool, len(array))
	for i, v := range array {
		result[i] = v.ValueString() == "1"
	}
	return
}

// SMove v1.0.0后可用
// 命令格式: SMOVE source destination member
// 时间复杂度: O(1)
// 将成员从源集合移动到目标集合, 原子操作
// 如果源集合不存在或者不包含指定的member, 将不会有操作被执行并且返回0;
// 否则成员将会从源集合移动到目标集合, 如果成员已经存在于目标集合, 命令将只会从源集合中移除成员
// 如果源集合或者目标集合所持有的数据类型不是集合, 将会返回一个错误
// 返回值类型: Integer, 移动成功返回1, 如果源集合不包含指定成员没有操作被执行时返回0
func (c *Client) SMove(src, dst string, member interface{}) (bool, error) {
	cmd := args.Get()
	cmd.Append("SMOVE", src, dst)
	cmd.AppendArgs(member)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}

	return reply.Bool()
}

// SPop v1.0.0后可用
// 命令格式: SPOP key [count]
// 时间复杂度: 没有指定count参数时为O(1), 否则为O(N), N为count参数
// 从key指定的集合中随机的移除并返回一个或者count个成员
// 默认情况下, SPOP只会pop一个成员, 当提供了count参数后, 将会返回由count个成员组成的数组, 取决于集合的基数
// 返回值类型:
// 1. 没有提供count选项时: Bulk String, 返回被移除的成员, 如果key不存在时返回nil
// 2. 提供了count参数时: Array, 返回被移除的成员, 如果key不存在时返回一个空的数组
func (c *Client) SPop(key string, count int64) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("SPOP", key)
	if count > 1 {
		cmd.AppendArgs(count)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SRandMember v1.0.0后可用
// 命令格式: SRANDMEMBER key [count]
// v2.6.0后开始支持count参数
// 时间复杂度: 没有指定count参数时为O(1), 否则为O(N), N为count参数
// 当没有提供count参数时, 将从key指定的集合中随机返回一个元素
// count > 0: 1. 返回的元素中没有重复的值
//			  2. 如果count大于集合的基数, 命令将只返回整个集合的去重后的元素, 而没有额外的元素
//			  3. 返回值里的元素顺序并非真正的随机, 所以如果需要的话取决于客户端的随机算法
// count < 0: 1. 返回值可能存在重复的元素
//  		  2. 准确的count个元素, 或者当集合为空时总是返回空数组
// 			  3. 返回值里元素的顺序是真的顺序
// 返回值类型:
// 没有count参数: Bulk String, 返回随机的成员, 如果key不存在时返回nil
// 有count参数: Array, 返回随机的成员列表, 如果key不存在返回空数组
func (c *Client) SRandMember(key string, count int64) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("SRANDMEMBER", key)
	if count != 0 {
		cmd.AppendArgs(count)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SRem v1.0.0后可用
// 命令格式: SREM key member [member ...]
// 时间复杂度: O(N), N为需要被移除的成员数量
// 从集合中删除指定的成员, 如果集合中不包含指定的成员, 该成员在删除时将会被忽略, 如果key不存在将会被视为空集合, 命令返回0
// 如果key所持有的数据类型不是集合, 将会返回一个错误
// 返回值类型: Integer, 集合被删除的元素个数
// v2.4.0开始支持多个成员参数
func (c *Client) SRem(key string, members ...interface{}) (int64, error) {
	cmd := args.Get()
	cmd.Append("SREM", key)
	cmd.AppendArgs(members...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// SScan v2.8.0后可用
// 命令格式: SSCAN key cursor [MATCH pattern] [COUNT count]
// 时间复杂度: O(N), N为scan结果中元素数量
// 返回值类型: Array, 数组的元素为集合的成员
func (c *Client) SScan(key string, cursor int64, option *set.ScanOption) (result *set.ScanResult, err error) {
	cmd := args.Get()
	cmd.Append("SSCAN", key)
	cmd.AppendArgs(cursor)
	if option != nil {
		if option.Match != "" {
			cmd.Append("MATCH", option.Match)
		}
		if option.Count > 0 {
			cmd.AppendArgs("COUNT", option.Count)
		}
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}

	array := reply.Array()
	result = &set.ScanResult{}
	result.Cursor, err = array[0].Integer()
	result.Members = make([]string, 0, len(array[1].Array()))
	for _, v := range array[1].Array() {
		result.Members = append(result.Members, v.ValueString())
	}
	return
}

// SUnion v1.0.0后可用
// 命令格式: SUNION key [key ...]
// 时间复杂度: O(N), N所有集合中成员总数
// 获取指定集合的并集, 如果集合不存在将会被视为空集合
// 返回值类型: Array, 返回并集中的成员
func (c *Client) SUnion(keys ...string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("SUNION")
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// SUnionStore v1.0.0后可用
// 命令格式: SUNIONSTORE destination key [key ...]
// 时间复杂度: O(N), N所有集合中成员总数
// 与SUNION一样, 获取集合的并集, 不同的是SUNIONSTORE会将并集存储在dst指向的集合中
// 如果dst已经存在, SUNIONSTORE会将其旧值覆盖
// 返回值类型: Integer, 返回存储后dst的成员数量
func (c *Client) SUnionStore(dst string, keys ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("SUNIONSTORE", dst)
	cmd.Append(keys...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}
