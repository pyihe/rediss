package rediss

import (
	"github.com/pyihe/go-pkg/maths"
	"github.com/pyihe/rediss/args"
)

// BLMove v6.2.0后可用
// 命令格式: BLMOVE source destination LEFT | RIGHT LEFT | RIGHT timeout
// 时间复杂度: O(1)
// 当source有元素时, BLMOVE与LMOVE一样
// 如果source没有元素时, BLMOVE将会阻塞直到有另一个客户端push元素到source中或者直到超时
// timeout为最长阻塞时间, 双精度数, 单位为秒,
// 返回值类型: Bulk String, 从source pop到destination的元素
// 函数参数说明:
// src: 源队列, 移出元素的队列
// dst: 目的队列, 元素push进的队列
// fromSide: pop类型: LEFT|RIGHT
// toSide: push类型: LEFT|RIGHT
// timeout: pop超时时间, 单位秒
func (c *Client) BLMove(src, fromSide, dst, toSide string, timeout float64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("BLMOVE", src, dst, fromSide, toSide)
	cmd.AppendArgs(timeout)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommandWithoutTimeout(cmdBytes)
}

// BLMPop v7.0.0后可用
// 命令格式: BLMPOP timeout numkeys key [key ...] LEFT | RIGHT [COUNT count]
// 时间复杂度: O(N+M), N为key的数量, M为返回的元素数量
// 当任意一个key对应的list有元素时, BLMPOP与LMPOP一样
// 当所有的list为空时, redis将会阻塞该连接, 直到有另一个客户端push元素到list中
// 或者直到timeout, 当timeout为0时, 将会永久阻塞
// 返回值类型:
// 1. 当没有元素可以pop或者超时, 返回nil
// 2. 一个双元素数组, 第一个元素是list的名称, 第二个元素是由该list的元素组成的数组
// 函数参数说明:
// timeout: 超时时间, 单位秒
// keys: pop元素的list
// from: LEFT|RIGHT
// count: pop的元素个数(可选参数)
func (c *Client) BLMPop(timeout float64, keys []string, from string, count int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("BLMPOP")
	cmd.AppendArgs(timeout, len(keys))
	cmd.Append(keys...)
	cmd.Append(from)
	if count > 1 {
		cmd.AppendArgs("COUNT", count)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// BLPop v2.0.0后可用
// 命令格式: BLPOP key [key ...] timeout
// 时间复杂度: O(N), N为提供的key的数量
// BLPOP是阻塞式的, 当给定的key对应的所有list没有元素可以pop时, redis将阻塞连接
// 返回值类型:
// 1. 当没有元素可以pop或者超时时将会返回多个nil
// 2. 一个双元素数组, 第一个元素是list的名称, 第二个元素是由该list的元素组成的数组
// 函数参数说明:
// keys: 从哪些list pop元素
// timeout: 超时时间
func (c *Client) BLPop(keys []string, timeout float64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("BLPOP")
	cmd.Append(keys...)
	cmd.AppendArgs(timeout)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// BRPop v2.0.0后可用
// 命令格式: BRPOP key [key ...] timeout
// 时间复杂度: O(N), N为提供的key的数量
// RLPOP是阻塞式的, 当给定的key对应的所有list没有元素可以pop时, redis将阻塞连接
// 返回值类型:
// 1. 当没有元素可以pop或者超时时将会返回多个nil
// 2. 一个双元素数组, 第一个元素是list的名称, 第二个元素是由该list的元素组成的数组
// keys: 从哪些队列pop元素
// timeout: 超时时间
func (c *Client) BRPop(keys []string, timeout float64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("BRPOP")
	cmd.Append(keys...)
	cmd.AppendArgs(timeout)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// BRPopLPush v2.2.0后可用, v6.2.0后废弃
// 命令格式: BRPOPLPUSH source destination timeout
// 时间复杂度: O(1)
// 当src为空时, redis将会阻塞连接, 直到有元素被push或者timeout超时, 0值的timeout将会永久阻塞
// 从v6.0.0开始, timeout从整型变为双精度浮点型
// 返回值类型: Bulk String, 有元素时返回的是元素, 没有元素时回复为空
// 函数参数说明:
// src: 源队列
// dst: 目的队列
// timeout: 超时时间
func (c *Client) BRPopLPush(src, dst string, timeout float64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("BRPOPLPUSH", src, dst)
	cmd.AppendArgs(timeout)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LIndex v1.0.0后可用
// 命令格式: LINDEX key index
// 时间复杂度: O(N), N为需要遍历的元素数量, 这使得请求第一个或者最后一个元素时的时间复杂度为O(1)
// 返回key对应list中指定位置的元素, 索引从0开始, 负数表示从list尾部开始, -1表示最后一个元素
// 如果key对应的值类型不是list, 将会返回一个错误
// 返回值类型: Bulk String, 返回指定位置的元素, 如果索引越界了则返回nil
// 函数参数说明:
// key: 命令执行于哪个队列
// index: 指定索引
func (c *Client) LIndex(key string, index int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LINDEX", key)
	cmd.AppendArgs(index)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LInsert v2.2.0后可用
// 命令格式: LINSERT key BEFORE | AFTER pivot element
// 时间复杂度: O(N), N为找到基准值之前需要遍历的元素个数
// 在指定list的基准值之前或者之后插入元素, 如果key不存在, 视为空list并且没有操作执行
// 如果key存在但不是list类型时, 将会返回错误
// 返回值类型: Integer, 返回插入元素后的列表长度, 如果没有找到基准元素返回-1
// 函数参数说明:
// key: 需要insert的队列
// to: BEFORE|AFTER
// pivot: 基准值
// element: 插入的元素值
func (c *Client) LInsert(key string, to string, pivot, element interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LINSERT", key, to)
	cmd.AppendArgs(pivot, element)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LLen v1.0.0后可用
// 命令格式: LLEN key
// 时间复杂度: O(1)
// 返回指定队列的长度, 如果队列不存在, 被认为是空队列并且返回0, 如果key数据类型不是队列, 将会返回错误
// 返回值类型: Integer, 返回队列的长度
func (c *Client) LLen(key string) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LLEN", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LMove v6.2.0后可用
// 命令格式: LMOVE source destination LEFT | RIGHT LEFT | RIGHT
// 时间复杂度: O(1)
// 自动返回并且移除src的头部元素或者尾部元素(取决于from参数)
// 然后将元素push进dst队列的头部或者尾部(取决于from参数)
// 返回值类型: Bulk String, pop和push的元素
// 函数参数说明:
// src: 源队列
// dst: 目的队列
// fromTo: LEFT|RIGHT LEFT|RIGHT
func (c *Client) LMove(src, dst, fromTo string) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LMOVE", src, dst, fromTo)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LMPop v7.0.0后可用
// 命令格式: LMPOP numkeys key [key ...] LEFT | RIGHT [COUNT count]
// 时间复杂度: O(N+M), N为提供的key的数量, M是返回的元素数量
// 从提供的keys中第一个非空list pop一个或者多个元素
// LPOP和RPOP只作用于一个key, 但是可以返回多个元素
// BLPOP和BRPOP作用于多个key, 但是一个key只返回一个元素
// 返回值类型:
// 1. 如果没有元素可以pop, 返回nil
// 2. 双元素的数组, 第一个元素为key, 第二个元素为该key pop出的元素值
// 函数参数说明:
// keys: 提供的key
// from: LEFT|RIGHT
// count: 需要pop的元素个数, 默认为1
func (c *Client) LMPop(keys []string, from string, count int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LMPOP")
	cmd.AppendArgs(len(keys))
	cmd.Append(keys...)
	cmd.Append(from)
	if count > 1 {
		cmd.AppendArgs("COUNT", count)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LPop v1.0.0后可用
// 命令格式: LPOP key [count]
// 时间复杂度: O(N), N为返回的元素个数
// 移除并返回key对应队列的第一个元素
// 命令默认pop队列头部的第一个元素, 当提供了可选的count参数后, redis回复将由count个元素组成, 取决于队列的长度
// 返回值类型:
// 1. 当没有count参数时: Bulk String, 返回第一个元素的值, 如果key不存在则返回nil
// 2. 当有count参数时: Array, 返回pop的元素数组, 如果key不存在返回nil
// v6.2.0开始支持count参数
func (c *Client) LPop(key string, count int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LPOP", key)
	if count > 1 {
		cmd.AppendArgs(count)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LPos v6.0.6后可用
// 命令格式: LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
// 时间复杂度: O(N), N为list拥有的元素数量
// 命令返回匹配到指定队列指定元素的位置, 位置从0开始
// 如果没有选项被指定, 命令将会从头到尾扫描队列, 寻找第一个匹配的元素, 如果元素被找到, 返回位置索引, 如果没有匹配到, 则返回nil
// 如果指定了RANK选项，并且有多个匹配结果, 将会根据RANK选项返回第RANK个匹配结果的索引, 对于负数的RANK, 匹配将会从队尾到队首开始
// 如果指定了COUNT选项, 将会返回前COUNT个匹配结果的位置, 当COUNT为0时, 将会返回所有的匹配结果
// 如果指定了COUNT, 但没有匹配结果, 将返回空的数组, 如果没有指定COUNT, 没有匹配结果时将会返回nil
// 如果指定了MAXLEN, 命令将只会比较MAXLEN个元素, 当指定MAXLEN并且值为0时, 将会比较所有的元素
// 返回值类型: Integer, 返回匹配成功的元素的位置, 如果指定了COUNT参数, 将会返回位置数组, 没有匹配结果时返回空数组
// 函数参数说明:
// key: 指定队列
// element: 需要匹配的元素
// rank: RANK参数选项, 指定返回匹配到的第几个元素位置
// count: COUNT参数选项, -1表示不带COUNT选项, 0表示返回所有匹配成功的元素位置, 其余表示返回COUNT个匹配结果
// maxLen: MAXLEN参数选项, -1表示不带MAXLEN选项, 0表示比较完所有的元素, 其余表示只做MAXLEN次比较
func (c *Client) LPos(key string, element interface{}, rank, count, maxLen int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LPOS", key)
	cmd.AppendArgs(element)
	if rank != 0 {
		cmd.AppendArgs("RANK", rank)
	}
	if count >= 0 {
		cmd.AppendArgs("COUNT", count)
	}
	if maxLen >= 0 {
		cmd.AppendArgs("MAXLEN", maxLen)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LPush v1.0.0后可用
// 命令格式: LPUSH key element [element ...]
// 时间复杂度: O(N), N为元素的数量
// 向key指定的队列头部插入指定的元素, 如果key不存在则会先创建一个空的队列在push, 如果key存储的数据类型不是队列, 将会返回错误
// 返回值类型:　Integer, push成功后队列的长度
// v2.4.0开始可以push多个元素
func (c *Client) LPush(key string, elements ...interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LPUSH", key)
	cmd.AppendArgs(elements...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LPushX v2.2.0后可用
// 命令格式: LPUSHX key element [element ...]
// 时间复杂度: O(N), N为push元素的数量
// 当且仅当key存在并且key存储的是队列类型时, 向key指定的队列插入元素
// 返回值类型: Integer, push成功后队列的长度
// v4.0.0开始接受多个元素
func (c *Client) LPushX(key string, elements ...interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LPUSHX", key)
	cmd.AppendArgs(elements...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LRange v1.0.0后可用
// 命令格式: LRANGE key start stop
// 时间复杂度: O(S+N), 对小队列来说, S为队列头到start的距离, 对于大队列来说, S为队列头或者队列尾(取决于谁的距离更近)
// N为range指定的元素数量
// 返回key中指定的元素, start和stop都是基于0的索引,start和stop同样可以为负数, 表示从队尾开始遍历
// 遍历结果包含stop所处位置的元素, 即元素位置为[start, stop]的闭区间
// 超出范围的索引不会产生错误, 如果start大于列表的结尾, 则返回一个空列表; 如果stop大于列表的实际末尾, Redis会将其视为列表的最后一个元素
// 返回值类型: Array, 返回range的结果
func (c *Client) LRange(key string, start, stop int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LRANGE", key)
	cmd.AppendArgs(start, stop)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LRem v1.0.0后可用
// 命令格式: LREM key count element
// 时间复杂度: O(N+M), N为list的长度, M为被移除的元素数量
// 命令将会移除list中count个element
// count > 0: 以从头到尾的顺序移除count个element
// count < 0: 以从尾到头的顺序移除count个element
// count = 0: 移除所有的element
// 当key不存在时, 将会被认为是空队列
// 返回值类型: Integer, 被移除的element数量
func (c *Client) LRem(key string, count int64, element interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LREM", key)
	cmd.AppendArgs(count, element)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LSet v1.0.0后可用
// 命令格式: LSET key index element
// 时间复杂度: O(N), N为list的长度
// 将索引为index的元素设置为element, 如果索引越界了, 将会返回错误
// 返回值类型: Simple String
func (c *Client) LSet(key string, index int64, element interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LSET", key)
	cmd.AppendArgs(index, element)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// LTrim v1.0.0后可用
// 命令格式: LTRIM key start stop
// 时间复杂度: O(N), N为需要移除的元素数量
// 修剪指定的列表, 使其只包含指定范围的元素, start和stop都是基于0的索引, 命令执行后列表的元素为[start, stop]闭区间范围内的元素
// start和stop同样可以为负数, 表示从列表尾部开始计算
// 索引越界时不会产生错误, 如果start大于列表的末尾, 或者start>end, 结果将是一个空列表(这会导致key被删除), 如果end大于列表的末尾, Redis会将其视为列表的最后一个元素
// 返回值类型: Simple String
func (c *Client) LTrim(key string, start, stop int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("LTRIM", key)
	cmd.AppendArgs(start, stop)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// RPop v1.0.0后可用
// 命令格式: RPOP key [count]
// 时间复杂度: O(N), N为返回的元素个数
// 移除并返回key对应列表的最后一个(或者count个)元素
// 默认情况下, 命令返回列表最后一个元素, 当提供了可选的count参数后, 回复将会由count个元素组成, 取决于列表的长度
// 返回值类型:
// 1. 当没有指定count参数时: 返回最后一个元素的值, key不存在时返回nil
// 2. 当指定count参数时: 返回一个pop的元素的列表, key不存在时返回nil
func (c *Client) RPop(key string, count int64) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("RPOP", key)
	count = maths.MaxInt64(1, count)
	cmd.AppendArgs(count)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// RPopLPush v1.2.0后可用, v6.2.0后废弃
// 命令格式: RPOPLPUSH source destination
// 时间复杂度: O(1)
// 自动返回并移除src中的尾部最后一个元素, 然后push进dst的头部
// 如果src不存在, 将会返回nil, 没有操作被执行; 如果src和dst相同, 该操作相当于从列表中删除最后一个元素并将其作为列表的第一个元素push, 因此可以认为是列表旋转命令
// 返回值类型: Bulk String, 被操作的元素(pop&push)
func (c *Client) RPopLPush(src, dst string) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("RPOPLPUSH", src, dst)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// RPush v1.0.0后可用
// 命令格式: RPUSH key element [element ...]
// 时间复杂度: O(N), N为elements的数量
// 将指定的元素插入进key所在的列表中, 如果key不存在, 命令将会创建一个空的列表然后再执行push操作, 如果key存储的数据类型不是list, 将会返回错误
// 返回值类型: Integer, 返回push后列表的长度
// v2.4.0后开始接受多个元素
func (c *Client) RPush(key string, elements ...interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("RPUSH", key)
	cmd.AppendArgs(elements...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// RPushX v2.2.0后可用
// 命令格式: RPUSHX key element [element ...]
// 时间复杂度: O(N), N为elements的数量
// 只有当key存在并且值类型为list时才会将elements插入进key对应的list
// 与RPUSH相比不同的是, 当key不存在时RPUSHX不会执行任何操作
// 返回值类型: Integer, push操作后的list长度
func (c *Client) RPushX(key string, elements ...interface{}) (*Reply, error) {
	cmd := args.Get()
	cmd.Append("RPUSHX", key)
	cmd.AppendArgs(elements...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}
