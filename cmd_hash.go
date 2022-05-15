package rediss

import "strconv"

// HDel v2.0.0后可用
// 时间复杂度: O(N), N为要删除的字段数
// 用于移除key存储的hash数据中的指定字段, 对于key中不存在的字段将会被忽略, 如果key不存在命令将会返回0
// 返回值类型: Integer, 返回实际被移除的字段数量, 不包含指定但不存在的字段
func (c *Client) HDel(key string, fields ...string) (*Reply, error) {
	args := getArgs()
	args.Append("HDEL", key)
	args.Append(fields...)
	return c.sendCommand(args)
}

// HExists v2.0.0后可用
// 时间复杂度: O(1)
// 获取key中指定字段是否存在
// 返回值类型: Integer, 如果存在返回1, 如果key不存在或者key不包含该字段则返回0
func (c *Client) HExists(key string, field string) (*Reply, error) {
	args := getArgs()
	args.Append("HEXISTS", key, field)
	return c.sendCommand(args)
}

// HGet v2.0.0后可用
// 时间复杂度:O(1)
// 获取hash字段的值
// 返回值类型: Bulk String, key和field同时存在返回字段值, 否则返回nil
func (c *Client) HGet(key string, field string) (*Reply, error) {
	args := getArgs()
	args.Append("HGET", key, field)
	return c.sendCommand(args)
}

// HGetAll v2.0.0后可用
// 时间复杂度: O(N), N为hash的大小
// 获取key对应hash的所有field和value
// 返回值类型: Array, <field, value>的列表, 或者空列表(如果key不存在)
func (c *Client) HGetAll(key string) (*Reply, error) {
	args := getArgs()
	args.Append("HGETALL", key)
	return c.sendCommand(args)
}

// HIncrBy v2.0.0后可用
// 时间复杂度: O(1)
// 给hash指定字段添加指定的增量, 如果field不存在, 则在操作之前会将field的值置为0
// 返回值类型: Integer, 返回增加操作后的值
func (c *Client) HIncrBy(key string, field string, increment int64) (*Reply, error) {
	args := getArgs()
	args.Append("HINCRBY", key, field)
	args.AppendArgs(increment)
	return c.sendCommand(args)
}

// HIncrByFloat v2.6.0后可用
// 时间复杂度: O(1)
// 给hash指定字段添加指定的浮点值, 如果field不存在, 则在操作之前会将field的值置为0, 出现以下中的一种情况, 将会返回错误:
// 1. field包含错误类型的值(类型不为string)
// 2. 字段当前的值或者指定的增量无法被解析成双精度浮点数
// 返回值类型: Bulk String, 返回incr操作后字段的值
func (c *Client) HIncrByFloat(key string, field string, increment float64) (*Reply, error) {
	args := getArgs()
	args.Append("HINCRBYFLOAT", key, field)
	args.AppendArgs(increment)
	return c.sendCommand(args)
}

// HKeys v2.0.0后可用
// 时间复杂度: O(N), N为hash的大小
// 获取存储在key中的hash值的所有的字段
// 返回值类型: Array, 返回hash字段的列表, 如果key不存在则返回空列表
func (c *Client) HKeys(key string) (*Reply, error) {
	args := getArgs()
	args.Append("HKEYS", key)
	return c.sendCommand(args)
}

// HLen v2.0.0后可用
// 时间复杂度: O(1)
// 获取hash的字段数量
// 返回值类型: Integer, 返回key对应hash值的字段数量, 如果key不存在则返回0
func (c *Client) HLen(key string) (*Reply, error) {
	args := getArgs()
	args.Append("HLEN", key)
	return c.sendCommand(args)
}

// HMGet v2.0.0后可用
// 时间复杂度: O(N), N为请求的field数量
// 获取hash中指定字段的值, 对于每个指定但不存在的field, 将会返回一个nil
// 返回值类型: Array, 返回field值的列表, 返回顺序与请求顺序一致
func (c *Client) HMGet(key string, field ...string) (*Reply, error) {
	args := getArgs()
	args.Append("HMGET", key)
	args.Append(field...)
	return c.sendCommand(args)
}

// HMSet v2.0.0后可用, 从v4.0.0开始, 此命令被废弃, 被HSET取代
// 时间复杂度: O(N), N为设置键值对的数量
// 设置多个hash的<field, value>键值对, 对于已经存在的field字段, 其值将被覆盖, 如果key不存在, 将会创建一个key并赋值
// 返回值类型: Simple String
func (c *Client) HMSet(key string, fieldValue ...interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("HMSET", key)
	args.AppendArgs(fieldValue...)
	return c.sendCommand(args)
}

// HRandField v6.2.0后可用
// 时间复杂度: O(N), N为返回字段的数量
// 从hash中随机获取一个或者多个字段, 如果只提供key参数, 将会随机返回一个field
// 如果提供的count参数为正数, 将返回不同字段的一个数组, 数组长度为count或者key所有field的数量(如果count大于HLEN时)
// 如果提供的count参数为负数, 将会允许返回多个相同的字段, 并且此时返回的字段数为count的绝对值
// WITHVALUES 选项将会使回复包含对应字段的value
// 返回值类型:
// 1. Bulk String: 如果没有指定count参数, 则命令返回一个随机字段, 如果key不存在则返回nil
// 2. Array: 如果指定了count参数, 命令返回字段的数组, 如果key不存在则返回空的数组; 如果使用了WITHVALUES, 返回值将会是字段和值的数组
func (c *Client) HRandField(key string, count int64, withValues bool) (*Reply, error) {
	args := getArgs()
	args.Append("HRANDFIELD", key)
	if count != 0 {
		args.AppendArgs(count)
		if withValues {
			args.Append("WITHVALUES")
		}
	}
	return c.sendCommand(args)
}

// HScan v2.8.0后可用
// 时间复杂度: O(N), 每次调用O(1), O(N)用于完整的迭代，包括足够的命令调用以使光标返回0; N是集合内的元素数。
// 递增的遍历hash的字段以及对应的值
// 返回值类型: Array
func (c *Client) HScan(key string, cursor int, pattern string, count int64, valueType string) (*Reply, error) {
	args := getArgs()
	args.Append("HSCAN", key, strconv.FormatInt(int64(cursor), 10))
	if pattern != "" {
		args.Append("MATCH", pattern)
	}
	if count > 0 {
		args.AppendArgs("COUNT", count)
	}
	if valueType != "" {
		args.Append("TYPE", valueType)
	}
	return c.sendCommand(args)
}

// HSet v2.0.0后可用
// 时间复杂度: 如果只添加一对, 则是O(1), 否则为O(N), N为键值对的数量
// 设置hash的一对键值对, 如果字段已经存在, 将会被覆盖
// 返回值类型: Integer, 返回添加的键值对的数量
func (c *Client) HSet(key string, fieldValue ...interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("HSET", key)
	args.AppendArgs(fieldValue...)
	return c.sendCommand(args)
}

// HSetNX v2.0.0后可用
// 时间复杂度: O(1)
// 只有当字段不存在时才设置hash对应field的值
// 返回值类型: Integer, 如果设置成功返回1, 否则返回0
func (c *Client) HSetNX(key string, field string, value interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("HSETNX", key, field)
	args.AppendArgs(value)
	return c.sendCommand(args)
}

// HStrLen v3.2.0后可用
// 时间复杂度: O(1)
// 获取hash指定字段的值的长度
// 返回值类型: Integer, 返回key中field字段对应的值的长度
func (c *Client) HStrLen(key string, field string) (*Reply, error) {
	args := getArgs()
	args.Append("HSTRLEN", key, field)
	return c.sendCommand(args)
}

// HVals v2.0.0后可用
// 时间复杂度: O(N)
// 获取hash所有字段对应的值
// 返回值类型: Array, 返回key对应所有字段值的列表
func (c *Client) HVals(key string) (*Reply, error) {
	args := getArgs()
	args.Append("HVALS", key)
	return c.sendCommand(args)
}
