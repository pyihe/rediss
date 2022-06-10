package rediss

import (
	"strconv"

	"github.com/pyihe/rediss/args"
	"github.com/pyihe/rediss/model/hash"
	"github.com/pyihe/rediss/pool"
)

// HDel v2.0.0后可用
// 命令格式: HDEL key field [field ...]
// v2.4.0开始支持多个字段参数
// 时间复杂度: O(N), N为要删除的字段数
// 用于移除key存储的hash数据中的指定字段, 对于key中不存在的字段将会被忽略, 如果key不存在命令将会返回0
// 返回值类型: Integer, 返回实际被移除的字段数量, 不包含指定但不存在的字段
func (c *Client) HDel(key string, fields ...string) (int64, error) {
	cmd := args.Get()
	cmd.Append("HDEL", key)
	cmd.Append(fields...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// HExists v2.0.0后可用
// 命令格式: HEXISTS key field
// 时间复杂度: O(1)
// 获取key中指定字段是否存在
// 返回值类型: Integer, 如果存在返回1, 如果key不存在或者key不包含该字段则返回0
func (c *Client) HExists(key string, field string) (bool, error) {
	cmd := args.Get()
	cmd.Append("HEXISTS", key, field)
	cmdbytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdbytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// HGet v2.0.0后可用
// 命令格式: HGET key field
// 时间复杂度:O(1)
// 获取hash字段的值
// 返回值类型: Bulk String, key和field同时存在返回字段值, 否则返回nil
func (c *Client) HGet(key string, field string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("HGET", key, field)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// HGetAll v2.0.0后可用
// 命令格式: HGETALL key
// 时间复杂度: O(N), N为hash的大小
// 获取key对应hash的所有field和value
// 返回值类型: Array, <field, value>的列表, 或者空列表(如果key不存在)
func (c *Client) HGetAll(key string) (result hash.FieldValue, err error) {
	cmd := args.Get()
	cmd.Append("HGETALL", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}
	if err = reply.Error(); err != nil {
		return nil, err
	}

	result = hash.NewFieldValue()
	var fieldArray = reply.Array()
	for i := 0; i < len(fieldArray)-1; i += 2 {
		field := fieldArray[i].ValueString()
		value := fieldArray[i+1].Bytes()
		result.Set(field, value)
	}
	return
}

// HIncrBy v2.0.0后可用
// 命令格式: HINCRBY key field increment
// 时间复杂度: O(1)
// 给hash指定字段添加指定的增量, 如果field不存在, 则在操作之前会将field的值置为0
// 返回值类型: Integer, 返回增加操作后的值
func (c *Client) HIncrBy(key string, field string, increment int64) (int64, error) {
	cmd := args.Get()
	cmd.Append("HINCRBY", key, field)
	cmd.AppendArgs(increment)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// HIncrByFloat v2.6.0后可用
// 命令格式: HINCRBYFLOAT key field increment
// 时间复杂度: O(1)
// 给hash指定字段添加指定的浮点值, 如果field不存在, 则在操作之前会将field的值置为0, 出现以下中的一种情况, 将会返回错误:
// 1. field包含错误类型的值(类型不为string)
// 2. 字段当前的值或者指定的增量无法被解析成双精度浮点数
// 返回值类型: Bulk String, 返回incr操作后字段的值
func (c *Client) HIncrByFloat(key string, field string, increment float64) (float64, error) {
	cmd := args.Get()
	cmd.Append("HINCRBYFLOAT", key, field)
	cmd.AppendArgs(increment)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Float()
}

// HKeys v2.0.0后可用
// 命令格式: HKEYS key
// 时间复杂度: O(N), N为hash的大小
// 获取存储在key中的hash值的所有的字段
// 返回值类型: Array, 返回hash字段的列表, 如果key不存在则返回空列表
func (c *Client) HKeys(key string) (result []string, err error) {
	cmd := args.Get()
	cmd.Append("HKEYS", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}

	array := reply.Array()
	result = make([]string, 0, len(array))
	for _, v := range array {
		result = append(result, v.ValueString())
	}
	return
}

// HLen v2.0.0后可用
// 命令格式: HLEN key
// 时间复杂度: O(1)
// 获取hash的字段数量
// 返回值类型: Integer, 返回key对应hash值的字段数量, 如果key不存在则返回0
func (c *Client) HLen(key string) (int64, error) {
	cmd := args.Get()
	cmd.Append("HLEN", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// HMGet v2.0.0后可用
// 命令格式: HMGET key field [field ...]
// 时间复杂度: O(N), N为请求的field数量
// 获取hash中指定字段的值, 对于每个指定但不存在的field, 将会返回一个nil
// 返回值类型: Array, 返回field值的列表, 返回顺序与请求顺序一致
func (c *Client) HMGet(key string, field ...string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("HMGET", key)
	cmd.Append(field...)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}

// HMSet v2.0.0后可用, 从v4.0.0开始, 此命令被废弃, 被HSET取代
// 命令格式: HMSET key field value [ field value ...]
// 时间复杂度: O(N), N为设置键值对的数量
// 设置多个hash的<field, value>键值对, 对于已经存在的field字段, 其值将被覆盖, 如果key不存在, 将会创建一个key并赋值
// 返回值类型: Simple String
func (c *Client) HMSet(key string, fvs hash.FieldValue) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("HMSET", key)
	fvs.Range(func(key string, value interface{}) (breakOut bool) {
		cmd.Append(key)
		cmd.AppendArgs(value)
		return
	})
	cmdbytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdbytes)
}

// HRandField v6.2.0后可用
// 命令格式: HRANDFIELD key [ count [WITHVALUES]]
// 时间复杂度: O(N), N为返回字段的数量
// 从hash中随机获取一个或者多个字段, 如果只提供key参数, 将会随机返回一个field
// 如果提供的count参数为正数, 将返回不同字段的一个数组, 数组长度为count或者key所有field的数量(如果count大于HLEN时)
// 如果提供的count参数为负数, 将会允许返回多个相同的字段, 并且此时返回的字段数为count的绝对值
// WITHVALUES 选项将会使回复包含对应字段的value
// 返回值类型:
// 1. Bulk String: 如果没有指定count参数, 则命令返回一个随机字段, 如果key不存在则返回nil
// 2. Array: 如果指定了count参数, 命令返回字段的数组, 如果key不存在则返回空的数组; 如果使用了WITHVALUES, 返回值将会是字段和值的数组
func (c *Client) HRandField(key string, count int64, withValues bool) (result hash.FieldValue, err error) {
	cmd := args.Get()
	cmd.Append("HRANDFIELD", key)
	if count != 0 {
		cmd.AppendArgs(count)
		if withValues {
			cmd.Append("WITHVALUES")
		}
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return nil, err
	}
	if err = reply.Error(); err != nil {
		return nil, err
	}

	result = hash.NewFieldValue()
	switch count {
	case 0:
		result.Set(reply.ValueString(), nil)
	default:
		array := reply.Array()
		if withValues {
			for i := 0; i < len(array)-1; i += 2 {
				field := array[i].ValueString()
				value := array[i+1].Bytes()
				result.Set(field, value)
			}
		} else {
			for _, k := range array {
				result.Set(k.ValueString(), nil)
			}
		}
	}
	return
}

// HScan v2.8.0后可用
// 命令格式: HSCAN key cursor [MATCH pattern] [COUNT count]
// 时间复杂度: O(N), 每次调用O(1), O(N)用于完整的迭代，包括足够的命令调用以使光标返回0; N是集合内的元素数。
// 递增的遍历hash的字段以及对应的值
// 返回值类型: Array, 数组元素为包含两个元素, 字段和字段值
func (c *Client) HScan(key string, cursor int, pattern string, count int64) (result *hash.ScanResult, err error) {
	cmd := args.Get()
	cmd.Append("HSCAN", key, strconv.FormatInt(int64(cursor), 10))
	if pattern != "" {
		cmd.Append("MATCH", pattern)
	}
	if count > 0 {
		cmd.AppendArgs("COUNT", count)
	}
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return
	}
	if err = reply.Error(); err != nil {
		return
	}

	var array = reply.Array()
	if len(array) != 2 {
		return
	}
	var fvArray = array[1].Array()
	result = &hash.ScanResult{FieldValues: hash.NewFieldValue()}
	result.Cursor, _ = array[0].Integer()
	for i := 0; i < len(fvArray)-1; i += 2 {
		field := fvArray[i].ValueString()
		value := fvArray[i+1].ValueString()
		result.FieldValues.Set(field, value)
	}
	return
}

// HSet v2.0.0后可用
// 命令格式: HSET key field value [ field value ...]
// 时间复杂度: 如果只添加一对, 则是O(1), 否则为O(N), N为键值对的数量
// 设置hash的一对键值对, 如果字段已经存在, 将会被覆盖
// 返回值类型: Integer, 返回添加的键值对的数量
func (c *Client) HSet(key string, fvs hash.FieldValue) (int64, error) {
	cmd := args.Get()
	cmd.Append("HSET", key)
	fvs.Range(func(key string, value interface{}) (breakOut bool) {
		cmd.Append(key)
		cmd.AppendArgs(value)
		return
	})
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// HSetNX v2.0.0后可用
// 命令格式: HSETNX key field value
// 时间复杂度: O(1)
// 只有当字段不存在时才设置hash对应field的值
// 返回值类型: Integer, 如果设置成功返回1, 否则返回0
func (c *Client) HSetNX(key string, field string, value interface{}) (bool, error) {
	cmd := args.Get()
	cmd.Append("HSETNX", key, field)
	cmd.AppendArgs(value)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return false, err
	}
	return reply.Bool()
}

// HStrLen v3.2.0后可用
// 命令格式: HSTRLEN key field
// 时间复杂度: O(1)
// 获取hash指定字段的值的长度
// 返回值类型: Integer, 返回key中field字段对应的值的长度
func (c *Client) HStrLen(key string, field string) (int64, error) {
	cmd := args.Get()
	cmd.Append("HSTRLEN", key, field)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)

	reply, err := c.sendCommand(cmdBytes)
	if err != nil {
		return 0, err
	}
	return reply.Integer()
}

// HVals v2.0.0后可用
// 命令格式: HVALS key
// 时间复杂度: O(N)
// 获取hash所有字段对应的值
// 返回值类型: Array, 返回key对应所有字段值的列表
func (c *Client) HVals(key string) (*pool.Reply, error) {
	cmd := args.Get()
	cmd.Append("HVALS", key)
	cmdBytes := cmd.Bytes()
	args.Put(cmd)
	return c.sendCommand(cmdBytes)
}
