package rediss

import (
	"strconv"
	"strings"
)

// Del v1.0.0后可用
// 命令格式: DEL key [key ...]
// 时间复杂度: O(N), N为被移除的key的数量; 当key存储的数据类型不是string时, 复杂度为O(M), M为key所存储数据类型的元素个数
// 移除一个存储string类型的key的复杂度为O(1)
// 移除指定的key, 当key不存在时, 将会被忽略
// 返回值类型: Integer, 返回被移除的key的数量
func (c *Client) Del(keys ...string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("DEL")
	args.Append(keys...)
	return c.sendCommand(args)
}

// Dump v2.6.0后可用
// 命令格式: DUMP key
// 时间复杂度: O(1)为找到key的复杂度, 序列化的复杂度为O(NM), N为组成值的redis对象的数量, M为对象的平均大小
// 以Redis特定的格式序列化存储在key中的值并将其返回给用户, 可以使用RESTORE命令将返回的值合成回Redis键
// 序列化格式是不透明且非标准的, 但它具有一些语义特征:
// 1. 它包含一个64位校验和, 用于确保检测到错误; RESTORE命令确保在使用序列化值合成密钥之前检查校验和
// 2. 值被序列化的格式与RDB使用的相同
// 3. 一个RDB版本被编码在序列化值内部, 因此RDB格式不兼容的不同Redis版本将拒绝处理序列化值
// 序列化值不包含过期信息; 为了捕获当前值的生存时间, 应使用PTTL命令
// 如果key不存在, 返回nil。否则返回序列化之后的值
// 返回值类型: Bulk String, 序列化值
func (c *Client) Dump(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("DUMP", key)
	reply, err := c.sendCommand(args)
	if reply == nil && err == nil {
		err = ErrKeyNotExists
	}
	return reply, err
}

// Exists v1.0.0后可用
// 命令格式: EXISTS key [key ...]
// 时间复杂度: O(N), N为key的数量
// 判断key是否存在
// 用户应该知道, 如果在参数中多次提到相同的现有key, 它将被计算多次; 所以如果somekey存在, EXISTS somekey somekey将返回2
// 返回值类型: Integer, 返回提供的key中存在的数量
func (c *Client) Exists(keys ...string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("EXISTS")
	args.Append(keys...)
	return c.sendCommand(args)
}

// Expire v1.0.0后可用
// 命令格式: EXPIRE key seconds [ NX | XX | GT | LT]
// 时间复杂度: O(1)
// 设置key的有效期时间, 单位为秒
// 超时时间只会被删除或者覆盖的命令清除, 比如DEL, SET, GETSET等
// 所有从概念上改变存储在键中的值而不用新值替换它的操作将使超时保持不变, 比如LPUSH, INCR等
// 使用PERSIST命令同样将会使timeout被清除
// key被重命名时, timeout将会转移到新的key中
// EXPIRE/PEXPIRE设置负数的timeout以及EXPIREAT/PEXPIREAT将会使key被删除而不是过期
// 可选项:
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 对于已经设置了timeout的key, EXPIRE将会返回0并且不会更改既有的timeout
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// v7.0.0开始支持NX, XX, GT, LT选项
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) Expire(key string, sec int64, op string) (*Reply, error) {
	args := getArgs()
	args.Append("EXPIRE", key)
	args.AppendArgs(sec)
	switch strings.ToUpper(op) {
	case "NX", "XX", "GT", "LT":
		args.Append(op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}

	return c.sendCommand(args)
}

// ExpireAt v1.2.0后可用
// 命令格式: EXPIREAT key unix-time-seconds [ NX | XX | GT | LT]
// 时间复杂度: O(1)
// EXPIREAT与EXPIRE一样, 不同的是EXPIREAT设置的是时间戳
// 选项:
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// v7.0.0开始支持NX, XX, GT, LT选项
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) ExpireAt(key string, unix int64, op string) (*Reply, error) {
	args := getArgs()
	args.Append("PEXPIREAT", key)
	args.AppendArgs(unix)
	switch strings.ToUpper(op) {
	case "NX", "XX", "GT", "LT":
		args.Append(op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}

// ExpireTime v7.0.0后可用
// 命令格式: EXPIRETIME key
// 时间复杂度: O(1)
// 返回给定key的过期时间戳, 格式为时间戳, 精确到秒
// 返回值类型: Integer, 返回key过期的时间戳, 负数标识错误:
// 1. key存在但没有设置过期时间时返回-1
// 2. key不存在返回-2
func (c *Client) ExpireTime(key string) (*Reply, error) {
	args := getArgs()
	args.Append("EXPIRETIME", key)
	return c.sendCommand(args)
}

// PExpire v2.6.0后可用
// 命令格式: PEXPIRE key milliseconds [ NX | XX | GT | LT]
// 时间复杂度: O(1)
// 设置key的过期时间, 单位为毫秒
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// v7.0.0开始支持NX, XX, GT, LT选项
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) PExpire(key string, millSec int64, op string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("PEXPIRE", key)
	args.AppendArgs(millSec)
	switch strings.ToUpper(op) {
	case "NX", "XX", "GT", "LT":
		args.Append(op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}

// PExpireAt v2.6.0后可用
// 命令格式: PEXPIREAT key unix-time-milliseconds [ NX | XX | GT | LT]
// 时间复杂度: O(1)
// 设置key的过期时间点, 时间点单位为毫秒
// NX: 只有当key没有过期时才设置过期时间
// XX: 仅当key已过期时才设置过期时间
// GT: 仅在新到期时间大于当前到期时间时设置到期时间
// LT: 仅在新到期时间小于当前到期时设置到期
// 返回值类型: Integer, 如果设置成功返回1, 设置失败返回0(比如key不存在, 因为参数而跳过操作)
// v7.0.0开始支持NX, XX, GT, LT选项
// 函数参数说明:
// op: [NX|XX|GT|LT]
func (c *Client) PExpireAt(key string, millUnix int64, op string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("PEXPIREAT", key)
	args.AppendArgs(millUnix)
	switch strings.ToUpper(op) {
	case "NX", "XX", "GT", "LT":
		args.Append(op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}

// PExpireTime v7.0.0后可用
// 命令格式: PEXPIRETIME key
// 时间复杂度: O(1)
// 获取key过期时间戳, 精确到毫秒
// 返回值类型: 返回key过期的时间戳, 负数标识错误:
// 1. key存在但没有设置过期时间时返回-1
// 2. key不存在返回-2
func (c *Client) PExpireTime(key string) (*Reply, error) {
	args := getArgs()
	args.Append("PEXPIRETIME", key)
	return c.sendCommand(args)
}

// Keys v1.0.0后可用
// 命令格式: KEYS pattern
// 时间复杂度: O(N), N为数据库中的key数量
// 返回所有符合给定模式pattern的key
// 返回值类型: Array, 匹配到的key的数组
func (c *Client) Keys(pattern string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("KEYS", pattern)
	return c.sendCommand(args)
}

// Move v1.0.0后可用
// 命令格式: MOVE key db
// 时间复杂度: O(1)
//用于将当前数据库指定的key移动到给定的数据库中
// 返回值类型: Integer, 成功返回1, 失败返回0
func (c *Client) Move(key string, targetDB int) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("MOVE", key, targetDB)
	return c.sendCommand(args)
}

// Persist v2.2.0后可用
// 命令格式: PERSIST key
// 时间复杂度: O(1)
// 移除key的过期时间, key将永久保持
// 返回值类型: Integer, 移除成功返回1, key不存在或者key没有过期时间, 返回0
func (c *Client) Persist(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("PERSIST", key)
	return c.sendCommand(args)
}

// PTTL v2.6.0后可用
// 命令格式: PTTL key
// 时间复杂度: O(1)
// 以毫秒为单位返回key剩余的过期时间
// 在2.6版本以前, 如果key不存在或者key存在但没有过期时间时都返回-1
// 2.6版本后, 如果key不存在返回-1, 如果key存在但没有过期时间时返回-2
// 返回值类型: Integer, key不存在时返回-2, key存在但没有过期时间时返回-1, 否则以毫秒为单位返回剩余过期时间
func (c *Client) PTTL(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("PTTL", key)
	return c.sendCommand(args)
}

// TTL v1.0.0后可用
// 命令格式: TTL key
// 时间复杂度: O(1)
// 以秒为单位返回key剩余的过期时间
// 返回值类型: Integer, key不存在时返回-2, key存在但没有过期时间时返回-1, 否则以秒为单位返回剩余过期时间
func (c *Client) TTL(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("TTL", key)
	return c.sendCommand(args)
}

//
// RandomKey v1.0.0后可用
// 命令格式: RANDOMKEY
// 时间复杂度: O(1)
// 从当前数据库中随机返回一个key
// 返回值类型: Bulk String, 如果数据库没有key, 返回nil, 否则随机返回一个key
func (c *Client) RandomKey() (*Reply, error) {
	args := getArgs()
	args.AppendArgs("RANDOMKEY")
	return c.sendCommand(args)
}

// Rename v1.0.0后可用
// 命令格式: RENAME key newkey
// 时间复杂度: O(1)
// 修改key的名称为newKey
// 当newKey已经存在时, 其值将会被覆盖
// 返回值类型: Simple String, 修改成功时返回OK, 失败返回错误
// v3.2.0后, 如果key和newKey相同不再返回错误
func (c *Client) Rename(key, newKey string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("RENAME", key, newKey)
	return c.sendCommand(args)
}

// RenameNX v1.0.0后可用
// 命令格式: RENAMENX key newkey
// 时间复杂度: O(1)
// 仅当newKey不存在时将key改名为newKey
// 返回值类型: 修改成功返回1, 如果newKey已经存在返回0
// v3.2.0后, 如果key和newKey相同不再返回错误
func (c *Client) RenameNX(key, newKey string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("RENAMENX", key, newKey)
	return c.sendCommand(args)
}

// Scan v2.8.0后可用
// 命令格式: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
// v6.0.0后添加TYPE参数
// 时间复杂度: O(N), N为scan的元素数量
// 用于迭代数据库中的key
// SCAN 命令是一个基于游标的迭代器, 每次被调用之后, 都会向用户返回一个新的游标, 用户在下次迭代时需要使用这个新游标作为SCAN命令的游标参数, 以此来延续之前的迭代过程。
// SCAN返回一个包含两个元素的数组, 第一个元素是用于进行下一次迭代的新游标, 而第二个元素则是一个数组, 这个数组中包含了所有被迭代的元素。
// 如果新游标返回0表示迭代已结束。
// 命令格式: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
// COUNT: 每次最多迭代多少个元素
// MATCH: 只迭代给定样式的元素
// TYPE: 遍历的值类型, 如ZSET, GEOHASH
// 返回值类型: Array, 返回遍历的key的数组
func (c *Client) Scan(cursor int, pattern string, count int64, valueType string) (*Reply, error) {
	args := getArgs()
	args.Append("SCAN", strconv.FormatInt(int64(cursor), 10))
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

// Type v1.0.0后可用
// 命令格式: TYPE key
// 返回key所存储的值的类型
// 返回值类型:
// none: key不存在
// string: 字符串
// list: 列表
// set: 集合
// zset: 有序集
// hash: 哈希表
func (c *Client) Type(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("TYPE", key)
	return c.sendCommand(args)
}
