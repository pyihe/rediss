package rediss

import "strings"

// Append v2.0.0后可用
// 时间复杂度: O(1)
// 如果key已经存在并且是字符串类型, APPEND会将value追加到key对应值的最后;
// 如果key不存在, APPEND将会创建该key并初始化为空字符串，最后将value追加到key的最后
// 返回值类型: Integer, 返回append操作后的字符串长度
func (c *Client) Append(key string, value interface{}) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("APPEND", key, value)
	return c.sendCommand(args)
}

// Decr v1.0.0后可用
// 时间复杂度: O(1)
// 将key存储的值减1, 如果key不存在, 将会在执行操作前将key的值置为0
// 如果key包含错误的值类型或者包含无法表示为整型的字符串, 将会返回错误
// 本操作仅限于64位有符号整数
// 返回值类型: Integer, 返回递减后的值
func (c *Client) Decr(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("DECR", key)
	return c.sendCommand(args)
}

// DecrBy v1.0.0后可用
// 时间复杂度: O(1)
// 将key存储的值减去decrement, 如果key不存在, 将会在执行操作前将key的值置为0
// 如果key包含错误的值类型或者包含无法表示为整型的字符串, 将会返回错误
// 本操作仅限于64位有符号整数
// 返回值类型: Integer, 返回递减后的值
func (c *Client) DecrBy(key string, decrement int64) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("DECRBY", key, decrement)
	return c.sendCommand(args)
}

// Get v1.0.0后可用
// 时间复杂度: O(1)
// 获取key存储的值, 如果key不存在则返回nil
// Get只用于操作字符串类型的key, 如果key存储的不是字符串类型, 则返回错误
// 返回值类型: Bulk String或者nil(key不存在时)
func (c *Client) Get(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("GET", key)
	return c.sendCommand(args)
}

// GetDel v6.2.0后可用
// 时间复杂度: O(1)
// 获取key的值后将key删除, 只能操作于字符串类型的key
// 返回值类型: Bulk String或者nil(key不存在时)
func (c *Client) GetDel(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("GETDEL", key)
	return c.sendCommand(args)
}

// GetEx v6.2.0后可用
// 时间复杂度: O(1)
// 获取key的值, 然后可选的设置key的过期时间
// 命令参数:
// EX: 指定有效期(单位: 秒)
// PX: 指定有效期(单位: 毫秒)
// EXAT: 指定过期时间点(以秒为单位的时间戳)
// PXAT: 指定过期时间点(以毫秒为单位的时间戳)
// PERSIST: 移除key的有效期, 使其长期有效
// 返回值类型: Bulk String或者nil(key不存在时)
func (c *Client) GetEx(key string, op string, opValue int64) (*Reply, error) {
	args := getArgs()
	args.Append("GETEX", key)
	switch strings.ToUpper(op) {
	case "EX", "PX", "EXAT", "PXAT":
		args.AppendArgs(op, opValue)
	case "PERSIST":
		args.Append(op)
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}

// GetRange v2.4.0后可用
// 时间复杂度: O(N), N为返回的字符串长度; 因为从既有的字符串创建子串代价很小, 所以对于小字符串来说可以看作是O(1)
// 获取key的值的子串, start和end可以为负, 表示从字符串末尾开始, -1表示最后一个字符, -2表示倒数第二个字符
// 返回值类型: Bulk String
func (c *Client) GetRange(key string, start, end int64) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("GETRANGE", key, start, end)
	return c.sendCommand(args)
}

// GetSet v6.2.0前可用
// 时间复杂度: O(1)
// 获取key存储的旧值, 并存储新值value
// 返回值类型: Bulk String, 返回key存储的旧值或者nil(key不存在时)
// v6.2.0后可用带有GET参数的SET命令替代GETSET
func (c *Client) GetSet(key string, value interface{}) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("GETSET", key, value)
	return c.sendCommand(args)
}

// Incr v1.0.0后可用
// 时间复杂度: O(1)
// 将存储在key中的值加1, 如果key不存在, 执行命令前将key的值置为0
// 如果key包含错误的数据类型或者包含不能转换为整型的字符串将会返回错误
// 本操作仅限于64位有符号整型
// Redis以整数表示形式存储整数，因此对于实际保存整数的字符串值, 存储整数的字符串表示形式没有开销
// 返回值类型: Integer, 返回加1后的key值
func (c *Client) Incr(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("INCR", key)
	return c.sendCommand(args)
}

// IncrBy v1.0.0后可用
// 时间复杂度: O(1)
// 将存储在key中的值增加increment, 如果key不存在, 执行命令前将key的值置为0
// 如果key包含错误的数据类型或者包含不能转换为整型的字符串将会返回错误
// 本操作仅限于64位有符号整型
// Redis以整数表示形式存储整数，因此对于实际保存整数的字符串值, 存储整数的字符串表示形式没有开销
// 返回值类型: Integer, 返回加1后的key值
func (c *Client) IncrBy(key string, increment int64) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("INCRBY", key, increment)
	return c.sendCommand(args)
}

// IncrByFloat v2.6.0后可用
// 时间复杂度: O(1)
// 给存储值为浮点类型的key值增加increment, 通过使用负数的增量来实现减法;
// 如果key不存在, 执行操作前会将key值置为0
// 一下情况之一将会返回错误:
// 1. key值类型不为string
// 2. key当前存储的值或者增量increment不能被解析为双精度浮点数
//
// 字符串键中已经包含的值和增量参数都可以选择以指数表示法提供，但是增量后计算的值始终以相同的格式存储,
// 即, 一个整数后跟（如果需要）一个点, 以及表示数字的小数部分的可变位数。始终删除尾随零
//
// 无论计算的实际内部精度如何, 输出的精度都固定在小数点后17位
// 返回值类型: Bulk String, 计算增量后的key值
func (c *Client) IncrByFloat(key string, increment float64) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("INCRBYFLOAT", key, increment)
	return c.sendCommand(args)
}

// LCS v7.0.0后可用
// 时间复杂度: O(N*M), N和M分别为s1和s2的长度
// LCS实现最长公共子序列算法, 这与最长公共字符串算法不同, 因为在字符串中匹配到的字符不需要是连续的
// 例如在字符串"foo"和"fao"中, LCS的结果为: "fo", LCS在检测两个字符串的相似
// LCS在评估两个字符串的相似程度是非常有用的, 字符串可以用其他事物替代: 如DNA序列、文本等等
// 因为时间复杂度的原因, 执行此命令时要么旋转一个不同的redis实例运行, 要么确保针对非常小的字符串运行该命令
// 返回结果类型:
// 1. 如果没有修饰符, 则返回表示最长公共子字符串的字符串
// 2. 当给定LEN参数时, 返回最长公共子字符串的长度
// 3. 当给定 IDX 时，该命令返回一个数组，其中包含 LCS 长度和两个字符串中的所有范围、每个字符串的开始和结束偏移量，其中有匹配项。
//    当给定 WITHMATCHLEN 时，表示匹配的每个数组也将具有匹配的长度
func (c *Client) LCS(key1, key2 string, LEN, IDX, withMatchLen bool, minMatchLen int64) (*Reply, error) {
	args := getArgs()
	args.Append("LCS", key1, key2)
	if LEN {
		args.Append("LEN")
	}
	if IDX {
		args.Append("IDX")
	}
	if minMatchLen > 0 {
		args.AppendArgs("MINMATCHLEN", minMatchLen)
	}
	if withMatchLen {
		args.Append("WITHMATCHLEN")
	}
	return c.sendCommand(args)
}

// MGet v1.0.0后可用
// 时间复杂度: O(N)
// 返回指定keys的值, 对于每个不存在或者值类型不为字符串的key, 该key的结果将会返回nil
// 返回值类型: Array
func (c *Client) MGet(keys ...string) (*Reply, error) {
	args := getArgs()
	args.Append("MGET")
	args.Append(keys...)
	return c.sendCommand(args)
}

// MSet v1.0.1后可用
// 时间复杂度: O(N), N是需要Set的key数量
// 将给定的key设置为指定的值, 对于已经存在的key, 将会用新值替换旧值
// MSET命令是原子操作, 所以将不会存在一些key设置成功, 而一些key确设置失败
// 返回值类型: Simple String, 总是OK, 因为MSET不会失败
func (c *Client) MSet(kvs ...interface{}) (*Reply, error) {
	if n := len(kvs); n == 0 || n%2 != 0 {
		return nil, ErrInvalidArgumentFormat
	}
	args := getArgs()
	args.Append("MSET")
	args.AppendArgs(kvs...)
	return c.sendCommand(args)
}

// MSetNX v1.0.1后也用
// 时间复杂度: O(N), N为需要设置的key的数量
// 与MSET不同的是, 执行MSETNX时只要一个key存在, 则命令不会执行任何操作, 即MSETNX成功的前提是需要设置的key全部都不存在
// 同样MSETNX也是原子操作
// 返回值类型: Integer, 如果所有key都被设置成功返回1; 否则返回0
func (c *Client) MSetNX(kvs ...interface{}) (*Reply, error) {
	if n := len(kvs); n == 0 || n%2 != 0 {
		return nil, ErrInvalidArgumentFormat
	}
	args := getArgs()
	args.Append("MSETNX")
	args.AppendArgs(kvs...)
	return c.sendCommand(args)
}

// PSetEX v2.6.0后可用
// 时间复杂度: O(1)
// 设置key的值为value, 同时设置key的有效期为milli毫秒
// 返回值类型: Simple String, Ok
func (c *Client) PSetEX(key string, value interface{}, milli int64) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("PSETEX", key, value, milli)
	return c.sendCommand(args)
}

// Set v1.0.0后可用
// 时间复杂度: O(1)
// 将key的值设置为value, 如果key早已有值了, 则忽略旧值并将其覆盖, 并且如果旧值存在有效期, 有效期也将被忽略
// SET命令支持一系列可选参数:
// EX: 以秒为单位设置key的有效期 (v2.6.12后可用)
// PX: 以毫秒为单位设置key的有效期 (v2.6.12后可用)
// EXAT: 将key的过期时间设置为以秒为单位的时间戳 (v6.2.0后可用)
// PXAT: 将key的过期时间设置为以毫秒为单位的时间戳  (v6.2.0后可用)
// NX: 当key不存在时才设置 (v2.6.12后可用)
// XX: 当key已经存在时才设置 (v2.6.12后可用)
// KEEPTTL: 如果key已经存在, 则保持既有的有效期 (v6.0.0后可用)
// GET: 设置key值的同时, 返回旧值(如果存在), 如果不存在旧值, 则返回nil, 如果旧值类型不是string, 则返回错误并终止SET命令的执行 (v6.2.0后可用)
// v7.0.0开始允许NX和GET选项一起使用, 之前只能单独使用
// 返回值类型:
// OK: 命令执行成功
// nil: NX或者XX条件不满足时
// Bulk String: 带有GET选项时, 返回旧值; 或者返回nil, key不存在
// 函数参数说明:
// op: [NX|XX]
// get: [true|false]
// expireOp: [EX|PX|EXAT|PXAT|KEEPTTL]
func (c *Client) Set(key string, value interface{}, op string, get bool, expireOp string, expireValue int64) (*Reply, error) {
	args := getArgs()
	args.Append("SET", key)
	args.AppendArgs(value)
	switch strings.ToUpper(op) {
	case "NX", "XX":
		args.Append(op)
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if get {
		args.Append("GET")
	}
	switch strings.ToUpper(expireOp) {
	case "EX", "PX", "EXAT", "PXAT":
		args.AppendArgs(expireOp, expireValue)
	case "KEEPTTL":
		args.Append("KEEPTTL")
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}

// SetEX v2.0.0后可用
// 时间复杂度: O(1)
// 设置key的同时设置其有效期, 单位为秒, SETEX命令是原子操作, 如果有效期不可用时将返回错误
// 返回值类型: Simple String
func (c *Client) SetEX(key string, value interface{}, sec int64) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("SETEX", key, value, sec)
	return c.sendCommand(args)
}

// SetNX v1.0.0后可用
// 时间复杂度: O(1)
// 将key值设置为value当且仅当key不存在时, key如果存在则不做任何操作
// 返回值类型: Integer
func (c *Client) SetNX(key string, value interface{}) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("SETNX", key, value)
	return c.sendCommand(args)
}

// SetRange v2.2.0后可用
// 时间复杂度: 当value是小字符串时为O(1), 否则为O(M), M为value的长度
// 从offset开始到字符串结尾覆盖key存储的字符串
// 返回值类型: Integer, 返回操作后的字符串长度
func (c *Client) SetRange(key string, offset int64, value interface{}) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("SETRANGE", key, offset, value)
	return c.sendCommand(args)
}

// StrLen v2.2.0后可用
// 时间复杂度: O(1)
// 返回key对应的字符串的长度, 如果key值不是字符串则返回错误
// 返回值类型: Integer, 返回字符串长度或者0(当key不存在时)
func (c *Client) StrLen(key string) (*Reply, error) {
	args := getArgs()
	args.AppendArgs("STRLEN", key)
	return c.sendCommand(args)
}
