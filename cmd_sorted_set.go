package rediss

import "strings"

// BZMPop v7.0.0后可用
// 命令格式: BZMPOP timeout numkeys key [key ...] MIN | MAX [COUNT count]
// 时间复杂度: O(K)+O(N*log(M)) K为key的数量, N为有序集合中存储的成员数量, M为pop的元素数量
// 当任何一个有序集合不为空时, BZMPOP与ZMPOP命令相同, 当所有的有序集合都为空时,
// redis将会阻塞连接直到另一个客户端添加了成员到任意一个有序集合中
// 或者redis将会阻塞连接直到timeout(双精度浮点型, 表示阻塞最大时长)超时, 零值的timeout表示阻塞时间为永久
// 返回值类型: Array, 如果没有成员可以pop时返回nil,
// 否则将返回双元素数组, 第一个元素是有序集合的名称, 第二个元素是一个数组, 为从该有序集合中pop出的元素组成,
// 每个元素同样也是一个包含成员以及它的分数的数组
func (c *Client) BZMPop(timeout float64, keys []string, op string, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("BZMPOP")
	args.AppendArgs(timeout, len(keys))
	args.Append(keys...)
	switch strings.ToUpper(op) {
	case "MIN", "MAX":
		args.Append(op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if count > 1 {
		args.AppendArgs("COUNT", count)
	}
	return c.sendCommand(args)
}

// BZPopMax v5.0.0后可用
// 命令格式: BZPOPMAX key [key ...] timeout
// 时间复杂度: O(log(N)), N为有序集合中的成员数量
// 阻塞式的按照给定顺序从提供的有序集合中第一个非空的集合pop出分数最高的那个成员
// 当所有有序集合都为空时, 命令将会阻塞, 直到有元素可以pop或者timeout超时, timeout表示阻塞的最长时间, 零值的timeout表示永久阻塞
// 返回值类型: Array, 没有元素可以pop或者超时时, 返回nil
// 三元素, 第一个元素为pop元素的有序集合名字, 第二个元素为它本身, 第三个元素为被pop元素的分数
// v6.0.0后开始timeout由整型变为双精度浮点型
func (c *Client) BZPopMax(keys []string, timeout float64) (*Reply, error) {
	args := getArgs()
	args.Append("BZPOPMAX")
	args.Append(keys...)
	args.AppendArgs(timeout)
	return c.sendCommand(args)
}

// BZPopMin v5.0.0后可用
// 命令格式: BZPOPMIN key [key ...] timeout
// 时间复杂度: O(log(N)), N为有序集合中的成员数量
// 阻塞式的按照给定顺序从提供的有序集合中第一个非空的集合pop出分数最低的那个成员
// 当所有有序集合都为空时, 命令将会阻塞, 直到有元素可以pop或者timeout超时, timeout表示阻塞的最长时间, 零值的timeout表示永久阻塞
// 返回值类型: Array, 没有元素可以pop或者超时时, 返回nil
// 三元素, 第一个元素为pop元素的有序集合名字, 第二个元素为它本身, 第三个元素为被pop元素的分数
// v6.0.0后开始timeout由整型变为双精度浮点型
func (c *Client) BZPopMin(keys []string, timeout float64) (*Reply, error) {
	args := getArgs()
	args.Append("BZPOPMIN")
	args.Append(keys...)
	args.AppendArgs(timeout)
	return c.sendCommand(args)
}

// ZAdd v1.2.0后可用
// 命令格式: ZADD key [ NX | XX] [ GT | LT] [CH] [INCR] score member [ score member ...]
// 时间复杂度: O(log(N)), N为有序集合中的元素数量
// 向指定有序集合中添加指定的成员
// 如果成员早已存在于有序集合中, 成员的分数将被更新并且重新插入在正确的位置上
// 如果key不存在, 则创建一个以指定成员为唯一成员的新排序集，就像排序集为空一样
// 如果key所持有的数据类型不为有序集合, 将会返回错误
// 分数为双精度浮点型
// 返回值类型: Integer,
// 当没有提供可选参数时, 返回新增元素的个数
// 如果指定了CH参数, 返回新增以及更新的元素个数
// 如果提供了INCR参数, 返回值将会变为Bulk String, 返回成员的新分数(双精度浮点数)表示为字符串，如果操作被中止（当使用 XX 或 NX 选项调用时），则为 nil
//
// 函数参数说明:
// key: 指定有序集合的名字
// xOp: [XX|NX], XX表示如果元素存在则只更新分数; NX表示只当元素不存在时才添加, 已经存在则不进行操作
// tOp: [LT|GT], LT表示对于已经存在的元素, 只当插入的新分数小于旧值分数时才更新, GT表示只当大于旧值分数时才更新
// ch: 将返回值从添加的新元素的数量修改为改变的元素总数（CH是changed的缩写）。更改的元素是添加的新元素和已更新分数的元素。因此，命令行中指定的与过去得分相同的元素不计算在内。注意：通常 ZADD 的返回值只计算添加的新元素的数量
// incr: 指定此选项时，ZADD 的作用类似于 ZINCRBY。在此模式下只能指定一个分数元素对
func (c *Client) ZAdd(key string, xOp string, tOp string, ch, incr bool, scoreMemPair ...interface{}) (*Reply, error) {
	if n := len(scoreMemPair); n == 0 || n%2 != 0 {
		return nil, ErrInvalidArgumentFormat
	}
	args := getArgs()
	args.Append("ZADD", key)
	switch strings.ToUpper(xOp) {
	case "NX", "XX":
		args.Append(xOp)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	switch strings.ToUpper(tOp) {
	case "GT", "LT":
		args.Append(tOp)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if ch {
		args.Append("CH")
	}
	if incr {
		args.Append("INCR")
	}
	args.AppendArgs(scoreMemPair...)
	return c.sendCommand(args)
}

// ZCard v1.2.0后可用
// 命令格式: ZCARD key
// 时间复杂度: O(1)
// 获取指定key的有序集合的基数
// 返回值类型: Integer, 返回有序集合的基数, 如果key不存在返回0
func (c *Client) ZCard(key string) (*Reply, error) {
	args := getArgs()
	args.Append("ZCARD", key)
	return c.sendCommand(args)
}

// ZCount v2.0.0后可用
// 命令格式: ZCOUNT key min max
// 时间复杂度: O(log(N)), N为有序集合中元素的个数
// 获取有序集合中分数在[min, max]之间的元素个数
// 返回值类型: Integer, 返回在指定分数范围内的元素个数
func (c *Client) ZCount(key string, min, max int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZCOUNT", key)
	args.AppendArgs(min, max)
	return c.sendCommand(args)
}

// ZDiff v6.2.0后可用
// 命令格式: ZDIFF numkeys key [key ...] [WITHSCORES]
// 时间复杂度: O(L + (N-K)log(N)), L为所有有序集合的成员总数, N为第一个有序集合的成员数量, K为结果集合的成员数量
// 获取指定有序集合之间的差异
// 返回值类型: Array, 返回差异成员(如果提供了WITHSCORES参数, 则会携带分数)
func (c *Client) ZDiff(keys []string, withScore bool) (*Reply, error) {
	args := getArgs()
	args.Append("ZDIFF")
	args.AppendArgs(len(keys))
	args.Append(keys...)
	if withScore {
		args.Append("WITHSCORES")
	}
	return c.sendCommand(args)
}

// ZDiffStore v6.2.0后可用
// 命令格式: ZDIFFSTORE destination numkeys key [key ...]
// 时间复杂度: O(L + (N-K)log(N)), L为所有有序集合的成员总数, N为第一个有序集合的成员数量, K为结果集合的成员数量
// 获取指定有序集合之间的差异, 并存储在指定的key中, 如果dst已经存在则其旧值将会被覆盖
// 返回值类型: Integer, 返回存储进dst的元素数量
func (c *Client) ZDiffStore(dst string, keys []string) (*Reply, error) {
	args := getArgs()
	args.Append("ZDIFFSTORE", dst)
	args.AppendArgs(len(keys))
	args.Append(keys...)
	return c.sendCommand(args)
}

// ZIncrBy v1.2.0后可用
// 命令格式: ZINCRBY key increment member
// 时间复杂度: O(log(N)), N为有序集合中的元素数量
// 对有序集合中的指定元素的分数进行增量, 如果元素不存在则会以增量为分数添加一个新元素, 如果key不存在, 则创建一个以指定成员为唯一成员的新排序集
// 如果key持有的数据类型不是有序集合, 将会返回一个错误
// 返回值类型: Bulk String, 返回增量后的分数
func (c *Client) ZIncrBy(key string, increment float64, member interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("ZINCRBY", key)
	args.AppendArgs(increment, member)
	return c.sendCommand(args)
}

// ZInter v6.2.0后可用
// 命令格式: ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] [WITHSCORES]
// 时间复杂度: O(NK)+O(Mlog(M)), N为最小有序集合的基数, K为指定的有序集合数, M为结果集合中的元素数量
// 计算有序集合的交集
// 默认情况下, 一个元素的结果分数是它在它所在的排序集中的分数的总和; 因为交集要求元素是每个给定有序集合的成员, 所以结果有序集合中每个元素的分数等于输入排序集的数量
// 使用WEIGHTS选项, 可以为每个输入排序集指定一个乘法因子, 即每个有序集合的每个元素的分数在传递给聚合函数之前都会乘以该因子; 当未给出WEIGHTS时, 乘法因子默认为1
// 使用AGGREGATE选项，可以指定联合结果的聚合方式, 此选项默认为SUM, 即并集中元素的分数为所有有序集合中的总和, 当此选项设置为MIN或MAX时, 结果集将包含元素在所有有序集合中的最小或最大分数
// 返回值类型: Array, 交集的结果(如果给出了WITHSCORES选项, 则可以选择它们的分数)
func (c *Client) ZInter(keys []string, weights []float64, op string, withScore bool) (*Reply, error) {
	args := getArgs()
	args.Append("ZINTER")
	args.AppendArgs(len(keys))
	args.Append(keys...)
	if len(weights) > 0 {
		args.AppendArgs("WEIGHTS")
		for _, w := range weights {
			args.AppendArgs(w)
		}
	}
	switch strings.ToUpper(op) {
	case "SUM", "MIN", "MAX":
		args.Append("AGGREGATE", op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if withScore {
		args.Append("WITHSCORES")
	}
	return c.sendCommand(args)
}

// ZInterStore v2.0.0后可用
// 命令格式: ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]
// 时间复杂度: O(NK)+O(Mlog(M)), N为最小有序集合的基数, K为指定的有序集合数, M为结果集合中的元素数量
// 计算有序集合的交集, 并将结果存储在目标中
// 默认情况下, 一个元素的结果分数是它在它所在的排序集中的分数的总和; 因为交集要求元素是每个给定有序集合的成员, 所以结果有序集合中每个元素的分数等于输入排序集的数量
// 使用WEIGHTS选项, 可以为每个输入排序集指定一个乘法因子, 即每个有序集合的每个元素的分数在传递给聚合函数之前都会乘以该因子; 当未给出WEIGHTS时, 乘法因子默认为1
// 使用AGGREGATE选项，可以指定联合结果的聚合方式, 此选项默认为SUM, 即并集中元素的分数为所有有序集合中的总和, 当此选项设置为MIN或MAX时, 结果集将包含元素在所有有序集合中的最小或最大分数
// 如果dst已经存在, 其旧值将被覆盖
// 返回值类型: Array, 交集的结果(如果给出了WITHSCORES选项, 则可以选择它们的分数)
func (c *Client) ZInterStore(dst string, keys []string, weights []float64, op string) (*Reply, error) {
	args := getArgs()
	args.Append("ZINTERSTORE", dst)
	args.AppendArgs(len(keys))
	args.Append(keys...)
	if len(weights) > 0 {
		args.AppendArgs("WEIGHTS")
		for _, w := range weights {
			args.AppendArgs(w)
		}
	}
	switch strings.ToUpper(op) {
	case "SUM", "MIN", "MAX":
		args.Append("AGGREGATE", op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}

// ZLexCount v2.8.9后可用
// 命令格式: ZLEXCOUNT key min max
// 时间复杂度: O(log(N)), N为有序集合中成员数量
// 当有序集合中所有元素的分数相同时, 为了强制字典序排序, 此命令返回介于min和max之间的元素个数
// min和max必须以(或者[开头, +和-分别表示正无穷和负无穷字符串, 所以"ZLEXCOUNT setName + -"表示返回有序集合的成员总数
// 返回值类型: Integer, 返回指定范围内元素总数
func (c *Client) ZLexCount(key string, min, max string) (*Reply, error) {
	args := getArgs()
	args.Append("ZLEXCOUNT", key, min, max)
	return c.sendCommand(args)
}

// ZMPop v7.0.0后可用
// 命令格式: ZMPOP numkeys key [key ...] MIN | MAX [COUNT count]
// 时间复杂度: O(K)+O(N*log(M)), K为提供的key的数量, N为有序集合的元素总数, M为pop的元素数量
// 从第一个非空的有序集合中pop一个或者多个元素(成员-分数对)
// 当指定MIN参数时, 弹出的元素是第一个非空排序集中得分最低的元素; MAX参数使得分最高的元素被弹出
// COUNT指定需要pop的元素数量, 默认为1
// 返回值类型: Array, 如果没有元素可以pop时返回nil; 有元素可以pop时返回一个双元素的数组, 第一个元素为有序集合名称, 第二个元素为成员-分数的数组
func (c *Client) ZMPop(keys []string, op string, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZMPOP")
	args.AppendArgs(len(keys))
	args.Append(keys...)
	switch strings.ToUpper(op) {
	case "MIN", "MAX":
		args.Append(op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if count > 1 {
		args.AppendArgs("COUNT", count)
	}
	return c.sendCommand(args)
}

// ZMScore v6.2.0后可用
// 命令格式: ZMSCORE key member [member ...]
// 时间复杂度: O(N), N为members的数量
// 获取有序集合key中指定成员的分数
// 对于每个不存在的成员, 返回nil
// 返回值类型: Array, 与成员关联的分数或者nil列表
func (c *Client) ZMScore(key string, members ...interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("ZMSCORE", key)
	args.AppendArgs(members...)
	return c.sendCommand(args)
}

// ZPopMax v5.0.0后可用
// 命令格式: ZPOPMAX key [count]
// 时间复杂度: O(log(N)*M), N为有序集合中的元素数量, M为被pop的元素数量
// 从指定的有序集合中移除并且返回count个分数最高的成员
// 如果没有指定count, count默认值为1; 如果count值大于有序集合的基数将不会产生错误, 当返回多个元素时, 将会根据分数由高到低的pop
// 返回值类型: Array, 返回成员和分数的数组
func (c *Client) ZPopMax(key string, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZPOPMAX", key)
	if count > 1 {
		args.AppendArgs(count)
	}
	return c.sendCommand(args)
}

// ZPopMin v5.0.0后可用
// 命令格式: ZPOPMIN key [count]
// 时间复杂度: O(log(N)*M), N为有序集合中的元素数量, M为被pop的元素数量
// 与ZPOPMAX相似, 不同之处在于ZPOPMIN返回的是分数由低到高的count个元素
// 返回值类型: Array, 返回成员和分数的数组
func (c *Client) ZPopMin(key string, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZPOPMIN", key)
	if count > 1 {
		args.AppendArgs(count)
	}
	return c.sendCommand(args)
}

// ZRandMember v6.2.0后可用
// 命令格式: ZRANDMEMBER key [ count [WITHSCORES]]
// 时间复杂度: O(N), N为返回的元素数量
// 如果不指定count和WITHSCORES参数, 将从有序集合中随机返回一个元素
// count > 0: 返回去重后的元素数组, 数组长度为count和集合基数的最小值
// count < 0: 返回没去重的元素数组, 数组长度为count的绝对值
// 如果指定了WITHSCORE参数, 将会同时返回每个元素的分数
// 返回值类型:
// 1. Bulk String: 没有指定count参数时将会返回被选中的元素, 如果key不存在返回nil
// 2. Array: 如果指定了count, 将会返回被选中元素的数组, key不存在时返回空数组; 如果使用了WITHSCORES, 数组将带有分数
func (c *Client) ZRandMember(key string, count int64, withScore bool) (*Reply, error) {
	args := getArgs()
	args.Append("ZRANDMEMBER", key)
	if count != 0 {
		args.AppendArgs(count)
		if withScore {
			args.Append("WITHSCORES")
		}
	}
	return c.sendCommand(args)
}

// ZRange v1.2.0后可用
// 命令格式: ZRANGE key min max [ BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
// v6.2.0开始支持REV、BYSCORE、BYLEX、LIMIT参数
// 时间复杂度: O(log(N)+M), N为有序集合的元素数量, M为返回的元素个数
// 返回指定有序集合中指定范围内的元素
// ZRANGE可以执行不同类型的范围查询: 按索引、按分数或者按字典序
// 从v6.2.0开始, ZRANGE可以替代一下命令:  ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX以及ZREVRANGEBYLEX
// 结果集默认顺序为分数由低到高, 相同分数的按照字典序, REV选项将会反转顺序: 分数由高到低, 同时字典序也将反转
// LIMIT参数可以用来从匹配到的元素中获取一个子范围内的元素(类似于 SQL 中的 SELECT LIMIT 偏移量、计数)
// count为负数时会返回<offset>中的所有元素, 请记住, 如果<offset>很大, 则需要遍历已排序的集合以获取<offset>元素, 然后才能返回元素, 这可能会增加O(N)时间复杂度
// WITHSCORES将同时返回每个元素的分数
// 默认情况, 查询为按索引查询, 将会返回索引在[min, max]之间的元素, 索引也可以是负数，表示距排序集末尾的偏移量
// 		如果<start>大于排序集的结束索引或<stop>, 则返回一个空列表
//		如果<stop>大于排序集的结束索引, Redis将使用排序集的最后一个元素
//
// 按分数查询, 将会返回分数在[min, max]之间的元素, <start>和<stop>可以是-inf和+inf, 分别表示负无穷和正无穷
// 默认情况下, 由<start>和<stop>指定的分数间隔是闭区间;可以通过在分数前面加上字符(来表示开区间
//
// 使用REV选项反转排序集, 索引0为得分最高的元素
// 默认情况下, <start>必须小于或等于<stop>才能返回任何内容, 但是如果选择了BYSCORE或BYLEX选项, 则<start>是要考虑的最高分数, 而<stop>是要考虑的最低分数
// 因此<start>必须大于或等于<stop>才能排序归还任何东西
//
// 按照字典序, 当指定BYLEX选项时, 返回排序集中在<start>和<stop>词典封闭范围间隔之间的元素范围
// 请注意, 字典顺序依赖于具有相同分数的所有元素; 当元素有不同的分数时, 回复是未指定的
// 有效的<start>和<stop>必须以(或[开头, 以分别指定范围间隔是独占还是包含
// <start>和<stop>的+或-的特殊值分别表示正负无限字符串, 前提是所有元素分数相同
// REV选项颠倒<start>和<stop>元素的顺序, 其中<start>必须按字典顺序大于<stop>才能产生非空结果
//
// 返回值类型: Array, 返回由有序集合元素组成的数组
func (c *Client) ZRange(key string, min, max interface{}, by string, rev, withScores bool, offset, limit int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZRANGE", key)
	args.AppendArgs(min, max)
	switch strings.ToUpper(by) {
	case "BYSCORE", "BYLEX":
		args.Append(by)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if rev {
		args.Append("REV")
	}
	if limit > 0 && offset >= 0 {
		args.AppendArgs("LIMIT", offset, limit)
	}
	if withScores {
		args.Append("WITHSCORES")
	}
	return c.sendCommand(args)
}

// ZRangeByLex v2.8.9后可用, v6.2.0后废弃
// 命令格式: ZRANGEBYLEX key min max [LIMIT offset count]
// v2.0.0开始支持WITHSCORES参数
// 时间复杂度: O(log(N)+M) N为有序集合的元素数量, M为返回的元素个数
// 如果有序集合中所有的成员分数相同, 此命令按照字典序返回min和max之间的元素
// 有效的start和stop必须以(或[开头, 以指定范围项是分别是互斥的还是包含的; start和stop的+或-的特殊值具有特殊含义或正无限和负无限字符串
// 返回值类型: Array, 返回指定范围内的元素
func (c *Client) ZRangeByLex(key, min, max string, offset, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZRANGELEX", key, min, max)
	if count > 0 && offset >= 0 {
		args.AppendArgs("LIMIT", offset, count)
	}
	return c.sendCommand(args)
}

// ZRangeByScore v1.0.5后可用, v6.2.0开始废弃
// 命令格式: ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
// 时间复杂度: O(log(N)+M), N为有序集合中的元素数量, M为返回的元素数量
// 按照分数返回[min, max]之间的元素, 元素顺序为由低到高, 如果指定了WITHSCORES参数, 同时将返回每个元素的分数
// min和max可以是-inf和+inf, 分别表示负无穷和正无穷
// 返回值类型: Array, 返回指定分数范围内的元素(可以选择同时返回分数)
func (c *Client) ZRangeByScore(key string, min, max interface{}, withScore bool, offset, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZRANGEBYSCORE", key)
	args.AppendArgs(min, max)
	if withScore {
		args.Append("WITHSCORES")
	}
	if count > 0 && offset >= 0 {
		args.AppendArgs("LIMIT", offset, count)
	}
	return c.sendCommand(args)
}

// ZRangeStore v6.2.0后可用
// 命令格式: ZRANGESTORE dst src min max [ BYSCORE | BYLEX] [REV] [LIMIT offset count]
// 时间复杂度: O(log(N)+M), N为有序集合中的元素数量, M为存储在dst中的元素数量
// 与ZRANGE相似, 不同之处在于ZRANGESTORE将结果集合存储在dst中
// 返回值类型: Integer, 返回结果有序集合中的元素数量
func (c *Client) ZRangeStore(dst, src string, min, max interface{}, by string, rev bool, offset, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZRANGESTORE", dst, src)
	args.AppendArgs(min, max)
	switch strings.ToUpper(by) {
	case "BYSCORE", "BYLEX":
		args.Append(by)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if rev {
		args.Append("REV")
	}
	if count > 0 && offset >= 0 {
		args.AppendArgs("LIMIT", offset, count)
	}
	return c.sendCommand(args)
}

// ZRank v2.0.0后可用
// 命令格式: ZRANK key member
// 时间复杂度: O(log(N))
// 返回存储在key的有序集合中成员的排名, 分数从低到高排序; 排名(或索引)从0开始, 这意味着得分最低的成员排名为0
// 返回值类型:
// 1. 如果member存在于有序集合, 返回类型为Integer: 返回member的排名
// 2. 如果member不存在与有序集合或者key不存在, 返回nil
func (c *Client) ZRank(key string, member interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("ZRANK", key)
	args.AppendArgs(member)
	return c.sendCommand(args)
}

// ZRem v1.2.0后可用
// 命令格式: ZREM key member [member ...]
// v2.4.0开始支持多个成员参数
// 时间复杂度: O(M*log(N)), N为有序集合中元素的数量, M为需要被移除的元素数量
// 从key对应的有序集合中移除指定的成员, 不存在的成员将被忽略
// 如果key对应的数据类型不是有序集合将会返回一个错误
// 返回值类型: Integer, 返回从有序集合中移除的元素数量, 不包含不存在的元素
func (c *Client) ZRem(key string, members ...interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("ZREM", key)
	args.AppendArgs(members...)
	return c.sendCommand(args)
}

// ZRemRangeByLex v2.8.9后可用
// 命令格式: ZREMRANGEBYLEX key min max
// 时间复杂度: O(log(N)+M), N为有序集合中元素的数量, M为需要被删除的元素数量
// 当有序集合中所有的元素都有相同的分数时, 为了强制字典排序, 此命令移除所有位于min和max之间的元素
// min和max的含义与ZRANGEBYLEX命令相同; 同样, 如果使用相同的min和max参数调用ZRANGEBYLEX, 此命令实际上会删除相同的元素
// 有效的min和max必须以(或[开头, 以指定范围项是分别是互斥的还是包含的; min和max的+或-的特殊值具有特殊含义或正无限和负无限字符串
// 返回值类型: Integer, 返回被移除的元素数量
func (c *Client) ZRemRangeByLex(key string, min, max string) (*Reply, error) {
	args := getArgs()
	args.Append("ZREMRANGEBYLEX", key, min, max)
	return c.sendCommand(args)
}

// ZRevRank v2.0.0后可用
// 命令格式: ZREVRANK key member
// 时间复杂度: O(log(N))
// 返回存储在key的有序集合中成员的排名, 分数从高到低排序; 排名(或索引)从0开始, 这意味着得分最高的成员排名为0
// 返回值类型:
// 1. 如果member存在于有序集合中, 返回Integer: 返回member的排名
// 2. 如果member不存在或者key不存在, 返回Bulk String: nil
func (c *Client) ZRevRank(key string, member interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("ZREVRANK", key)
	args.AppendArgs(member)
	return c.sendCommand(args)
}

// ZScan v2.8.0后可用
// 命令格式: ZSCAN key cursor [MATCH pattern] [COUNT count]
// 时间复杂度: O(N), N为集合中的元素数量
// 迭代集合中的元素
// 返回值类型: Array, 数组元素包含两个元素, 成员及其分数
func (c *Client) ZScan(key string, cursor int64, pattern string, count int64) (*Reply, error) {
	args := getArgs()
	args.Append("ZSCAN", key)
	args.AppendArgs(cursor)
	if pattern != "" {
		args.Append("MATCH", pattern)
	}
	if count > 0 {
		args.AppendArgs("COUNT", count)
	}
	return c.sendCommand(args)
}

// ZScore v1.2.0后可用
// 命令格式: ZSCORE key member
// 时间复杂度: O(1)
// 获取指定有序集合中指定成员的分数
// 如果成员不存在, 或者key不存在, 返回nil
// 返回值类型: Bulk String, 返回成员的分数, 因为是双精度浮点型, 所以返回的是string
func (c *Client) ZScore(key string, member interface{}) (*Reply, error) {
	args := getArgs()
	args.Append("ZSCORE", key)
	args.AppendArgs(member)
	return c.sendCommand(args)
}

// ZUnion v6.2.0后可用
// 命令格式: ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] [WITHSCORES]
// 时间复杂度: O(N)+O(M*log(M)), N为指定有序集合的所有成员总数, M为结果有序集合中的元素数量
// 计算指定有序集合的并集
// 默认情况下, 一个元素的结果分数是它在它所在的排序集中的分数的总和
// 使用WEIGHTS选项, 可以为每个输入排序集指定一个乘法因子; 这意味着每个输入排序集中的每个元素的分数在传递给聚合函数之前都会乘以该因子; 未给出WEIGHTS时, 乘法因子默认为1
// 使用AGGREGATE选项, 可以指定联合结果的聚合方式; 此选项默认为SUM, 其中元素的分数在其存在的输入中求和; 当此选项设置为MIN或MAX时, 结果集将包含元素在其存在的输入中的最小或最大分数
// 返回值类型: Array, 返回并集的结果(如果指定了分数, 同时会返回每个成员的分数)
func (c *Client) ZUnion(keys []string, weights []float64, op string, withScore bool) (*Reply, error) {
	args := getArgs()
	args.Append("ZUNION")
	args.AppendArgs(len(keys))
	args.Append(keys...)
	if len(weights) == len(keys) {
		args.Append("WEIGHTS")
		for _, w := range weights {
			args.AppendArgs(w)
		}
	}
	switch strings.ToUpper(op) {
	case "SUM", "MIN", "MAX":
		args.Append("AGGREGATE", op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	if withScore {
		args.Append("WITHSCORES")
	}
	return c.sendCommand(args)
}

// ZUnionStore v2.0.0后可用
// 命令格式: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]
// 时间复杂度: O(N)+O(M*log(M)), N为指定有序集合的所有成员总数, M为结果有序集合中的元素数量
// 计算指定有序集合的并集, 并将结果存储进dst
// 返回值类型: Integer, 返回存储进dst中的元素数量
func (c *Client) ZUnionStore(dst string, keys []string, weights []float64, op string) (*Reply, error) {
	args := getArgs()
	args.Append("ZUNIONSTORE", dst)
	args.AppendArgs(len(keys))
	args.Append(keys...)
	if len(weights) == len(keys) {
		args.Append("WEIGHTS")
		for _, w := range weights {
			args.AppendArgs(w)
		}
	}
	switch strings.ToUpper(op) {
	case "SUM", "MIN", "MAX":
		args.Append("AGGREGATE", op)
	case "":
		break
	default:
		return nil, ErrInvalidArgumentFormat
	}
	return c.sendCommand(args)
}
