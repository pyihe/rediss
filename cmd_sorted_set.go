package rediss

import "strings"

// BZMPop v7.0.0后可用
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
// 时间复杂度: O(1)
// 获取指定key的有序集合的基数
// 返回值类型: Integer, 返回有序集合的基数, 如果key不存在返回0
func (c *Client) ZCard(key string) (*Reply, error) {
	args := getArgs()
	args.Append("ZCARD", key)
	return c.sendCommand(args)
}

// ZCount v2.0.0后可用
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
